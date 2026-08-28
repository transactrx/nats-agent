package agent

import (
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/wire"
)

func fakeValidator(cfg IDTValidation, reply func(req validateRequest) (validateResponse, error)) *idtValidator {
	v := newIDTValidator(nil, &wire.AgentAccess{AppID: "appX", FunctionID: "fnX"}, cfg, nil)
	v.call = reply
	return v
}

func TestAuthorizeDisabledPassesThrough(t *testing.T) {
	v := fakeValidator(IDTValidation{Enabled: false}, nil)
	id, e := v.authorize("IDT-1.c", "s1")
	if e != nil || id.Verified || id.IDT != "IDT-1.c" {
		t.Fatalf("disabled validator must pass through: %+v %v", id, e)
	}
}

func TestAuthorizeMissingIDTIs403(t *testing.T) {
	v := fakeValidator(IDTValidation{Enabled: true}, nil)
	_, e := v.authorize("", "s1")
	if e == nil || e.Status != 403 || e.ApiStatusCode != wire.CodeForbidden || e.ErrorMessage != "MISSING_IDT" {
		t.Fatalf("want 403 MISSING_IDT, got %+v", e)
	}
}

func TestAuthorizeGrantedAndCached(t *testing.T) {
	calls := 0
	v := fakeValidator(IDTValidation{Enabled: true, CacheTTL: time.Minute}, func(req validateRequest) (validateResponse, error) {
		calls++
		if req.AgentID != "appX" || req.FunctionID != "fnX" || (req.IDT != "IDT-1.c" && req.IDT != "IDT-2.c") {
			t.Fatalf("bad request to identity: %+v", req)
		}
		return validateResponse{Valid: true, FunctionGranted: true, UserID: "u1", AccountID: "a1"}, nil
	})
	for i := 0; i < 3; i++ {
		id, e := v.authorize("IDT-1.c", "s1")
		if e != nil || !id.Verified || id.UserID != "u1" || id.AccountID != "a1" {
			t.Fatalf("want verified u1/a1, got %+v %v", id, e)
		}
	}
	if calls != 1 {
		t.Fatalf("allow must be cached per (idtid,session): identity called %d times", calls)
	}
	// Different session or rotated token → new identity call.
	v.authorize("IDT-1.c", "s2")
	v.authorize("IDT-2.c", "s1")
	if calls != 3 {
		t.Fatalf("cache key must include sessionId and idtid: calls=%d", calls)
	}
}

func TestAuthorizeDeniedNotCached(t *testing.T) {
	calls := 0
	reason := "DENIED_FN"
	v := fakeValidator(IDTValidation{Enabled: true, CacheTTL: time.Minute}, func(validateRequest) (validateResponse, error) {
		calls++
		return validateResponse{Valid: true, FunctionGranted: false, Reason: &reason}, nil
	})
	for i := 0; i < 2; i++ {
		_, e := v.authorize("IDT-1.c", "s1")
		if e == nil || e.Status != 403 || e.ErrorMessage != "DENIED_FN" {
			t.Fatalf("want 403 DENIED_FN, got %+v", e)
		}
	}
	if calls != 2 {
		t.Fatalf("deny must not be cached: calls=%d", calls)
	}
}

func TestAuthorizeInvalidTokenReason(t *testing.T) {
	reason := "TOKEN_REVOKED"
	v := fakeValidator(IDTValidation{Enabled: true}, func(validateRequest) (validateResponse, error) {
		return validateResponse{Valid: false, Reason: &reason}, nil
	})
	_, e := v.authorize("IDT-1.c", "s1")
	if e == nil || e.ErrorMessage != "TOKEN_REVOKED" {
		t.Fatalf("want TOKEN_REVOKED, got %+v", e)
	}
}

func TestAuthorizeIdentityErrorFailClosedThenOpen(t *testing.T) {
	boom := func(validateRequest) (validateResponse, error) { return validateResponse{}, errTest }
	_, e := fakeValidator(IDTValidation{Enabled: true}, boom).authorize("IDT-1.c", "s1")
	if e == nil || e.Status != 403 || e.ErrorMessage != "VALIDATE_ERROR" {
		t.Fatalf("fail-closed: want 403 VALIDATE_ERROR, got %+v", e)
	}
	id, e := fakeValidator(IDTValidation{Enabled: true, FailOpen: true}, boom).authorize("IDT-1.c", "s1")
	if e != nil || id.Verified {
		t.Fatalf("fail-open: want pass-through unverified, got %+v %v", id, e)
	}
}

func TestAuthorizeObserveOnlyNeverBlocks(t *testing.T) {
	reason := "DENIED_FN"
	v := fakeValidator(IDTValidation{Enabled: true, ObserveOnly: true}, func(validateRequest) (validateResponse, error) {
		return validateResponse{Valid: true, FunctionGranted: false, Reason: &reason}, nil
	})
	id, e := v.authorize("IDT-1.c", "s1")
	if e != nil || id.Verified {
		t.Fatalf("observe-only must pass through unverified: %+v %v", id, e)
	}
}

func TestIDTValidationFromEnvDefaults(t *testing.T) {
	t.Setenv("IDT_VALIDATION", "")
	t.Setenv("NATS_IDENTITY_BASE_PATH", "")
	t.Setenv("NATS_IDENTITY_VALIDATE_SUBJECT", "")
	t.Setenv("IDT_VALIDATE_TIMEOUT_SECONDS", "")
	t.Setenv("IDT_VALIDATE_CACHE_SECONDS", "")
	c := IDTValidationFromEnv()
	if c.Enabled || c.Subject != "trx.identityservice.validateInternalToken" || c.Timeout != 5*time.Second || c.CacheTTL != 300*time.Second {
		t.Fatalf("defaults wrong: %+v", c)
	}
	t.Setenv("IDT_VALIDATION", "true")
	t.Setenv("IDT_VALIDATE_CACHE_SECONDS", "0")
	c = IDTValidationFromEnv()
	if !c.Enabled || c.CacheTTL != 0 {
		t.Fatalf("env parse wrong: %+v", c)
	}
}

var errTest = errTestType{}

type errTestType struct{}

func (errTestType) Error() string { return "boom" }
