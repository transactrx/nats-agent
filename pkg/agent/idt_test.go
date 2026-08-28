package agent

import (
	"strings"
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

func TestAuthorizeCacheKeyedOnFullTokenNotPrefix(t *testing.T) {
	calls := 0
	v := fakeValidator(IDTValidation{Enabled: true, CacheTTL: time.Minute}, func(req validateRequest) (validateResponse, error) {
		calls++
		if req.IDT == "IDT-1.good" {
			return validateResponse{Valid: true, FunctionGranted: true, UserID: "u1", AccountID: "a1"}, nil
		}
		reason := "DENIED_FORGED"
		return validateResponse{Valid: false, Reason: &reason}, nil
	})
	id, e := v.authorize("IDT-1.good", "s1")
	if e != nil || !id.Verified {
		t.Fatalf("want verified allow, got %+v %v", id, e)
	}
	if calls != 1 {
		t.Fatalf("want 1 call after first allow, got %d", calls)
	}
	// Same uuid prefix, forged cipher tail, same session: must NOT ride the
	// cached allow — the cache key must authenticate on the full token.
	_, e = v.authorize("IDT-1.forged", "s1")
	if e == nil || e.Status != 403 || e.ErrorMessage != "DENIED_FORGED" {
		t.Fatalf("forged token sharing a cached prefix must be denied, got %+v", e)
	}
	if calls != 2 {
		t.Fatalf("forged token must trigger a fresh identity call: calls=%d", calls)
	}
}

func TestAuthorizeCacheExpires(t *testing.T) {
	calls := 0
	v := fakeValidator(IDTValidation{Enabled: true, CacheTTL: 10 * time.Millisecond}, func(validateRequest) (validateResponse, error) {
		calls++
		return validateResponse{Valid: true, FunctionGranted: true, UserID: "u1", AccountID: "a1"}, nil
	})
	if _, e := v.authorize("IDT-1.c", "s1"); e != nil {
		t.Fatalf("unexpected error: %v", e)
	}
	time.Sleep(20 * time.Millisecond)
	if _, e := v.authorize("IDT-1.c", "s1"); e != nil {
		t.Fatalf("unexpected error: %v", e)
	}
	if calls != 2 {
		t.Fatalf("expired cache entry must trigger a fresh identity call: calls=%d", calls)
	}
}

func TestAuthorizeFailOpenDoesNotCoverMissingToken(t *testing.T) {
	v := fakeValidator(IDTValidation{Enabled: true, FailOpen: true}, nil)
	_, e := v.authorize("", "s1")
	if e == nil || e.Status != 403 || e.ErrorMessage != "MISSING_IDT" {
		t.Fatalf("FailOpen must not bypass a missing token: want 403 MISSING_IDT, got %+v", e)
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

func TestAuthorizeGrantedButEmptyUserIDIsDenied(t *testing.T) {
	calls := 0
	v := fakeValidator(IDTValidation{Enabled: true, CacheTTL: time.Minute}, func(validateRequest) (validateResponse, error) {
		calls++
		return validateResponse{Valid: true, FunctionGranted: true, UserID: "  ", AccountID: "a1"}, nil
	})
	id, e := v.authorize("IDT-1.c", "s1")
	if e == nil || e.Status != 403 || e.ErrorMessage != "INVALID_IDENTITY" {
		t.Fatalf("want 403 INVALID_IDENTITY, got %+v %+v", id, e)
	}
	if id.Verified {
		t.Fatalf("denied identity must not be verified: %+v", id)
	}
	// Must not be cached: a second call re-consults identity.
	if _, e := v.authorize("IDT-1.c", "s1"); e == nil || e.ErrorMessage != "INVALID_IDENTITY" {
		t.Fatalf("second call: want 403 INVALID_IDENTITY, got %+v", e)
	}
	if calls != 2 {
		t.Fatalf("empty-userId deny must not be cached: calls=%d", calls)
	}
}

func TestAuthorizeObserveOnlyEmptyUserIDPassesThroughUnverified(t *testing.T) {
	v := fakeValidator(IDTValidation{Enabled: true, ObserveOnly: true}, func(validateRequest) (validateResponse, error) {
		return validateResponse{Valid: true, FunctionGranted: true, UserID: "", AccountID: "a1"}, nil
	})
	id, e := v.authorize("IDT-1.c", "s1")
	if e != nil || id.Verified {
		t.Fatalf("observe-only must pass through unverified even with empty userId: %+v %v", id, e)
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

func TestAuthorizeObserveOnlyNeverCachesAllows(t *testing.T) {
	calls := 0
	v := fakeValidator(IDTValidation{Enabled: true, ObserveOnly: true, CacheTTL: time.Minute}, func(validateRequest) (validateResponse, error) {
		calls++
		return validateResponse{Valid: true, FunctionGranted: true, UserID: "u1", AccountID: "a1"}, nil
	})
	for i := 0; i < 3; i++ {
		id, e := v.authorize("IDT-1.c", "s1")
		if e != nil || id.Verified {
			t.Fatalf("observe-only allow must stay unverified pass-through: %+v %v", id, e)
		}
	}
	if calls != 3 {
		t.Fatalf("observe-only must never cache an allow (undercounts rollout signal): calls=%d", calls)
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

func TestNewRejectsIDTValidationWithoutAccessBeforeNATSConnect(t *testing.T) {
	// No NATS_URL is set (or reachable) in this test process; if the
	// misconfig check ran after the connect attempt this would instead fail
	// with a connection error, not the intended message.
	t.Setenv("NATS_URL", "")
	t.Setenv("APP_ID", "")
	t.Setenv("APP_FUNCTION_ID", "")
	_, err := New(Config{
		Name:          "x",
		Description:   "d",
		IDTValidation: &IDTValidation{Enabled: true},
	})
	if err == nil {
		t.Fatalf("want error for IDT_VALIDATION=true with no Access")
	}
	if !strings.Contains(err.Error(), "APP_ID") {
		t.Fatalf("error should mention APP_ID, got: %v", err)
	}
}

var errTest = errTestType{}

type errTestType struct{}

func (errTestType) Error() string { return "boom" }
