# nats-agent: IDT-verified requests + agent access card (protocol 1.1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every agent built on this library declares `access:{appId,functionId}` in its card and, when enabled, refuses chat/invoke/sessions requests whose `X-TRX-IDT` header identity does not validate against that function — enforced once, in the library, before the ack.

**Architecture:** Additive protocol 1.1: new card field, new NATS header, new 403 code. Server side: `pkg/agent/idt.go` holds the validator (identity NATS call + per-session cache); `parseTurn` and the sessions handlers call `a.authorize(msg, sessionID)` which yields a verified `Identity` or a 403 envelope. Client side: `agentclient.WithIDT(ctx, idt)` attaches the header on every authenticated endpoint. Tests use a fake identity responder on the embedded NATS server.

**Tech Stack:** Go 1.26, `nats.go`, `transactrx/nats-service` v1.4.46 (`NatsMessage.Header`, `Client.DoRequest(..., Header, ...)`), embedded `nats-server` in tests.

**Spec:** `/Users/yceleiro/Documents/TransactRx/GitHub/RSAssistant/docs/superpowers/specs/2026-08-28-idt-agent-access-design.md` (§4).

## Global Constraints

- Branch: `feature/idt-agent-access` from `Development`.
- Additive wire changes only; `ProtocolVersion` becomes `"1.1"`; clients must keep ignoring unknown fields.
- Header name: `X-TRX-IDT` (same as `trx-gofiber-session`). Env names identical to `ai-agent-go-service`: `APP_ID`, `APP_FUNCTION_ID`, `IDT_VALIDATION`, `IDT_OBSERVE_ONLY`, `IDT_FAIL_OPEN`, `NATS_IDENTITY_BASE_PATH` (default `trx.identityservice`), `NATS_IDENTITY_VALIDATE_SUBJECT` (default `validateInternalToken`), `IDT_VALIDATE_TIMEOUT_SECONDS` (default 5). New: `IDT_VALIDATE_CACHE_SECONDS` (default 300).
- Fail closed: validation on + missing header ⇒ 403 `MISSING_IDT`; identity error ⇒ 403 `VALIDATE_ERROR` unless `IDT_FAIL_OPEN=true`. `IDT_VALIDATION=true` without appId/functionId ⇒ `agent.New` error.
- Deny decisions are never cached. Allow decisions cached by `(idtid, sessionId, appId, functionId)`.
- Unauthenticated endpoints stay unauthenticated: `discover`, `card`, `ping`, `cancel`.
- Commits: do **not** commit automatically — user commits. Tasks end with `git add` + suggested message.
- Test: `go test ./...` from repo root (embedded NATS, no external deps). Build: `go build ./... && go vet ./...`.

---

### Task 0: Branch

- [ ] **Step 1**
```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/nats-agent
git checkout Development && git pull --ff-only && git checkout -b feature/idt-agent-access
go build ./... && go test ./... 2>&1 | tail -3
```
Expected: `ok  github.com/transactrx/nats-agent` (root package tests pass).

---

### Task 1: Wire additions (`AgentAccess`, header, error code, version 1.1)

**Files:**
- Modify: `pkg/wire/wire.go` (`ProtocolVersion` line 11; `AgentCard` struct lines 89-104; error codes block ~327-335; add header const near subject consts)
- Test: `pkg/wire/wire_test.go` (create)

**Interfaces (produces):**
```go
const ProtocolVersion = "1.1"
const HeaderIDT = "X-TRX-IDT"
type AgentAccess struct { AppID string `json:"appId"`; FunctionID string `json:"functionId"` }
// AgentCard gains: Access *AgentAccess `json:"access,omitempty"`
const CodeForbidden = 4031
```

- [ ] **Step 1: Failing test**

`pkg/wire/wire_test.go`:
```go
package wire

import (
	"encoding/json"
	"testing"
)

func TestAgentCardAccessRoundTrip(t *testing.T) {
	card := AgentCard{Name: "a", Description: "d", Access: &AgentAccess{AppID: "appX", FunctionID: "fnX"}}
	data, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	var back AgentCard
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.Access == nil || back.Access.AppID != "appX" || back.Access.FunctionID != "fnX" {
		t.Fatalf("access lost in round trip: %+v", back.Access)
	}
	// Absent access must marshal to no "access" key (legacy cards stay identical).
	data, _ = json.Marshal(AgentCard{Name: "b", Description: "d"})
	if string(data) != "" && containsKey(data, "access") {
		t.Fatalf("empty access must be omitted: %s", data)
	}
	if ProtocolVersion != "1.1" || CodeForbidden != 4031 || HeaderIDT != "X-TRX-IDT" {
		t.Fatalf("protocol constants: %s %d %s", ProtocolVersion, CodeForbidden, HeaderIDT)
	}
}

func containsKey(data []byte, key string) bool {
	var m map[string]json.RawMessage
	_ = json.Unmarshal(data, &m)
	_, ok := m[key]
	return ok
}
```

- [ ] **Step 2: Run** `go test ./pkg/wire/` → FAIL (undefined `AgentAccess`, `HeaderIDT`, `CodeForbidden`).

- [ ] **Step 3: Implement** in `pkg/wire/wire.go`:

Line 11: `const ProtocolVersion = "1.1"`.

After the subject consts block add:
```go
// HeaderIDT is the NATS message header carrying the caller's Internal
// Delegation Token on chat/invoke/sessions requests (SPEC §5.1). Same name
// trx-gofiber-session uses on the webapp→agent hop.
const HeaderIDT = "X-TRX-IDT"
```

In `AgentCard`, after `Endpoints`, add:
```go
	// Access is the agent's identity registration: the application id and
	// the single RBAC function a user must hold to talk to this agent
	// (SPEC §4.2, IDENTITY-AND-AUTHORITY §2.1). Absent = undeclared.
	Access *AgentAccess `json:"access,omitempty"`
```
After the `Capabilities` struct add:
```go
// AgentAccess maps an agent onto the identity model: an agent IS an
// application (appId) exposing one access function (functionId).
type AgentAccess struct {
	AppID      string `json:"appId"`
	FunctionID string `json:"functionId"`
}
```
Error codes block: add `CodeForbidden = 4031` after `CodeMissingField`.

- [ ] **Step 4: Run** `go test ./pkg/wire/ ./...` → PASS. (`e2e_test.go` compares `found.ProtocolVersion` to `wire.ProtocolVersion`, so it still passes.)

- [ ] **Step 5: Stage**
```bash
git add pkg/wire/wire.go pkg/wire/wire_test.go
# suggested: feat(wire): protocol 1.1 — AgentAccess card field, X-TRX-IDT header, 4031 forbidden
```

---

### Task 2: Server-side validator (`pkg/agent/idt.go`) + config

**Files:**
- Create: `pkg/agent/idt.go`
- Create: `pkg/agent/idt_test.go`
- Modify: `pkg/agent/agent.go` — `Config` (lines 55-72), `Agent` struct (75-89), `New` (99-150), `Card()` (177-209)

**Interfaces (produces):**
```go
// Config additions
Access        *wire.AgentAccess // fallback env APP_ID / APP_FUNCTION_ID
IDTValidation *IDTValidation    // nil → IDTValidationFromEnv()

type IDTValidation struct {
	Enabled, ObserveOnly, FailOpen bool
	Subject  string        // full subject, default "trx.identityservice.validateInternalToken"
	Timeout  time.Duration // default 5s
	CacheTTL time.Duration // default 300s; 0 disables cache
}
func IDTValidationFromEnv() IDTValidation

type Identity struct {
	UserID, AccountID string
	Verified          bool   // true only when identity confirmed the token AND the function
	IDT               string // raw header value (may be "" when validation is off)
}

// internal
type idtValidator struct{...}
func newIDTValidator(nc *nats.Conn, access *wire.AgentAccess, cfg IDTValidation, logger *log.Logger) *idtValidator
func (v *idtValidator) authorize(idt, sessionID string) (Identity, *nats_service.NatsServiceError)
```

- [ ] **Step 1: Failing test** `pkg/agent/idt_test.go` (unit; fake identity via a function field, no NATS):

```go
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
		if req.AgentID != "appX" || req.FunctionID != "fnX" || req.IDT != "IDT-1.c" {
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
```

- [ ] **Step 2: Run** `go test ./pkg/agent/` → FAIL (undefined types).

- [ ] **Step 3: Implement** `pkg/agent/idt.go`:

```go
package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// IDTValidation configures inbound Internal Delegation Token checks. Zero
// value = disabled. Fields mirror the ai-agent-go-service env contract so an
// org-wide deployment configures every agent the same way.
type IDTValidation struct {
	Enabled     bool          // IDT_VALIDATION
	ObserveOnly bool          // IDT_OBSERVE_ONLY: validate + log, never block
	FailOpen    bool          // IDT_FAIL_OPEN: allow when identity is unreachable
	Subject     string        // NATS_IDENTITY_BASE_PATH + "." + NATS_IDENTITY_VALIDATE_SUBJECT
	Timeout     time.Duration // IDT_VALIDATE_TIMEOUT_SECONDS
	CacheTTL    time.Duration // IDT_VALIDATE_CACHE_SECONDS; 0 = validate every request
}

const (
	defaultIdentityBasePath  = "trx.identityservice"
	defaultValidateSubject   = "validateInternalToken"
	defaultValidateTimeout   = 5 * time.Second
	defaultValidateCacheTTL  = 300 * time.Second
	idtCacheMaxEntries       = 10_000
	reasonMissingIDT         = "MISSING_IDT"
	reasonValidateError      = "VALIDATE_ERROR"
	reasonDeniedFn           = "DENIED_FN"
)

// IDTValidationFromEnv reads the org env contract.
func IDTValidationFromEnv() IDTValidation {
	c := IDTValidation{
		Enabled:     envBool("IDT_VALIDATION"),
		ObserveOnly: envBool("IDT_OBSERVE_ONLY"),
		FailOpen:    envBool("IDT_FAIL_OPEN"),
		Timeout:     defaultValidateTimeout,
		CacheTTL:    defaultValidateCacheTTL,
	}
	base := strings.TrimSpace(os.Getenv("NATS_IDENTITY_BASE_PATH"))
	if base == "" {
		base = defaultIdentityBasePath
	}
	suffix := strings.TrimSpace(os.Getenv("NATS_IDENTITY_VALIDATE_SUBJECT"))
	if suffix == "" {
		suffix = defaultValidateSubject
	}
	c.Subject = base + "." + suffix
	if raw := strings.TrimSpace(os.Getenv("IDT_VALIDATE_TIMEOUT_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			c.Timeout = time.Duration(n) * time.Second
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IDT_VALIDATE_CACHE_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			c.CacheTTL = time.Duration(n) * time.Second
		}
	}
	return c
}

// accessFromEnv is the Config.Access fallback (APP_ID / APP_FUNCTION_ID).
func accessFromEnv() *wire.AgentAccess {
	appID := strings.TrimSpace(os.Getenv("APP_ID"))
	fnID := strings.TrimSpace(os.Getenv("APP_FUNCTION_ID"))
	if appID == "" && fnID == "" {
		return nil
	}
	return &wire.AgentAccess{AppID: appID, FunctionID: fnID}
}

func envBool(key string) bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(key)), "true")
}

// Identity is what the runtime learned about the caller of a request.
type Identity struct {
	UserID    string
	AccountID string
	// Verified is true only when identity confirmed the token AND granted
	// this agent's function. False in pass-through modes (validation off,
	// observe-only, fail-open) — treat the body's userId as caller-asserted.
	Verified bool
	// IDT is the raw header value, exposed so agents can forward it on
	// agent→agent / agent→tool calls via agentclient.WithIDT.
	IDT string
}

// Wire shapes of identity's validateInternalToken.
type validateRequest struct {
	IDT        string `json:"idt"`
	AgentID    string `json:"agentId"`
	FunctionID string `json:"functionId"`
}

type validateResponse struct {
	Valid           bool    `json:"valid"`
	FunctionGranted bool    `json:"functionGranted"`
	UserID          string  `json:"userId"`
	AccountID       string  `json:"accountId"`
	Reason          *string `json:"reason"`
}

type cacheEntry struct {
	id      Identity
	expires time.Time
}

type idtValidator struct {
	cfg    IDTValidation
	access *wire.AgentAccess
	nc     *nats.Conn
	logger *log.Logger
	// call performs the identity round-trip; replaced in unit tests.
	call func(req validateRequest) (validateResponse, error)

	mu    sync.Mutex
	cache map[string]cacheEntry
}

func newIDTValidator(nc *nats.Conn, access *wire.AgentAccess, cfg IDTValidation, logger *log.Logger) *idtValidator {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultValidateTimeout
	}
	if cfg.Subject == "" {
		cfg.Subject = defaultIdentityBasePath + "." + defaultValidateSubject
	}
	v := &idtValidator{cfg: cfg, access: access, nc: nc, logger: logger, cache: map[string]cacheEntry{}}
	v.call = v.natsCall
	return v
}

func (v *idtValidator) natsCall(req validateRequest) (validateResponse, error) {
	var resp validateResponse
	body, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	msg, err := v.nc.Request(v.cfg.Subject, body, v.cfg.Timeout)
	if err != nil {
		return resp, err
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return resp, fmt.Errorf("decoding identity reply: %w", err)
	}
	return resp, nil
}

// idPrefix returns "IDT-<uuid>" from "IDT-<uuid>.<cipher>" — safe for logs
// and cache keys; never log the cipher tail.
func idPrefix(idt string) string {
	if i := strings.IndexByte(idt, '.'); i >= 0 {
		return idt[:i]
	}
	return idt
}

func forbidden(reason string) *nats_service.NatsServiceError {
	e := nats_service.NewAuthorizationError(reason, wire.CodeForbidden, nil)
	return &e
}

// authorize decides whether a request carrying idt (header value, may be
// empty) for sessionID may proceed. Returns the caller Identity on allow, or
// a 403 envelope. Pass-through modes return Verified=false, never an error.
func (v *idtValidator) authorize(idt, sessionID string) (Identity, *nats_service.NatsServiceError) {
	idt = strings.TrimSpace(idt)
	if !v.cfg.Enabled {
		return Identity{IDT: idt}, nil
	}
	appID, fnID := v.access.AppID, v.access.FunctionID
	if idt == "" {
		if v.cfg.ObserveOnly || v.cfg.FailOpen {
			v.logger.Printf("IDT_METRIC event=validate.pass_through reason=%s agent=%s function=%s observe=%v failOpen=%v",
				reasonMissingIDT, appID, fnID, v.cfg.ObserveOnly, v.cfg.FailOpen)
			return Identity{}, nil
		}
		v.logger.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s", reasonMissingIDT, appID, fnID)
		return Identity{}, forbidden(reasonMissingIDT)
	}

	key := idPrefix(idt) + "|" + sessionID + "|" + appID + "|" + fnID
	if v.cfg.CacheTTL > 0 {
		v.mu.Lock()
		if e, ok := v.cache[key]; ok && time.Now().Before(e.expires) {
			v.mu.Unlock()
			id := e.id
			id.IDT = idt
			return id, nil
		}
		v.mu.Unlock()
	}

	resp, err := v.call(validateRequest{IDT: idt, AgentID: appID, FunctionID: fnID})
	if err != nil {
		if v.cfg.ObserveOnly || v.cfg.FailOpen {
			v.logger.Printf("IDT_METRIC event=validate.pass_through reason=%s agent=%s function=%s idtid=%s err=%q",
				reasonValidateError, appID, fnID, idPrefix(idt), err.Error())
			return Identity{IDT: idt}, nil
		}
		v.logger.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s idtid=%s err=%q",
			reasonValidateError, appID, fnID, idPrefix(idt), err.Error())
		return Identity{}, forbidden(reasonValidateError)
	}

	reason := ""
	switch {
	case !resp.Valid:
		if resp.Reason != nil {
			reason = *resp.Reason
		}
		if reason == "" {
			reason = "INVALID_IDT"
		}
	case !resp.FunctionGranted:
		reason = reasonDeniedFn
	}
	if reason != "" {
		if v.cfg.ObserveOnly {
			v.logger.Printf("IDT_METRIC event=validate.observe decision=deny reason=%s agent=%s function=%s idtid=%s user=%s",
				reason, appID, fnID, idPrefix(idt), resp.UserID)
			return Identity{IDT: idt}, nil
		}
		v.logger.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s idtid=%s user=%s account=%s",
			reason, appID, fnID, idPrefix(idt), resp.UserID, resp.AccountID)
		return Identity{}, forbidden(reason)
	}

	id := Identity{UserID: resp.UserID, AccountID: resp.AccountID, Verified: !v.cfg.ObserveOnly, IDT: idt}
	if v.cfg.ObserveOnly {
		v.logger.Printf("IDT_METRIC event=validate.observe decision=allow agent=%s function=%s idtid=%s user=%s", appID, fnID, idPrefix(idt), resp.UserID)
	} else {
		v.logger.Printf("IDT_METRIC event=validate.allow agent=%s function=%s idtid=%s user=%s account=%s", appID, fnID, idPrefix(idt), resp.UserID, resp.AccountID)
	}
	if v.cfg.CacheTTL > 0 {
		v.mu.Lock()
		if len(v.cache) >= idtCacheMaxEntries {
			now := time.Now()
			for k, e := range v.cache {
				if now.After(e.expires) {
					delete(v.cache, k)
				}
			}
			if len(v.cache) >= idtCacheMaxEntries { // still full: drop everything rather than grow unbounded
				v.cache = map[string]cacheEntry{}
			}
		}
		v.cache[key] = cacheEntry{id: Identity{UserID: id.UserID, AccountID: id.AccountID, Verified: id.Verified}, expires: time.Now().Add(v.cfg.CacheTTL)}
		v.mu.Unlock()
	}
	return id, nil
}
```

- [ ] **Step 4: Wire into `Config`/`Agent`/`New`/`Card`** in `pkg/agent/agent.go`:

`Config` — add after `Metadata`:
```go
	// Access registers this agent with the identity model (card "access").
	// nil → read APP_ID / APP_FUNCTION_ID from the environment.
	Access *wire.AgentAccess
	// IDTValidation controls inbound token checks. nil → IDTValidationFromEnv().
	IDTValidation *IDTValidation
```
`Agent` struct — add field `idt *idtValidator`.

`New` — after the `OutputModalities` defaulting and before the NATS URL block:
```go
	if cfg.Access == nil {
		cfg.Access = accessFromEnv()
	}
	if cfg.IDTValidation == nil {
		v := IDTValidationFromEnv()
		cfg.IDTValidation = &v
	}
	if cfg.IDTValidation.Enabled && (cfg.Access == nil || cfg.Access.AppID == "" || cfg.Access.FunctionID == "") {
		return nil, fmt.Errorf("agent %q: IDT_VALIDATION=true requires Access.AppID and Access.FunctionID (env APP_ID / APP_FUNCTION_ID)", cfg.Name)
	}
```
In the returned `&Agent{...}` add `idt: newIDTValidator(svc.GetNatsService(), cfg.Access, *cfg.IDTValidation, nil)`, and after construction log once:
```go
	if cfg.IDTValidation.Enabled {
		log.Printf("agent %q: IDT validation enabled (subject=%s appId=%s functionId=%s observeOnly=%v failOpen=%v cacheTTL=%s)",
			cfg.Name, cfg.IDTValidation.Subject, cfg.Access.AppID, cfg.Access.FunctionID, cfg.IDTValidation.ObserveOnly, cfg.IDTValidation.FailOpen, cfg.IDTValidation.CacheTTL)
	}
```
`Card()` — add `Access: a.cfg.Access,` to the returned `wire.AgentCard`.

- [ ] **Step 5: Run** `go test ./pkg/agent/ ./...` → PASS.

- [ ] **Step 6: Stage**
```bash
git add pkg/agent/idt.go pkg/agent/idt_test.go pkg/agent/agent.go
# suggested: feat(agent): IDT validator (identity NATS call, per-session cache) + Access/IDTValidation config
```

---

### Task 3: Enforce in `parseTurn` and sessions handlers; expose `Turn.Identity`

**Files:**
- Modify: `pkg/agent/agent.go` — `Turn` (lines 37-41), `parseTurn` (355-380)
- Modify: `pkg/agent/sessions.go` — five handlers (`handleSessionsList` … `handleSessionsSetFavorite`)
- Test: `e2e_test.go` (add tests + fake identity helper)

**Interfaces (produces):** `Turn.Identity Identity`. When `Identity.Verified`, `Turn.UserID` (and sessions `req.UserID`) is overwritten with the verified user id.

- [ ] **Step 1: Failing e2e tests** — append to `e2e_test.go`:

```go
// ─── IDT enforcement ──────────────────────────────────────────────────────

// startFakeIdentity answers validateInternalToken on the embedded server.
// grant maps idt → (userId); tokens not in the map are DENIED_FN; "revoked"
// is TOKEN_REVOKED.
func startFakeIdentity(t *testing.T, subject string, grant map[string]string) *int32 {
	t.Helper()
	nc := testConn(t)
	var calls int32
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		atomic.AddInt32(&calls, 1)
		var req struct {
			IDT        string `json:"idt"`
			AgentID    string `json:"agentId"`
			FunctionID string `json:"functionId"`
		}
		_ = json.Unmarshal(m.Data, &req)
		resp := map[string]any{"valid": true, "functionGranted": false, "reason": "DENIED_FN"}
		if req.IDT == "revoked.x" {
			resp = map[string]any{"valid": false, "functionGranted": false, "reason": "TOKEN_REVOKED"}
		} else if u, ok := grant[req.IDT]; ok && req.AgentID == "appTest" && req.FunctionID == "fnTest" {
			resp = map[string]any{"valid": true, "functionGranted": true, "userId": u, "accountId": "acc1", "reason": nil}
		}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	if err != nil {
		t.Fatalf("fake identity subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return &calls
}

func startSecuredAgent(t *testing.T, name, identitySubject string, observe bool) (*memSessionStore, *[]string) {
	t.Helper()
	store := newMemSessionStore()
	var seenUsers []string
	var mu sync.Mutex
	a, err := agent.New(agent.Config{
		Name:        name,
		Description: "Secured agent for IDT tests.",
		NATSURL:     testURL,
		Access:      &wire.AgentAccess{AppID: "appTest", FunctionID: "fnTest"},
		IDTValidation: &agent.IDTValidation{
			Enabled: true, ObserveOnly: observe, Subject: identitySubject, Timeout: 2 * time.Second, CacheTTL: time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	a.UseSessionStore(store)
	a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
		mu.Lock()
		seenUsers = append(seenUsers, turn.UserID+"|"+fmt.Sprint(turn.Identity.Verified)+"|"+turn.Identity.IDT)
		mu.Unlock()
		stream.Text("ok")
		stream.Done(wire.StopEndTurn, nil)
		return nil
	})
	if err := a.Start(); err != nil {
		t.Fatalf("agent.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	return store, &seenUsers
}

func TestIDTChatAllowedDeniedMissing(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	calls := startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})
	_, seen := startSecuredAgent(t, "securedAgent", subj, false)
	c := agentclient.NewFromConn(testConn(t))
	msg := wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}}

	// allowed: verified user overrides caller-asserted userId
	run, err := c.Chat(agentclient.WithIDT(context.Background(), "IDT-good.c"), "securedAgent",
		wire.ChatRequest{UserID: "mallory", SessionID: "s1", Message: msg})
	if err != nil {
		t.Fatalf("allowed chat: %v", err)
	}
	for range run.Events {
	}
	if len(*seen) != 1 || (*seen)[0] != "alice|true|IDT-good.c" {
		t.Fatalf("handler saw %v, want verified alice", *seen)
	}

	// denied function → 403 before ack
	_, err = c.Chat(agentclient.WithIDT(context.Background(), "IDT-bad.c"), "securedAgent", wire.ChatRequest{Message: msg})
	assertForbidden(t, err, "DENIED_FN")

	// revoked token
	_, err = c.Chat(agentclient.WithIDT(context.Background(), "revoked.x"), "securedAgent", wire.ChatRequest{Message: msg})
	assertForbidden(t, err, "TOKEN_REVOKED")

	// missing header
	_, err = c.Chat(context.Background(), "securedAgent", wire.ChatRequest{Message: msg})
	assertForbidden(t, err, "MISSING_IDT")

	// card is public and carries access
	card, err := c.Card(context.Background(), "securedAgent")
	if err != nil || card.Access == nil || card.Access.FunctionID != "fnTest" {
		t.Fatalf("card must be public with access: %+v %v", card, err)
	}
	if atomic.LoadInt32(calls) < 3 {
		t.Fatalf("identity should have been consulted, calls=%d", *calls)
	}
}

func TestIDTSessionsGatedAndUserOverridden(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})
	store, _ := startSecuredAgent(t, "securedSessions", subj, false)
	store.put("alice", wire.SessionGetResponse{SessionID: "s-alice", Title: "mine"})
	store.put("bob", wire.SessionGetResponse{SessionID: "s-bob", Title: "his"})
	c := agentclient.NewFromConn(testConn(t))

	// bob's id in the body, alice's token → alice's sessions
	resp, err := c.SessionsList(agentclient.WithIDT(context.Background(), "IDT-good.c"), "securedSessions", "bob")
	if err != nil {
		t.Fatalf("sessionsList: %v", err)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].SessionID != "s-alice" {
		t.Fatalf("verified identity must override body userId: %+v", resp.Sessions)
	}
	_, err = c.SessionsList(context.Background(), "securedSessions", "alice")
	assertForbidden(t, err, "MISSING_IDT")
}

func TestIDTObserveOnlyPassesThrough(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{})
	_, seen := startSecuredAgent(t, "observeAgent", subj, true)
	c := agentclient.NewFromConn(testConn(t))
	run, err := c.Chat(context.Background(), "observeAgent", wire.ChatRequest{UserID: "u", Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}}})
	if err != nil {
		t.Fatalf("observe-only must not block: %v", err)
	}
	for range run.Events {
	}
	if len(*seen) != 1 || (*seen)[0] != "u|false|" {
		t.Fatalf("observe-only must keep caller userId, unverified: %v", *seen)
	}
}

func TestIDTMisconfigRejectedAtNew(t *testing.T) {
	_, err := agent.New(agent.Config{Name: "bad", Description: "d", NATSURL: testURL,
		IDTValidation: &agent.IDTValidation{Enabled: true}})
	if err == nil {
		t.Fatal("IDT_VALIDATION without Access must fail agent.New")
	}
}

func assertForbidden(t *testing.T, err error, reason string) {
	t.Helper()
	var se *agentclient.ServiceError
	if !errors.As(err, &se) {
		t.Fatalf("want ServiceError, got %v", err)
	}
	if se.Status != 403 || se.ApiStatusCode != wire.CodeForbidden || se.ErrorMessage != reason {
		t.Fatalf("want 403/%d/%s, got %d/%d/%s", wire.CodeForbidden, reason, se.Status, se.ApiStatusCode, se.ErrorMessage)
	}
}
```
Add imports `"errors"` and `"sync/atomic"` to `e2e_test.go`.

- [ ] **Step 2: Run** `go test ./ -run 'TestIDT' ` → FAIL (`agentclient.WithIDT`, `Turn.Identity` undefined).

- [ ] **Step 3: Implement server side**

`Turn`:
```go
type Turn struct {
	wire.ChatRequest
	RunID  string
	Logger *log.Logger
	// Identity is the runtime's verdict on the caller (see IDTValidation).
	// When Identity.Verified, UserID has been replaced by the verified id.
	Identity Identity
}
```
`parseTurn` — after `req.SessionID` defaulting, before `return`:
```go
	id, aerr := a.idt.authorize(msg.Header.Get(wire.HeaderIDT), req.SessionID)
	if aerr != nil {
		return nil, aerr
	}
	if id.Verified {
		req.UserID = id.UserID
	}
	return &Turn{ChatRequest: req, RunID: "run_" + uuid.New().String(), Logger: msg.Logger, Identity: id}, nil
```
(`msg.Header` is `nats.Header`; `Get` on a nil map returns `""`.)

`sessions.go` — add helper:
```go
// authorizeSession applies IDT enforcement to a sessions endpoint. sessionID
// may be "" (list). On a verified identity the body's userId is replaced.
func (a *Agent) authorizeSession(msg *nats_service.NatsMessage, userID *string, sessionID string) *nats_service.NatsServiceError {
	id, aerr := a.idt.authorize(msg.Header.Get(wire.HeaderIDT), sessionID)
	if aerr != nil {
		return aerr
	}
	if id.Verified {
		*userID = id.UserID
	}
	return nil
}
```
In each handler, right after `if verr != nil { return verr }`, insert (list uses `""` for sessionID; others `req.SessionID`):
```go
	if aerr := a.authorizeSession(msg, &req.UserID, req.SessionID); aerr != nil {
		return aerr
	}
```

- [ ] **Step 4: Implement client side** (needed for the tests) — see Task 4, then run both.

---

### Task 4: Client — `WithIDT` context + header on authenticated calls

**Files:**
- Modify: `pkg/agentclient/client.go` — `requestJSON` (138-155), `Chat` (189-210), `Invoke`, five `Session*` methods, `Card`/`Ping` unchanged (they pass ctx but never send the header).

**Interfaces (produces):**
```go
func WithIDT(ctx context.Context, idt string) context.Context
func IDTFromContext(ctx context.Context) string
```

- [ ] **Step 1: Implement**

Add to `client.go`:
```go
type idtCtxKey struct{}

// WithIDT returns a context carrying the caller's Internal Delegation Token.
// Every authenticated call (Chat, Invoke, Sessions*) made with this context
// sends it as the X-TRX-IDT NATS header. Empty idt is a no-op.
func WithIDT(ctx context.Context, idt string) context.Context {
	if idt == "" {
		return ctx
	}
	return context.WithValue(ctx, idtCtxKey{}, idt)
}

// IDTFromContext returns the token set by WithIDT, or "".
func IDTFromContext(ctx context.Context) string {
	v, _ := ctx.Value(idtCtxKey{}).(string)
	return v
}

func idtHeader(ctx context.Context) nats_service_client.Header {
	idt := IDTFromContext(ctx)
	if idt == "" {
		return nil
	}
	return nats_service_client.Header{wire.HeaderIDT: []string{idt}}
}
```
Change `requestJSON` signature to `func requestJSON[T any](ctx context.Context, c *Client, subject string, body any) (*T, error)` and the call to `c.svc.DoRequest("", subject, idtHeader(ctx), data, c.timeout)`. Update every caller: `Card(ctx, …)` and `Ping` pass `context.Background()`-derived ctx unchanged (header only present if the caller set it — harmless); `Chat`, `Invoke`, `Session*` pass their `ctx`.

- [ ] **Step 2: Run** `go build ./... && go test ./...` → all PASS including `TestIDT*`.

- [ ] **Step 3: Stage**
```bash
git add pkg/agent/agent.go pkg/agent/sessions.go pkg/agentclient/client.go e2e_test.go
# suggested: feat: enforce X-TRX-IDT on chat/invoke/sessions before ack; agentclient.WithIDT
```

---

### Task 5: CLI passthrough

**Files:**
- Modify: `cmd/nats-agents/main.go` (flag set at line ~97; every `ac.Chat/ac.Invoke/ac.Sessions*` call)

- [ ] **Step 1:** Add flag `idt := fs.String("idt", os.Getenv("TRX_IDT"), "Internal Delegation Token to send as X-TRX-IDT (default $TRX_IDT)")`. After flag parsing, wrap the root context once: `ctx = agentclient.WithIDT(ctx, *idt)` so every downstream call inherits it (find the single place `ctx` is created — `grep -n "context\." cmd/nats-agents/main.go`).
- [ ] **Step 2:** `go build ./... && go test ./ -run TestCLI` → PASS (CLI test agents have validation off).
- [ ] **Step 3: Stage** `git add cmd/nats-agents/main.go` — suggested: `feat(cli): --idt / $TRX_IDT passthrough`.

---

### Task 6: Docs (SPEC 1.1, README, IDENTITY-AND-AUTHORITY)

**Files:** `SPEC.md`, `README.md`, `IDENTITY-AND-AUTHORITY.md`

- [ ] **Step 1: SPEC.md**
  - Title: `Specification v1.1`.
  - §4.2 card example: add `"access": { "appId": "copayassistanceAppId", "functionId": "copayassistanceFnId" }` and bullet: *`access` (optional, v1.1) registers the agent with the identity model — an agent is an application (`appId`) exposing one access function (`functionId`); a caller may use the agent iff the user behind their token holds that function. Absent = undeclared; UIs treat undeclared agents as inaccessible when enforcing.*
  - §5.1: new paragraph *Authentication (v1.1): requests to `chat`, `invoke`, `sessions*` carry the caller's Internal Delegation Token in NATS header `X-TRX-IDT`. An agent with validation enabled verifies it with identity (`validateInternalToken {idt, agentId=access.appId, functionId=access.functionId}`) before acking; on failure the reply is the error envelope with status 403 / code 4031 and nothing is published to `streamSubject`. When verified, `userId` is derived from the token and the body's value is ignored. `discover`, `card`, `ping`, `cancel` are unauthenticated.*
  - §9 table: `| 4031 | Forbidden — IDT missing, invalid, or function not granted (message = reason: MISSING_IDT, DENIED_FN, TOKEN_REVOKED, SESSION_EXPIRED, TOKEN_NOT_FOUND, VALIDATE_ERROR) |`.
  - §11: note the Go library's `Config.Access`, `Config.IDTValidation`, env contract, `agentclient.WithIDT`.
- [ ] **Step 2: README.md** — in "Serving an agent" add the `Access` field and env vars table (Global Constraints list); in "Talking to an agent" show `agentclient.WithIDT(ctx, idt)`.
- [ ] **Step 3: IDENTITY-AND-AUTHORITY.md** — §3 add *Implemented 2026-08: inbound token on `X-TRX-IDT`, verified before ack, `userId` derived when verified.* §9 add *Implemented: card `access` = registration; enforcement in `pkg/agent/idt.go`.* §10 mark steps 1 (inbound part) and 2 done; downscoping still open.
- [ ] **Step 4: Stage** `git add SPEC.md README.md IDENTITY-AND-AUTHORITY.md` — suggested: `docs: protocol 1.1 — access card field, X-TRX-IDT, 4031`.

---

### Task 7: Release prep

- [ ] **Step 1:** `go vet ./... && go test ./...` clean.
- [ ] **Step 2:** After the user merges, tag `v0.2.0` (MINOR: additive protocol change). Until then RSAssistant consumes the branch via a `replace` directive (see RSAssistant plan Task 1).

## Self-review

- Spec §4.1 wire ✓ T1; §4.2 config/env ✓ T2; §4.3 server (parseTurn + sessions, 403 before ack, userId override, cache, observe/fail-open, misconfig error) ✓ T2/T3; §4.4 client ✓ T4; CLI ✓ T5; §4.5 tests ✓ T3; docs ✓ T6.
- Names consistent: `wire.HeaderIDT`, `wire.CodeForbidden`, `wire.AgentAccess`, `agent.IDTValidation`, `agent.Identity`, `Turn.Identity`, `agentclient.WithIDT/IDTFromContext`, `idtValidator.authorize`, `Agent.authorizeSession`.
