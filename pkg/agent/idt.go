package agent

import (
	"crypto/sha256"
	"encoding/hex"
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
	FailOpen    bool          // IDT_FAIL_OPEN: allow when identity is unreachable; never for a missing token
	Subject     string        // NATS_IDENTITY_BASE_PATH + "." + NATS_IDENTITY_VALIDATE_SUBJECT
	Timeout     time.Duration // IDT_VALIDATE_TIMEOUT_SECONDS
	CacheTTL    time.Duration // IDT_VALIDATE_CACHE_SECONDS; 0 = validate every request
}

const (
	defaultIdentityBasePath = "trx.identityservice"
	defaultValidateSubject  = "validateInternalToken"
	defaultValidateTimeout  = 5 * time.Second
	defaultValidateCacheTTL = 300 * time.Second
	idtCacheMaxEntries      = 10_000
	reasonMissingIDT        = "MISSING_IDT"
	reasonValidateError     = "VALIDATE_ERROR"
	reasonDeniedFn          = "DENIED_FN"
	reasonInvalidIdentity   = "INVALID_IDENTITY"
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
		} else {
			log.Printf("IDT_METRIC event=config.warn var=IDT_VALIDATE_TIMEOUT_SECONDS value=%q using_default=%s", raw, defaultValidateTimeout)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IDT_VALIDATE_CACHE_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			c.CacheTTL = time.Duration(n) * time.Second
		} else {
			log.Printf("IDT_METRIC event=config.warn var=IDT_VALIDATE_CACHE_SECONDS value=%q using_default=%s", raw, defaultValidateCacheTTL)
		}
	}
	return c
}

// accessFromEnv is the Config.Access fallback (APP_ID / APP_FUNCTION_ID).
// Returns nil unless BOTH are set: a partial declaration (e.g. APP_ID with no
// APP_FUNCTION_ID) would otherwise advertise a card with an empty
// functionId, which downstream identity checks treat as BAD_REQUEST for
// every caller.
func accessFromEnv() *wire.AgentAccess {
	appID := strings.TrimSpace(os.Getenv("APP_ID"))
	fnID := strings.TrimSpace(os.Getenv("APP_FUNCTION_ID"))
	if appID == "" && fnID == "" {
		return nil
	}
	if appID == "" || fnID == "" {
		log.Printf("IDT_METRIC event=access.misconfig reason=partial_env")
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
	if cfg.Enabled && access == nil {
		// agent.New already refuses this configuration; guard here too so the
		// invariant (authorize can always deref v.access) holds locally.
		logger.Printf("IDT_METRIC event=validate.misconfig reason=missing_access")
		access = &wire.AgentAccess{}
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

// idPrefixLogMax caps the fallback (dotless) idPrefix so a malformed or
// oversized header can't blow up log lines.
const idPrefixLogMax = 40

// idPrefix returns "IDT-<uuid>" from "IDT-<uuid>.<cipher>" — safe for LOGS
// ONLY; never log the cipher tail. Not used for cache keys (see cacheKey):
// the prefix alone does not authenticate the token, so caching on it would
// let "IDT-<same-uuid>.<forged-cipher>" ride an existing allow.
func idPrefix(idt string) string {
	if i := strings.IndexByte(idt, '.'); i >= 0 {
		if i > idPrefixLogMax {
			return idt[:idPrefixLogMax]
		}
		return idt[:i]
	}
	if len(idt) > idPrefixLogMax {
		return idt[:idPrefixLogMax]
	}
	return idt
}

// cacheKey authenticates on the full token (via its hash), never the
// human-readable prefix alone, so a forged cipher tail sharing a cached
// token's prefix cannot ride that cache entry.
func cacheKey(idt, sessionID, appID, fnID string) string {
	sum := sha256.Sum256([]byte(idt))
	return hex.EncodeToString(sum[:]) + "|" + sessionID + "|" + appID + "|" + fnID
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
		// FailOpen only covers identity being unreachable, never a missing
		// token — a caller that sends no header at all must always be
		// denied unless we're in observe-only (log, don't block).
		if v.cfg.ObserveOnly {
			v.logger.Printf("IDT_METRIC event=validate.pass_through reason=%s agent=%s function=%s observe=%v failOpen=%v",
				reasonMissingIDT, appID, fnID, v.cfg.ObserveOnly, v.cfg.FailOpen)
			return Identity{}, nil
		}
		v.logger.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s", reasonMissingIDT, appID, fnID)
		return Identity{}, forbidden(reasonMissingIDT)
	}

	key := cacheKey(idt, sessionID, appID, fnID)
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
			v.logger.Printf("IDT_METRIC event=validate.pass_through reason=%s agent=%s function=%s idtid=%q err=%q",
				reasonValidateError, appID, fnID, idPrefix(idt), err.Error())
			return Identity{IDT: idt}, nil
		}
		v.logger.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s idtid=%q err=%q",
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
	case strings.TrimSpace(resp.UserID) == "":
		// Valid and granted but identity didn't return a user id: a verified
		// caller would otherwise silently overwrite a legitimate body userId
		// with "". Treat as a deny in enforcing mode; observe-only just logs.
		reason = reasonInvalidIdentity
	}
	if reason != "" {
		if v.cfg.ObserveOnly {
			v.logger.Printf("IDT_METRIC event=validate.observe decision=deny reason=%s agent=%s function=%s idtid=%q user=%s",
				reason, appID, fnID, idPrefix(idt), resp.UserID)
			return Identity{IDT: idt}, nil
		}
		v.logger.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s idtid=%q user=%s account=%s",
			reason, appID, fnID, idPrefix(idt), resp.UserID, resp.AccountID)
		return Identity{}, forbidden(reason)
	}

	id := Identity{UserID: resp.UserID, AccountID: resp.AccountID, Verified: !v.cfg.ObserveOnly, IDT: idt}
	if v.cfg.ObserveOnly {
		v.logger.Printf("IDT_METRIC event=validate.observe decision=allow agent=%s function=%s idtid=%q user=%s", appID, fnID, idPrefix(idt), resp.UserID)
		// Never cache in observe-only: caching an allow would silence the
		// per-request observe log after the first hit, undercounting the
		// rollout signal this mode exists to produce.
		return id, nil
	}
	v.logger.Printf("IDT_METRIC event=validate.allow agent=%s function=%s idtid=%q user=%s account=%s", appID, fnID, idPrefix(idt), resp.UserID, resp.AccountID)
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
