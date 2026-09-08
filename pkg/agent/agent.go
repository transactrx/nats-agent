// Package agent implements the server side of the NATS Agent Protocol
// (SPEC.md). Supply a card and a chat handler; the runtime provides the
// subject space, discovery, ack/stream/heartbeat plumbing, cancellation, and
// optional sync + session endpoints. Built on transactrx/nats-service, so
// every endpoint is also visible to nats-discover.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/nats-agent/pkg/wire"
)

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Names that would collide with the common discovery subjects.
var reservedNames = map[string]bool{"discover": true, "announce": true}

// Endpoint paths owned by the runtime; extensions may not use them.
var reservedEndpoints = map[string]bool{
	"card": true, "ping": true, "chat": true, "cancel": true, "invoke": true,
}

// Turn is one chat (or invoke) request as seen by the handler.
type Turn struct {
	wire.ChatRequest
	RunID  string
	Logger *log.Logger
	// Identity is the runtime's verdict on the caller (see IDTValidation).
	// When Identity.Verified, UserID has been replaced by the verified id.
	Identity Identity
}

// ChatHandler runs one streaming chat turn. Emit events through the stream;
// if the handler returns without emitting a terminal event, the runtime
// emits done (nil error), error (non-nil), or done/cancelled (run was
// cancelled). The context is cancelled when the run is cancelled or the
// agent shuts down.
type ChatHandler func(ctx context.Context, turn *Turn, stream *Stream) error

// InvokeHandler runs one synchronous turn (capability "sync").
type InvokeHandler func(ctx context.Context, turn *Turn) (*wire.InvokeResponse, error)

// Config describes the agent. Name and Description are required; everything
// else has sensible defaults. NATS connection settings fall back to the org
// conventions: NATS_URL, NATS_JWT, NATS_KEY.
type Config struct {
	Name string
	// Region adds a separately queued regional alias without advertising a second agent.
	Region           string
	DisplayName      string
	Description      string
	Version          string
	RepositoryURL    string
	Tags             []string
	Skills           []wire.Skill
	InputModalities  []string // default ["text"]
	OutputModalities []string // default ["text"]
	Attachments      bool
	Metadata         map[string]any

	// Access registers this agent with the identity model (card "access").
	// nil → read APP_ID / APP_FUNCTION_ID from the environment.
	Access *wire.AgentAccess
	// IDTValidation controls inbound token checks. nil → IDTValidationFromEnv().
	IDTValidation *IDTValidation

	// Optional NATS overrides (default: environment).
	NATSURL string
	NATSJWT string
	NATSKey string
}

// Agent is a running (or startable) protocol agent.
type Agent struct {
	cfg       Config
	svc       *nats_service.NatService
	regional  *nats_service.NatService
	nc        *nats.Conn
	chat      ChatHandler
	invoke    InvokeHandler
	sessions  SessionStore
	extra     []nats_service.EndpointRegistration
	startTime time.Time
	idt       *idtValidator

	mu   sync.Mutex
	runs map[string]*run

	discoverSub       *nats.Subscription
	cancelSub         *nats.Subscription
	regionalCancelSub *nats.Subscription
}

type run struct {
	cancel    context.CancelFunc
	cancelled bool
}

// New creates an agent. The NATS connection is established immediately
// (fail-fast); endpoints are registered on Start.
func New(cfg Config) (*Agent, error) {
	if !nameRe.MatchString(cfg.Name) {
		return nil, fmt.Errorf("invalid agent name %q: must match %s", cfg.Name, nameRe)
	}
	if cfg.Region != "" && !nameRe.MatchString(cfg.Region) {
		return nil, fmt.Errorf("invalid agent region %q", cfg.Region)
	}
	if reservedNames[cfg.Name] {
		return nil, fmt.Errorf("agent name %q is reserved", cfg.Name)
	}
	if cfg.Description == "" {
		return nil, fmt.Errorf("agent description is required")
	}
	if len(cfg.InputModalities) == 0 {
		cfg.InputModalities = []string{"text"}
	}
	if len(cfg.OutputModalities) == 0 {
		cfg.OutputModalities = []string{"text"}
	}
	if cfg.Access == nil {
		cfg.Access = accessFromEnv()
	}
	// A caller-supplied Access must be complete regardless of whether IDT
	// validation is enabled: a partial Access still ends up on the public
	// card, and downstream identity checks treat an empty functionId as
	// BAD_REQUEST for every caller.
	if cfg.Access != nil && (cfg.Access.AppID == "" || cfg.Access.FunctionID == "") {
		return nil, fmt.Errorf("agent %q: Access requires both AppID and FunctionID (got appId=%q functionId=%q)",
			cfg.Name, cfg.Access.AppID, cfg.Access.FunctionID)
	}
	if cfg.IDTValidation == nil {
		v := IDTValidationFromEnv()
		cfg.IDTValidation = &v
	}
	if cfg.IDTValidation.Enabled && (cfg.Access == nil || cfg.Access.AppID == "" || cfg.Access.FunctionID == "") {
		return nil, fmt.Errorf("agent %q: IDT_VALIDATION=true requires Access.AppID and Access.FunctionID (env APP_ID / APP_FUNCTION_ID)", cfg.Name)
	}

	url := cfg.NATSURL
	if url == "" {
		url = os.Getenv("NATS_URL")
	}
	if url == "" {
		return nil, fmt.Errorf("NATS_URL is not set")
	}
	jwt := cfg.NATSJWT
	if jwt == "" {
		jwt = os.Getenv("NATS_JWT")
	}
	key := cfg.NATSKey
	if key == "" {
		key = os.Getenv("NATS_KEY")
	}

	svc, err := nats_service.NewLowLevel(
		wire.AgentPrefix+"."+cfg.Name, "agent."+cfg.Name, url, jwt, key,
		1024*2, 1024*300,
	)
	if err != nil {
		return nil, fmt.Errorf("connecting agent %q to NATS: %w", cfg.Name, err)
	}
	svc.SetDescription(cfg.Description)
	if cfg.RepositoryURL != "" {
		svc.SetRepositoryURL(cfg.RepositoryURL)
	}

	var regional *nats_service.NatService
	if cfg.Region != "" {
		alias := cfg.Name + "_" + cfg.Region
		regional, err = nats_service.NewLowLevel(wire.AgentPrefix+"."+alias, "agent."+alias, url, jwt, key, 1024*2, 1024*300)
		if err != nil {
			return nil, fmt.Errorf("connecting regional agent: %w", err)
		}
		regional.SetDescription("Regional route for " + cfg.Name + " in " + cfg.Region)
	}
	idt := newIDTValidator(svc.GetNatsService(), cfg.Access, *cfg.IDTValidation, nil)
	if cfg.IDTValidation.Enabled {
		// Log the validator's effective (defaulted) subject, not
		// cfg.IDTValidation.Subject, which may still be "" here.
		log.Printf("agent %q: IDT validation enabled (subject=%s appId=%s functionId=%s observeOnly=%v failOpen=%v cacheTTL=%s)",
			cfg.Name, idt.cfg.Subject, cfg.Access.AppID, cfg.Access.FunctionID, cfg.IDTValidation.ObserveOnly, cfg.IDTValidation.FailOpen, cfg.IDTValidation.CacheTTL)
	}

	return &Agent{
		cfg:      cfg,
		svc:      svc,
		regional: regional,
		nc:       svc.GetNatsService(),
		runs:     map[string]*run{},
		idt:      idt,
	}, nil
}

// OnChat sets the streaming chat handler (required).
func (a *Agent) OnChat(h ChatHandler) { a.chat = h }

// OnInvoke sets the synchronous handler and advertises capability "sync".
func (a *Agent) OnInvoke(h InvokeHandler) { a.invoke = h }

// UseSessionStore wires the session endpoints and advertises capability
// "sessions".
func (a *Agent) UseSessionStore(s SessionStore) { a.sessions = s }

// AddEndpoint registers an agent-specific extension endpoint in the agent's
// subject space, documented for nats-discover. Must be called before Start.
func (a *Agent) AddEndpoint(reg nats_service.EndpointRegistration) error {
	if reservedEndpoints[reg.Path] {
		return fmt.Errorf("endpoint path %q is reserved by the agent protocol", reg.Path)
	}
	a.extra = append(a.extra, reg)
	return nil
}

// Conn exposes the underlying NATS connection (e.g. for a tool registry
// sharing the connection).
func (a *Agent) Conn() *nats.Conn { return a.nc }

// Authorize checks msg's X-TRX-IDT header against this agent's configured
// access function. Use from AddEndpoint handlers that expose user data;
// returns the 403 envelope to hand back on deny. Built-in chat/invoke/
// sessions endpoints call this automatically.
func (a *Agent) Authorize(msg *nats_service.NatsMessage, sessionID string) (Identity, *nats_service.NatsServiceError) {
	return a.idt.authorize(msg.Header.Get(wire.HeaderIDT), sessionID)
}

// Card builds the agent card from the config and wired capabilities.
func (a *Agent) Card() wire.AgentCard {
	endpoints := []string{"card", "ping", "chat", "cancel"}
	if a.invoke != nil {
		endpoints = append(endpoints, "invoke")
	}
	if a.sessions != nil {
		endpoints = append(endpoints, "sessionsList", "sessionsGet", "sessionsDelete", "sessionsRename", "sessionsSetFavorite")
	}
	for _, e := range a.extra {
		endpoints = append(endpoints, e.Path)
	}
	return wire.AgentCard{
		ProtocolVersion: wire.ProtocolVersion,
		Kind:            wire.KindAgent,
		Name:            a.cfg.Name,
		DisplayName:     a.cfg.DisplayName,
		Description:     a.cfg.Description,
		Version:         a.cfg.Version,
		RepositoryURL:   a.cfg.RepositoryURL,
		Tags:            a.cfg.Tags,
		Access:          a.cfg.Access,
		Capabilities: wire.Capabilities{
			Streaming:   true,
			Sync:        a.invoke != nil,
			Sessions:    a.sessions != nil,
			Attachments: a.cfg.Attachments,
		},
		InputModalities:  a.cfg.InputModalities,
		OutputModalities: a.cfg.OutputModalities,
		Skills:           a.cfg.Skills,
		Endpoints:        endpoints,
		Metadata:         a.cfg.Metadata,
	}
}

// Start registers all endpoints and subscriptions and begins serving.
func (a *Agent) Start() error {
	if a.chat == nil {
		return fmt.Errorf("agent %q has no chat handler", a.cfg.Name)
	}
	a.startTime = time.Now()

	regs := []nats_service.EndpointRegistration{
		{
			Path:        "card",
			Description: "Agent card: self-description with capabilities, skills, and endpoints (agent protocol v" + wire.ProtocolVersion + ").",
			Response:    &nats_service.ResponseDoc{Description: "AgentCard", ContentType: "application/json"},
			Handler:     a.handleCard,
		},
		{
			Path:        "ping",
			Description: "Liveness probe.",
			Response:    &nats_service.ResponseDoc{Description: "PingResponse", ContentType: "application/json"},
			Handler:     a.handlePing,
		},
		{
			Path:        "chat",
			Description: "Streaming chat turn. Request: {sessionId?, userId?, message, streamSubject, metadata?}. Replies with an ack; events stream to streamSubject until a terminal done/error event.",
			Response:    &nats_service.ResponseDoc{Description: "ChatAck", ContentType: "application/json"},
			Handler:     a.handleChat,
		},
		{
			Path:        "cancel",
			Description: "Cancel a run by runId. Broadcast to all instances; the owning instance replies {cancelled:true}.",
			Response:    &nats_service.ResponseDoc{Description: "CancelResponse", ContentType: "application/json"},
			// The queue-balanced delivery must stay silent: the real work
			// happens on the broadcast subscription below, which every
			// instance (including this one) receives. Responding here would
			// race the owner's reply.
			Handler: func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
				fwd := nats_service.NewForwardedError(msg.Path)
				return &fwd
			},
		},
	}
	if a.invoke != nil {
		regs = append(regs, nats_service.EndpointRegistration{
			Path:        "invoke",
			Description: "Synchronous single-shot turn. Request: chat request without streamSubject; reply is the complete result.",
			Response:    &nats_service.ResponseDoc{Description: "InvokeResponse", ContentType: "application/json"},
			Handler:     a.handleInvoke,
		})
	}
	if a.sessions != nil {
		regs = append(regs, a.sessionRegistrations()...)
	}
	regs = append(regs, a.extra...)

	if err := a.svc.AddEndpointWithDocs(regs); err != nil {
		return err
	}

	if a.regional != nil {
		if err := a.regional.AddEndpointWithDocs(regs); err != nil {
			return err
		}
		if err := a.regional.Start(); err != nil {
			return err
		}
	}

	// Common discovery: one reply per agent (queue group), card on match.
	discoverSub, err := a.nc.QueueSubscribe(wire.AgentDiscoverSubject, "agent."+a.cfg.Name, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		var filter wire.DiscoverFilter
		if len(msg.Data) > 0 {
			if err := json.Unmarshal(msg.Data, &filter); err != nil {
				return // malformed filters get silence, like a non-match
			}
		}
		card := a.Card()
		if !card.MatchesFilter(filter) {
			return
		}
		data, _ := json.Marshal(card)
		_ = a.nc.Publish(msg.Reply, data)
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", wire.AgentDiscoverSubject, err)
	}
	a.discoverSub = discoverSub

	// Cancel broadcast: every instance listens; only the run owner replies.
	cancelHandler := func(msg *nats.Msg) {
		var req wire.CancelRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.RunID == "" {
			return
		}
		a.mu.Lock()
		r, ok := a.runs[req.RunID]
		if ok {
			r.cancelled = true
			r.cancel()
		}
		a.mu.Unlock()
		if ok && msg.Reply != "" {
			data, _ := json.Marshal(wire.CancelResponse{Cancelled: true})
			_ = a.nc.Publish(msg.Reply, data)
		}
	}
	cancelSub, err := a.nc.Subscribe(wire.AgentSubject(a.cfg.Name, "cancel"), cancelHandler)
	if err != nil {
		return fmt.Errorf("subscribing to cancel broadcast: %w", err)
	}
	a.cancelSub = cancelSub
	if a.regional != nil {
		a.regionalCancelSub, err = a.nc.Subscribe(wire.AgentSubject(a.cfg.Name+"_"+a.cfg.Region, "cancel"), cancelHandler)
		if err != nil {
			return err
		}
	}

	if err := a.svc.Start(); err != nil {
		return err
	}
	// Start returns only after both subscription sets are registered. Callers
	// may immediately issue requests from another connection.
	if err := a.nc.FlushTimeout(5 * time.Second); err != nil {
		return err
	}
	if a.regional != nil {
		if err := a.regional.GetNatsService().FlushTimeout(5 * time.Second); err != nil {
			return err
		}
	}
	log.Printf("agent %q serving on %s.> (instance %s)", a.cfg.Name, wire.AgentPrefix+"."+a.cfg.Name, a.svc.GetInstanceId())
	return nil
}

// Shutdown drains subscriptions. In-flight runs keep their contexts until
// the process exits.
func (a *Agent) Shutdown() error {
	if a.regional != nil {
		_ = a.regional.Shutdown()
	}
	if a.discoverSub != nil {
		_ = a.discoverSub.Drain()
	}
	if a.regionalCancelSub != nil {
		_ = a.regionalCancelSub.Drain()
	}
	if a.cancelSub != nil {
		_ = a.cancelSub.Drain()
	}
	return a.svc.Shutdown()
}

func (a *Agent) handleCard(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	data, err := json.Marshal(a.Card())
	if err != nil {
		e := nats_service.NewServerError("marshaling card", wire.CodeInternal, err)
		return &e
	}
	msg.ResponseBody = data
	return nil
}

func (a *Agent) handlePing(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	data, _ := json.Marshal(wire.PingResponse{
		Status:        "ok",
		Name:          a.cfg.Name,
		InstanceID:    a.svc.GetInstanceId(),
		Version:       a.cfg.Version,
		UptimeSeconds: int64(time.Since(a.startTime).Seconds()),
	})
	msg.ResponseBody = data
	return nil
}

func (a *Agent) parseTurn(msg *nats_service.NatsMessage, needStream bool) (*Turn, *nats_service.NatsServiceError) {
	var req wire.ChatRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		e := nats_service.NewValidationError("invalid request body", wire.CodeInvalidBody, err)
		return nil, &e
	}
	if len(req.Message.Content) == 0 {
		e := nats_service.NewValidationError("message with content is required", wire.CodeMissingField, nil)
		return nil, &e
	}
	if req.Message.Role == "" {
		req.Message.Role = "user"
	}
	if needStream && req.StreamSubject == "" {
		e := nats_service.NewValidationError("streamSubject is required", wire.CodeMissingField, nil)
		return nil, &e
	}
	if req.SessionID == "" {
		req.SessionID = uuid.New().String()
	}
	id, aerr := a.Authorize(msg, req.SessionID)
	if aerr != nil {
		return nil, aerr
	}
	if id.Verified {
		req.UserID = id.UserID
	}
	return &Turn{
		ChatRequest: req,
		RunID:       "run_" + uuid.New().String(),
		Logger:      msg.Logger,
		Identity:    id,
	}, nil
}

func (a *Agent) handleChat(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	turn, verr := a.parseTurn(msg, true)
	if verr != nil {
		return verr
	}

	ack := wire.ChatAck{
		Accepted:      true,
		RunID:         turn.RunID,
		SessionID:     turn.SessionID,
		Agent:         a.cfg.Name,
		InstanceID:    a.svc.GetInstanceId(),
		StreamSubject: turn.StreamSubject,
	}
	data, err := json.Marshal(ack)
	if err != nil {
		e := nats_service.NewServerError("marshaling ack", wire.CodeInternal, err)
		return &e
	}
	msg.ResponseBody = data

	ctx, cancel := context.WithCancel(context.Background())
	r := &run{cancel: cancel}
	a.mu.Lock()
	a.runs[turn.RunID] = r
	a.mu.Unlock()

	stream := newStream(a.nc, turn.StreamSubject, turn.RunID, turn.Logger)

	go func() {
		defer func() {
			a.mu.Lock()
			delete(a.runs, turn.RunID)
			a.mu.Unlock()
			cancel()
		}()
		defer func() {
			if rec := recover(); rec != nil {
				turn.Logger.Printf("chat handler panic (run %s): %v", turn.RunID, rec)
				stream.Error(fmt.Sprintf("agent panic: %v", rec), wire.CodeInternal)
			}
		}()

		stream.start(turn.SessionID)
		err := a.chat(ctx, turn, stream)

		a.mu.Lock()
		wasCancelled := r.cancelled
		a.mu.Unlock()

		switch {
		case stream.terminated():
			// handler already ended the run
		case wasCancelled:
			stream.Done(wire.StopCancelled, nil)
		case err != nil:
			stream.Error(err.Error(), wire.CodeInternal)
		default:
			stream.Done(wire.StopEndTurn, nil)
		}
	}()

	return nil
}

func (a *Agent) handleInvoke(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	turn, verr := a.parseTurn(msg, false)
	if verr != nil {
		return verr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.mu.Lock()
	a.runs[turn.RunID] = &run{cancel: cancel}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.runs, turn.RunID)
		a.mu.Unlock()
	}()

	resp, err := a.invoke(ctx, turn)
	if err != nil {
		e := nats_service.NewServerError(err.Error(), wire.CodeInternal, err)
		return &e
	}
	if resp.RunID == "" {
		resp.RunID = turn.RunID
	}
	if resp.SessionID == "" {
		resp.SessionID = turn.SessionID
	}
	data, merr := json.Marshal(resp)
	if merr != nil {
		e := nats_service.NewServerError("marshaling response", wire.CodeInternal, merr)
		return &e
	}
	msg.ResponseBody = data
	return nil
}
