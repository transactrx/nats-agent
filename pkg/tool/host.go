// Package tool implements the server side of network agentic tools
// (SPEC.md §8). A Host is one deployable process serving any number of
// tools; each hosted tool is indistinguishable on the wire from a tool
// running alone.
package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/nats-agent/pkg/idt"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// Tool is the contract a network tool implements. It is intentionally
// identical to the local-tool contract used by the copay chat application,
// so an in-process tool can be lifted onto the network unchanged.
type Tool interface {
	Name() string
	Description() string
	InputSchema() map[string]any
	Run(ctx context.Context, input map[string]any) (string, error)
}

// Call is everything the host knows about one run request (v1.2).
type Call struct {
	ToolUseID string
	SessionID string
	Agent     string
	Input     map[string]any
	Metadata  map[string]any
	// Identity is the host's verdict on the caller. When Identity.Verified,
	// UserID came from the token; otherwise it is the caller's assertion.
	Identity idt.Identity
}

// CallTool is an optional extension of Tool. A tool implementing it
// receives the full request (caller identity, session, correlation id) and
// may return structured content; the host then calls RunCall instead of Run.
// The run context also carries the caller's token (idt.TokenFromContext) so
// the tool can forward it on onward authenticated calls.
type CallTool interface {
	Tool
	RunCall(ctx context.Context, call Call) ([]wire.ToolResultContent, error)
}

// Info carries the deployment metadata a Tool implementation doesn't know
// about itself. All fields are optional.
type Info struct {
	DisplayName    string
	Version        string
	RepositoryURL  string
	Tags           []string
	TimeoutSeconds int // advisory worst-case runtime; default 30
	Metadata       map[string]any
	// Access registers the tool with the identity model (card "access").
	// Required when the host validates tokens.
	Access *wire.AgentAccess
	// MaxConcurrent bounds simultaneous runs of this tool per host process;
	// excess requests get 429/4291. 0 = unbounded.
	MaxConcurrent int
}

const defaultTimeoutSeconds = 30

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
var reservedNames = map[string]bool{"discover": true, "announce": true}

type hostedTool struct {
	tool        Tool
	info        Info
	card        wire.ToolCard
	svc         *nats_service.NatService
	nc          *nats.Conn
	discoverSub *nats.Subscription
	startTime   time.Time
	validator   *idt.Validator
	slots       chan struct{}
}

// Host serves registered tools over NATS.
type Host struct {
	url, jwt, key string
	tools         map[string]*hostedTool
	started       bool
	validation    idt.Validation
}

// NewHost reads the org NATS env conventions (NATS_URL, NATS_JWT, NATS_KEY)
// and the IDT validation env contract (idt.ValidationFromEnv).
func NewHost() (*Host, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		return nil, fmt.Errorf("NATS_URL is not set")
	}
	h := NewHostWithNATS(url, os.Getenv("NATS_JWT"), os.Getenv("NATS_KEY"))
	h.validation = idt.ValidationFromEnv()
	return h, nil
}

// NewHostWithNATS uses explicit connection settings (tests, special
// deployments). Token validation is off until SetIDTValidation.
func NewHostWithNATS(url, jwt, key string) *Host {
	return &Host{url: url, jwt: jwt, key: key, tools: map[string]*hostedTool{}}
}

// SetIDTValidation configures inbound token checks on every tool's run
// endpoint (SPEC §8.4). Must be called before Start.
func (h *Host) SetIDTValidation(v idt.Validation) error {
	if h.started {
		return fmt.Errorf("host already started")
	}
	h.validation = v
	return nil
}

// Register adds a tool to the host. info may be nil. Must be called before
// Start.
func (h *Host) Register(t Tool, info *Info) error {
	if h.started {
		return fmt.Errorf("host already started")
	}
	name := t.Name()
	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid tool name %q: must match %s", name, nameRe)
	}
	if reservedNames[name] {
		return fmt.Errorf("tool name %q is reserved", name)
	}
	if _, dup := h.tools[name]; dup {
		return fmt.Errorf("tool %q registered twice", name)
	}
	var i Info
	if info != nil {
		i = *info
	}
	if i.TimeoutSeconds <= 0 {
		i.TimeoutSeconds = defaultTimeoutSeconds
	}
	if i.Access != nil && (i.Access.AppID == "" || i.Access.FunctionID == "") {
		return fmt.Errorf("tool %q: Access requires both AppID and FunctionID", name)
	}
	if i.MaxConcurrent < 0 {
		return fmt.Errorf("tool %q: MaxConcurrent must not be negative", name)
	}
	h.tools[name] = &hostedTool{
		tool: t,
		info: i,
		card: wire.ToolCard{
			ProtocolVersion: wire.ProtocolVersion,
			Kind:            wire.KindTool,
			Name:            name,
			DisplayName:     i.DisplayName,
			Description:     t.Description(),
			Version:         i.Version,
			RepositoryURL:   i.RepositoryURL,
			Tags:            i.Tags,
			InputSchema:     t.InputSchema(),
			TimeoutSeconds:  i.TimeoutSeconds,
			Access:          i.Access,
			Metadata:        i.Metadata,
		},
	}
	return nil
}

// Cards returns the cards of all registered tools.
func (h *Host) Cards() []wire.ToolCard {
	out := make([]wire.ToolCard, 0, len(h.tools))
	for _, ht := range h.tools {
		out = append(out, ht.card)
	}
	return out
}

// Start brings every registered tool onto the wire: its own subject space
// and queue group, discovery reply, and a startup announcement.
func (h *Host) Start() error {
	if len(h.tools) == 0 {
		return fmt.Errorf("no tools registered")
	}
	if h.validation.Enabled {
		for name, ht := range h.tools {
			if ht.card.Access == nil {
				return fmt.Errorf("tool %q: IDT validation requires Info.Access (appId + functionId)", name)
			}
		}
	}
	h.started = true

	for name, ht := range h.tools {
		svc, err := nats_service.NewLowLevel(
			wire.ToolPrefix+"."+name, "tool."+name, h.url, h.jwt, h.key,
			1024*2, 1024*300,
		)
		if err != nil {
			return fmt.Errorf("connecting tool %q to NATS: %w", name, err)
		}
		svc.SetDescription(ht.card.Description)
		if ht.card.RepositoryURL != "" {
			svc.SetRepositoryURL(ht.card.RepositoryURL)
		}
		ht.svc = svc
		ht.nc = svc.GetNatsService()
		ht.startTime = time.Now()
		ht.validator = idt.NewValidator(ht.nc, ht.card.Access, h.validation, nil)
		if ht.info.MaxConcurrent > 0 {
			ht.slots = make(chan struct{}, ht.info.MaxConcurrent)
		}
		if h.validation.Enabled {
			log.Printf("tool %q: IDT validation enabled (subject=%s appId=%s functionId=%s observeOnly=%v failOpen=%v)",
				name, ht.validator.Config().Subject, ht.card.Access.AppID, ht.card.Access.FunctionID, h.validation.ObserveOnly, h.validation.FailOpen)
		}

		regs := []nats_service.EndpointRegistration{
			{
				Path:        "card",
				Description: "Tool card: model-facing description and input schema (agent protocol v" + wire.ProtocolVersion + ").",
				Response:    &nats_service.ResponseDoc{Description: "ToolCard", ContentType: "application/json"},
				Handler:     ht.handleCard,
			},
			{
				Path:        "ping",
				Description: "Liveness probe.",
				Response:    &nats_service.ResponseDoc{Description: "PingResponse", ContentType: "application/json"},
				Handler:     ht.handlePing,
			},
			{
				Path:        "run",
				Description: "Execute the tool. Request: {toolUseId?, input, userId?, sessionId?, agent?, metadata?}; header X-TRX-IDT when the card declares access. Execution errors return status:\"error\" in a 200 reply (model-visible data).",
				Response:    &nats_service.ResponseDoc{Description: "ToolRunResponse", ContentType: "application/json"},
				Handler:     ht.handleRun,
			},
		}
		if err := svc.AddEndpointWithDocs(regs); err != nil {
			return fmt.Errorf("registering tool %q endpoints: %w", name, err)
		}

		discoverSub, err := ht.nc.QueueSubscribe(wire.ToolDiscoverSubject, "tool."+name, ht.handleDiscover)
		if err != nil {
			return fmt.Errorf("subscribing tool %q to discovery: %w", name, err)
		}
		ht.discoverSub = discoverSub

		if err := svc.Start(); err != nil {
			return fmt.Errorf("starting tool %q: %w", name, err)
		}

		// Startup announcement so agents pick the tool up immediately.
		if data, err := json.Marshal(ht.card); err == nil {
			_ = ht.nc.Publish(wire.ToolAnnounceSubject, data)
		}
		log.Printf("tool %q serving on %s.>", name, wire.ToolPrefix+"."+name)
	}
	return nil
}

// Shutdown drains all tool subscriptions.
func (h *Host) Shutdown() {
	for _, ht := range h.tools {
		if ht.discoverSub != nil {
			_ = ht.discoverSub.Drain()
		}
		if ht.svc != nil {
			_ = ht.svc.Shutdown()
		}
	}
}

func (ht *hostedTool) handleDiscover(msg *nats.Msg) {
	if msg.Reply == "" {
		return
	}
	var filter wire.DiscoverFilter
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &filter); err != nil {
			return
		}
	}
	if !ht.card.MatchesFilter(filter) {
		return
	}
	data, _ := json.Marshal(ht.card)
	_ = ht.nc.Publish(msg.Reply, data)
}

func (ht *hostedTool) handleCard(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	data, err := json.Marshal(ht.card)
	if err != nil {
		e := nats_service.NewServerError("marshaling card", wire.CodeInternal, err)
		return &e
	}
	msg.ResponseBody = data
	return nil
}

func (ht *hostedTool) handlePing(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	data, _ := json.Marshal(wire.PingResponse{
		Status:        "ok",
		Name:          ht.card.Name,
		InstanceID:    ht.svc.GetInstanceId(),
		Version:       ht.card.Version,
		UptimeSeconds: int64(time.Since(ht.startTime).Seconds()),
	})
	msg.ResponseBody = data
	return nil
}

func (ht *hostedTool) handleRun(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	var req wire.ToolRunRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		e := nats_service.NewValidationError("invalid request body", wire.CodeInvalidBody, err)
		return &e
	}
	if req.Input == nil {
		req.Input = map[string]any{}
	}

	// Authenticate before doing any work (SPEC §8.4). The decision cache is
	// partitioned by session, like an agent's.
	id, denied := ht.validator.Authorize(msg.Header.Get(wire.HeaderIDT), req.SessionID)
	if denied != nil {
		return denied
	}
	if id.Verified {
		req.UserID = id.UserID
	}

	if ht.slots != nil {
		select {
		case ht.slots <- struct{}{}:
			defer func() { <-ht.slots }()
		default:
			return &nats_service.NatsServiceError{Status: 429, ErrorMessage: "tool is at capacity", ApiStatusCode: wire.CodeBusy}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ht.card.TimeoutSeconds)*time.Second)
	defer cancel()
	ctx = idt.WithToken(ctx, id.IDT)

	start := time.Now()
	var content []wire.ToolResultContent
	var err error
	if ct, ok := ht.tool.(CallTool); ok {
		content, err = ct.RunCall(ctx, Call{
			ToolUseID: req.ToolUseID,
			SessionID: req.SessionID,
			Agent:     req.Agent,
			Input:     req.Input,
			Metadata:  req.Metadata,
			Identity:  id,
		})
	} else {
		var result string
		result, err = ht.tool.Run(ctx, req.Input)
		content = []wire.ToolResultContent{{Text: result}}
	}
	resp := wire.ToolRunResponse{
		ToolUseID: req.ToolUseID,
		LatencyMs: time.Since(start).Milliseconds(),
	}
	if err != nil {
		// Model-visible failure, not a protocol error.
		resp.Status = wire.ToolStatusError
		resp.Error = err.Error()
		msg.Logger.Printf("tool %s run failed (user=%s agent=%s): %v", ht.card.Name, req.UserID, req.Agent, err)
	} else {
		resp.Status = wire.ToolStatusSuccess
		resp.Content = content
	}

	data, merr := json.Marshal(resp)
	if merr != nil {
		e := nats_service.NewServerError("marshaling response", wire.CodeInternal, merr)
		return &e
	}
	msg.ResponseBody = data
	return nil
}
