// Package agentclient is the consumer side of the NATS Agent Protocol: UIs,
// services, and other agents use it to discover agents and run chat turns
// against them.
package agentclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"

	idtpkg "github.com/transactrx/nats-agent/pkg/idt"
	"github.com/transactrx/nats-agent/pkg/progress"
	"github.com/transactrx/nats-agent/pkg/wire"
)

const (
	// DefaultRequestTimeout bounds simple request/reply calls (card, ping,
	// chat ack, sessions).
	DefaultRequestTimeout = 10 * time.Second
	// DefaultDiscoverWindow is how long Discover collects scatter-gather
	// replies.
	DefaultDiscoverWindow = 2 * time.Second
	// DeadRunTimeout is how long a run may go without any event (agents
	// heartbeat every ≤15s) before the client declares it dead.
	DeadRunTimeout = 45 * time.Second
	// RenewTimeout bounds one renew request/reply.
	RenewTimeout = 5 * time.Second
)

var (
	// ErrRenewUnsupported: the run (or agent) offers no renew subject.
	ErrRenewUnsupported = errors.New("agentclient: run does not support renew")
	// ErrRenewRejected: the agent answered and refused the token; the
	// wrapping error carries the agent's reason. Transport failures (no
	// responders, timeout) are returned as other errors so callers can retry.
	ErrRenewRejected = errors.New("agentclient: renew rejected")
	// ErrRenewTransport: the renew got no reply (no responders, timeout,
	// connection error); the wrapping error also carries the cause. The
	// outcome is unknown (on a timeout the agent may have applied the token
	// and the reply was lost); retrying with the same token is safe.
	ErrRenewTransport = errors.New("agentclient: renew transport error")
)

// ServiceError is a protocol-level error reply from an agent (§9 envelope).
type ServiceError struct {
	nats_service.NatsServiceError
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("agent error %d (code %d): %s", e.Status, e.ApiStatusCode, e.ErrorMessage)
}

// Client talks to protocol agents. Create one per process and share it.
type Client struct {
	nc       *nats.Conn
	svc      *nats_service_client.Client
	ownsConn bool
	timeout  time.Duration
}

// New connects using the org env conventions (NATS_URL, NATS_JWT, NATS_KEY).
func New() (*Client, error) {
	svc, err := nats_service_client.NewClient()
	if err != nil {
		return nil, err
	}
	return &Client{nc: svc.GetNatsClient(), svc: svc, ownsConn: true, timeout: DefaultRequestTimeout}, nil
}

// NewFromConn wraps an existing connection; the caller keeps ownership.
func NewFromConn(nc *nats.Conn) *Client {
	return &Client{
		nc:      nc,
		svc:     nats_service_client.NewClientFromConnection(nc),
		timeout: DefaultRequestTimeout,
	}
}

// Close releases the connection if the client owns it.
func (c *Client) Close() {
	if c.ownsConn {
		c.nc.Close()
	}
}

// Conn exposes the underlying NATS connection.
func (c *Client) Conn() *nats.Conn { return c.nc }

// Discover scatter-gathers agent cards from every agent matching the filter.
// window <= 0 uses DefaultDiscoverWindow.
func (c *Client) Discover(ctx context.Context, filter wire.DiscoverFilter, window time.Duration) ([]wire.AgentCard, error) {
	if window <= 0 {
		window = DefaultDiscoverWindow
	}
	replies, err := scatterGather(ctx, c.nc, wire.AgentDiscoverSubject, filter, window)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var cards []wire.AgentCard
	for _, data := range replies {
		var card wire.AgentCard
		if err := json.Unmarshal(data, &card); err != nil || card.Name == "" || seen[card.Name] {
			continue
		}
		seen[card.Name] = true
		cards = append(cards, card)
	}
	return cards, nil
}

// scatterGather publishes a request and collects every reply that arrives
// within the window. Shared by agent and tool discovery.
func scatterGather(ctx context.Context, nc *nats.Conn, subject string, req any, window time.Duration) ([][]byte, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err := nc.PublishRequest(subject, inbox, body); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(window)
	var replies [][]byte
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return replies, nil
		}
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			remaining = time.Until(ctxDeadline)
			if remaining <= 0 {
				return replies, ctx.Err()
			}
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			return replies, nil // timeout: window closed
		}
		replies = append(replies, msg.Data)
	}
}

// WithIDT returns a context carrying the caller's Internal Delegation Token.
// Every authenticated call (Chat, Invoke, Sessions*) made with this context
// sends it as the X-TRX-IDT NATS header, as do tool runs made through
// toolclient. Empty idt is a no-op.
func WithIDT(ctx context.Context, idt string) context.Context {
	return idtpkg.WithToken(ctx, idt)
}

// WithIDTSource returns a context whose token is read from src on every
// call, so a long turn picks up a renewed token for its next request. It
// applies to everything that reads the token from ctx (Chat, Invoke,
// Sessions*, toolclient runs). When src returns "" the WithIDT value is used.
// A WithIDT/WithToken set on a derived context does not override the
// source. nil src is a no-op.
func WithIDTSource(ctx context.Context, src func() string) context.Context {
	return idtpkg.WithTokenSource(ctx, src)
}

// IDTFromContext returns the token from the WithIDTSource source, else the
// one set by WithIDT, or "".
func IDTFromContext(ctx context.Context) string {
	return idtpkg.TokenFromContext(ctx)
}

func idtHeader(ctx context.Context) nats_service_client.Header {
	return idtpkg.Header(ctx)
}

type userContextKey struct{}

// WithUserContext returns a context carrying the user's display context
// (time zone, name). Chat and Invoke made with it fill ChatRequest.Context
// when the request leaves it nil, so it follows the user from agent to agent.
// The agent runtime already puts the inbound turn's context on the handler
// ctx, so consults made with that ctx forward it automatically. nil is a no-op.
func WithUserContext(ctx context.Context, uc *wire.UserContext) context.Context {
	if uc = uc.Normalize(); uc == nil {
		return ctx
	}
	return context.WithValue(ctx, userContextKey{}, uc)
}

// UserContextFromContext returns the context set by WithUserContext, or nil.
func UserContextFromContext(ctx context.Context) *wire.UserContext {
	uc, _ := ctx.Value(userContextKey{}).(*wire.UserContext)
	return uc
}

func withContextFrom(ctx context.Context, req *wire.ChatRequest) {
	if req.Context == nil {
		if uc := UserContextFromContext(ctx); uc != nil {
			cp := *uc
			req.Context = &cp
		}
	}
}

// requestJSON does one documented request/reply against an agent endpoint.
func requestJSON[T any](ctx context.Context, c *Client, subject string, body any) (*T, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, svcErr, err := c.svc.DoRequest("", subject, idtHeader(ctx), data, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("request to %s: %w", subject, err)
	}
	if svcErr != nil {
		return nil, &ServiceError{*svcErr}
	}
	var out T
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		return nil, fmt.Errorf("decoding reply from %s: %w", subject, err)
	}
	return &out, nil
}

// Card fetches one agent's card directly. Unauthenticated by protocol: never
// sends X-TRX-IDT, even if the caller's ctx carries one via WithIDT.
func (c *Client) Card(ctx context.Context, agent string) (*wire.AgentCard, error) {
	return requestJSON[wire.AgentCard](context.Background(), c, wire.AgentSubject(agent, "card"), struct{}{})
}

// Ping checks agent liveness. Unauthenticated by protocol: never sends
// X-TRX-IDT, even if the caller's ctx carries one via WithIDT.
func (c *Client) Ping(ctx context.Context, agent string) (*wire.PingResponse, error) {
	return requestJSON[wire.PingResponse](context.Background(), c, wire.AgentSubject(agent, "ping"), struct{}{})
}

// Run is one live chat turn: the ack plus the event stream. The Events
// channel closes after the terminal event (which is always delivered last).
// Transport pings are consumed internally for liveness and not delivered.
// Events must be drained until the terminal event: the reader goroutine
// blocks on a full channel.
type Run struct {
	Ack    wire.ChatAck
	Events <-chan wire.AgentEvent
	// Caps is Ack.Caps, zero when the agent sent none (older agents).
	Caps wire.AckCaps

	client *Client
	agent  string
}

// Cancel asks the owning instance to cancel this run. The run still ends
// with a done event (stopReason "cancelled") on the stream.
func (r *Run) Cancel(ctx context.Context) (bool, error) {
	return r.client.Cancel(ctx, r.agent, r.Ack.RunID)
}

// Renew hands the running turn a fresh token (no expiry sent).
// ErrRenewUnsupported when the agent offered no renew subject.
func (r *Run) Renew(ctx context.Context, idt string) error {
	return r.RenewWithExpiry(ctx, idt, time.Time{})
}

// RenewWithExpiry is Renew carrying the new token's expiry (zero time = none).
func (r *Run) RenewWithExpiry(ctx context.Context, idt string, expiresAt time.Time) error {
	req := wire.RenewRequest{RunID: r.Ack.RunID, IDT: idt}
	if !expiresAt.IsZero() {
		req.ExpiresAtMs = expiresAt.UnixMilli()
	}
	return Renew(ctx, r.client.nc, r.Caps.RenewSubject, req)
}

// Renew sends req to a run's renew subject (ChatAck.Caps.RenewSubject), for
// callers that hold the subject without a Run. Empty subject →
// ErrRenewUnsupported; refused → ErrRenewRejected wrapping the reason; no
// reply within RenewTimeout → ErrRenewTransport wrapping the cause; ctx
// ended → the ctx error.
func Renew(ctx context.Context, nc *nats.Conn, subject string, req wire.RenewRequest) error {
	if subject == "" {
		return ErrRenewUnsupported
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, RenewTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, subject, data)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("renew request to %s: %w", subject, cerr)
		}
		return fmt.Errorf("%w: renew request to %s: %w", ErrRenewTransport, subject, err)
	}
	var resp wire.RenewResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return fmt.Errorf("decoding renew reply from %s: %w", subject, err)
	}
	if !resp.Applied {
		return fmt.Errorf("%w: %s", ErrRenewRejected, resp.Reason)
	}
	return nil
}

// ChatOptions tunes one chat turn. The zero value behaves exactly as Chat.
type ChatOptions struct {
	// IdleWindow > 0 cuts the run when no progress (any event but a ping)
	// arrives for this long: the client publishes a best-effort cancel and
	// ends the stream with an error event, Reason wire.ReasonProgressIdle.
	// 0 = no idle cut.
	IdleWindow time.Duration
}

// Chat starts a streaming chat turn. If req.StreamSubject is empty, a unique
// inbox is generated. The returned Run's Events channel delivers everything
// from start through the terminal event; if the agent goes silent past
// DeadRunTimeout or ctx is cancelled, the client synthesizes a terminal
// error event (and, for ctx cancellation, fires a best-effort cancel).
func (c *Client) Chat(ctx context.Context, agent string, req wire.ChatRequest) (*Run, error) {
	return c.ChatWith(ctx, agent, req, ChatOptions{})
}

// ChatWith is Chat with options (see ChatOptions).
func (c *Client) ChatWith(ctx context.Context, agent string, req wire.ChatRequest, o ChatOptions) (*Run, error) {
	if len(req.Message.Content) == 0 {
		return nil, fmt.Errorf("chat request needs a message with content")
	}
	if req.StreamSubject == "" {
		req.StreamSubject = nats.NewInbox()
	}
	withContextFrom(ctx, &req)

	msgs := make(chan *nats.Msg, 512)
	sub, err := c.nc.ChanSubscribe(req.StreamSubject, msgs)
	if err != nil {
		return nil, fmt.Errorf("subscribing to stream subject: %w", err)
	}

	ack, err := requestJSON[wire.ChatAck](ctx, c, wire.AgentSubject(agent, "chat"), req)
	if err != nil {
		sub.Unsubscribe()
		return nil, err
	}

	out := make(chan wire.AgentEvent, 64)
	run := &Run{Ack: *ack, Events: out, client: c, agent: agent}
	if ack.Caps != nil {
		run.Caps = *ack.Caps
	}

	r := newStreamReader(ack.RunID, o.IdleWindow, msgs, out)
	r.cancelRemote = func() {
		data, _ := json.Marshal(wire.CancelRequest{RunID: ack.RunID})
		_ = c.nc.Publish(wire.AgentSubject(agent, "cancel"), data)
	}

	go func() {
		defer sub.Unsubscribe()
		defer close(out)
		defer r.stop()
		dead := time.NewTimer(DeadRunTimeout)
		defer dead.Stop()
		r.resetDead = func() {
			if !dead.Stop() {
				<-dead.C
			}
			dead.Reset(DeadRunTimeout)
		}

		for {
			select {
			case <-ctx.Done():
				// Best-effort server-side cancel, then end the local stream.
				r.cancelRemote()
				r.terminate("context cancelled: "+ctx.Err().Error(), "")
				return
			case <-dead.C:
				r.terminate(fmt.Sprintf("run dead: no events for %s", DeadRunTimeout), "")
				return
			case <-r.idle:
				if r.onIdle() {
					return
				}
			case m := <-msgs:
				if r.onMsg(m) {
					return
				}
			}
		}
	}()

	return run, nil
}

// streamReader is the per-run state of ChatWith's reader goroutine. The
// idle decision lives here so every path applies the same rule: the run is
// cut only when the watcher has expired, no progress frame was handled since,
// and none is queued.
type streamReader struct {
	runID  string
	window time.Duration
	msgs   <-chan *nats.Msg
	out    chan<- wire.AgentEvent
	// watcher is one-shot; revive replaces it. nil (and idle nil, never
	// ready) without an idle window.
	watcher      *progress.Watcher
	idle         <-chan struct{}
	resetDead    func()
	cancelRemote func()
}

func newStreamReader(runID string, window time.Duration, msgs <-chan *nats.Msg, out chan<- wire.AgentEvent) *streamReader {
	r := &streamReader{runID: runID, window: window, msgs: msgs, out: out, resetDead: func() {}, cancelRemote: func() {}}
	if window > 0 {
		r.watcher = progress.NewWatcher(window)
		r.idle = r.watcher.Expired()
	}
	return r
}

// stop releases the current watcher.
func (r *streamReader) stop() {
	if r.watcher != nil {
		r.watcher.Stop()
	}
}

func (r *streamReader) expired() bool {
	if r.watcher == nil {
		return false
	}
	select {
	case <-r.watcher.Expired():
		return true
	default:
		return false
	}
}

// revive replaces an expired watcher with a fresh one for the same window,
// keeping the last stage. The one place that turns "expired, but progress
// arrived" back into a live window.
func (r *streamReader) revive() {
	old := r.watcher
	r.watcher = progress.NewWatcher(r.window)
	if stage := old.LastStage(); stage != "" {
		r.watcher.Touch(stage)
	}
	old.Stop()
	r.idle = r.watcher.Expired()
}

func (r *streamReader) terminate(message, reason string) {
	r.out <- wire.AgentEvent{
		RunID: r.runID, Type: wire.EventError,
		Error: message, ErrorStatus: wire.CodeUpstream, Reason: reason,
	}
}

// onMsg handles one stream frame; true when the run is over (terminal
// delivered). Progress after the watcher expired revives it.
func (r *streamReader) onMsg(m *nats.Msg) bool {
	var ev wire.AgentEvent
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		return false
	}
	if ev.RunID != "" && ev.RunID != r.runID {
		return false // stray traffic on a shared stream subject
	}
	r.resetDead()
	if r.watcher != nil {
		if ev.IsProgress() && r.expired() {
			r.revive()
		}
		r.watcher.Observe(&ev)
	}
	if ev.Type == wire.EventPing {
		return false
	}
	r.out <- ev
	return ev.IsTerminal()
}

// onIdle runs when the watcher expired. Frames already queued arrived in
// time (the reader was slow, not the agent), so they are handled first; a
// progress frame among them revives the watcher and the run goes on, a
// terminal ends it normally. Only with nothing (or only pings) queued is
// the run cut. true when the run is over.
func (r *streamReader) onIdle() bool {
	for r.expired() {
		select {
		case m := <-r.msgs:
			if r.onMsg(m) {
				return true
			}
		default:
			message := fmt.Sprintf("run idle: no progress for %s", r.window)
			if stage := r.watcher.LastStage(); stage != "" {
				message += " (last stage: " + stage + ")"
			}
			r.cancelRemote()
			r.terminate(message, wire.ReasonProgressIdle)
			return true
		}
	}
	return false
}

// Cancel asks all instances of an agent to cancel a run; the owner replies.
// Returns false (without error) when nothing owns the run — already
// finished, unknown, or the owner died.
func (c *Client) Cancel(ctx context.Context, agent, runID string) (bool, error) {
	data, err := json.Marshal(wire.CancelRequest{RunID: runID})
	if err != nil {
		return false, err
	}
	msg, err := c.nc.Request(wire.AgentSubject(agent, "cancel"), data, 2*time.Second)
	if err != nil {
		return false, nil // silence = nobody owns it
	}
	var resp wire.CancelResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return false, err
	}
	return resp.Cancelled, nil
}

// Invoke runs a synchronous turn (capability "sync").
func (c *Client) Invoke(ctx context.Context, agent string, req wire.ChatRequest) (*wire.InvokeResponse, error) {
	req.StreamSubject = ""
	withContextFrom(ctx, &req)
	return requestJSON[wire.InvokeResponse](ctx, c, wire.AgentSubject(agent, "invoke"), req)
}

// ─── Sessions (capability "sessions") ─────────────────────────────────────

func (c *Client) SessionsList(ctx context.Context, agent, userID string) (*wire.SessionsListResponse, error) {
	return requestJSON[wire.SessionsListResponse](ctx, c, wire.AgentSubject(agent, "sessionsList"), wire.SessionsListRequest{UserID: userID})
}

func (c *Client) SessionGet(ctx context.Context, agent, userID, sessionID string) (*wire.SessionGetResponse, error) {
	return requestJSON[wire.SessionGetResponse](ctx, c, wire.AgentSubject(agent, "sessionsGet"), wire.SessionGetRequest{UserID: userID, SessionID: sessionID})
}

func (c *Client) SessionDelete(ctx context.Context, agent, userID, sessionID string) (*wire.SessionDeleteResponse, error) {
	return requestJSON[wire.SessionDeleteResponse](ctx, c, wire.AgentSubject(agent, "sessionsDelete"), wire.SessionDeleteRequest{UserID: userID, SessionID: sessionID})
}

func (c *Client) SessionRename(ctx context.Context, agent, userID, sessionID, title string) (*wire.SessionRenameResponse, error) {
	return requestJSON[wire.SessionRenameResponse](ctx, c, wire.AgentSubject(agent, "sessionsRename"), wire.SessionRenameRequest{UserID: userID, SessionID: sessionID, Title: title})
}

func (c *Client) SessionSetFavorite(ctx context.Context, agent, userID, sessionID string, favorite bool) (*wire.SessionSetFavoriteResponse, error) {
	return requestJSON[wire.SessionSetFavoriteResponse](ctx, c, wire.AgentSubject(agent, "sessionsSetFavorite"), wire.SessionSetFavoriteRequest{UserID: userID, SessionID: sessionID, Favorite: favorite})
}
