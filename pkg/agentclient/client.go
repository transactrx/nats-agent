// Package agentclient is the consumer side of the NATS Agent Protocol: UIs,
// services, and other agents use it to discover agents and run chat turns
// against them.
package agentclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"

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

// Card fetches one agent's card directly.
func (c *Client) Card(ctx context.Context, agent string) (*wire.AgentCard, error) {
	return requestJSON[wire.AgentCard](ctx, c, wire.AgentSubject(agent, "card"), struct{}{})
}

// Ping checks agent liveness.
func (c *Client) Ping(ctx context.Context, agent string) (*wire.PingResponse, error) {
	return requestJSON[wire.PingResponse](ctx, c, wire.AgentSubject(agent, "ping"), struct{}{})
}

// Run is one live chat turn: the ack plus the event stream. The Events
// channel closes after the terminal event (which is always delivered last).
// Transport pings are consumed internally for liveness and not delivered.
type Run struct {
	Ack    wire.ChatAck
	Events <-chan wire.AgentEvent

	client *Client
	agent  string
}

// Cancel asks the owning instance to cancel this run. The run still ends
// with a done event (stopReason "cancelled") on the stream.
func (r *Run) Cancel(ctx context.Context) (bool, error) {
	return r.client.Cancel(ctx, r.agent, r.Ack.RunID)
}

// Chat starts a streaming chat turn. If req.StreamSubject is empty, a unique
// inbox is generated. The returned Run's Events channel delivers everything
// from start through the terminal event; if the agent goes silent past
// DeadRunTimeout or ctx is cancelled, the client synthesizes a terminal
// error event (and, for ctx cancellation, fires a best-effort cancel).
func (c *Client) Chat(ctx context.Context, agent string, req wire.ChatRequest) (*Run, error) {
	if len(req.Message.Content) == 0 {
		return nil, fmt.Errorf("chat request needs a message with content")
	}
	if req.StreamSubject == "" {
		req.StreamSubject = nats.NewInbox()
	}

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

	go func() {
		defer sub.Unsubscribe()
		defer close(out)
		dead := time.NewTimer(DeadRunTimeout)
		defer dead.Stop()

		terminate := func(message string) {
			out <- wire.AgentEvent{
				RunID: ack.RunID, Type: wire.EventError,
				Error: message, ErrorStatus: wire.CodeUpstream,
			}
		}

		for {
			select {
			case <-ctx.Done():
				// Best-effort server-side cancel, then end the local stream.
				data, _ := json.Marshal(wire.CancelRequest{RunID: ack.RunID})
				_ = c.nc.Publish(wire.AgentSubject(agent, "cancel"), data)
				terminate("context cancelled: " + ctx.Err().Error())
				return
			case <-dead.C:
				terminate(fmt.Sprintf("run dead: no events for %s", DeadRunTimeout))
				return
			case m := <-msgs:
				var ev wire.AgentEvent
				if err := json.Unmarshal(m.Data, &ev); err != nil {
					continue
				}
				if ev.RunID != "" && ev.RunID != ack.RunID {
					continue // stray traffic on a shared stream subject
				}
				if !dead.Stop() {
					<-dead.C
				}
				dead.Reset(DeadRunTimeout)
				if ev.Type == wire.EventPing {
					continue
				}
				out <- ev
				if ev.IsTerminal() {
					return
				}
			}
		}
	}()

	return run, nil
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
