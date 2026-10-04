// Package toolclient consumes network agentic tools (SPEC.md §8): direct
// discovery and execution, plus a Registry that keeps an agent's view of the
// tool mesh current so newly deployed tools become usable without redeploys.
package toolclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"

	"github.com/transactrx/nats-agent/pkg/idt"
	"github.com/transactrx/nats-agent/pkg/wire"
)

const (
	DefaultDiscoverWindow = 2 * time.Second
	// runTimeoutMargin is added to a tool card's advisory timeoutSeconds to
	// size the request timeout.
	runTimeoutMargin = 10 * time.Second
	// defaultRunTimeout applies when neither the caller nor the card names
	// one: the host's default advisory timeout plus margin.
	defaultRunTimeout = 30*time.Second + runTimeoutMargin
)

// ServiceError is a protocol-level error reply from a tool (§9 envelope).
type ServiceError struct {
	nats_service.NatsServiceError
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("tool error %d (code %d): %s", e.Status, e.ApiStatusCode, e.ErrorMessage)
}

// Client talks to network tools.
type Client struct {
	nc       *nats.Conn
	svc      *nats_service_client.Client
	ownsConn bool
}

// New connects using the org env conventions (NATS_URL, NATS_JWT, NATS_KEY).
func New() (*Client, error) {
	svc, err := nats_service_client.NewClient()
	if err != nil {
		return nil, err
	}
	return &Client{nc: svc.GetNatsClient(), svc: svc, ownsConn: true}, nil
}

// NewFromConn wraps an existing connection; the caller keeps ownership.
func NewFromConn(nc *nats.Conn) *Client {
	return &Client{nc: nc, svc: nats_service_client.NewClientFromConnection(nc)}
}

// Close releases the connection if the client owns it.
func (c *Client) Close() {
	if c.ownsConn {
		c.nc.Close()
	}
}

// Discover scatter-gathers tool cards from every tool matching the filter.
func (c *Client) Discover(ctx context.Context, filter wire.DiscoverFilter, window time.Duration) ([]wire.ToolCard, error) {
	if window <= 0 {
		window = DefaultDiscoverWindow
	}
	body, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}
	inbox := nats.NewInbox()
	sub, err := c.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err := c.nc.PublishRequest(wire.ToolDiscoverSubject, inbox, body); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(window)
	seen := map[string]bool{}
	var cards []wire.ToolCard
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return cards, nil
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			return cards, nil
		}
		var card wire.ToolCard
		if err := json.Unmarshal(msg.Data, &card); err != nil || card.Name == "" || seen[card.Name] {
			continue
		}
		seen[card.Name] = true
		cards = append(cards, card)
	}
}

// Card fetches one tool's card directly.
func (c *Client) Card(ctx context.Context, name string) (*wire.ToolCard, error) {
	data, _ := json.Marshal(struct{}{})
	resp, svcErr, err := c.svc.DoRequest("", wire.ToolSubject(name, "card"), nil, data, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("request to tool %s: %w", name, err)
	}
	if svcErr != nil {
		return nil, &ServiceError{*svcErr}
	}
	var card wire.ToolCard
	if err := json.Unmarshal(resp.Data, &card); err != nil {
		return nil, fmt.Errorf("decoding card of tool %s: %w", name, err)
	}
	return &card, nil
}

// Run executes a tool. timeout <= 0 defaults to the standard advisory
// timeout plus margin; when the caller knows the card, prefer Registry.Run
// so the timeout matches the tool's own advertisement.
//
// A token set on ctx with idt.WithToken (or agentclient.WithIDT) is sent as
// the X-TRX-IDT header (SPEC §8.4). Run returns when ctx is done even if the
// tool has not replied; the tool itself is not interrupted.
func (c *Client) Run(ctx context.Context, name string, req wire.ToolRunRequest, timeout time.Duration) (*wire.ToolRunResponse, error) {
	if timeout <= 0 {
		timeout = defaultRunTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("running tool %s: %w", name, err)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	type result struct {
		out *wire.ToolRunResponse
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, svcErr, err := c.svc.DoRequest("", wire.ToolSubject(name, "run"), idt.Header(ctx), body, timeout)
		if err != nil {
			done <- result{err: fmt.Errorf("running tool %s: %w", name, err)}
			return
		}
		if svcErr != nil {
			done <- result{err: &ServiceError{*svcErr}}
			return
		}
		var out wire.ToolRunResponse
		if err := json.Unmarshal(resp.Data, &out); err != nil {
			done <- result{err: fmt.Errorf("decoding reply of tool %s: %w", name, err)}
			return
		}
		done <- result{out: &out}
	}()
	select {
	case r := <-done:
		return r.out, r.err
	case <-ctx.Done():
		return nil, fmt.Errorf("running tool %s: %w", name, ctx.Err())
	}
}
