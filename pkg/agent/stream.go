package agent

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// Heartbeat cadence: the spec requires an event at least every 15s while a
// run is live; we check twice as often and ping after 10s of silence.
const (
	heartbeatCheck = 5 * time.Second
	heartbeatIdle  = 10 * time.Second
)

// Stream emits protocol events for one run. All methods are safe for
// concurrent use; events after the terminal event are dropped.
type Stream struct {
	nc      *nats.Conn
	subject string
	runID   string
	logger  *log.Logger

	mu       sync.Mutex
	seq      int
	terminal bool
	lastPub  time.Time
	stopHB   chan struct{}
}

func newStream(nc *nats.Conn, subject, runID string, logger *log.Logger) *Stream {
	return &Stream{
		nc:      nc,
		subject: subject,
		runID:   runID,
		logger:  logger,
		stopHB:  make(chan struct{}),
	}
}

// start emits the mandatory first event (seq 0) and begins heartbeats.
func (s *Stream) start(sessionID string) {
	s.publish(wire.AgentEvent{Type: wire.EventStart, SessionID: sessionID})
	go s.heartbeat()
}

// Text emits an incremental piece of assistant text.
func (s *Stream) Text(delta string) {
	if delta == "" {
		return
	}
	s.publish(wire.AgentEvent{Type: wire.EventText, TextDelta: delta})
}

// ToolUse reports that the agent is executing a tool. Input must be
// JSON-marshalable (typically map[string]any or json.RawMessage).
func (s *Stream) ToolUse(toolUseID, toolName string, input any) {
	raw, err := json.Marshal(input)
	if err != nil {
		s.logger.Printf("run %s: cannot marshal input of tool %s: %v", s.runID, toolName, err)
		raw = []byte("{}")
	}
	s.publish(wire.AgentEvent{Type: wire.EventToolUse, ToolUseID: toolUseID, ToolName: toolName, ToolInput: raw})
}

// ToolResult reports a finished tool call. Pass toolErr "" on success.
func (s *Stream) ToolResult(toolUseID, toolName, result, toolErr string) {
	s.publish(wire.AgentEvent{Type: wire.EventToolResult, ToolUseID: toolUseID, ToolName: toolName, ToolResult: result, ToolError: toolErr})
}

// Data emits an agent-specific structured event. Clients ignore unknown
// kinds.
func (s *Stream) Data(kind string, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		s.logger.Printf("run %s: cannot marshal %q data payload: %v", s.runID, kind, err)
		return
	}
	s.publish(wire.AgentEvent{Type: wire.EventData, Kind: kind, Payload: raw})
}

// Status emits human-readable progress text.
func (s *Stream) Status(text string) {
	s.publish(wire.AgentEvent{Type: wire.EventStatus, StatusText: text})
}

// Done terminates the run successfully.
func (s *Stream) Done(stopReason string, usage *wire.Usage) {
	s.publish(wire.AgentEvent{Type: wire.EventDone, StopReason: stopReason, Usage: usage})
}

// Error terminates the run with a failure the client should surface.
func (s *Stream) Error(message string, code int) {
	s.publish(wire.AgentEvent{Type: wire.EventError, Error: message, ErrorStatus: code})
}

func (s *Stream) terminated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.terminal
}

func (s *Stream) publish(ev wire.AgentEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		if ev.Type != wire.EventPing {
			s.logger.Printf("run %s: dropping %s event after terminal", s.runID, ev.Type)
		}
		return
	}
	ev.RunID = s.runID
	ev.Seq = s.seq
	s.seq++

	data, err := json.Marshal(ev)
	if err != nil {
		s.logger.Printf("run %s: cannot marshal %s event: %v", s.runID, ev.Type, err)
		return
	}
	if err := s.nc.Publish(s.subject, data); err != nil {
		s.logger.Printf("run %s: publish to %s failed: %v", s.runID, s.subject, err)
	}
	s.lastPub = time.Now()

	if ev.Type == wire.EventDone || ev.Type == wire.EventError {
		s.terminal = true
		close(s.stopHB)
	}
}

func (s *Stream) heartbeat() {
	ticker := time.NewTicker(heartbeatCheck)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopHB:
			return
		case <-ticker.C:
			s.mu.Lock()
			idle := time.Since(s.lastPub) >= heartbeatIdle
			s.mu.Unlock()
			if idle {
				s.publish(wire.AgentEvent{Type: wire.EventPing})
			}
		}
	}
}
