package natsagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/tool"
	"github.com/transactrx/nats-agent/pkg/toolclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// The embedded server lives for the whole test binary. Connections created
// by nats-service install a ClosedHandler that exits the process, so tests
// never close them explicitly; process exit cleans up.
var testURL string

func TestMain(m *testing.M) {
	opts := &server.Options{Port: -1, JetStream: false}
	srv, err := server.NewServer(opts)
	if err != nil {
		panic(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		panic("embedded nats-server did not become ready")
	}
	testURL = srv.ClientURL()
	m.Run()
}

func testConn(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(testURL)
	if err != nil {
		t.Fatalf("connecting test client: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// ─── fixtures ─────────────────────────────────────────────────────────────

type memSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*wire.SessionGetResponse // userID/sessionID
}

func newMemSessionStore() *memSessionStore {
	return &memSessionStore{sessions: map[string]*wire.SessionGetResponse{}}
}

func (s *memSessionStore) key(userID, sessionID string) string { return userID + "/" + sessionID }

func (s *memSessionStore) List(_ context.Context, userID string) ([]wire.SessionMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []wire.SessionMeta
	for k, v := range s.sessions {
		if len(k) > len(userID) && k[:len(userID)+1] == userID+"/" {
			out = append(out, wire.SessionMeta{SessionID: v.SessionID, Title: v.Title, Favorite: v.Favorite})
		}
	}
	return out, nil
}

func (s *memSessionStore) Get(_ context.Context, userID, sessionID string) (*wire.SessionGetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[s.key(userID, sessionID)]
	if !ok {
		return nil, agent.ErrSessionNotFound
	}
	cp := *sess
	return &cp, nil
}

func (s *memSessionStore) Delete(_ context.Context, userID, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[s.key(userID, sessionID)]; !ok {
		return agent.ErrSessionNotFound
	}
	delete(s.sessions, s.key(userID, sessionID))
	return nil
}

func (s *memSessionStore) Rename(_ context.Context, userID, sessionID, title string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[s.key(userID, sessionID)]
	if !ok {
		return agent.ErrSessionNotFound
	}
	sess.Title = title
	return nil
}

func (s *memSessionStore) SetFavorite(_ context.Context, userID, sessionID string, fav bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[s.key(userID, sessionID)]
	if !ok {
		return agent.ErrSessionNotFound
	}
	sess.Favorite = fav
	return nil
}

func (s *memSessionStore) put(userID string, sess wire.SessionGetResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[s.key(userID, sess.SessionID)] = &sess
}

type echoTool struct{}

func (echoTool) Name() string        { return "echo" }
func (echoTool) Description() string { return "Echo the input back as JSON. For tests." }
func (echoTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"say": map[string]any{"type": "string"}}}
}
func (echoTool) Run(_ context.Context, input map[string]any) (string, error) {
	b, _ := json.Marshal(input)
	return string(b), nil
}

type failTool struct{}

func (failTool) Name() string                { return "alwaysfail" }
func (failTool) Description() string         { return "Always fails. For tests." }
func (failTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (failTool) Run(_ context.Context, _ map[string]any) (string, error) {
	return "", fmt.Errorf("intentional failure")
}

func startTestAgent(t *testing.T) (*agent.Agent, *memSessionStore) {
	t.Helper()
	store := newMemSessionStore()
	a, err := agent.New(agent.Config{
		Name:        "testAgent",
		DisplayName: "Test Agent",
		Description: "Agent used by the nats-agent e2e tests.",
		Version:     "0.1.0",
		Tags:        []string{"test"},
		Skills:      []wire.Skill{{Name: "echoing", Description: "echoes things"}},
		NATSURL:     testURL,
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	a.UseSessionStore(store)
	a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
		text := ""
		if len(turn.Message.Content) > 0 {
			text = turn.Message.Content[0].Text
		}
		if text == "block" {
			<-ctx.Done() // wait for cancellation
			return ctx.Err()
		}
		stream.Status("thinking")
		stream.ToolUse("tu_1", "echo", map[string]any{"say": text})
		stream.ToolResult("tu_1", "echo", `{"say":"`+text+`"}`, "")
		stream.Text("you said: ")
		stream.Text(text)
		stream.Done(wire.StopEndTurn, &wire.Usage{InputTokens: 1, OutputTokens: 2, TotalTokens: 3})
		return nil
	})
	if err := a.Start(); err != nil {
		t.Fatalf("agent.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	return a, store
}

// ─── agent tests ──────────────────────────────────────────────────────────

func TestAgentDiscoveryAndCard(t *testing.T) {
	startTestAgent(t)
	c := agentclient.NewFromConn(testConn(t))

	cards, err := c.Discover(context.Background(), wire.DiscoverFilter{}, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	var found *wire.AgentCard
	for i := range cards {
		if cards[i].Name == "testAgent" {
			found = &cards[i]
		}
	}
	if found == nil {
		t.Fatalf("testAgent not discovered; got %d cards", len(cards))
	}
	if found.Kind != wire.KindAgent || found.ProtocolVersion != wire.ProtocolVersion {
		t.Errorf("bad card identity: kind=%q protocolVersion=%q", found.Kind, found.ProtocolVersion)
	}
	if !found.Capabilities.Streaming || !found.Capabilities.Sessions || found.Capabilities.Sync {
		t.Errorf("bad capabilities: %+v", found.Capabilities)
	}

	// Filters: skill match, skill mismatch.
	cards, _ = c.Discover(context.Background(), wire.DiscoverFilter{Skill: "echoing"}, 500*time.Millisecond)
	if len(cards) != 1 {
		t.Errorf("skill filter should match exactly testAgent, got %d cards", len(cards))
	}
	cards, _ = c.Discover(context.Background(), wire.DiscoverFilter{Skill: "nope"}, 500*time.Millisecond)
	if len(cards) != 0 {
		t.Errorf("non-matching skill filter returned %d cards", len(cards))
	}

	card, err := c.Card(context.Background(), "testAgent")
	if err != nil {
		t.Fatalf("Card: %v", err)
	}
	if card.Description == "" || len(card.Endpoints) == 0 {
		t.Errorf("direct card fetch incomplete: %+v", card)
	}

	ping, err := c.Ping(context.Background(), "testAgent")
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if ping.Status != "ok" || ping.Name != "testAgent" || ping.InstanceID == "" {
		t.Errorf("bad ping: %+v", ping)
	}
}

func TestAgentChatStream(t *testing.T) {
	startTestAgent(t)
	c := agentclient.NewFromConn(testConn(t))

	run, err := c.Chat(context.Background(), "testAgent", wire.ChatRequest{
		UserID:  "tester",
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hello"}}},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if !run.Ack.Accepted || run.Ack.RunID == "" || run.Ack.SessionID == "" {
		t.Fatalf("bad ack: %+v", run.Ack)
	}

	var events []wire.AgentEvent
	for ev := range run.Events {
		events = append(events, ev)
	}
	types := make([]string, len(events))
	lastSeq := -1
	text := ""
	for i, ev := range events {
		types[i] = ev.Type
		if ev.Seq <= lastSeq {
			t.Errorf("seq not increasing: %d after %d (type %s)", ev.Seq, lastSeq, ev.Type)
		}
		lastSeq = ev.Seq
		if ev.RunID != run.Ack.RunID {
			t.Errorf("event %d has runId %q, want %q", i, ev.RunID, run.Ack.RunID)
		}
		if ev.Type == wire.EventText {
			text += ev.TextDelta
		}
	}
	want := []string{wire.EventStart, wire.EventStatus, wire.EventToolUse, wire.EventToolResult, wire.EventText, wire.EventText, wire.EventDone}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("event types = %v, want %v", types, want)
	}
	if text != "you said: hello" {
		t.Errorf("assembled text = %q", text)
	}
	final := events[len(events)-1]
	if final.StopReason != wire.StopEndTurn || final.Usage == nil || final.Usage.TotalTokens != 3 {
		t.Errorf("bad done event: %+v", final)
	}
	if events[0].SessionID != run.Ack.SessionID {
		t.Errorf("start event sessionId %q != ack sessionId %q", events[0].SessionID, run.Ack.SessionID)
	}
}

func TestAgentChatValidation(t *testing.T) {
	startTestAgent(t)
	c := agentclient.NewFromConn(testConn(t))

	_, err := c.Chat(context.Background(), "testAgent", wire.ChatRequest{
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{}},
	})
	if err == nil {
		t.Fatal("empty message should fail client-side")
	}

	// Force a server-side rejection: bypass the client check.
	nc := testConn(t)
	body, _ := json.Marshal(map[string]any{"streamSubject": "x.y"})
	msg, err := nc.Request(wire.AgentSubject("testAgent", "chat"), body, 5*time.Second)
	if err != nil {
		t.Fatalf("raw request: %v", err)
	}
	var svcErr struct {
		Status        int `json:"status"`
		ApiStatusCode int `json:"apiStatusCode"`
	}
	if err := json.Unmarshal(msg.Data, &svcErr); err != nil {
		t.Fatalf("decoding error reply: %v (%s)", err, msg.Data)
	}
	if svcErr.Status != 400 || svcErr.ApiStatusCode != wire.CodeMissingField {
		t.Errorf("want 400/%d, got %d/%d", wire.CodeMissingField, svcErr.Status, svcErr.ApiStatusCode)
	}
}

func TestAgentCancel(t *testing.T) {
	startTestAgent(t)
	c := agentclient.NewFromConn(testConn(t))

	run, err := c.Chat(context.Background(), "testAgent", wire.ChatRequest{
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "block"}}},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	// First event (start) proves the run is live before we cancel.
	first := <-run.Events
	if first.Type != wire.EventStart {
		t.Fatalf("first event = %s, want start", first.Type)
	}

	ok, err := run.Cancel(context.Background())
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !ok {
		t.Fatal("cancel not acknowledged by owner")
	}

	var final wire.AgentEvent
	for ev := range run.Events {
		final = ev
	}
	if final.Type != wire.EventDone || final.StopReason != wire.StopCancelled {
		t.Errorf("run should end done/cancelled, got %s/%s (%s)", final.Type, final.StopReason, final.Error)
	}

	// Cancelling an unknown run returns false without error.
	ok, err = c.Cancel(context.Background(), "testAgent", "run_does_not_exist")
	if err != nil || ok {
		t.Errorf("unknown run cancel = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestAgentSessions(t *testing.T) {
	_, store := startTestAgent(t)
	c := agentclient.NewFromConn(testConn(t))
	ctx := context.Background()

	store.put("alice", wire.SessionGetResponse{
		SessionID: "s1",
		Title:     "First chat",
		Messages: []wire.StoredMessage{
			{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}},
			{Role: "assistant", Content: []wire.ContentBlock{{Text: "hello"}}},
		},
	})

	list, err := c.SessionsList(ctx, "testAgent", "alice")
	if err != nil {
		t.Fatalf("SessionsList: %v", err)
	}
	if len(list.Sessions) != 1 || list.Sessions[0].SessionID != "s1" {
		t.Fatalf("bad list: %+v", list)
	}

	got, err := c.SessionGet(ctx, "testAgent", "alice", "s1")
	if err != nil {
		t.Fatalf("SessionGet: %v", err)
	}
	if len(got.Messages) != 2 || got.Messages[1].Content[0].Text != "hello" {
		t.Fatalf("bad transcript: %+v", got)
	}

	if _, err := c.SessionRename(ctx, "testAgent", "alice", "s1", "Renamed"); err != nil {
		t.Fatalf("SessionRename: %v", err)
	}
	if _, err := c.SessionSetFavorite(ctx, "testAgent", "alice", "s1", true); err != nil {
		t.Fatalf("SessionSetFavorite: %v", err)
	}
	got, _ = c.SessionGet(ctx, "testAgent", "alice", "s1")
	if got.Title != "Renamed" || !got.Favorite {
		t.Errorf("rename/favorite not applied: %+v", got)
	}

	// Unknown session maps to 404/4041.
	_, err = c.SessionGet(ctx, "testAgent", "alice", "missing")
	var svcErr *agentclient.ServiceError
	if err == nil {
		t.Fatal("missing session should error")
	} else if ok := errorAs(err, &svcErr); !ok || svcErr.Status != 404 || svcErr.ApiStatusCode != wire.CodeUnknownSession {
		t.Errorf("want 404/%d, got %v", wire.CodeUnknownSession, err)
	}

	if _, err := c.SessionDelete(ctx, "testAgent", "alice", "s1"); err != nil {
		t.Fatalf("SessionDelete: %v", err)
	}
	list, _ = c.SessionsList(ctx, "testAgent", "alice")
	if len(list.Sessions) != 0 {
		t.Errorf("session not deleted: %+v", list)
	}
}

func errorAs[T error](err error, target *T) bool {
	for err != nil {
		if e, ok := err.(T); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// ─── tool tests ───────────────────────────────────────────────────────────

func TestToolHostAndClient(t *testing.T) {
	h := tool.NewHostWithNATS(testURL, "", "")
	if err := h.Register(echoTool{}, &tool.Info{Version: "1.0.0", Tags: []string{"test"}}); err != nil {
		t.Fatalf("Register echo: %v", err)
	}
	if err := h.Register(failTool{}, nil); err != nil {
		t.Fatalf("Register alwaysfail: %v", err)
	}
	if err := h.Start(); err != nil {
		t.Fatalf("host.Start: %v", err)
	}
	t.Cleanup(h.Shutdown)

	tc := toolclient.NewFromConn(testConn(t))
	ctx := context.Background()

	cards, err := tc.Discover(ctx, wire.DiscoverFilter{}, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	names := map[string]bool{}
	for _, c := range cards {
		names[c.Name] = true
		if c.Kind != wire.KindTool {
			t.Errorf("tool card %s has kind %q", c.Name, c.Kind)
		}
	}
	if !names["echo"] || !names["alwaysfail"] {
		t.Fatalf("hosted tools not both discovered: %v", names)
	}

	card, err := tc.Card(ctx, "echo")
	if err != nil {
		t.Fatalf("Card: %v", err)
	}
	if card.InputSchema == nil || card.TimeoutSeconds != 30 {
		t.Errorf("bad echo card: %+v", card)
	}

	resp, err := tc.Run(ctx, "echo", wire.ToolRunRequest{
		ToolUseID: "tu_9",
		Input:     map[string]any{"say": "ping"},
		UserID:    "tester",
		Agent:     "testAgent",
	}, 0)
	if err != nil {
		t.Fatalf("Run echo: %v", err)
	}
	if resp.Status != wire.ToolStatusSuccess || resp.ToolUseID != "tu_9" || len(resp.Content) != 1 {
		t.Fatalf("bad run response: %+v", resp)
	}
	var echoed map[string]any
	if err := json.Unmarshal([]byte(resp.Content[0].Text), &echoed); err != nil || echoed["say"] != "ping" {
		t.Errorf("bad echoed content: %q", resp.Content[0].Text)
	}

	// Execution failure is model-visible data, not a protocol error.
	resp, err = tc.Run(ctx, "alwaysfail", wire.ToolRunRequest{Input: map[string]any{}}, 0)
	if err != nil {
		t.Fatalf("Run alwaysfail should not be a protocol error: %v", err)
	}
	if resp.Status != wire.ToolStatusError || resp.Error != "intentional failure" {
		t.Errorf("bad failure response: %+v", resp)
	}
}

func TestToolRegistryPickup(t *testing.T) {
	h := tool.NewHostWithNATS(testURL, "", "")
	if err := h.Register(echoTool{}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := h.Start(); err != nil {
		t.Fatalf("host.Start: %v", err)
	}
	t.Cleanup(h.Shutdown)

	tc := toolclient.NewFromConn(testConn(t))
	changed := make(chan struct{}, 8)
	reg := toolclient.NewRegistry(tc, toolclient.RegistryOptions{
		Deny:          []string{"alwaysfail"},
		SweepInterval: time.Hour, // announcements only after the initial sweep
		OnChange:      func() { changed <- struct{}{} },
	})
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry.Start: %v", err)
	}
	t.Cleanup(reg.Stop)

	if _, ok := reg.Get("echo"); !ok {
		t.Fatal("initial sweep missed echo")
	}

	// A tool deployed later announces itself and appears without a sweep;
	// the denied tool is filtered even though it announces too.
	h2 := tool.NewHostWithNATS(testURL, "", "")
	if err := h2.Register(failTool{}, nil); err != nil {
		t.Fatalf("Register alwaysfail: %v", err)
	}
	type lateTool struct{ echoTool }
	_ = lateTool{}
	if err := h2.Register(renamedEcho{}, nil); err != nil {
		t.Fatalf("Register late: %v", err)
	}
	if err := h2.Start(); err != nil {
		t.Fatalf("h2.Start: %v", err)
	}
	t.Cleanup(h2.Shutdown)

	deadline := time.After(3 * time.Second)
	for {
		if _, ok := reg.Get("echo_late"); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("announced tool echo_late never reached the registry")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, ok := reg.Get("alwaysfail"); ok {
		t.Error("denied tool leaked into the registry")
	}

	// Running through the registry uses the card's advisory timeout.
	resp, err := reg.Run(context.Background(), "echo_late", wire.ToolRunRequest{Input: map[string]any{"say": "hi"}})
	if err != nil {
		t.Fatalf("registry.Run: %v", err)
	}
	if resp.Status != wire.ToolStatusSuccess {
		t.Errorf("bad registry run: %+v", resp)
	}
	if _, err := reg.Run(context.Background(), "unknown", wire.ToolRunRequest{}); err == nil {
		t.Error("running an unregistered tool should error")
	}
}

type renamedEcho struct{ echoTool }

func (renamedEcho) Name() string { return "echo_late" }

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
