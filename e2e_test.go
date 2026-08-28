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
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

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

// startSecuredAgent returns the session store and a race-safe getter for the
// userIDs (and identity) the handlers observed, in call order.
func startSecuredAgent(t *testing.T, name, identitySubject string, observe bool) (*memSessionStore, func() []string) {
	t.Helper()
	store := newMemSessionStore()
	var seenUsers []string
	var mu sync.Mutex
	record := func(turn *agent.Turn) {
		mu.Lock()
		seenUsers = append(seenUsers, turn.UserID+"|"+fmt.Sprint(turn.Identity.Verified)+"|"+turn.Identity.IDT)
		mu.Unlock()
	}
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
		record(turn)
		stream.Text("ok")
		stream.Done(wire.StopEndTurn, nil)
		return nil
	})
	a.OnInvoke(func(ctx context.Context, turn *agent.Turn) (*wire.InvokeResponse, error) {
		record(turn)
		return &wire.InvokeResponse{Message: wire.Message{Role: "assistant", Content: []wire.ContentBlock{{Text: "ok"}}}}, nil
	})
	if err := a.Start(); err != nil {
		t.Fatalf("agent.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	getSeen := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(seenUsers))
		copy(out, seenUsers)
		return out
	}
	return store, getSeen
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
	if got := seen(); len(got) != 1 || got[0] != "alice|true|IDT-good.c" {
		t.Fatalf("handler saw %v, want verified alice", got)
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
	if got := atomic.LoadInt32(calls); got < 3 {
		t.Fatalf("identity should have been consulted, calls=%d", got)
	}
}

func TestIDTInvokeDenied(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{})
	_, seen := startSecuredAgent(t, "securedInvoke", subj, false)
	c := agentclient.NewFromConn(testConn(t))
	msg := wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}}

	_, err := c.Invoke(agentclient.WithIDT(context.Background(), "IDT-bad.c"), "securedInvoke", wire.ChatRequest{Message: msg})
	assertForbidden(t, err, "DENIED_FN")
	if got := seen(); len(got) != 0 {
		t.Fatalf("denied invoke must never reach the handler: %v", got)
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

	// The override applies on a non-list handler too: body says bob, but
	// s-alice only exists under alice, and alice's token verifies.
	getResp, err := c.SessionGet(agentclient.WithIDT(context.Background(), "IDT-good.c"), "securedSessions", "bob", "s-alice")
	if err != nil {
		t.Fatalf("sessionGet: %v", err)
	}
	if getResp.SessionID != "s-alice" || getResp.Title != "mine" {
		t.Fatalf("verified identity must override body userId on Get too: %+v", getResp)
	}
}

func TestIDTSessionDeleteDeniedPreservesSession(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})
	store, _ := startSecuredAgent(t, "securedSessionDelete", subj, false)
	store.put("alice", wire.SessionGetResponse{SessionID: "s-alice", Title: "mine"})
	c := agentclient.NewFromConn(testConn(t))

	_, err := c.SessionDelete(agentclient.WithIDT(context.Background(), "IDT-bad.c"), "securedSessionDelete", "alice", "s-alice")
	assertForbidden(t, err, "DENIED_FN")

	got, err := store.Get(context.Background(), "alice", "s-alice")
	if err != nil {
		t.Fatalf("denied delete must not touch the store: %v", err)
	}
	if got.SessionID != "s-alice" || got.Title != "mine" {
		t.Fatalf("session was altered despite denied delete: %+v", got)
	}
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
	if got := seen(); len(got) != 1 || got[0] != "u|false|" {
		t.Fatalf("observe-only must keep caller userId, unverified: %v", got)
	}
}

func TestIDTMisconfigRejectedAtNew(t *testing.T) {
	_, err := agent.New(agent.Config{Name: "bad", Description: "d", NATSURL: testURL,
		IDTValidation: &agent.IDTValidation{Enabled: true}})
	if err == nil {
		t.Fatal("IDT_VALIDATION without Access must fail agent.New")
	}
}

// TestIDTChatDenyDoesNotPublishToStream proves a denied chat request never
// causes any event to reach the caller-provided streamSubject: the deny
// happens in parseTurn, before the ack (and before the stream even exists).
func TestIDTChatDenyDoesNotPublishToStream(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{})
	startSecuredAgent(t, "securedStreamDeny", subj, false)

	nc := testConn(t)
	streamSubj := "test.stream." + t.Name()
	streamMsgs := make(chan *nats.Msg, 16)
	sub, err := nc.ChanSubscribe(streamSubj, streamMsgs)
	if err != nil {
		t.Fatalf("subscribing to stream subject: %v", err)
	}
	defer sub.Unsubscribe()

	c := agentclient.NewFromConn(testConn(t))
	msg := wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}}
	_, err = c.Chat(agentclient.WithIDT(context.Background(), "IDT-bad.c"), "securedStreamDeny",
		wire.ChatRequest{StreamSubject: streamSubj, Message: msg})
	assertForbidden(t, err, "DENIED_FN")

	select {
	case m := <-streamMsgs:
		t.Fatalf("denied chat must not publish to streamSubject, got: %s", string(m.Data))
	case <-time.After(300 * time.Millisecond):
	}
}

// TestIDTCardAndPingNeverSendHeader proves discover/card/ping stay
// unauthenticated even when the caller's ctx carries an IDT: the client must
// never attach X-TRX-IDT to those requests.
func TestIDTCardAndPingNeverSendHeader(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})
	startSecuredAgent(t, "cardPingNoHeader", subj, false)

	nc := testConn(t)
	cardMsgs := make(chan *nats.Msg, 4)
	pingMsgs := make(chan *nats.Msg, 4)
	subC, err := nc.ChanSubscribe(wire.AgentSubject("cardPingNoHeader", "card"), cardMsgs)
	if err != nil {
		t.Fatalf("subscribing to card subject: %v", err)
	}
	defer subC.Unsubscribe()
	subP, err := nc.ChanSubscribe(wire.AgentSubject("cardPingNoHeader", "ping"), pingMsgs)
	if err != nil {
		t.Fatalf("subscribing to ping subject: %v", err)
	}
	defer subP.Unsubscribe()

	c := agentclient.NewFromConn(testConn(t))
	ctx := agentclient.WithIDT(context.Background(), "IDT-good.c")
	if _, err := c.Card(ctx, "cardPingNoHeader"); err != nil {
		t.Fatalf("card: %v", err)
	}
	if _, err := c.Ping(ctx, "cardPingNoHeader"); err != nil {
		t.Fatalf("ping: %v", err)
	}

	select {
	case m := <-cardMsgs:
		if m.Header.Get(wire.HeaderIDT) != "" {
			t.Fatalf("card must never carry X-TRX-IDT, got %v", m.Header)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("card request was never observed")
	}
	select {
	case m := <-pingMsgs:
		if m.Header.Get(wire.HeaderIDT) != "" {
			t.Fatalf("ping must never carry X-TRX-IDT, got %v", m.Header)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ping request was never observed")
	}
}

// TestExtensionEndpointAuthorize proves an AddEndpoint extension can enforce
// IDT itself via the exported Authorize helper: a raw request with no header
// gets the standard 403 envelope, and a request with a granted header
// reaches the handler.
func TestExtensionEndpointAuthorize(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})

	a, err := agent.New(agent.Config{
		Name:        "securedExtension",
		Description: "Secured agent with an extension endpoint.",
		NATSURL:     testURL,
		Access:      &wire.AgentAccess{AppID: "appTest", FunctionID: "fnTest"},
		IDTValidation: &agent.IDTValidation{
			Enabled: true, Subject: subj, Timeout: 2 * time.Second, CacheTTL: time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
		stream.Done(wire.StopEndTurn, nil)
		return nil
	})
	if err := a.AddEndpoint(nats_service.EndpointRegistration{
		Path:        "secret",
		Description: "Extension endpoint gated by Authorize.",
		Handler: func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
			if _, aerr := a.Authorize(msg, ""); aerr != nil {
				return aerr
			}
			msg.ResponseBody = []byte(`{"ok":true}`)
			return nil
		},
	}); err != nil {
		t.Fatalf("AddEndpoint: %v", err)
	}
	if err := a.Start(); err != nil {
		t.Fatalf("agent.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })

	nc := testConn(t)
	subject := wire.AgentSubject("securedExtension", "secret")

	// No IDT header at all: denied with the standard error envelope (403 /
	// 4031), same as a built-in endpoint would produce.
	deniedMsg := nats.NewMsg(subject)
	deniedMsg.Data = []byte(`{}`)
	reply, err := nc.RequestMsg(deniedMsg, 2*time.Second)
	if err != nil {
		t.Fatalf("raw request: %v", err)
	}
	var envelope struct {
		Status        int    `json:"status"`
		ErrorMessage  string `json:"errorMessage"`
		ApiStatusCode int    `json:"apiStatusCode"`
	}
	if err := json.Unmarshal(reply.Data, &envelope); err != nil {
		t.Fatalf("decoding error envelope: %v (%s)", err, reply.Data)
	}
	if envelope.Status != 403 || envelope.ApiStatusCode != wire.CodeForbidden || envelope.ErrorMessage != "MISSING_IDT" {
		t.Fatalf("want 403/%d/MISSING_IDT, got %+v", wire.CodeForbidden, envelope)
	}

	// Allowed IDT header: the handler runs and its body comes back.
	allowedMsg := nats.NewMsg(subject)
	allowedMsg.Data = []byte(`{}`)
	allowedMsg.Header = nats.Header{}
	allowedMsg.Header.Set(wire.HeaderIDT, "IDT-good.c")
	reply, err = nc.RequestMsg(allowedMsg, 2*time.Second)
	if err != nil {
		t.Fatalf("allowed raw request: %v", err)
	}
	if string(reply.Data) != `{"ok":true}` {
		t.Fatalf("want handler body, got %s", reply.Data)
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
