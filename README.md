# nats-agent

Go reference implementation of the **NATS Agent Protocol** ([SPEC.md](SPEC.md)):
AI agents and network agentic tools that live on NATS, discoverable the same
way transactrx microservices are, speaking a common streaming chat protocol.

Built on [`transactrx/nats-service`](https://github.com/transactrx/nats-service),
so every agent and tool endpoint is also visible to `nats-discover` and uses
the same error envelope and env conventions (`NATS_URL`, `NATS_JWT`,
`NATS_KEY`).

| Package | Role |
|---|---|
| `pkg/wire` | Wire types of the protocol (cards, events, requests) — language-neutral shapes |
| `pkg/agent` | Agent server runtime: card/ping/chat/cancel plumbing, heartbeats, sessions, extensions |
| `pkg/agentclient` | Consumer side: discover agents, run chat turns, manage sessions |
| `pkg/tool` | Network-tool host: serve N tools from one process, each with its own subject space |
| `pkg/toolclient` | Consumer side: discover/run tools; `Registry` keeps an agent's tool view live |

## Serving an agent

```go
a, err := agent.New(agent.Config{
    Name:        "copayAssistant",
    Description: "Data analyst for the copay programs platform.",
    Version:     "1.0.0",
    Skills:      []wire.Skill{{Name: "sql", Description: "Answers questions from the copay database."}},
})
if err != nil { log.Panic(err) } // fail fast

a.UseSessionStore(myDynamoStore) // optional: advertises capability "sessions"

a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
    stream.Status("thinking…")
    // ... call trx.inferenceGateway, run tools ...
    stream.ToolUse("tu_1", "query_copay_db", input)
    stream.ToolResult("tu_1", "query_copay_db", result, "")
    stream.Text("There were 42 failed claims.")
    stream.Done(wire.StopEndTurn, usage)
    return nil // runtime emits done/error/cancelled if the handler didn't
})

if err := a.Start(); err != nil { log.Panic(err) }
```

The runtime provides: the `trx.agent.copayAssistant.>` subject space with
queue group `agent.copayAssistant`, the agent card, discovery replies on
`trx.agent.discover`, run tracking + broadcast `cancel`, stream heartbeats
(≤15s), and terminal-event guarantees (panics become `error` events).

## Talking to an agent

```go
c, _ := agentclient.New()
cards, _ := c.Discover(ctx, wire.DiscoverFilter{}, 0) // every agent on the mesh

run, _ := c.Chat(ctx, "copayAssistant", wire.ChatRequest{
    UserID:  "manuel",
    Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "How many claims failed yesterday?"}}},
})
for ev := range run.Events { // closes after the terminal event
    switch ev.Type {
    case wire.EventText:  fmt.Print(ev.TextDelta)
    case wire.EventDone:  fmt.Println("\n[", ev.StopReason, "]")
    case wire.EventError: fmt.Println("failed:", ev.Error)
    }
}
```

The client generates the stream subject, filters transport pings, watches for
dead runs (45s silence), and translates context cancellation into a
best-effort server-side cancel.

## Hosting network tools

One deployable process, N tools, each a first-class citizen on the wire:

```go
h, _ := tool.NewHost()
h.Register(quickchart.New(...), &tool.Info{Version: "1.0.0", Tags: []string{"visualization"}})
h.Register(websearch.New(...), &tool.Info{Version: "1.0.0", TimeoutSeconds: 25})
if err := h.Start(); err != nil { log.Panic(err) }
```

`Tool` is the same interface the copay chat app uses for local tools
(`Name/Description/InputSchema/Run`), so lifting a local tool onto the
network is a move, not a rewrite.

## Using network tools from an agent

```go
tc, _ := toolclient.New()
reg := toolclient.NewRegistry(tc, toolclient.RegistryOptions{})
reg.Start(ctx) // initial sweep + live announce pickup + 5m re-sweeps

for _, card := range reg.Tools() {
    // card.Name/Description/InputSchema go verbatim into the gateway's tools array
}
resp, _ := reg.Run(ctx, "create_chart", wire.ToolRunRequest{Input: input, UserID: userID, Agent: "copayAssistant"})
```

## Tests

`go test ./...` runs the e2e suite against an embedded NATS server: discovery
filters, chat streaming and event ordering, cancellation, sessions, tool
hosting, protocol-vs-model-visible errors, and live registry pickup.
