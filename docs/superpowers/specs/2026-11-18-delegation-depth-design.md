# Delegation depth: only orchestrators call agents (protocol v1.6)

Date: 2026-11-18 · Status: draft, awaiting approval · Approach: A + B, tools never call agents (C later, out of scope)

## Intent

Only an orchestrator (today: RSAssistant) may delegate a turn to another agent.
Every other agent is a leaf: when RSAssistant asks Agent1, Agent1 must not ask
Agent2. Today nothing in nats-agent enforces this; RSAssistant's own check
(`hops > 2`, RSAssistant `pkg/agenthost/agent.go`) only sees a count the
caller chooses to send, and no other agent forwards it.

```
User/BFF    ─► RSAssistant ─► Agent1            allowed (depth 1)
Scheduler   ─► RSAssistant ─► Agent1            allowed (scheduler is an entry: depth 0)
Scheduler   ─► Agent1                           allowed (depth 0)
RSAssistant ─► Agent1 ─x─► Agent2               refused (B at Agent1, A at Agent2)
RSAssistant ─► Agent1 ─► tool ─x─► Agent2       refused (B at the tool host)
Scheduler   ─► Agent1 ─► tool ─x─► Agent2       refused (B at the tool host)
```

Tools never call agents (decided 2026-11-18). The scheduler is two parts: its
`trx.tool.schedule` tool (only reads agent cards) and its executor, a
background loop that is not a tool and fires each due job as one chat on the
agent that created it, from `context.Background()` (an entry, depth 0).

## Goals

- The library carries the delegation depth from an inbound turn to every
  outbound agent call automatically (no per-agent code).
- Every agent host refuses a turn deeper than its limit (A).
- A non-orchestrator agent cannot start an agent call from its turn (B): the
  call fails locally, before anything is sent, with a clear error.
- Additive and backward compatible: old callers, old agents and entry points
  (BFF, dock-gateway, agentExecution worker, agentSchedulerService) work
  unchanged.

## Non-goals

- Tamper-proof enforcement (a caller that forges depth 0 on raw NATS). That is
  approach C (NATS publish permissions) or a signed depth in the IDT; later.
- Cycle detection by agent name (depth 1 makes cycles impossible for
  conforming agents).
- Changes to agentSchedulerService (see "Scheduler").

## Anti-goals (failure even if it "works")

- Any existing flow breaks: a user turn, a scheduled run, a dock run, a group
  chat, RSAssistant consulting a specialist.
- A user-visible "access denied" for what is a depth refusal (the scheduler
  pauses jobs on 403 `DENIED_FN`; RSAssistant shows 403 as no access).

## Facts this design rests on (verified in code, 2026-11-18)

- `agent.runContext` already puts the inbound `turn.Context` on the handler
  ctx, and `agentclient.Chat/Invoke` fill `req.Context` from ctx
  (`withContextFrom`). Depth uses this exact path. (The IDT is not put on
  the handler ctx by the runtime; RSAssistant adds it itself with
  `WithIDTSource` on a ctx derived from the handler ctx, so the depth marker
  still reaches its consult calls.)
- `handleInvoke` builds its own ctx (`agent.go` ~770); it must get the same.
- The tool host builds the tool ctx from `context.Background()` + IDT only
  (`pkg/tool/host.go` ~384); `ToolRunRequest` has no depth field.
- Only RSAssistant calls agents from inside a turn (all local repos checked:
  snowflake-operations-agent, copayprogramsapi(-idt), opensearchAiChatApi,
  backendBatchProcessingAI, deploymentAIAssistantApi make none).
- Other agentclient callers are entries with no turn ctx: copayAssistance
  (HTTP BFF), dock-gateway (raw NATS), agentExecution worker, and
  agentSchedulerService (fires from `context.Background()`, targets only the
  agent that created the schedule).
- RSAssistant sends `Metadata["hops"] = turn.Hops+1` today.

## Approaches considered

- **A: depth carried in ctx + receiver limit** (picked, with B). Automatic in
  normal code; caller-asserted, so forgeable by deliberate raw calls.
- **B: orchestrator flag, sender refuses** (picked, with A). Catches the
  mistake at the source with a clear error; same forgeability.
- **C: NATS publish permissions** (later). Only hard guarantee; infra work,
  owner unknown.
- Signed depth in IDT: tamper-proof but needs Identity changes (additive-only
  repo) and token minting/renew changes. Rejected for now.
- Helper each agent calls: not automatic; repeats today's gap. Rejected.

## Design

### Wire (v1.6, additive)

- `ChatRequest.Delegation *Delegation` and `ToolRunRequest.Delegation *Delegation`,
  JSON `delegation`, omitempty.
- `type Delegation struct { Depth int \`json:"depth"\` }`.
- Depth = number of agent→agent hops already taken to reach this turn.
  Absent/null = 0 (an entry call). Old agents ignore the field (SPEC §10).
- `ProtocolVersion = "1.6"`.
- New error code `CodeDelegationDepth = 4003`, sent with status **400**, never
  403, so no caller reads it as "user lacks access" (anti-goal).

### Inbound (agent runtime)

- `parseTurn` computes `depth = max(req.Delegation.Depth, legacy Metadata["hops"])`
  (hops accepted as a JSON number; anything else ignored). Negative → 0.
- If `depth > MaxDelegationDepth` → validation error 400 /
  `CodeDelegationDepth`, message `delegation depth exceeded (depth=N, max M)`.
  The turn never starts.
- `Turn.DelegationDepth int` (new field) exposes it to handlers.
- `runContext` and `handleInvoke` put a delegation marker on the handler ctx:
  `{depth, mayDelegate: cfg.Orchestrator}`.

### Outbound (agentclient)

- `ChatWith` and `Invoke`, when ctx carries a marker:
  - `mayDelegate == false` → return `ErrDelegationNotAllowed` before any NATS
    traffic (B).
  - else set `req.Delegation.Depth = max(existing, marker.depth+1)`; never
    lower a value the caller set.
- No marker (BFF, scheduler, worker, tests) → request unchanged; the receiver
  reads it as depth 0.
- `Card`, `Ping`, `Cancel`, renew and `Sessions*` are not turns: untouched.

### Tools

- `toolclient.Run`: when ctx carries a marker and `req.Delegation` is nil, send
  `Delegation{Depth: marker.depth}` (a tool run is not a hop).
- Tool host: puts a marker `{depth: req depth, mayDelegate: false}` on the
  tool ctx, and `tool.Call.Delegation` (new field). Tools never call agents:
  a `Chat`/`Invoke` made with the tool ctx fails with
  `ErrDelegationNotAllowed` before anything is sent (B), whatever the
  caller's depth. This closes leaf (depth 0) → tool → agent. `Card` and
  `Ping` stay allowed (the scheduler's tool reads cards). Tool hosts need no
  config. The depth on tool runs is kept for logs and the later scheduler fix.
- Options considered: tools inherit the caller's permission (extra wire
  field `mayDelegate`, SPEC §8.4 change) — rejected for now; can be added
  later additively if an orchestrating tool is ever needed.

### Config

- `Config.Orchestrator bool` (default false = leaf). Env fallback
  `AGENT_ORCHESTRATOR=true` (builder's call on the name; must match the
  existing env conventions in `Config`).
- `Config.MaxDelegationDepth int`: 0 → default 1. Values > 1 allowed for
  future multi-level orchestrators.
- Card: no change required. (Builder may add `metadata`-free capability
  `orchestrator` later; not needed now.)

### Package layout

- Marker helpers in a small new package (e.g. `pkg/delegation`) used by
  `agent`, `agentclient`, `tool`, `toolclient`, to avoid import cycles
  (`toolclient` does not import `agentclient` today). Exported helpers:
  `WithDepth(ctx, depth, mayDelegate)`, `FromContext(ctx)`. Builder's call on
  exact names.

### SPEC.md

- New §13 "Delegation depth (v1.6)": field, meaning, receiver limit, error
  code, orchestrator rule, compatibility table. §5.1 and §8.4 request
  examples gain the optional field; §9 lists 4003.

## Compatibility

| Caller → Receiver | Result |
|---|---|
| old/entry caller → new agent | no field → depth 0 → accepted |
| old RSAssistant (`hops:1`) → new agent | depth 1 → accepted |
| new orchestrator → old agent | field ignored → as today |
| new leaf agent with code that calls an agent | **fails** `ErrDelegationNotAllowed` |
| scheduler executor (any version) → any agent | depth 0 → accepted |
| tool (new host) whose run code calls an agent | **fails** `ErrDelegationNotAllowed` |

The only behavior changes are rows 4 and 6. No local agent or tool does
either today (tool hosts checked: agentExecution sandbox, agentSchedulerService). No local agent does this today; any agent
that upgrades must set `Orchestrator: true` if it delegates. RSAssistant must
set it in the same change that bumps the library, or its consultations fail.

## Rollout

1. nats-agent: implement + tests; tag v0.7.0 (MINOR, additive).
2. RSAssistant: bump to v0.7.0 and set `Orchestrator: true` in one PR; keep
   its `Metadata["hops"]` and `MaxHops` check (harmless, still valid).
3. Specialists and scheduler: no change needed; they get the leaf rule when
   they next upgrade.

## Testing

- Unit (nats-agent): depth parse (field, legacy hops, both, garbage,
  negative); limit refusal code/status; marker on chat and invoke ctx;
  Chat/Invoke from leaf ctx fail with no NATS publish; orchestrator ctx sets
  depth+1 and never lowers; no-marker ctx leaves request unchanged; toolclient
  forwards depth; tool host marker; tool → agent refused at the tool host
  for depth-0 and depth-1 callers, with no NATS publish; `Card` from a tool
  ctx still works.
- E2E in-repo (embedded NATS, like `e2e_test.go`): orchestrator → leaf ok;
  leaf → leaf refused at sender; forged depth 2 refused at receiver; old-style
  request (no field) accepted.
- Local stack after build (LocalDev `rsassistant-scheduler`, browser via
  Playwright): RSAssistant consults a specialist (depth 1 logged); scheduled
  RSAssistant run consults a specialist; a leaf → leaf call refused; an
  agent on the old library still works.

## Known gap: indirect path through the scheduler

`trx.tool.schedule` targets `call.Agent`, a name the caller sets. A leaf could
schedule a run naming another agent; the scheduler later starts that agent at
depth 0. It is delayed, needs the user's access to that agent, and no agent
does it today. Possible additive fix later (scheduler repo): refuse `create`
when the tool call carries `delegation.depth ≥ 1` and `call.Agent` is not the
caller. Not in this change unless decided otherwise.

## Left to the builder

Exact package/helper names, env var name, log wording, test layout.
