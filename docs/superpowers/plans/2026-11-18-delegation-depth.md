# Delegation Depth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Only orchestrator agents (RSAssistant) may delegate turns; the library carries delegation depth automatically, every agent host refuses depth > 1, and leaf agents and tools cannot start agent calls.

**Architecture:** A per-turn marker `{depth, mayDelegate}` rides on the handler ctx (same path as `agentclient.WithUserContext`). `agentclient.Chat/Invoke` refuse when `mayDelegate` is false, else stamp `delegation.depth = depth+1`. `parseTurn` reads depth (new field or legacy `metadata.hops`) and refuses over the limit with 400/4003. Tool runs carry depth; the tool host's marker always has `mayDelegate=false`.

**Tech Stack:** Go, nats.go, transactrx/nats-service, embedded nats-server for tests.

**Spec:** `docs/superpowers/specs/2026-11-18-delegation-depth-design.md`

**Repos/worktrees:**
- nats-agent: `/Users/yceleiro/Documents/TransactRx/GitHub/nats-agent/.claude/worktrees/delegation-depth` (branch `feature/delegation-depth`) — Tasks 1–5.
- RSAssistant: `/Users/yceleiro/Documents/TransactRx/GitHub/RSAssistant/.claude/worktrees/rsassistant-agent-chaining-0358dd` — Task 6.
- LocalDev: `/Users/yceleiro/Documents/TransactRx/LocalDev` — Task 7.

## Global Constraints

- Additive and backward compatible only; no existing exported signature changes.
- `wire.ProtocolVersion = "1.6"`.
- Wire field JSON name `delegation`, shape `{"depth": <int>}`, omitempty, on `ChatRequest` and `ToolRunRequest`.
- Absent/null delegation = depth 0. Negative = 0.
- Legacy `metadata.hops` (JSON number) is read; effective depth = max(field, hops).
- Refusal: status **400** (`nats_service.NewValidationError`), `wire.CodeDelegationDepth = 4003`, message `delegation depth exceeded (depth=N, max M)`. Never 403.
- `Config.Orchestrator bool` default false; env fallback `AGENT_ORCHESTRATOR=true` (only when field false). `Config.MaxDelegationDepth int`, 0 → 1.
- Leaf/tool error: `agentclient.ErrDelegationNotAllowed`, returned before any NATS publish.
- `Card`, `Ping`, `Cancel`, renew, `Sessions*` never checked or stamped.
- Commits: only with the user's explicit OK (user rule). Run `go vet ./...` and `go test -race ./...` before any push.

## Review Focus

1. Handler that derives ctx (`WithCancel`, `WithoutCancel`, `WithIDTSource`) from the turn ctx → marker must survive (`context.WithoutCancel` keeps values). Test in Task 2.
2. Orchestrator call where caller already set `Delegation{Depth: 5}` → never lowered (sent as 5, receiver refuses). Test in Task 2.
3. `metadata.hops` as a non-number (string `"1"`, bool) → ignored, depth from field only, turn not refused. Test in Task 1.
4. Invoke path (sync) gets the same refusal and marker as chat. Test in Task 3.
5. Tool called by an orchestrator (depth 0) still cannot call an agent (`mayDelegate=false` regardless). Test in Task 4.

---

### Task 1: Wire fields + `pkg/delegation`

**Files:**
- Modify: `pkg/wire/wire.go` (ProtocolVersion; `ChatRequest`; `ToolRunRequest`; codes block ~441)
- Create: `pkg/delegation/delegation.go`
- Test: `pkg/delegation/delegation_test.go`

**Interfaces:**
- Produces:
  - `type wire.Delegation struct { Depth int \`json:"depth"\` }`
  - `wire.ChatRequest.Delegation *wire.Delegation \`json:"delegation,omitempty"\``
  - `wire.ToolRunRequest.Delegation *wire.Delegation \`json:"delegation,omitempty"\``
  - `const wire.CodeDelegationDepth = 4003`
  - `type delegation.Marker struct { Depth int; MayDelegate bool }`
  - `func delegation.With(ctx context.Context, m Marker) context.Context`
  - `func delegation.From(ctx context.Context) (Marker, bool)`
  - `func delegation.Depth(d *wire.Delegation, metadata map[string]any) int`

- [ ] **Step 1: Write failing tests** in `pkg/delegation/delegation_test.go`:
  - `TestDepth`: table — `(nil,nil)→0`; `({2},nil)→2`; `(nil,{"hops":float64(1)})→1`; `({1},{"hops":float64(2)})→2`; `({-3},nil)→0`; `(nil,{"hops":"1"})→0`; `(nil,{"hops":true})→0`; `(nil,{"hops":float64(-1)})→0`; `(nil,{"hops":json.Number("2")})→2`.
  - `TestMarkerRoundTrip`: `From(context.Background())` → `ok==false`; `From(With(ctx, Marker{1,true}))` → `{1,true},true`; value survives `context.WithoutCancel` and `context.WithCancel`.
  - `TestWireJSON`: `json.Marshal(wire.ChatRequest{...no Delegation})` contains no `"delegation"`; with `&Delegation{Depth:1}` contains `"delegation":{"depth":1}`; unmarshal of a body without the field gives nil.
- [ ] **Step 2:** `go test ./pkg/delegation/ -run . -v` → FAIL (package/types missing).
- [ ] **Step 3: Implement** types above; `ProtocolVersion = "1.6"`. `Depth` accepts `float64`, `int`, `int64`, `json.Number` for hops (truncate), else ignore; result `max(0, field, hops)`.
- [ ] **Step 4:** `go test ./pkg/delegation/ ./pkg/wire/ -v` → PASS; `go build ./...` OK.
- [ ] **Step 5: Commit (with user OK)** `feat(wire): delegation depth field and ctx marker (v1.6)`

### Task 2: agentclient outbound rule

**Files:**
- Modify: `pkg/agentclient/client.go` (`ChatWith` ~459, `Invoke` ~690; new error var near ~39)
- Test: `pkg/agentclient/client_test.go` (package `agentclient`), `e2e_test.go`

**Interfaces:**
- Consumes: `delegation.From`, `delegation.Marker`, `wire.Delegation`.
- Produces: `var agentclient.ErrDelegationNotAllowed = errors.New("agentclient: this agent may not call other agents (not an orchestrator)")`; unexported `func applyDelegation(ctx context.Context, req *wire.ChatRequest) error`.

- [ ] **Step 1: Write failing tests** (`client_test.go`, unit on `applyDelegation`):
  - `TestApplyDelegationNoMarker`: req unchanged (`req.Delegation == nil`), nil error.
  - `TestApplyDelegationLeaf`: marker `{0,false}` → `errors.Is(err, ErrDelegationNotAllowed)`, req unchanged.
  - `TestApplyDelegationOrchestrator`: marker `{0,true}` → `req.Delegation.Depth == 1`; marker `{1,true}` → 2.
  - `TestApplyDelegationNeverLowers`: marker `{0,true}`, req pre-set `{Depth:5}` → stays 5.
  - `TestApplyDelegationDerivedCtx`: marker set, then `context.WithoutCancel` + `WithIDTSource` → still applied.
  - e2e `TestLeafChatRefusedBeforePublish`: start agent "probe" counting chats; client `Chat(delegation.With(ctx,{0,false}), "probe", req)` → `ErrDelegationNotAllowed`, probe count 0 after 200 ms; same for `Invoke`.
- [ ] **Step 2:** `go test ./pkg/agentclient/ -run ApplyDelegation -v` and `go test -run TestLeafChatRefused .` → FAIL.
- [ ] **Step 3: Implement** `applyDelegation`; call it first in `ChatWith` (before `ChanSubscribe`) and in `Invoke` (before `requestJSON`), returning its error. Copy the `Delegation` struct (don't mutate the caller's pointer).
- [ ] **Step 4:** both commands → PASS; `go test ./pkg/agentclient/ -race` PASS.
- [ ] **Step 5: Commit (with user OK)** `feat(agentclient): only orchestrators delegate; stamp depth+1`

### Task 3: Agent runtime inbound limit + marker

**Files:**
- Modify: `pkg/agent/agent.go` (`Config` ~76; `Turn` ~39; `New` ~222 defaults; `runContext` ~195; `parseTurn` ~629; `handleInvoke` ~770)
- Test: `pkg/agent/delegation_test.go` (new, package `agent`), `e2e_test.go`

**Interfaces:**
- Consumes: Task 1, Task 2.
- Produces: `Config.Orchestrator bool`, `Config.MaxDelegationDepth int`, `Turn.DelegationDepth int`; handler ctx carries `delegation.Marker{Depth: turn.DelegationDepth, MayDelegate: cfg.Orchestrator}` for chat and invoke.

- [ ] **Step 1: Write failing tests:**
  - unit `TestConfigDefaults`: `New` with MaxDelegationDepth 0 → effective 1; `t.Setenv("AGENT_ORCHESTRATOR","true")` with field false → orchestrator true; `"false"`/unset → false. (Use whatever seam `New` allows; if `New` needs NATS, test via e2e agent with `testURL`.)
  - e2e `TestDepthRefusedAtReceiver`: raw `agentclient.Chat(context.Background(), "leafA", req{Delegation:{2}})` → `*ServiceError` with `Status==400`, `ApiStatusCode==4003`, message contains `depth=2, max 1`; handler never ran.
  - e2e `TestLegacyHopsRefused`: `Metadata{"hops":2}` → same 400/4003.
  - e2e `TestDepthOneAccepted`: `{Depth:1}` → turn runs, handler sees `turn.DelegationDepth==1`.
  - e2e `TestNoFieldAccepted`: request without field → runs, depth 0.
  - e2e `TestOrchestratorToLeaf`: agent "orch" (`Orchestrator:true`) whose handler calls `client.Chat(ctx,"leafB",…)`; leafB records depth → 1; orch's turn ends `done`.
  - e2e `TestLeafToLeafRefused`: agent "leafC" (default) whose handler calls `client.Chat(ctx,"leafB",…)` → handler gets `ErrDelegationNotAllowed`; leafB count unchanged.
  - e2e `TestInvokeDepth`: invoke with `{Depth:2}` → 400/4003; leaf invoke handler calling `Chat` → `ErrDelegationNotAllowed`.
- [ ] **Step 2:** `go test -run 'Depth|Hops|Orchestrator|LeafToLeaf|InvokeDepth|NoField' . ./pkg/agent/` → FAIL.
- [ ] **Step 3: Implement.** In `New`: default MaxDelegationDepth, env fallback (`strings.EqualFold(strings.TrimSpace(os.Getenv("AGENT_ORCHESTRATOR")),"true")`). In `parseTurn` (after body checks, before `Authorize` is fine either way; prefer before so refused calls do no identity work): compute depth, refuse with `nats_service.NewValidationError(fmt.Sprintf("delegation depth exceeded (depth=%d, max %d)", d, max), wire.CodeDelegationDepth, nil)`, set `Turn.DelegationDepth`. `runContext` takes the agent's orchestrator flag (change it to a method or add a param — unexported, free to change) and wraps parent with `delegation.With`. `handleInvoke` same.
- [ ] **Step 4:** run Step 2 command → PASS; then full `go test -race ./...` → PASS (no existing test regresses).
- [ ] **Step 5: Commit (with user OK)** `feat(agent): refuse turns over max delegation depth; orchestrator flag`

### Task 4: Tool runs carry depth; tools never delegate

**Files:**
- Modify: `pkg/toolclient/client.go` (`Run` ~131), `pkg/tool/host.go` (`Call` ~36; `handleRun` ~351/384)
- Test: `e2e_test.go`

**Interfaces:**
- Consumes: Task 1, Task 2.
- Produces: `tool.Call.Delegation int` (the caller's depth); tool ctx carries `delegation.Marker{Depth: <req depth>, MayDelegate: false}`.

- [ ] **Step 1: Write failing e2e tests:**
  - `TestToolRunCarriesDepth`: tool implementing `CallTool` records `call.Delegation`; `toolclient.Run(delegation.With(ctx,{1,true}),…)` → recorded 1; plain ctx → 0 and request JSON has no `delegation`.
  - `TestToolCannotCallAgent`: tool whose `RunCall` does `agentclient.Chat(ctx,"leafB",…)`; run from ctx with marker `{0,true}` and again `{1,false}` → tool sees `ErrDelegationNotAllowed` both times; leafB count 0.
  - `TestToolCanReadCard`: same tool ctx → `client.Card(ctx,"leafB")` succeeds.
  - `TestPlainToolRunGetsMarker`: non-`CallTool` tool's `Run(ctx,…)` sees `delegation.From(ctx)` → `{0,false},true`.
- [ ] **Step 2:** `go test -run 'ToolRunCarries|ToolCannot|ToolCanRead|PlainToolRun' .` → FAIL.
- [ ] **Step 3: Implement.** `toolclient.Run`: if marker present and `req.Delegation == nil` → set `&wire.Delegation{Depth: m.Depth}` (no +1). Host: `d := delegation.Depth(req.Delegation, nil)` (tools ignore hops); `ctx = delegation.With(ctx, Marker{Depth:d, MayDelegate:false})` right after the IDT line; pass `Delegation: d` into `Call`.
- [ ] **Step 4:** Step 2 command → PASS; `go test -race ./...` → PASS.
- [ ] **Step 5: Commit (with user OK)** `feat(tool): forward delegation depth; tools never call agents`

### Task 5: SPEC + README

**Files:**
- Modify: `SPEC.md` (title v1.6; §5.1 request example + field row; §8.4 field row; §9 add 4003; new §13), `README.md` (one paragraph: orchestrator flag, env, error).

- [ ] **Step 1:** Write §13 "Delegation depth (v1.6)": meaning, absent=0, legacy hops, receiver limit (default 1), 400/4003, orchestrator rule (B), tool rule (tools never call agents), entries (no marker → depth 0), compatibility table copied from the spec.
- [ ] **Step 2:** `go vet ./... && go test -race ./...` → PASS (docs-only sanity).
- [ ] **Step 3: Commit (with user OK)** `docs(spec): delegation depth v1.6`
- [ ] **Step 4 (user decision):** push + PR to `Development`; after merge, tag `v0.7.0` (follow repo's release flow; v0.6.0 came via `Development`→main merge PR).

### Task 6: RSAssistant adoption

**Files (RSAssistant worktree):**
- Modify: `go.mod`/`go.sum` (nats-agent v0.7.0; until tagged, TEMP `replace github.com/transactrx/nats-agent => /Users/yceleiro/Documents/TransactRx/GitHub/nats-agent/.claude/worktrees/delegation-depth`, removed before PR)
- Modify: `pkg/agenthost/agent.go:53` (`agent.Config{… Orchestrator: true, …}`)
- Test: `pkg/agenthost/agenthost_test.go`

**Interfaces:**
- Consumes: `agent.Config.Orchestrator`, `agentclient.ErrDelegationNotAllowed`.

- [ ] **Step 1: Write failing test** `TestAgentHostIsOrchestrator`: start the agent host as existing tests do, run a turn whose orchestrator consults a stub specialist; stub records `turn.DelegationDepth == 1` and turn ends done. (If existing tests already consult a stub, extend that one with the depth assertion.)
- [ ] **Step 2:** `go test ./pkg/agenthost/ -run Orchestrator -v` → FAIL (ErrDelegationNotAllowed before flag).
- [ ] **Step 3:** add replace + `go get`/`go mod tidy`; set `Orchestrator: true`. Keep `Metadata["hops"]` and `MaxHops` check unchanged.
- [ ] **Step 4:** `go vet ./... && go test -race ./...` → PASS; `golangci-lint run` if configured.
- [ ] **Step 5: Commit (with user OK)** `feat(agenthost): declare orchestrator for nats-agent v0.7.0`

### Task 7: Local E2E (LocalDev + Playwright)

Needs sandbox off, `aws sso login --profile default`, and Docker infra.

- [ ] **Step 1:** Point LocalDev at the code under test: RSAssistant project → this worktree (Task 6 TEMP replace in place); agentSchedulerService stays on its configured worktree (it is unchanged; any version works). Add a stack or reuse `rsassistant-scheduler` + one specialist (e.g. `snowflake-operations-agent` on Development = old library, proves compat).
- [ ] **Step 2:** `python3 eco.py start rsassistant-scheduler` (+ specialist); `python3 eco.py status` all up.
- [ ] **Step 3 (Playwright):** open `https://localhost:8447/rsassistant/`, ask a question that consults the specialist → answer arrives; RSAssistant log shows `hops=0` for the turn; specialist received the chat (old lib ignores `delegation`, reads nothing — compat).
- [ ] **Step 4:** In chat, create a schedule due in ~2 min that consults the specialist (avoid weekdays ~13:00 UTC); wait for the fire; scheduler log shows run outcome success; RSAssistant log shows the scheduled turn at depth 0 and the consult succeeding.
- [ ] **Step 5:** Negative check with a throwaway Go program (scratchpad, not committed) on local NATS: `agentclient.Chat` to `RSAssistant` with `Delegation{Depth:2}` → 400/4003; with ctx marker `{0,false}` → `ErrDelegationNotAllowed`.
- [ ] **Step 6:** Delete the test schedule; `eco.py stop` apps then infra (`--infra`); report results with log lines.
