# NATS Agent Protocol — Specification v1.8 (draft)

A wire protocol for AI agents that live on NATS. Any process that implements
this spec is an **agent**: it owns its own subject space, is discoverable the
same way transactrx microservices are, and speaks a common conversational
protocol so that UIs, other services, and other agents can talk to it without
knowing anything about its internals.

The spec also defines **network agentic tools** (§8): AI tools exposed as
NATS services in their own subject space, discoverable the same way, which
agents pick up dynamically and offer to the model — as opposed to local tools
compiled into an agent.

The protocol is language-neutral JSON over NATS. Reference implementation is
Go (`github.com/transactrx/nats-agent`); Rust and .NET bindings follow the
same wire contract.

**Roadmap context (non-normative).** The protocol separates UIs from agents.
Phase 1: extract this protocol and library, convert the copay chat assistant
into the first agent with its existing UI hardcoded to it. Phase 2: a
universal UI that lists discovered agents and lets an authorized user chat
with any of them. Phase 3: agents discover and consult each other. Access
control and permissions are a later stage; the wire contract already carries
`userId` and `metadata` end-to-end so enforcement can be added without
breaking changes.

**Security direction (non-normative).** Requests will eventually carry a
verified identity token, and every agent will enforce permissions in its own
deterministic layer — never in the model or its prompt. Note that today's
`userId` (§5.1) is an *unauthenticated assertion by the caller*: do not build
authority decisions on it. See
[IDENTITY-AND-AUTHORITY.md](IDENTITY-AND-AUTHORITY.md) for the design
discussion, including agents as `app_id`/`app_functions` principals,
role-and-context authority, delegation, user-owned sessions and storage, and
scheduled execution on a user's behalf.

---

## 1. Design goals

1. **Own subject space per agent.** An agent named `copayAssistant` owns
   `trx.agent.copayAssistant.>` and nothing else.
2. **Discoverable like microservices.** All agents answer a small set of
   common subjects for discovery, in addition to `nats-discover` working as it
   does today (the Go lib is built on `transactrx/nats-service`, so every
   agent endpoint is also a documented nats-service endpoint).
3. **Conversation-first.** The primary interaction is a streaming chat turn
   with tool-use visibility, extracted from the copay chat application.
4. **Portable.** Nothing in the wire contract depends on Go. Streaming uses an
   explicit caller-provided subject (not multi-reply on the request inbox),
   because reply-inbox multi-response semantics are unreliable across client
   libraries.
5. **Composable.** Message/content shapes are identical to
   `trx.inferenceGateway`, so an agent can pass conversation state straight
   through to the gateway, and an agent can call another agent as if it were a
   model.

## 2. Terminology

| Term | Meaning |
|---|---|
| **agent name** | Stable identifier, `[a-zA-Z0-9_-]+`, e.g. `copayAssistant`. Used in subjects. |
| **agent card** | Self-description document (§4.2). |
| **run** | One streaming chat turn: request → ack → event stream → terminal event. |
| **session** | Multi-turn conversation identified by `sessionId`. Persistence is the agent's concern; the wire contract only carries the id. |
| **instance** | One running process of an agent. Instances of the same agent share a queue group. |
| **local tool** | Tool implemented inside the agent process. Invisible on the wire except as `toolUse`/`toolResult` events. |
| **network tool** | Tool served over NATS in the `trx.tool.*` space (§8), usable by any agent that discovers it. |

## 3. Subject namespace

```
trx.agent.discover                     common: scatter-gather discovery (all agents)
trx.agent.<name>.card                  well-known: agent card
trx.agent.<name>.ping                  well-known: liveness
trx.agent.<name>.chat                  well-known: streaming chat turn
trx.agent.<name>.cancel                well-known: cancel a run
trx.agent.<name>.invoke                capability "sync": synchronous single-shot
trx.agent.<name>.sessionsList          capability "sessions"
trx.agent.<name>.sessionsGet           capability "sessions"
trx.agent.<name>.sessionsDelete        capability "sessions"
trx.agent.<name>.sessionsRename        capability "sessions" (optional)
trx.agent.<name>.sessionsSetFavorite   capability "sessions" (optional)
trx.agent.<name>.<anything-else>       agent-specific extensions, free-form

trx.tool.discover                      common: scatter-gather tool discovery (§8)
trx.tool.announce                      common: tools publish their card on startup (§8)
trx.tool.<name>.card                   well-known: tool card
trx.tool.<name>.ping                   well-known: liveness
trx.tool.<name>.run                    well-known: execute the tool
```

Rules:

- All request/reply subjects use JSON bodies and the `nats-service` error
  envelope (§9).
- Endpoint names are a **single subject token** in camelCase (hence
  `sessionsList`, not `sessions.list`): `nats-service` routes non-wildcard
  endpoints by their first token, so dotted endpoint names would collide.
  This also matches the existing org convention (`listModels`,
  `AssistantSessionsList`).
- Instances subscribe to their request subjects in queue group
  `agent.<name>`, so requests load-balance across instances.
- `trx.agent.<name>.cancel` is additionally subscribed **without** a queue
  group by every instance (broadcast), because the instance that owns a run is
  not necessarily the one a queue-balanced message would reach.
- Discovery (`trx.agent.discover`) is subscribed in queue group
  `agent.<name>` so exactly **one instance per agent** replies.
- Agents MAY register additional endpoints in their own space; discovery of
  those goes through the agent card (`endpoints` field) and `nats-discover`.
  Agent-specific extension endpoints are unauthenticated unless the agent
  calls the library's `Authorize` helper (Go: `(*agent.Agent).Authorize`).

## 4. Discovery

### 4.1 Scatter-gather

A client publishes to `trx.agent.discover` with a reply inbox and collects
replies for a window it chooses (recommended default 2s, first-answer use
cases can stop early).

Request (all fields optional; empty object matches every agent):

```json
{ "name": "copayAssistant", "skill": "sql", "tags": ["copay"] }
```

An agent replies with its **agent card** if it matches the filter, and stays
silent otherwise. Matching: `name` exact, `skill` matches any skill name,
`tags` must all be present in the card's tags.

### 4.2 Agent card

Returned by discovery and by `trx.agent.<name>.card` (request body `{}`).

```json
{
  "protocolVersion": "1.0",
  "kind": "agent",
  "name": "copayAssistant",
  "displayName": "Copay Assistant",
  "description": "Data analyst for the copay programs platform.",
  "version": "1.3.0",
  "repositoryUrl": "https://github.com/transactrx/copayprogramsapi",
  "tags": ["copay", "analytics"],
  "category": "Pharmacy",
  "capabilities": {
    "streaming": true,
    "sync": false,
    "sessions": true,
    "attachments": false,
    "userContext": true
  },
  "inputModalities": ["text"],
  "outputModalities": ["text"],
  "skills": [
    {
      "name": "sql",
      "description": "Answers questions by querying the copay Postgres database.",
      "examples": ["How many claims were processed last week?"]
    }
  ],
  "endpoints": ["chat", "cancel", "card", "ping", "sessionsList", "sessionsGet", "sessionsDelete"],
  "access": { "appId": "copayassistanceAppId", "functionId": "copayassistanceFnId" },
  "metadata": {}
}
```

- `protocolVersion` is the version of THIS spec the agent implements.
- `version` is the agent's own release version.
- `capabilities` gates the optional endpoint groups in §3. `streaming` MUST be
  true in v1 (chat is the core protocol); `sync` advertises `invoke`.
- `access` (optional, v1.1) registers the agent with the identity model — an
  agent is an application (`appId`) exposing one access function
  (`functionId`); a caller may use the agent iff the user behind their token
  holds that function. Absent = undeclared; UIs treat undeclared agents as
  inaccessible when enforcing.
- `capabilities.userContext` (optional, v1.3) says the agent reads the
  request `context` (§5.1) and answers in the user's local time. Callers may
  send `context` to any agent; agents without the capability ignore it.
- `category` (optional, v1.4): free-text, display-only grouping label chosen by the agent; short preferred; never used for routing or authorization.
- `metadata` is a free-form object for org-specific additions.

### 4.3 Ping

`trx.agent.<name>.ping`, request `{}`:

```json
{ "status": "ok", "name": "copayAssistant", "instanceId": "cp-7f3a", "version": "1.3.0", "uptimeSeconds": 86400 }
```

## 5. Chat protocol (core)

### 5.1 Request — `trx.agent.<name>.chat`

```json
{
  "sessionId": "6f9c2c1e-...",
  "userId": "manuel.elaraj",
  "message": {
    "role": "user",
    "content": [ { "text": "How many claims failed yesterday?" } ]
  },
  "streamSubject": "_INBOX.abc123",
  "metadata": { "isAdmin": true },
  "context": { "timeZone": "America/New_York", "userName": "Jane Doe", "locale": "en-US" },
  "delegation": { "depth": 1 }
}
```

- `message` is required; shape is identical to `inferenceGateway` `Message`
  (role + content blocks: `text`, `image{format,base64}`, `toolResult`, …).
- `sessionId` optional. Empty → the agent creates one and returns it in the
  ack and `start` event. How/-whether history is persisted is the agent's
  business.
- `streamSubject` required. Caller MUST be subscribed before sending.
- `userId` optional but recommended; agents that persist sessions key on it.
- `metadata` free-form, passed to the agent handler (auth hints, tenant, …).
- `context` optional (v1.3): **display-only** user context so the model knows
  who is asking and what "today" means for them. `timeZone` is an IANA name,
  `userName` a display name, `locale` a BCP 47 tag; all optional.
  - The UI sets it from the browser (time zone) and the portal session
    (name); relays and orchestrators forward it unchanged when consulting
    other agents.
  - Agents MUST NOT use it for authorization — identity comes from
    `X-TRX-IDT` only.
  - Agents normalize it and drop invalid parts instead of failing the turn:
    unknown time zones and `Local` are dropped; the name keeps letters,
    digits, spaces and `. ' - ,` (max 80 characters); malformed locales are
    dropped. With no valid time zone, agents use UTC and say so.
  - Standard prompt sentence (Go: `turn.ContextLine()`): "The user asking is
    Jane Doe (time zone America/New_York). Their current local time is
    Monday, October 19, 2026 9:30 PM EDT; UTC is 2026-10-20 01:30 UTC."

- `delegation` optional (v1.6): `{depth}`, how many agent-to-agent hops
  precede this turn. Absent = 0 (an entry call). An agent refuses a turn
  deeper than its limit with 400 / code 4003. See §13.
- `userInput` and `inputs` optional (v1.7): the caller can show asks
  (`{"supported": true}`) and answers parked asks. With `inputs`, an agent
  that opted in (card `capabilities.userInput`) accepts an empty `content`
  (an answer-only turn) when at least one input is valid. The ack then
  carries `caps.inputSubject`, the per-run `trx.agent.<name>.input.<token>`
  endpoint. See §14. v1.8: `userInput.features` lists the newer ask
  components the caller can draw (`person`, `link`, `notice`; §14.9).

**Authentication (v1.1):** requests to `chat`, `invoke`, `sessions*` carry the
caller's Internal Delegation Token in NATS header `X-TRX-IDT`. An agent with
validation enabled verifies it with identity
(`validateInternalToken {idt, agentId=access.appId, functionId=access.functionId}`)
before acking; on failure the reply is the error envelope with status 403 /
code 4031 and nothing is published to `streamSubject`. When verified,
`userId` is derived from the token and the body's value is ignored.
`discover`, `card`, `ping`, `cancel` are unauthenticated.

### 5.2 Ack (the request's reply)

Returned immediately, before inference starts:

```json
{
  "accepted": true,
  "runId": "run_01HZY...",
  "sessionId": "6f9c2c1e-...",
  "agent": "copayAssistant",
  "instanceId": "cp-7f3a",
  "streamSubject": "_INBOX.abc123"
}
```

Validation failures return the error envelope (§9) instead of an ack, and
nothing is published to `streamSubject`.

### 5.3 Event stream

Events are individual JSON messages published to `streamSubject`. Every event
carries `runId` and a `seq` starting at 0 with no gaps, so clients can detect
loss. Exactly one terminal event (`done` or `error`) ends the run.

```json
{ "runId": "run_01HZY...", "seq": 3, "type": "text", "textDelta": "There were " }
```

| `type` | Fields | Meaning |
|---|---|---|
| `start` | `sessionId` | Run started; always first (seq 0). |
| `text` | `textDelta` | Incremental assistant text. |
| `toolUse` | `toolUseId`, `toolName`, `toolInput` (object) | Agent is executing a tool. Emitted once per call, input complete. |
| `toolResult` | `toolUseId`, `toolName`, `toolResult` (string), `toolError` (string, optional) | Tool finished. Agents MAY truncate/redact `toolResult` for the wire. |
| `data` | `kind` (string), `payload` (object) | Agent-specific structured event (e.g. the chat app's `insight` becomes `kind:"insight"`). Clients ignore unknown kinds. |
| `status` | `statusText` | Human-readable progress ("querying database…"). Optional. |
| `ping` | — | Heartbeat. Agents MUST emit at least every 15s while the run is live and idle. |
| `done` | `stopReason`, `usage` `{inputTokens,outputTokens,totalTokens}` (optional) | Terminal success. `stopReason`: `endTurn`, `maxTokens`, `cancelled`, `maxIterations`. |
| `error` | `error`, `errorStatus` (int, §9 code) | Terminal failure. |

Notes:

- Tool granularity is deliberately coarser than the inference gateway's
  (`toolUseStart` + input deltas): agent clients care that a tool ran and what
  came back, not about streaming the tool's input JSON. The agent's internal
  gateway loop still consumes the fine-grained gateway events.
- Clients SHOULD treat >45s with no event (3 missed pings) as a dead run.

### 5.4 Cancel — `trx.agent.<name>.cancel`

Request `{ "runId": "run_01HZY..." }` → reply `{ "cancelled": true }` from the
owning instance (`{"cancelled": false}` if unknown/finished — since cancel is
broadcast, clients take the first `true` or give up after a short window; a
run that is cancelled ends with `done` + `stopReason:"cancelled"`).

## 6. Synchronous invoke (capability `sync`)

For task agents where streaming is pointless. Same request shape as chat
minus `streamSubject`; reply is the complete result:

```json
{
  "runId": "run_...",
  "sessionId": "...",
  "message": { "role": "assistant", "content": [ { "text": "42 claims failed." } ] },
  "stopReason": "endTurn",
  "usage": { "inputTokens": 900, "outputTokens": 40, "totalTokens": 940 }
}
```

## 7. Sessions (capability `sessions`)

Extracted 1:1 from the chat application, minus app-specific bits. Required if
the capability is advertised: `list`, `get`, `delete`. Optional: `rename`,
`setFavorite` (advertised via the card's `endpoints`).

| Endpoint | Request | Reply |
|---|---|---|
| `sessionsList` | `{userId}` | `{sessions:[{sessionId,title,favorite,createdAt,lastMessageAt}]}` |
| `sessionsGet` | `{userId, sessionId}` | `{sessionId,title,favorite,messages:[{role,content,timestamp}]}` |
| `sessionsDelete` | `{userId, sessionId}` | `{sessionId, deleted:true}` |
| `sessionsRename` | `{userId, sessionId, title}` | `{sessionId, title}` |
| `sessionsSetFavorite` | `{userId, sessionId, favorite}` | `{sessionId, favorite}` |

Timestamps are RFC 3339. `messages[].content` uses the same content-block
shape as §5.1.

## 8. Network agentic tools (`trx.tool.*`)

A network tool is a standalone NATS service that does one job for a model:
it publishes a self-description whose `name`/`description`/`inputSchema` can
be handed **verbatim** to the inference gateway as a `ToolSpec`, and it
executes on request. Unlike local tools, network tools are deployed, scaled,
and versioned independently of any agent, and every agent on the mesh can use
them the moment they appear.

### 8.1 Tool card

Returned by `trx.tool.<name>.card` (request `{}`), by discovery replies, and
published to `trx.tool.announce`:

```json
{
  "protocolVersion": "1.2",
  "kind": "tool",
  "name": "quickchart",
  "displayName": "Chart Renderer",
  "description": "Render a chart from a Chart.js config and return a URL to the PNG. Use for any visualization request.",
  "version": "1.0.0",
  "repositoryUrl": "https://github.com/transactrx/quickchart-tool",
  "tags": ["visualization"],
  "inputSchema": { "type": "object", "properties": { "chart": { "type": "object" } }, "required": ["chart"] },
  "timeoutSeconds": 30,
  "access": { "appId": "commonToolsAppId", "functionId": "chartRendererFnId" },
  "metadata": {}
}
```

- `description` is written **for the model** — it becomes the tool
  description the LLM sees.
- `inputSchema` is a JSON Schema object, same convention as
  `inferenceGateway` `ToolSpec.inputSchema`.
- `timeoutSeconds` is the advisory worst-case execution time; callers use it
  to size their request timeout.
- `access` (optional, v1.2) registers the tool with the identity model exactly
  like an agent card's (§4.2): a caller may run the tool iff the user behind
  their token holds `functionId`. Absent = runs are not authenticated.

### 8.2 Tool hosts

A **tool host** is one deployable process that serves any number of network
tools. Each hosted tool is indistinguishable on the wire from a tool running
alone: it has its own `trx.tool.<name>.>` subject space, its own card, its
own queue group `tool.<name>`, and replies individually to discovery. Hosting
is purely a packaging/deployment concern — it exists so the org does not end
up with one microservice per tool. The first host is
`commonAgenticTools`, which bundles `quickchart` and `serp_api` and grows
from there.

### 8.3 Discovery & announcement

- `trx.tool.discover` works exactly like agent discovery (§4.1): scatter-
  gather with optional filter `{name, tags}`, one reply (the tool card) per
  matching tool, queue group `tool.<name>` so one instance per tool answers.
- On startup, a tool additionally publishes its card to `trx.tool.announce`
  (fire-and-forget). Agents subscribe to it to pick up new tools without
  waiting for their next discovery sweep. Announcements are an optimization;
  agents MUST also sweep `trx.tool.discover` periodically (recommended every
  5 minutes) since announcements can be missed.

### 8.4 Execution — `trx.tool.<name>.run`

Request:

```json
{
  "toolUseId": "tu_01ABC...",
  "input": { "chart": { "...": "..." } },
  "userId": "manuel.elaraj",
  "sessionId": "6f9c2c1e-...",
  "agent": "copayAssistant",
  "metadata": {},
  "delegation": { "depth": 1 }
}
```

Reply:

```json
{
  "toolUseId": "tu_01ABC...",
  "status": "success",
  "content": [ { "text": "https://.../chart.png" } ],
  "latencyMs": 412
}
```

- `input` MUST conform to the card's `inputSchema`; the tool rejects
  violations with error code 4002.
- `status` is `success` or `error`; on `error`, `error` carries the message
  the model should see (execution errors are model-visible data, not protocol
  failures — the error envelope (§9) is reserved for malformed requests and
  tool crashes).
- `content` uses the gateway's `ToolResultContent` shape (`text` and/or
  `json` entries), so the caller maps the reply 1:1 onto a `toolResult`
  content block.
- `toolUseId` is the caller's correlation id, echoed back. `userId` and
  `agent` identify who is asking.
- `sessionId` (optional, v1.2) names the conversation the call belongs to, so
  a tool can keep per-conversation state. It is a locator, never a
  capability: a tool binds it to the verified caller.
- **Authentication (v1.2):** a tool whose card declares `access` may require
  the caller's Internal Delegation Token in NATS header `X-TRX-IDT`, verified
  exactly as in §5.1 against the tool's `access` before the tool runs. On
  failure the reply is the error envelope with status 403 / code 4031 (§9
  reasons) and the tool does not execute. When verified, `userId` is derived
  from the token and the body's value is ignored. `card`, `ping`, `discover`
  and `announce` are unauthenticated. Callers send the token only on `run`.
  Until downscoped delegation exists (IDENTITY-AND-AUTHORITY.md §4) the token
  forwarded is the caller's own; tools must not pass it to anything outside
  their own trust boundary.
- `delegation` (optional, v1.6): `{depth}`, the delegation depth of the turn
  that issued the call. A tool run is not a hop, so the depth is forwarded
  unchanged. Tools never call agents (§13.4).
- A tool at capacity replies 429 / code 4291 without executing.
- Execution is synchronous request/reply in v1; long-running tools with
  progress streaming are a candidate for a MINOR revision.

### 8.5 How agents use network tools

Non-normative, implemented by the library: the agent keeps a tool registry of
local tools plus discovered network tools (refreshed by announce +
periodic sweep, filtered by an allow/deny config). Each turn, the registry is
projected into the gateway's `tools` array; when the model calls a network
tool, the agent forwards the input to `trx.tool.<name>.run` and maps the
reply onto a `toolResult` block. On a name collision, the local tool wins and
the network tool is dropped with a warning.

## 9. Errors

Same envelope as every transactrx service (`nats-service`
`NatsServiceError`): `status` (HTTP-style), `apiStatusCode` (detail), and
`message`. Reserved detail codes for agents:

| Code | Meaning |
|---|---|
| 4001 | Invalid body (JSON unmarshal failed) |
| 4002 | Missing required field |
| 4003 | Delegation depth exceeded (400, never 403; §13) |
| 4031 | Forbidden — IDT missing, invalid, or function not granted (message = reason: MISSING_IDT, DENIED_FN, TOKEN_REVOKED, SESSION_EXPIRED, TOKEN_NOT_FOUND, VALIDATE_ERROR, INVALID_IDENTITY, INVALID_IDT) |
| 4041 | Unknown session |
| 4042 | Unknown run (cancel) |
| 4291 | Agent busy / at capacity |
| 5001 | Upstream failure (inference gateway, tool backend) |
| 5002 | Internal agent error |

## 10. Versioning & evolution

- `protocolVersion` in the card is `MAJOR.MINOR`. Additive changes (new event
  types, new optional fields, new capabilities) bump MINOR; clients MUST
  ignore unknown fields, event `type`s, and `data.kind`s. Breaking changes
  bump MAJOR and are announced by agents serving both versions.
- All JSON fields are camelCase. Absent and null are equivalent.

## 11. Implementation notes (non-normative)

- **Go library** (`github.com/transactrx/nats-agent`): wraps
  `transactrx/nats-service`; the developer supplies an agent card and a chat
  handler, and gets discovery, ack/stream/heartbeat/cancel plumbing, queue
  groups, and optional session endpoints (by providing a `SessionStore`
  implementation) for free. Env conventions unchanged: `NATS_URL`,
  `NATS_QUEUE_NAME`, `NATS_JWT`, `NATS_KEY`.
- **IDT enforcement** (Go library): `agent.Config.Access` sets the card's
  `access` (default: read from `APP_ID` / `APP_FUNCTION_ID`);
  `agent.Config.IDTValidation` controls inbound checks (default:
  `IDTValidationFromEnv()`, reading `IDT_VALIDATION`, `IDT_OBSERVE_ONLY`,
  `IDT_FAIL_OPEN`, `NATS_IDENTITY_BASE_PATH`,
  `NATS_IDENTITY_VALIDATE_SUBJECT`, `IDT_VALIDATE_TIMEOUT_SECONDS`,
  `IDT_VALIDATE_CACHE_SECONDS`, default cache 300s, keyed per session on
  sha256(full token)+session+app+function; denies and observe-only results
  are never cached). Callers set the outbound header with
  `agentclient.WithIDT(ctx, idt)` (equivalently `idt.WithToken`).
- **User context** (Go library, v1.3): the runtime normalizes
  `ChatRequest.Context` before the handler runs. `turn.Location()`,
  `turn.Now()` and `turn.ContextLine()` give the user's zone, local time and
  the standard prompt sentence. The handler ctx carries the context, so
  `agentclient.Chat`/`Invoke` made with it forward it to consulted agents;
  outside a handler use `agentclient.WithUserContext(ctx, uc)`. An explicit
  `req.Context` wins. `agent.Config.UserContext` advertises the capability.
- **Tool IDT enforcement** (Go library, v1.2): the validator lives in
  `pkg/idt` and is shared by agents and tool hosts. `tool.Info.Access` sets a
  tool card's `access`; `tool.NewHost()` reads the same env contract (or
  `Host.SetIDTValidation`), and a host with validation enabled refuses to
  start a tool without `Access`. A tool implementing `tool.CallTool` receives
  the verified `Identity`, `sessionId` and `toolUseId`, and its run context
  carries the token for onward calls. `tool.Info.MaxConcurrent` bounds
  simultaneous runs (429/4291). `toolclient` sends the token from the
  context on `run` only and returns when the context is done.
- **First implementor**: the copay chat application. `copayprogramsapi`'s
  assistant becomes agent `copayAssistant`; the `copayAssistance` SSE bridge
  switches to the agent client, hardcoded to `copayAssistant` until the
  universal UI exists. App-specific endpoints (system-prompt management,
  model selection) stay in the agent's extension space. The assistant's
  generic tools (`quickchart`, `serp_api`) are natural first network tools.
- **Agent-to-agent** (phase 3): nothing special on the wire — an agent
  discovers a peer and calls its `chat`/`invoke` like any other client,
  typically from inside a tool, subject to the caller's access level once
  permissions land.

## 12. Long turns (v1.5)

Protocol 1.5 lets a turn run as long as it keeps making progress, inside a
caller-set budget, with short-lived tokens renewed mid-run. Every addition
is optional; see §12.7.

### 12.1 Capability opt-in

Card `capabilities` gains two optional flags; the ack (§5.2) gains an
optional `caps` object describing this run:

```json
"capabilities": { "streaming": true, "progress": true, "renew": true }
```

```json
{ "accepted": true, "runId": "run_01HZY...", "caps": { "progress": true, "renewSubject": "trx.agent.copayAssistant.renew.Q2x0Yk..." } }
```

- `progress`: the agent emits progress frames (§12.3) and honors `budget`
  (§12.2); callers may cut its turns on progress idleness. Callers MUST
  NOT apply a no-progress cut to a run whose ack lacks `caps.progress`:
  such an agent only promised pings (the Go client ignores
  `ChatOptions.IdleWindow` for it).
- `renew`: the agent accepts a fresh IDT mid-run (§12.4). The ack's
  `caps.renewSubject` is present only when renew is available for this run.
- `caps` is absent on agents that opted into neither, and on v1.4 agents.
  Callers decide per run from `caps`, not from the card alone (a turn may
  land on an older instance mid-rollout).

### 12.2 Budget

The chat request may carry the caller's remaining time:

```json
"budget": { "remainingMs": 2940000 }
```

A 1.5 agent ends the turn by `remainingMs` minus a 5 s margin (floor 1 s),
so it can still send a clean terminal frame before the caller gives up.
Callers can rely on this only when the card or ack advertises `progress`.
If the handler does not end the turn itself, the runtime sends
`error` with `errorStatus` 5002, `error` `"time limit reached"` and
`reason:"time_cap"`. Absent `budget` = the agent's own limits, as before.

### 12.3 Progress frames

A `data` event (§5.3) of `kind:"progress"`:

```json
{ "runId": "run_01HZY...", "seq": 7, "type": "data", "kind": "progress", "payload": { "stage": "tool", "source": "sql", "detail": "querying claims" } }
```

- `stage` required (`model`, `tool`, `subagent`, …); `source`, `detail`
  optional, display only.
- At most one frame per 15 s per run; extra frames are dropped by the
  sender. Frames are emitted only for real work, never from a timer.
- For idle detection every event except `ping` counts as progress; pings
  prove liveness only. A caller may cut a turn after its idle window passes
  with no progress (`reason:"progress_idle"`).

### 12.4 Renew

A run with `caps.renewSubject` accepts a replacement IDT. The caller sends a
NATS request to that subject:

```json
{ "runId": "run_01HZY...", "idt": "IDT-....", "expiresAtMs": 1794480000000 }
```

Reply:

```json
{ "applied": true }
{ "applied": false, "reason": "identity_mismatch" }
```

- Subject rules: `trx.agent.<name>.renew.<22 random base64url characters>`,
  one per run, in the agent's own subject space (`<name>` is the agent's
  registered name, never a regional alias). It is subscribed on the owning
  instance's connection before the ack is sent and unsubscribed when the
  run ends; the agent's other instances stay silent on `renew.*` paths. A
  renew that arrives as or after the run ends gets no reply: the caller sees
  a timeout (Go: `ErrRenewTransport`). The subject is unguessable and is
  only ever sent to the caller in the ack.
- Why this space: an agent may already subscribe `trx.agent.<itsName>.>` and
  its callers may already publish `trx.agent.<thatName>.>`, so renew needs
  no extra NATS permission, and no other agent can subscribe to another
  agent's renew subjects. `_INBOX` is never used for tokens (every service
  may subscribe there).
- Callers MUST send a token only to a subject of exactly that shape for the
  agent they started the run on, with no wildcard (Go:
  `agentclient.ValidRenewSubject`; `Run.Renew` refuses anything else with
  `ErrRenewUnsupported`). Callers wait up to 12 s for the reply (the agent
  may make two fresh identity checks of up to 5 s each).
- Stray replies: every instance of the agent queue-subscribes
  `trx.agent.<name>.>`, so one of them also receives each renew. Current
  instances stay silent there, but an instance on an older build (rolling
  deploy, a regional twin) answers with a service error envelope (`status`
  header 404, body without `applied`), often before the owning run. Callers
  MUST skip any reply that has an error status header or lacks a boolean
  `applied`, and keep waiting for the run's reply; only an explicit
  `{"applied": false}` is a refusal. No well-formed reply in time = unknown
  outcome (Go: `ErrRenewTransport`), never a refusal.
- `runId` must match the run (`wrong_run` otherwise). `expiresAtMs`
  (optional) is the new token's expiry, unix ms.
- The token is validated with identity **without the decision cache**
  (`invalid_token` on failure), and must belong to the same user and account
  as the run's token (`identity_mismatch`). A verified run requires a
  verified token. With validation off or observe-only the run's admission
  rule applies, but a token that identity resolves to another user or
  account is still refused (compared with the caller first resolved for the
  run). If identity never resolved the run's caller (observe-only or
  fail-open without ids), a renew is accepted under the same rule as the
  admission was; the random per-run subject bounds the exposure.
  Malformed body = `bad_request`.
- Replay guard: when `expiresAtMs` is set and earlier than the expiry of the
  token the run holds (known from a previous renew), the renew is refused
  (`stale_token`). Equal or later expiries are accepted; unknown expiries
  are not compared.
- Accepted: the agent uses the new token for every new onward call; calls
  already in flight finish with the token they started with. The old token
  is never revoked by the agent; it expires on its own.
- A refused renew changes nothing; the run continues with its current token.

### 12.5 Terminal reasons

`done` and `error` events may carry `reason` when a limit, not the model,
ended the turn. Clients ignore unknown values.

| `reason` | Meaning |
|---|---|
| `time_cap` | Budget or safety cap reached |
| `progress_idle` | No progress within the idle window |
| `interrupted` | Agent instance shutting down |
| `max_iterations` | Agent's tool-loop limit reached |
| `subagent_timeout` | A consulted agent exceeded its time limit |
| `subagent_idle` | A consulted agent stopped making progress |
| `token_rotation_failed` | No fresh token could be delivered before the current one ran low |
| `unavailable` | Operational stop: a service the turn needs was unavailable |
| `awaiting_input` | An ask (§14) went unanswered within its live wait and is parked; answer it with a new turn (`inputs`) |

Stored history messages (`sessionsGet` `messages[]`, §7) may carry
`incomplete`: absent for a complete message, otherwise the reason it was cut
short (a value above, or `error`). Optional; clients that ignore it show the
partial text as before.

A cancel request (§5.4) may carry an optional `reason`:
`{ "runId": "run_01HZY...", "reason": "progress_idle" }`. A known cut reason
(`progress_idle`, `token_rotation_failed`, `interrupted`, `time_cap`,
`unavailable`) means
the caller's limits ended the turn: a 1.5 agent ends the run with an `error`
event carrying that `reason` (so it can keep its partial work) instead of
`done`/`cancelled`. Empty or unknown `reason` = a plain cancel, as before.
The cancel reply is unchanged. The cancel is as unauthenticated as it always
was: the reason only relabels a cancel that was already possible. When
cancels race, the first cause decides: a cut stays a cut if a plain cancel
follows, and a plain cancel stays a cancel if a cut follows. Callers that
cut a turn send the reasoned cancel BEFORE abandoning their own stream
(Go: call `CancelWithReason` before cancelling the `Chat` context, whose
cancel path publishes a plain cancel).

### 12.6 Token rotation (non-normative)

A caller holding a short-lived token schedules the next rotation from the
expiry identity actually returned (never from what it asked for), renews
when the current token has 8 min left, and retries with backoff (5 s, 10 s,
20 s, then 30 s) until 2 min are left; then it ends the turn with
`token_rotation_failed`. Go: `pkg/rotation`.

### 12.7 Compatibility

- Every 1.5 field, event kind and message is optional. No receiver requires
  any of them.
- A v1.4 agent ignores `budget`, never sends `caps`, progress frames or
  `reason`: callers keep today's behavior for that run (no idle cut, no
  renew, the first token for the whole turn).
- A v1.4 caller ignores `caps`, the `progress` data kind (unknown kinds are
  ignored, §5.3) and `reason`; it never sends `budget` or renews.
- An agent on 1.5 that has not opted in still honours a request `budget`,
  a reasoned cancel (any of the five cut reasons) and its own shutdown
  (`InterruptAll`): those end the turn with an `error` carrying `reason`
  (`time_cap`, `progress_idle`, `token_rotation_failed`, `interrupted`,
  `unavailable`).
  Everything else (`caps`, progress frames, renew) is emitted only after
  opt-in.
- The Go client gates its no-progress cut on the ack's `caps.progress`
  (§12.1): runs on agents without it behave exactly as plain `Chat`.

## 13. Delegation depth (v1.6)

Only an orchestrator may delegate a turn to another agent. Every other agent
is a leaf: if an orchestrator asks Agent1, Agent1 must not ask Agent2.

### 13.1 Field

`ChatRequest.delegation` and `ToolRunRequest.delegation` (optional):

```json
{ "delegation": { "depth": 1 } }
```

- `depth` is the number of agent-to-agent hops already taken to reach this
  turn. Absent or null = 0 (an entry call: UI, BFF, scheduler, worker).
  Negative = 0.
- Legacy (agent hosts, `chat`/`invoke`): `metadata.hops` (a JSON number) is
  still read. The effective depth is `max(delegation.depth, metadata.hops)`;
  other types are ignored. Tool runs read the `delegation` field only and
  ignore `metadata.hops`.

### 13.2 Receiver limit (A)

An agent refuses a `chat` or `invoke` whose effective depth exceeds its
limit (default 1) before authorization and before the turn starts (for
`chat`, before the ack): status **400**, code **4003**, message
`delegation depth exceeded (depth=N, max M)`. The status is never 403, so no
caller reads it as "user lacks access". The turn never starts. Handlers see
the depth as `Turn.DelegationDepth`.

### 13.3 Orchestrator rule (B)

An agent declares itself an orchestrator (Go: `Config.Orchestrator`, or env
`AGENT_ORCHESTRATOR=true`); the default is leaf. From inside a turn:

- A leaf that starts an agent call (`chat`, `invoke`) fails locally with
  `agentclient.ErrDelegationNotAllowed`, before anything is sent.
- An orchestrator sends `delegation.depth = max(existing, own depth + 1)`; the
  library never lowers a value the caller set. The agent limit is
  `Config.MaxDelegationDepth` (≤0 = 1).
- `card`, `ping`, `discover`, `cancel`, renew and `sessions*` are not turns:
  never checked or stamped.

### 13.4 Tools

Tools never call agents. A tool host runs each tool as a leaf, so a
`chat`/`invoke` made with the tool's context fails with
`ErrDelegationNotAllowed`, whatever the caller's depth. The caller's depth
(`delegation.depth` on `run`, unchanged) is exposed to the tool
(`tool.Call.Delegation`) for logs; tool hosts do not refuse on depth.
`card` and `ping` stay allowed.

### 13.5 Entries

Calls made with no turn context (BFF, dock gateway, scheduler executor,
worker, tests) carry no marker: the request is sent unchanged and the
receiver reads depth 0. A scheduled run is an entry.

### 13.6 Compatibility

| Caller → Receiver | Result |
|---|---|
| old/entry caller → new agent | no field → depth 0 → accepted |
| old RSAssistant (`hops:1`) → new agent | depth 1 → accepted |
| new orchestrator → old agent | field ignored → as today |
| new leaf agent with code that calls an agent | **fails** `ErrDelegationNotAllowed` |
| scheduler executor (any version) → any agent | depth 0 → accepted |
| tool (new host) whose run code calls an agent | **fails** `ErrDelegationNotAllowed` |

An agent that upgrades and delegates must set `Orchestrator: true`.
Enforcement is caller-asserted: a caller that forges depth 0 on raw NATS is
out of scope (NATS publish permissions or a signed depth are later work).

## 14. Asking the user (v1.7)

Protocol 1.7 lets an agent stop mid-turn and ask the human a structured
question (one field) or fill a form (several fields), then continue with the
validated answer. Every addition is optional; see §14.7.

### 14.1 Opt-in (both sides, per run)

- Card: `capabilities.userInput: true` — the agent may ask.
- Request: `ChatRequest.userInput: {"supported": true}` — the caller can show
  an ask to a human and route the answer back, for this run.
- Ack: `caps.inputSubject` is present **only when both** opted in. It is a
  per-run subject with an unguessable token, `trx.agent.<name>.input.<token>`
  (like `renewSubject`, §12.1). Callers decide per run from the ack.

When either side did not opt in, the agent's `AskUser` returns
`ErrUnsupported` at once: no event, no wait. The model is told it cannot ask
here and proceeds as before. An agent never waits for an answer nobody can
give.

```json
{ "accepted": true, "runId": "run_01HZY...", "caps": { "inputSubject": "trx.agent.copayAssistant.input.Q2x0Yk..." } }
```

### 14.2 Events (agent → caller)

Two `data` kinds on the event stream (§5.3):

```json
{ "type": "data", "kind": "ask", "data": { "askId": "ask_...", "waitMs": 300000, "origin": "copayAssistant", "ask": { } } }
{ "type": "data", "kind": "askClosed", "data": { "askId": "ask_...", "status": "answered" } }
```

- `ask`: `ask` is the Ask (§14.6); `waitMs` is the live window; `origin` is
  the asking agent's name (relays may prepend their own, reading like a
  path).
- `askClosed.status` is `answered`, `skipped`, `parked` or `superseded`
  (the user sent a message instead of answering). An agent sends one
  `askClosed` per `ask` before the terminal event on a best-effort basis:
  when the run ends while an ask is open (for example the handler gets
  `ErrRunEnded`, or a terminal event races the close) none may follow.
  Callers MUST therefore treat a terminal event as closing any ask still
  open as `parked`, so a card never stays open.
- Terminal `done` carries `reason: "awaiting_input"` (§12.5) when the turn
  ended because an ask was parked. Clients ignore unknown reasons.
- Every event except `ping` counts as progress for idle cuts (§12.3).

### 14.3 Input subject

The caller sends a NATS request to `caps.inputSubject`:

```json
{ "runId": "run_01HZY...", "askId": "ask_...", "action": "submit", "values": { } }
```

`action` is `submit`, `skip`, or `supersede` (the user sent a message
instead of answering: `values: {"text": "..."}`, non-empty; the ask closes
as `superseded`, the agent's `AskUser` returns `ErrSuperseded` with the
text). The reply is `{"accepted": true}` or
`{"accepted": false, "reason": "...", "errors": { }}`:

| `reason` | Meaning |
|---|---|
| `unknown_ask` | No such ask in this run (wrong `runId` or `askId`) |
| `closed` | The ask was already closed (answered, skipped, parked, superseded) |
| `invalid` | Unparseable request, unknown `action`, or values failed validation; `errors` maps field name to message (the `""` key = form-level, e.g. a rule) |
| `busy` | Reserved. Never sent: one open ask per run is enforced agent-side |

- The agent validates `values` against the ask (§14.6) before its handler or
  the model sees them. An `invalid` answer leaves the ask open for a retry.
- The first valid answer wins; a concurrent loser gets `closed`.
- After the run ends the subscription is gone: a late answer gets no
  responder (NATS "no responders"/timeout). The caller then answers via
  §14.4 if the ask was parked.

### 14.4 Answer-only turns (answering a parked ask)

When the live wait expires, the agent parks the ask and ends the turn
(`awaiting_input`). The caller answers with a new `chat` turn carrying
`ChatRequest.inputs`:

```json
{ "inputs": [ { "askId": "ask_...", "action": "submit", "values": { }, "ask": { } } ] }
```

`ask` is the original Ask, so a stateless agent can validate and understand
the answer. The agent validates each entry (ask well formed, action known,
values valid) and drops bad ones. `message` may be empty **only** when the
agent opted in (`capabilities.userInput`) and at least one input is valid;
otherwise an empty message is still a 400 missing-field error. Agents that
did not opt in ignore `inputs`. How valid inputs reach the model is the
agent's choice (Go: `Turn.ValidInputs`, `ask.FormatForModel`).

**Security.** `inputs` are caller-asserted: they carry no more authority than
the message text the same caller sends under the same identity (§5.1,
IDENTITY-AND-AUTHORITY.md). An agent that takes a side effect on a resumed
answer MUST verify it against its own record (for example the stored ask,
user and session), never the request's copy of the ask or values.

### 14.5 Timing

The live wait is the agent's (Go: `Config.AskWait`, env `AGENT_ASK_WAIT`,
default 5 min) and is announced as `waitMs`. It should stay shorter than any
caller idle window; ask events count as progress.

### 14.6 The Ask schema

One model tool, `request_user_input`, carries every question. The schema,
value encodings, limits and validator are specified by `pkg/ask` (Go is the
source of truth; `testdata/ask-vectors.json` holds shared test vectors). In
summary:

- Ask: `title` (required, ≤120 chars), `description` (≤500), `mode`
  (`ask` default, `create`, `edit`, `confirm`, `notice` v1.8), `danger`,
  `submitLabel`, `sections[]`, `fields[]` (0–30; 0 only for `confirm` and
  `notice`), `rules[]`.
- Field `type`: `text` (`format` email/url/phone), `longText`, `number`,
  `choice` (`options`, `multiple`, `allowOther`), `toggle`, `checkbox`,
  `date`, `time`, `datetime`, `dateRange`, `tags`, `table`, and (v1.8)
  `person` (`scope`), `link` (`href`), `hidden`; plus
  `required`, `readOnly`, `value` (prefill), `suggested`, bounds
  (`min`/`max`/`step`, lengths, `pattern`, item counts), `showIf`.
- Rules: `after`, `notBefore`, `atLeastOne`, `equal`, `notEqual`.
- Value encodings (timezone-safe): `date` `"YYYY-MM-DD"`; `time` `"HH:MM"`;
  `datetime` RFC 3339 with offset; `dateRange` `{"from","to"}` (+ `preset`);
  `choice` a value or array, Other as `{"other": "text"}`; `tags` string[];
  `table` row object(s); `number` number; `toggle`/`checkbox` bool;
  `person` `{"id","label"}` or a list of them; `link` never has a value.
- Limits: 30 fields, 8 sections, 200 options, 50 table rows, 32 KB payload.
  An ask over a limit is a tool error to the model, which can retry.
- Validation covers required, type/format, bounds, options, date bounds,
  rules, read-only fields unchanged; fields hidden by `showIf` are ignored.
- The answer given to the model: `{"status":"submitted","values":{},
  "display":{},"changed":[]}` (`changed` in edit mode), `{"status":"skipped"}`,
  or a superseded answer carrying the user's message text.

### 14.7 Compatibility

- Every 1.7 field, kind and reason is optional; no receiver requires any.
- Agent without `userInput`: never sends `ask`, `askClosed`, or
  `inputSubject`; ignores `userInput` and `inputs`; empty message stays 400.
- Caller without `userInput.supported`: gets no `inputSubject`; the agent's
  `AskUser` returns `ErrUnsupported`. Old callers ignore `ask`/`askClosed`
  (unknown data kinds, §5.3) and `awaiting_input` (§12.5).
- A caller that sends `userInput` to an older agent gets no `caps.inputSubject`
  and must not present asks for that run.

### 14.8 Go API notes (non-normative)

- `Stream.AskUser(ctx, ask.Ask)` returns the answer, or `ErrUnsupported`,
  `ErrSkipped` (with the answer), `ErrSuperseded`, `ErrParked`, `ErrBusy`,
  `ErrRunEnded`, or the ask's validation error. `ctx` MUST be the run
  context the handler received (or derived from it) so a cancelled or cut run
  stops the wait.
- `Stream.AskUserWith(ctx, a, agent.AskOptions{OnOpen: fn})` calls `fn(askID)`
  before the `ask` event (e.g. to store the ask); an error abandons the ask
  (no event) and is returned wrapped.
- After `ErrParked`, call `Stream.Park(askID)` once, right away: it emits
  `askClosed{parked}` and `done` with `awaiting_input`. Without it the
  library still closes the card before the terminal event.
- `Stream.InterruptAsk(text)` closes the open ask as `superseded` (the user
  typed instead of answering); `supersede` on the input subject (§14.3) or
  via `Stream.AnswerAsk` does the same.
- While `AskOptions.OnOpen` runs the ask is "opening": `InterruptAsk`,
  `OpenAsk` and answers ignore it (an answer gets `unknown_ask`) until
  `OnOpen` returns.
- `Park(askID)` emits `askClosed{parked}` only for the run's pending parked
  ask (after `ErrParked`); it always ends the turn as `awaiting_input`.
- `wire.AskEvent` / `wire.AskClosedEvent` and `wire.AskClosed*` decode the
  two data kinds.
- `Stream.AnswerAsk(askID, action, values)` answers the run's ask in
  process, with the same validation and reply as §14.3 (for a handler that
  routes answers itself).

### 14.9 Person, link, hidden and notice (v1.8)

Three ask components added in 1.8. A caller draws them only if it lists them
in `ChatRequest.userInput.features`:

```json
{ "userInput": { "supported": true, "features": ["person", "link", "notice"] } }
```

An agent never shows an ask that uses a component the caller did not list:
Go `AskUser` returns `ErrUnsupported` (no event), exactly as for a caller
without `userInput`. `features` absent = none (a 1.7 caller).

**`person` field** (`type: "person"`): pick people.
- Attributes: `multiple`, `minItems`/`maxItems`, `required`, `readOnly`,
  `help`, `options` (known people as quick picks: `value` = id, `label` =
  name, `description` = email/account), `value` (prefill: an id, ids, or
  `{"id","label"}` objects; labels for ids come from `options`), `scope`
  (`"account"`, the default = people in the signed-in user's account; other
  values are reserved and invalid now).
- Answer value: `{"id": "...", "label": "..."}` (non-`multiple`) or a list of
  them. Exactly the keys `id` (non-empty text) and `label` (optional text,
  ≤200 characters); no id twice in a list. Messages: "<Label> must be a
  person", "... must be a list of people", "... has a name longer than 200
  characters", "... lists the same person twice", plus the usual required and
  item-count messages.
- `label` is display only: the receiver MUST re-check every `id` (for
  example, same account) before acting on it. Display and `FormatForModel`
  show labels joined by ", " (the id when no label).
- Read-only and edit-mode `changed` checks compare people by id set (order
  and labels ignored), so a prefill given as ids matches its echoed objects.
  Rules see a read-only id prefill as `{"id","label"}` objects, and
  `equal`/`notEqual` between two person fields compare by id set.

**`link` field** (`type: "link"`): a button.
- Attributes: `label` (button text, required), `href` (required), `help`.
  `href` is an app-relative path (`/…`, not `//…` or `/\…`) or an `https://`
  URL with a host and no userinfo (`https://bank.com@evil.com` is invalid);
  anything else (`http:`, `javascript:`, relative words) is an invalid ask.
  A link can't be `required`, has no `value`/`suggested`, and can't be named
  by a rule or by another field's `showIf`.
- Display only: never in `values`, ignored by validation, dropped by
  normalization. An app-relative href opens inside the caller's app, https
  in a new tab.

**`notice` mode** (`mode: "notice"`): a card that only informs.
- Title, description, read-only fields and links; no Skip; one button
  (`submitLabel`, default "OK"). Fields must be links or `readOnly`; no
  `rules`.
- It never blocks: the agent emits `ask` (with `waitMs: 0`) and right away
  `askClosed{answered}`; Go `AskUser` returns an empty submitted Answer
  (`AskID` set, no values) without waiting or parking. The card stays
  visible after closing, with working links.

**`hidden` field** (`type: "hidden"`): context the server carries through
the form (item ids, version tags) without showing it.
- Attributes: `name` and `value` (required; text, number, true/false, or a
  list/object of those, no null). `label`, `help`, `required`, `suggested`,
  `options`, `readOnly`, `section`, `placeholder` and `showIf` are invalid
  (`showIf` could drop the server's context). Allowed in every mode,
  including `notice`.
- Clients MUST never render it. It is not a feature (no `userInput.features`
  entry): an older client that drops unknown fields still works, because a
  missing hidden value is filled from the ask.
- Validation: if the answer carries the field, its value must equal `value`
  exactly (JSON deep equal); anything else (including null) is "<name> can't
  be changed". Normalization always sets it to the ask's `value`.
- Answer: in `values` only; never in `display`, `changed`, or the text shown
  to the model (`FormatForModel`). It is always present in `values`. Rules
  can't name it ("hidden field has no user value"); another field's `showIf`
  may, and it reads the ask's `value` even when the client omits it.
- Trust: a hidden value is exactly as trustworthy as the ask it came from.
  On a live answer the agent built the ask itself. On a resumed input
  (`ChatRequest.inputs`) the ask JSON comes from the **caller**: the agent
  library checks the answer against that copy, not against one it kept. A
  caller that stores asks server side and sends its own stored copy (never
  one from the browser) keeps the values intact; even then, an agent MUST
  re-check hidden ids against live state before acting (item still there,
  still the user's, unchanged), never treat them as proof.

Go: `ask.TypePerson`, `ask.TypeLink`, `ask.TypeHidden`, `ask.ModeNotice`, `ask.ScopeAccount`,
`Field.Scope`, `Field.Href`, `ask.Person`, `ask.PersonValues(v)`;
`wire.FeaturePerson`, `wire.FeatureLink`, `wire.FeatureNotice`,
`wire.InputFeatures()` (a fresh copy; `wire.AllInputFeatures` kept for compatibility), `(*wire.UserInputSupport).Has(feature)`;
`agentclient.ChatOptions.UserInputFeatures`. Shared vectors for the new
validation rules are in `pkg/ask/testdata/ask-vectors.json`.
