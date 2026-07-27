# NATS Agent Protocol — Specification v1.0 (draft)

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
  "capabilities": {
    "streaming": true,
    "sync": false,
    "sessions": true,
    "attachments": false
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
  "metadata": {}
}
```

- `protocolVersion` is the version of THIS spec the agent implements.
- `version` is the agent's own release version.
- `capabilities` gates the optional endpoint groups in §3. `streaming` MUST be
  true in v1 (chat is the core protocol); `sync` advertises `invoke`.
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
  "metadata": { "isAdmin": true }
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

Validation failures return the error envelope (§8) instead of an ack, and
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
| `error` | `error`, `status` (int, §8 code) | Terminal failure. |

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
  "protocolVersion": "1.0",
  "kind": "tool",
  "name": "quickchart",
  "displayName": "Chart Renderer",
  "description": "Render a chart from a Chart.js config and return a URL to the PNG. Use for any visualization request.",
  "version": "1.0.0",
  "repositoryUrl": "https://github.com/transactrx/quickchart-tool",
  "tags": ["visualization"],
  "inputSchema": { "type": "object", "properties": { "chart": { "type": "object" } }, "required": ["chart"] },
  "timeoutSeconds": 30,
  "metadata": {}
}
```

- `description` is written **for the model** — it becomes the tool
  description the LLM sees.
- `inputSchema` is a JSON Schema object, same convention as
  `inferenceGateway` `ToolSpec.inputSchema`.
- `timeoutSeconds` is the advisory worst-case execution time; callers use it
  to size their request timeout.

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
  "agent": "copayAssistant",
  "metadata": {}
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
  `agent` identify who is asking, for auditing now and access control later.
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
