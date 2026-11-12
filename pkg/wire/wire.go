// Package wire defines the JSON wire types of the NATS Agent Protocol
// (SPEC.md). Everything here is language-neutral: the Rust and .NET bindings
// serialize the same shapes. Message and content-block types mirror
// trx.inferenceGateway so conversation state passes through the gateway
// unchanged.
package wire

import "encoding/json"

// ProtocolVersion is the version of the agent protocol this library speaks.
const ProtocolVersion = "1.5"

// Subject layout. Agents own <AgentPrefix>.<name>.>, tools own
// <ToolPrefix>.<name>.>.
const (
	AgentPrefix          = "trx.agent"
	ToolPrefix           = "trx.tool"
	AgentDiscoverSubject = AgentPrefix + ".discover"
	ToolDiscoverSubject  = ToolPrefix + ".discover"
	ToolAnnounceSubject  = ToolPrefix + ".announce"
)

// HeaderIDT is the NATS message header carrying the caller's Internal
// Delegation Token on chat/invoke/sessions requests (SPEC §5.1) and tool runs
// (SPEC §8.4). Same name
// trx-gofiber-session uses on the webapp→agent hop.
const HeaderIDT = "X-TRX-IDT"

// Card kinds, so a mixed consumer (e.g. a universal UI) can browse agents and
// tools through the same machinery.
const (
	KindAgent = "agent"
	KindTool  = "tool"
)

func AgentSubject(name, endpoint string) string {
	return AgentPrefix + "." + name + "." + endpoint
}

func ToolSubject(name, endpoint string) string {
	return ToolPrefix + "." + name + "." + endpoint
}

// ─── Conversation content (identical to inferenceGateway) ────────────────

type Message struct {
	Role    string         `json:"role"` // "user" or "assistant"
	Content []ContentBlock `json:"content"`
}

type ContentBlock struct {
	Text       string           `json:"text,omitempty"`
	Image      *ImageBlock      `json:"image,omitempty"`
	ToolUse    *ToolUseBlock    `json:"toolUse,omitempty"`
	ToolResult *ToolResultBlock `json:"toolResult,omitempty"`
}

type ImageBlock struct {
	Format string `json:"format"` // "png", "jpeg", "gif", "webp"
	Base64 string `json:"base64"`
}

type ToolUseBlock struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input,omitempty"`
}

type ToolResultBlock struct {
	ToolUseID string              `json:"toolUseId"`
	Content   []ToolResultContent `json:"content"`
	Status    string              `json:"status,omitempty"` // "success" or "error"
}

type ToolResultContent struct {
	Text string          `json:"text,omitempty"`
	JSON json.RawMessage `json:"json,omitempty"`
}

type Usage struct {
	InputTokens  int32 `json:"inputTokens"`
	OutputTokens int32 `json:"outputTokens"`
	TotalTokens  int32 `json:"totalTokens"`
}

// ─── Discovery ────────────────────────────────────────────────────────────

// DiscoverFilter is the request body on the discover subjects. An empty
// filter matches everything. Agents/tools that do not match stay silent.
type DiscoverFilter struct {
	Name  string   `json:"name,omitempty"`  // exact name match
	Skill string   `json:"skill,omitempty"` // agents only: matches any skill name
	Tags  []string `json:"tags,omitempty"`  // all must be present
}

type AgentCard struct {
	ProtocolVersion string   `json:"protocolVersion"`
	Kind            string   `json:"kind"` // always "agent"
	Name            string   `json:"name"`
	DisplayName     string   `json:"displayName,omitempty"`
	Description     string   `json:"description"`
	Version         string   `json:"version,omitempty"`
	RepositoryURL   string   `json:"repositoryUrl,omitempty"`
	Tags            []string `json:"tags,omitempty"`
	// Category is an optional, display-only grouping label chosen by the
	// agent (e.g. "Pharmacy"); short preferred. Never used for routing or
	// authorization (SPEC v1.4).
	Category         string         `json:"category,omitempty"`
	Capabilities     Capabilities   `json:"capabilities"`
	InputModalities  []string       `json:"inputModalities,omitempty"`
	OutputModalities []string       `json:"outputModalities,omitempty"`
	Skills           []Skill        `json:"skills,omitempty"`
	Endpoints        []string       `json:"endpoints,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`

	// Access is the agent's identity registration: the application id and
	// the single RBAC function a user must hold to talk to this agent
	// (SPEC §4.2, IDENTITY-AND-AUTHORITY §2.1). Absent = undeclared.
	Access *AgentAccess `json:"access,omitempty"`
}

type Capabilities struct {
	Streaming   bool `json:"streaming"`
	Sync        bool `json:"sync"`
	Sessions    bool `json:"sessions"`
	Attachments bool `json:"attachments"`
	// UserContext (v1.3): the agent reads ChatRequest.context (user's time
	// zone and name) and answers in the user's local time.
	UserContext bool `json:"userContext,omitempty"`
	// Progress (v1.5): the agent emits progress data events and honors
	// ChatRequest.budget, so callers may cut a turn on progress idleness.
	Progress bool `json:"progress,omitempty"`
	// Renew (v1.5): the agent accepts a fresh IDT mid-run on the ack's
	// caps.renewSubject.
	Renew bool `json:"renew,omitempty"`
}

// AgentAccess maps an agent onto the identity model: an agent IS an
// application (appId) exposing one access function (functionId).
type AgentAccess struct {
	AppID      string `json:"appId"`
	FunctionID string `json:"functionId"`
}

type Skill struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Examples    []string `json:"examples,omitempty"`
}

type PingResponse struct {
	Status        string `json:"status"` // "ok"
	Name          string `json:"name"`
	InstanceID    string `json:"instanceId"`
	Version       string `json:"version,omitempty"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
}

// ─── Chat protocol ────────────────────────────────────────────────────────

type ChatRequest struct {
	SessionID     string         `json:"sessionId,omitempty"`
	UserID        string         `json:"userId,omitempty"`
	Message       Message        `json:"message"`
	StreamSubject string         `json:"streamSubject"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	// Context (v1.3) is display-only user context (time zone, name, locale).
	// Never authorization. Agents read it normalized (see UserContext).
	Context *UserContext `json:"context,omitempty"`
	// Budget (v1.5) is the time the caller still allows this turn. Absent =
	// the agent's own limits apply.
	Budget *Budget `json:"budget,omitempty"`
}

// Budget (v1.5) carries the caller's remaining time for a turn.
type Budget struct {
	RemainingMs int64 `json:"remainingMs"`
}

// AckCaps (v1.5) tells the caller what this run of the agent supports.
type AckCaps struct {
	Progress     bool   `json:"progress,omitempty"`
	RenewSubject string `json:"renewSubject,omitempty"` // where RenewRequest goes
}

type ChatAck struct {
	Accepted      bool   `json:"accepted"`
	RunID         string `json:"runId"`
	SessionID     string `json:"sessionId"`
	Agent         string `json:"agent"`
	InstanceID    string `json:"instanceId"`
	StreamSubject string `json:"streamSubject"`
	// Caps (v1.5) is absent on older agents.
	Caps *AckCaps `json:"caps,omitempty"`
}

// Event types published to the stream subject.
const (
	EventStart      = "start"
	EventText       = "text"
	EventToolUse    = "toolUse"
	EventToolResult = "toolResult"
	EventData       = "data"
	EventStatus     = "status"
	EventPing       = "ping"
	EventDone       = "done"
	EventError      = "error"
)

// Stop reasons carried by the done event.
const (
	StopEndTurn       = "endTurn"
	StopMaxTokens     = "maxTokens"
	StopCancelled     = "cancelled"
	StopMaxIterations = "maxIterations"
)

// Cut reasons (v1.5) carried by AgentEvent.Reason on done/error events when a
// turn was ended by a limit rather than by the model.
const (
	ReasonTimeCap         = "time_cap"
	ReasonProgressIdle    = "progress_idle"
	ReasonInterrupted     = "interrupted"
	ReasonMaxIterations   = "max_iterations"
	ReasonSubagentTimeout = "subagent_timeout"
	ReasonSubagentIdle    = "subagent_idle"
	ReasonTokenRotation   = "token_rotation_failed"
)

// DataKindProgress (v1.5) is the data event kind whose payload is a
// ProgressPayload.
const DataKindProgress = "progress"

// ProgressPayload (v1.5) is the payload of a data event of kind "progress".
type ProgressPayload struct {
	Stage  string `json:"stage"`
	Source string `json:"source,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// RenewRequest (v1.5) hands a running turn a fresh IDT.
type RenewRequest struct {
	RunID       string `json:"runId"`
	IDT         string `json:"idt"`
	ExpiresAtMs int64  `json:"expiresAtMs,omitempty"` // expiry of the new token, unix ms
}

type RenewResponse struct {
	Applied bool   `json:"applied"`
	Reason  string `json:"reason,omitempty"`
}

type AgentEvent struct {
	RunID string `json:"runId"`
	Seq   int    `json:"seq"`
	Type  string `json:"type"`

	SessionID string `json:"sessionId,omitempty"` // start
	TextDelta string `json:"textDelta,omitempty"` // text

	ToolUseID  string          `json:"toolUseId,omitempty"`  // toolUse, toolResult
	ToolName   string          `json:"toolName,omitempty"`   // toolUse, toolResult
	ToolInput  json.RawMessage `json:"toolInput,omitempty"`  // toolUse
	ToolResult string          `json:"toolResult,omitempty"` // toolResult
	ToolError  string          `json:"toolError,omitempty"`  // toolResult

	Kind    string          `json:"kind,omitempty"`    // data
	Payload json.RawMessage `json:"payload,omitempty"` // data

	StatusText string `json:"statusText,omitempty"` // status

	StopReason string `json:"stopReason,omitempty"` // done
	Usage      *Usage `json:"usage,omitempty"`      // done

	Error       string `json:"error,omitempty"`       // error
	ErrorStatus int    `json:"errorStatus,omitempty"` // error, §9 code

	Reason string `json:"reason,omitempty"` // done, error (v1.5 cut reason)
}

// IsProgress reports whether this event shows the run is making progress.
// Every event counts except pings, which only prove liveness.
func (e *AgentEvent) IsProgress() bool {
	return e.Type != EventPing
}

// IsTerminal reports whether this event ends the run.
func (e *AgentEvent) IsTerminal() bool {
	return e.Type == EventDone || e.Type == EventError
}

type CancelRequest struct {
	RunID string `json:"runId"`
}

type CancelResponse struct {
	Cancelled bool `json:"cancelled"`
}

// InvokeResponse is the reply of the synchronous invoke endpoint
// (capability "sync"). The request shape is ChatRequest without
// streamSubject.
type InvokeResponse struct {
	RunID      string  `json:"runId"`
	SessionID  string  `json:"sessionId"`
	Message    Message `json:"message"`
	StopReason string  `json:"stopReason"`
	Usage      *Usage  `json:"usage,omitempty"`
}

// ─── Sessions (capability "sessions") ─────────────────────────────────────

type SessionMeta struct {
	SessionID     string `json:"sessionId"`
	Title         string `json:"title,omitempty"`
	Favorite      bool   `json:"favorite,omitempty"`
	CreatedAt     string `json:"createdAt,omitempty"`     // RFC 3339
	LastMessageAt string `json:"lastMessageAt,omitempty"` // RFC 3339
}

type StoredMessage struct {
	Role      string         `json:"role"`
	Content   []ContentBlock `json:"content"`
	Timestamp string         `json:"timestamp,omitempty"` // RFC 3339
	// Incomplete (v1.5) is empty for a complete message; otherwise the
	// reason the message was cut short (a Reason* value, e.g. time_cap,
	// interrupted, or "error"). Optional; older peers ignore it.
	Incomplete string `json:"incomplete,omitempty"`
}

type SessionsListRequest struct {
	UserID string `json:"userId"`
}

type SessionsListResponse struct {
	Sessions []SessionMeta `json:"sessions"`
}

type SessionGetRequest struct {
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
}

type SessionGetResponse struct {
	SessionID string          `json:"sessionId"`
	Title     string          `json:"title,omitempty"`
	Favorite  bool            `json:"favorite,omitempty"`
	Messages  []StoredMessage `json:"messages"`
}

type SessionDeleteRequest struct {
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
}

type SessionDeleteResponse struct {
	SessionID string `json:"sessionId"`
	Deleted   bool   `json:"deleted"`
}

type SessionRenameRequest struct {
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
	Title     string `json:"title"`
}

type SessionRenameResponse struct {
	SessionID string `json:"sessionId"`
	Title     string `json:"title"`
}

type SessionSetFavoriteRequest struct {
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
	Favorite  bool   `json:"favorite"`
}

type SessionSetFavoriteResponse struct {
	SessionID string `json:"sessionId"`
	Favorite  bool   `json:"favorite"`
}

// ─── Network tools ────────────────────────────────────────────────────────

type ToolCard struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Kind            string         `json:"kind"` // always "tool"
	Name            string         `json:"name"`
	DisplayName     string         `json:"displayName,omitempty"`
	Description     string         `json:"description"` // written for the model
	Version         string         `json:"version,omitempty"`
	RepositoryURL   string         `json:"repositoryUrl,omitempty"`
	Tags            []string       `json:"tags,omitempty"`
	InputSchema     map[string]any `json:"inputSchema"`
	TimeoutSeconds  int            `json:"timeoutSeconds,omitempty"`
	// Access (v1.2) registers the tool with the identity model, like an agent
	// card's access: a caller may run the tool iff the user behind their
	// token holds functionId. Absent = runs are not authenticated.
	Access   *AgentAccess   `json:"access,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type ToolRunRequest struct {
	ToolUseID string         `json:"toolUseId,omitempty"`
	Input     map[string]any `json:"input"`
	UserID    string         `json:"userId,omitempty"`
	// SessionID (v1.2) names the conversation the call belongs to, so a tool
	// can keep per-conversation state. It is a locator, never a capability:
	// tools bind it to the verified caller.
	SessionID string         `json:"sessionId,omitempty"`
	Agent     string         `json:"agent,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// Tool run statuses. Execution errors are model-visible data ("error"
// status), not protocol failures.
const (
	ToolStatusSuccess = "success"
	ToolStatusError   = "error"
)

type ToolRunResponse struct {
	ToolUseID string              `json:"toolUseId,omitempty"`
	Status    string              `json:"status"`
	Content   []ToolResultContent `json:"content,omitempty"`
	Error     string              `json:"error,omitempty"`
	LatencyMs int64               `json:"latencyMs"`
}

// ─── Error detail codes (§9) ──────────────────────────────────────────────

const (
	CodeInvalidBody    = 4001
	CodeMissingField   = 4002
	CodeForbidden      = 4031
	CodeUnknownSession = 4041
	CodeUnknownRun     = 4042
	CodeBusy           = 4291
	CodeUpstream       = 5001
	CodeInternal       = 5002
)

// MatchesFilter reports whether an agent card matches a discovery filter.
func (c *AgentCard) MatchesFilter(f DiscoverFilter) bool {
	if f.Name != "" && f.Name != c.Name {
		return false
	}
	if f.Skill != "" {
		found := false
		for _, s := range c.Skills {
			if s.Name == f.Skill {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return containsAllTags(c.Tags, f.Tags)
}

// MatchesFilter reports whether a tool card matches a discovery filter. The
// skill field never matches tools.
func (c *ToolCard) MatchesFilter(f DiscoverFilter) bool {
	if f.Name != "" && f.Name != c.Name {
		return false
	}
	if f.Skill != "" {
		return false
	}
	return containsAllTags(c.Tags, f.Tags)
}

func containsAllTags(have, want []string) bool {
	for _, w := range want {
		found := false
		for _, h := range have {
			if h == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
