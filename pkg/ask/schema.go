// Package ask defines the "ask the user" form schema, its structural checks
// and the value validator. Go is the source of truth: the web app mirrors
// Validate in TypeScript using testdata/ask-vectors.json.
//
// This package must not import pkg/agent.
package ask

// Limits.
const (
	MaxFields   = 30
	MaxSections = 8
	MaxOptions  = 200
	MaxRows     = 50
	MaxAskBytes = 32 << 10

	MaxTitle       = 120
	MaxDescription = 500
)

// Modes.
const (
	ModeAsk     = "ask"
	ModeCreate  = "create"
	ModeEdit    = "edit"
	ModeConfirm = "confirm"
	// ModeNotice (v1.8): a card that only informs (title, description,
	// read-only fields, links) with one button; it never waits for an answer.
	ModeNotice = "notice"
)

// Field types.
const (
	TypeText      = "text"
	TypeLongText  = "longText"
	TypeNumber    = "number"
	TypeChoice    = "choice"
	TypeToggle    = "toggle"
	TypeCheckbox  = "checkbox"
	TypeDate      = "date"
	TypeTime      = "time"
	TypeDateTime  = "datetime"
	TypeDateRange = "dateRange"
	TypeTags      = "tags"
	TypeTable     = "table"
	// TypePerson (v1.8): pick people; value {"id","label"} or a list of them.
	TypePerson = "person"
	// TypeLink (v1.8): a button to Href; display only, never in values.
	TypeLink = "link"
	// TypeHidden (v1.8): context the server carries through the form
	// (ids, version tags). Never shown; its Value comes back unchanged in
	// the answer's values (filled in when the client omits it).
	TypeHidden = "hidden"
)

// Person scopes.
const (
	// ScopeAccount: people in the signed-in user's account (the default).
	ScopeAccount = "account"
)

// MaxPersonLabel is the longest person label, in characters.
const MaxPersonLabel = 200

// Text formats.
const (
	FormatEmail = "email"
	FormatURL   = "url"
	FormatPhone = "phone"
)

// Date range preset ids.
const (
	PresetToday       = "today"
	PresetLast7d      = "last7d"
	PresetLast30d     = "last30d"
	PresetThisMonth   = "thisMonth"
	PresetLastMonth   = "lastMonth"
	PresetThisQuarter = "thisQuarter"
	PresetYTD         = "ytd"
)

// Condition ops.
const (
	OpEq       = "eq"
	OpNeq      = "neq"
	OpIn       = "in"
	OpEmpty    = "empty"
	OpNotEmpty = "notEmpty"
)

// Rule types.
const (
	RuleAfter      = "after"
	RuleNotBefore  = "notBefore"
	RuleAtLeastOne = "atLeastOne"
	RuleEqual      = "equal"
	RuleNotEqual   = "notEqual"
)

// Ask is the whole request shown to the user.
type Ask struct {
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	Mode        string    `json:"mode,omitempty"`
	Danger      bool      `json:"danger,omitempty"`
	SubmitLabel string    `json:"submitLabel,omitempty"`
	Sections    []Section `json:"sections,omitempty"`
	Fields      []Field   `json:"fields,omitempty"`
	Rules       []Rule    `json:"rules,omitempty"`
}

// Section groups fields.
type Section struct {
	ID        string `json:"id"`
	Title     string `json:"title,omitempty"`
	Collapsed bool   `json:"collapsed,omitempty"`
}

// Field is one question.
type Field struct {
	Name           string           `json:"name"`
	Label          string           `json:"label,omitempty"`
	Section        string           `json:"section,omitempty"`
	Type           string           `json:"type"`
	Format         string           `json:"format,omitempty"`
	Help           string           `json:"help,omitempty"`
	Placeholder    string           `json:"placeholder,omitempty"`
	Required       bool             `json:"required,omitempty"`
	ReadOnly       bool             `json:"readOnly,omitempty"`
	Value          any              `json:"value,omitempty"`
	Suggested      any              `json:"suggested,omitempty"`
	SuggestReason  string           `json:"suggestReason,omitempty"`
	Options        []Option         `json:"options,omitempty"`
	Multiple       bool             `json:"multiple,omitempty"`
	AllowOther     *bool            `json:"allowOther,omitempty"` // nil = true
	Display        string           `json:"display,omitempty"`
	Min            any              `json:"min,omitempty"`
	Max            any              `json:"max,omitempty"`
	Step           float64          `json:"step,omitempty"`
	Unit           string           `json:"unit,omitempty"`
	MinLength      int              `json:"minLength,omitempty"`
	MaxLength      int              `json:"maxLength,omitempty"`
	Pattern        string           `json:"pattern,omitempty"`
	PatternMessage string           `json:"patternMessage,omitempty"`
	MinItems       int              `json:"minItems,omitempty"`
	MaxItems       int              `json:"maxItems,omitempty"`
	Presets        []string         `json:"presets,omitempty"`
	Columns        []Column         `json:"columns,omitempty"`
	Rows           []map[string]any `json:"rows,omitempty"`
	ShowIf         *Condition       `json:"showIf,omitempty"`
	// Scope (person) is who can be picked: "" or "account".
	Scope string `json:"scope,omitempty"`
	// Href (link) is an app-relative path ("/...") or an https:// URL.
	Href string `json:"href,omitempty"`
}

// Option is one choice.
type Option struct {
	Value       string `json:"value"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

// Column is one table column.
type Column struct {
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
}

// Condition shows a field only when it holds.
type Condition struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
}

// Rule is a cross-field check.
type Rule struct {
	Type    string   `json:"type"`
	Fields  []string `json:"fields"`
	Message string   `json:"message"`
}

// FieldError is one validation failure. Field is "" for form-level rule errors.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (f Field) label() string {
	if f.Label != "" {
		return f.Label
	}
	return f.Name
}

func (f Field) allowOther() bool { return f.AllowOther == nil || *f.AllowOther }

func validType(t string) bool {
	switch t {
	case TypeText, TypeLongText, TypeNumber, TypeChoice, TypeToggle, TypeCheckbox,
		TypeDate, TypeTime, TypeDateTime, TypeDateRange, TypeTags, TypeTable,
		TypePerson, TypeLink, TypeHidden:
		return true
	}
	return false
}

func validPreset(p string) bool {
	switch p {
	case PresetToday, PresetLast7d, PresetLast30d, PresetThisMonth,
		PresetLastMonth, PresetThisQuarter, PresetYTD:
		return true
	}
	return false
}
