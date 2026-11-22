package ask

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// Answer statuses.
const (
	StatusSubmitted = "submitted"
	StatusSkipped   = "skipped"
	// StatusSuperseded: the user sent a message instead of answering.
	StatusSuperseded = "superseded"
)

// Answer is what the model receives as the tool result.
type Answer struct {
	AskID   string            `json:"-"`
	Status  string            `json:"status"`
	Values  map[string]any    `json:"values,omitempty"`
	Display map[string]string `json:"display,omitempty"`
	Changed []string          `json:"changed,omitempty"`
}

var presetNames = map[string]string{
	PresetToday: "Today", PresetLast7d: "Last 7 days", PresetLast30d: "Last 30 days",
	PresetThisMonth: "This month", PresetLastMonth: "Last month",
	PresetThisQuarter: "This quarter", PresetYTD: "Year to date",
}

// BuildAnswer normalizes values (visible fields only) and fills Display and,
// in edit mode, Changed.
func BuildAnswer(a Ask, askID string, values map[string]any) Answer {
	norm := Normalize(a, values)
	ans := Answer{AskID: askID, Status: StatusSubmitted, Values: norm, Display: map[string]string{}}
	for _, f := range a.Fields {
		v, ok := norm[f.Name]
		if !ok {
			continue
		}
		ans.Display[f.Name] = displayValue(f, v)
		if a.Mode == ModeEdit && !same(v, f.Value) {
			ans.Changed = append(ans.Changed, f.Name)
		}
	}
	return ans
}

// ToolResult is the compact JSON given to the model.
func (a Answer) ToolResult() string {
	if a.Status == StatusSkipped {
		return `{"status":"skipped"}`
	}
	b, _ := json.Marshal(a)
	return string(b)
}

// FormatForModel renders an answer for replay in history.
func FormatForModel(a Ask, in wire.AskInput) string {
	head := "[Earlier you asked the user: " + a.Title + ". "
	if in.Action == wire.InputActionSkip {
		return head + "They skipped it.]"
	}
	ans := BuildAnswer(a, in.AskID, in.Values)
	var parts []string
	for _, f := range a.Fields {
		if d, ok := ans.Display[f.Name]; ok {
			parts = append(parts, f.label()+" = "+d)
		}
	}
	if len(parts) == 0 {
		if a.Mode == ModeConfirm {
			return head + "They confirmed.]"
		}
		return head + "They submitted it with no values.]"
	}
	return head + "They answered: " + strings.Join(parts, "; ") + "]"
}

func optionLabel(f Field, v string) string {
	for _, o := range f.Options {
		if o.Value == v && o.Label != "" {
			return o.Label
		}
	}
	return v
}

func displayOne(f Field, v any) string {
	if m, ok := v.(map[string]any); ok {
		if o, ok := m["other"]; ok {
			return fmt.Sprint(o)
		}
	}
	if s, ok := v.(string); ok {
		return optionLabel(f, s)
	}
	return fmt.Sprint(v)
}

func list(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}
	if l, ok := v.([]string); ok {
		out := make([]any, len(l))
		for i, s := range l {
			out[i] = s
		}
		return out
	}
	return []any{v}
}

func displayValue(f Field, v any) string {
	switch f.Type {
	case TypeToggle, TypeCheckbox:
		if b, ok := v.(bool); ok {
			if b {
				return "Yes"
			}
			return "No"
		}
	case TypeChoice:
		var out []string
		for _, x := range list(v) {
			out = append(out, displayOne(f, x))
		}
		return joinOrNone(out)
	case TypeTags:
		var out []string
		for _, x := range list(v) {
			out = append(out, fmt.Sprint(x))
		}
		return joinOrNone(out)
	case TypeTable:
		var out []string
		for _, x := range list(v) {
			out = append(out, rowDisplay(f, x))
		}
		return joinOrNone(out)
	case TypeDate:
		if s, ok := v.(string); ok {
			if t, ok := parseDate(s); ok {
				return t.Format("Jan 2, 2006")
			}
		}
	case TypeDateTime:
		if s, ok := v.(string); ok {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				return t.Format("Jan 2, 2006 15:04")
			}
		}
	case TypeDateRange:
		if m, ok := v.(map[string]any); ok {
			return rangeDisplay(m)
		}
	case TypeNumber:
		if x, ok := toFloat(v); ok {
			s := strconv.FormatFloat(x, 'f', -1, 64)
			if f.Unit != "" {
				s += " " + f.Unit
			}
			return s
		}
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func rowDisplay(f Field, v any) string {
	m, ok := v.(map[string]any)
	if !ok || len(f.Columns) == 0 {
		return fmt.Sprint(v)
	}
	if c, ok := m[f.Columns[0].ID]; ok {
		return fmt.Sprint(c)
	}
	return fmt.Sprint(v)
}

func rangeDisplay(m map[string]any) string {
	fs, _ := m["from"].(string)
	ts, _ := m["to"].(string)
	from, ok1 := parseDate(fs)
	to, ok2 := parseDate(ts)
	if !ok1 || !ok2 {
		return fs + " – " + ts
	}
	layout := "Jan 2"
	if from.Year() != to.Year() {
		layout = "Jan 2, 2006"
	}
	s := from.Format(layout) + " – " + to.Format(layout)
	if p, ok := m["preset"].(string); ok && presetNames[p] != "" {
		s = presetNames[p] + " · " + s
	}
	return s
}

func joinOrNone(out []string) string {
	if len(out) == 0 {
		return "(none)"
	}
	return strings.Join(out, ", ")
}
