package ask

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ToolName is the model tool that shows the user a form.
const ToolName = "request_user_input"

// ToolDescription is the tool description sent to the model.
func ToolDescription() string {
	return "Show the user a validated form in chat and wait for the answer. One field is a quick question; several fields are a form. " +
		"Use it as described in the guidance. Returns the values and the labels the user saw, or {\"status\":\"skipped\"}."
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
func boolean(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}
func integer(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}
func enum(desc string, vals ...string) map[string]any {
	return map[string]any{"type": "string", "enum": vals, "description": desc}
}
func anyVal(desc string) map[string]any { return map[string]any{"description": desc} }
func arr(items map[string]any) map[string]any {
	return map[string]any{"type": "array", "items": items}
}
func obj(req []string, props map[string]any) map[string]any {
	m := map[string]any{"type": "object", "properties": props}
	if req != nil {
		m["required"] = req
	}
	return m
}

// ToolInputSchema is the JSON Schema (draft-07 style) of Ask.
func ToolInputSchema() map[string]any {
	field := obj([]string{"name", "type"}, map[string]any{
		"name":    str("Unique key of the answer"),
		"label":   str("Shown to the user"),
		"section": str("Section id"),
		"type": enum("Control", TypeText, TypeLongText, TypeNumber, TypeChoice, TypeToggle,
			TypeCheckbox, TypeDate, TypeTime, TypeDateTime, TypeDateRange, TypeTags, TypeTable),
		"format":         enum("text only", FormatEmail, FormatURL, FormatPhone),
		"help":           str("Hint under the field"),
		"placeholder":    str(""),
		"required":       boolean(""),
		"readOnly":       boolean("Shown, not editable"),
		"value":          anyVal("Prefill (edit mode)"),
		"suggested":      anyVal("Likely answer; shown, never selected"),
		"suggestReason":  str("Why it is suggested"),
		"options":        arr(obj([]string{"value"}, map[string]any{"value": str(""), "label": str(""), "description": str("")})),
		"multiple":       boolean("choice: many"),
		"allowOther":     boolean("choice: free text option, default true"),
		"display":        enum("Hint only", "auto", "chips", "list", "slider", "stepper"),
		"min":            anyVal("number, or date/time string"),
		"max":            anyVal("number, or date/time string"),
		"step":           map[string]any{"type": "number"},
		"unit":           str("number unit"),
		"minLength":      integer(""),
		"maxLength":      integer(""),
		"pattern":        str("Regex for text"),
		"patternMessage": str("Shown when pattern fails"),
		"minItems":       integer("multiple choice, tags, table"),
		"maxItems":       integer(""),
		"presets":        arr(map[string]any{"type": "string", "enum": []string{PresetToday, PresetLast7d, PresetLast30d, PresetThisMonth, PresetLastMonth, PresetThisQuarter, PresetYTD}}),
		"columns":        arr(obj([]string{"id"}, map[string]any{"id": str(""), "label": str("")})),
		"rows":           arr(map[string]any{"type": "object"}),
		"showIf": obj([]string{"field", "op"}, map[string]any{
			"field": str("Other field name"),
			"op":    enum("", OpEq, OpNeq, OpIn, OpEmpty, OpNotEmpty),
			"value": anyVal(""),
		}),
	})
	return obj([]string{"title"}, map[string]any{
		"title":       str(fmt.Sprintf("Required, max %d chars", MaxTitle)),
		"description": str(fmt.Sprintf("Max %d chars", MaxDescription)),
		"mode":        enum("Default ask", ModeAsk, ModeCreate, ModeEdit, ModeConfirm),
		"danger":      boolean("confirm: irreversible"),
		"submitLabel": str(""),
		"sections":    arr(obj([]string{"id"}, map[string]any{"id": str(""), "title": str(""), "collapsed": boolean("")})),
		"fields":      map[string]any{"type": "array", "items": field, "description": fmt.Sprintf("Max %d; none only for mode confirm", MaxFields)},
		"rules": arr(obj([]string{"type", "fields", "message"}, map[string]any{
			"type":    enum("", RuleAfter, RuleNotBefore, RuleAtLeastOne, RuleEqual, RuleNotEqual),
			"fields":  arr(map[string]any{"type": "string"}),
			"message": str(""),
		})),
	})
}

// ParseAsk converts the model's tool input into an Ask and checks it.
func ParseAsk(input map[string]any) (Ask, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return Ask{}, fmt.Errorf("invalid ask: %w", err)
	}
	var a Ask
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&a); err != nil {
		return Ask{}, fmt.Errorf("invalid ask: %w", err)
	}
	if err := CheckAsk(a); err != nil {
		return Ask{}, err
	}
	return a, nil
}
