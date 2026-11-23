package ask

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

// CheckAsk reports whether the ask itself is well formed: limits, names,
// references, patterns and bounds. The error text is shown to the model so it
// can fix the ask and retry.
func CheckAsk(a Ask) error {
	if b, err := json.Marshal(a); err != nil {
		return fmt.Errorf("ask cannot be encoded: %w", err)
	} else if len(b) > MaxAskBytes {
		return fmt.Errorf("ask too large: %d bytes (max %d)", len(b), MaxAskBytes)
	}
	if a.Title == "" {
		return fmt.Errorf("title is required")
	}
	if utf8.RuneCountInString(a.Title) > MaxTitle {
		return fmt.Errorf("title too long (max %d characters)", MaxTitle)
	}
	if utf8.RuneCountInString(a.Description) > MaxDescription {
		return fmt.Errorf("description too long (max %d characters)", MaxDescription)
	}
	switch a.Mode {
	case "", ModeAsk, ModeCreate, ModeEdit, ModeConfirm:
	default:
		return fmt.Errorf("invalid mode %q (use ask, create, edit or confirm)", a.Mode)
	}
	if len(a.Fields) > MaxFields {
		return fmt.Errorf("too many fields: %d (max %d)", len(a.Fields), MaxFields)
	}
	if len(a.Fields) == 0 && a.Mode != ModeConfirm {
		return fmt.Errorf("fields are required unless mode is confirm")
	}
	if len(a.Sections) > MaxSections {
		return fmt.Errorf("too many sections: %d (max %d)", len(a.Sections), MaxSections)
	}
	sections := map[string]bool{}
	for _, s := range a.Sections {
		if s.ID == "" {
			return fmt.Errorf("section id is required")
		}
		if sections[s.ID] {
			return fmt.Errorf("duplicate section id %q", s.ID)
		}
		sections[s.ID] = true
	}
	names := map[string]bool{}
	for _, f := range a.Fields {
		if f.Name == "" {
			return fmt.Errorf("field name is required")
		}
		if names[f.Name] {
			return fmt.Errorf("field %q: duplicate name", f.Name)
		}
		names[f.Name] = true
	}
	for _, f := range a.Fields {
		if err := checkField(f, names, sections); err != nil {
			return fmt.Errorf("field %q: %w", f.Name, err)
		}
	}
	return checkRules(a, fieldTypes(a))
}

func fieldTypes(a Ask) map[string]string {
	m := map[string]string{}
	for _, f := range a.Fields {
		m[f.Name] = f.Type
	}
	return m
}

func checkField(f Field, names, sections map[string]bool) error {
	if !validType(f.Type) {
		return fmt.Errorf("unknown type %q", f.Type)
	}
	if f.Section != "" && !sections[f.Section] {
		return fmt.Errorf("unknown section %q", f.Section)
	}
	if f.Format != "" {
		if f.Type != TypeText {
			return fmt.Errorf("format only applies to text fields")
		}
		switch f.Format {
		case FormatEmail, FormatURL, FormatPhone:
		default:
			return fmt.Errorf("unknown format %q", f.Format)
		}
	}
	if f.Pattern != "" {
		if _, err := regexp.Compile(f.Pattern); err != nil {
			return fmt.Errorf("invalid pattern: %v", err)
		}
	}
	if f.ShowIf != nil {
		c := f.ShowIf
		if c.Field == f.Name {
			return fmt.Errorf("showIf cannot name itself")
		}
		if !names[c.Field] {
			return fmt.Errorf("showIf names unknown field %q", c.Field)
		}
		switch c.Op {
		case OpEq, OpNeq, OpIn, OpEmpty, OpNotEmpty:
		default:
			return fmt.Errorf("unknown showIf op %q", c.Op)
		}
		if c.Op == OpIn {
			if _, ok := c.Value.([]any); !ok {
				return fmt.Errorf("showIf op in needs an array value")
			}
		}
	}
	for _, p := range f.Presets {
		if !validPreset(p) {
			return fmt.Errorf("unknown preset %q", p)
		}
	}
	if len(f.Rows) > MaxRows {
		return fmt.Errorf("too many rows: %d (max %d)", len(f.Rows), MaxRows)
	}
	if err := checkBounds(f); err != nil {
		return err
	}
	if f.MinLength > f.MaxLength && f.MaxLength > 0 {
		return fmt.Errorf("minLength is greater than maxLength")
	}
	if f.MinItems > f.MaxItems && f.MaxItems > 0 {
		return fmt.Errorf("minItems is greater than maxItems")
	}
	if f.Step < 0 {
		return fmt.Errorf("step must be positive")
	}
	ids := map[string]bool{}
	for _, col := range f.Columns {
		if col.ID == "" {
			return fmt.Errorf("column id is required")
		}
		if ids[col.ID] {
			return fmt.Errorf("duplicate column id %q", col.ID)
		}
		ids[col.ID] = true
	}
	if f.Type == TypeChoice {
		if err := checkChoice(f); err != nil {
			return err
		}
	}
	for _, p := range []struct {
		name string
		v    any
	}{{"value", f.Value}, {"suggested", f.Suggested}} {
		if isEmpty(p.v) {
			continue
		}
		if f.Type == TypeChoice {
			// membership is checked in checkChoice (free text is fine when
			// allowOther); here only the shape must fit.
			_, isList := p.v.([]any)
			_, isMap := p.v.(map[string]any)
			_, isStr := p.v.(string)
			if (f.Multiple && !isList) || (!f.Multiple && !isStr && !isMap) {
				return fmt.Errorf("%s does not fit the field: wrong shape for a choice", p.name)
			}
			continue
		}
		if msg := checkValue(Field{Name: f.Name, Label: f.Name, Type: f.Type, Format: f.Format,
			Options: f.Options, Multiple: f.Multiple, AllowOther: f.AllowOther, Min: f.Min, Max: f.Max,
			Step: f.Step, MinLength: f.MinLength, MaxLength: f.MaxLength, Pattern: f.Pattern,
			PatternMessage: f.PatternMessage, MinItems: f.MinItems, MaxItems: f.MaxItems,
			Presets: f.Presets, Rows: f.Rows}, p.v); msg != "" {
			return fmt.Errorf("%s does not fit the field: %s", p.name, msg)
		}
	}
	return nil
}

func checkBounds(f Field) error {
	for _, b := range []struct {
		name string
		v    any
	}{{"min", f.Min}, {"max", f.Max}} {
		if b.v == nil {
			continue
		}
		switch f.Type {
		case TypeNumber:
			if _, ok := toFloat(b.v); !ok {
				return fmt.Errorf("%s must be a number", b.name)
			}
		case TypeDate, TypeDateRange:
			if s, ok := b.v.(string); !ok || !isDate(s) {
				return fmt.Errorf("%s must be a date (YYYY-MM-DD)", b.name)
			}
		case TypeTime:
			if s, ok := b.v.(string); !ok || !isTime(s) {
				return fmt.Errorf("%s must be a time (HH:MM)", b.name)
			}
		case TypeDateTime:
			if s, ok := b.v.(string); !ok || !isDateTime(s) {
				return fmt.Errorf("%s must be an RFC 3339 date and time with offset", b.name)
			}
		}
	}
	if !boundsOrdered(f) {
		return fmt.Errorf("min is greater than max")
	}
	return nil
}

// boundsOrdered reports min > max for number and date-like bounds.
func boundsOrdered(f Field) bool {
	if f.Min == nil || f.Max == nil {
		return true
	}
	if f.Type == TypeNumber {
		lo, _ := toFloat(f.Min)
		hi, _ := toFloat(f.Max)
		return lo <= hi
	}
	var parse func(string) (time.Time, bool)
	switch f.Type {
	case TypeDate, TypeDateRange:
		parse = parseDate
	case TypeTime:
		parse = parseTime
	case TypeDateTime:
		parse = parseDateTime
	default:
		return true
	}
	ls, _ := f.Min.(string)
	hs, _ := f.Max.(string)
	lo, ok1 := parse(ls)
	hi, ok2 := parse(hs)
	return !ok1 || !ok2 || !lo.After(hi)
}

func checkChoice(f Field) error {
	if len(f.Options) == 0 {
		return fmt.Errorf("choice needs options")
	}
	if len(f.Options) > MaxOptions {
		return fmt.Errorf("too many options: %d (max %d)", len(f.Options), MaxOptions)
	}
	seen := map[string]bool{}
	for _, o := range f.Options {
		if o.Value == "" {
			return fmt.Errorf("option value is required")
		}
		if seen[o.Value] {
			return fmt.Errorf("duplicate option value %q", o.Value)
		}
		seen[o.Value] = true
	}
	if f.Suggested == nil || f.allowOther() {
		return nil
	}
	var sugg []any
	if arr, ok := f.Suggested.([]any); ok {
		sugg = arr
	} else {
		sugg = []any{f.Suggested}
	}
	for _, s := range sugg {
		str, _ := s.(string)
		if !seen[str] {
			return fmt.Errorf("suggested %q is not among the options", fmt.Sprint(s))
		}
	}
	return nil
}

func checkRules(a Ask, types map[string]string) error {
	for i, r := range a.Rules {
		n := i + 1
		switch r.Type {
		case RuleAfter, RuleNotBefore, RuleEqual, RuleNotEqual:
			if len(r.Fields) != 2 {
				return fmt.Errorf("rule %d: %s needs exactly 2 fields", n, r.Type)
			}
		case RuleAtLeastOne:
			if len(r.Fields) < 1 {
				return fmt.Errorf("rule %d: atLeastOne needs at least 1 field", n)
			}
		default:
			return fmt.Errorf("rule %d: unknown type %q", n, r.Type)
		}
		for _, f := range r.Fields {
			if _, ok := types[f]; !ok {
				return fmt.Errorf("rule %d: unknown field %q", n, f)
			}
			if r.Type == RuleAfter || r.Type == RuleNotBefore {
				switch types[f] {
				case TypeNumber, TypeDate, TypeTime, TypeDateTime:
				default:
					return fmt.Errorf("rule %d: %s needs number, date, time or datetime fields (%q is %s)", n, r.Type, f, types[f])
				}
			}
		}
		if (r.Type == RuleAfter || r.Type == RuleNotBefore) && types[r.Fields[0]] != types[r.Fields[1]] {
			return fmt.Errorf("rule %d: %s needs two fields of the same type", n, r.Type)
		}
		if r.Message == "" {
			return fmt.Errorf("rule %d: message is required", n)
		}
	}
	return nil
}

func isDate(s string) bool {
	if len(s) != 10 {
		return false
	}
	_, err := time.Parse("2006-01-02", s)
	return err == nil
}

func isTime(s string) bool {
	if len(s) != 5 {
		return false
	}
	_, err := time.Parse("15:04", s)
	return err == nil
}

func isDateTime(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}
