package ask

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Visible reports whether f is shown given the current values. A field with
// no showIf is always visible. The condition reads the raw value of the field
// it names (it does not chain through that field's own visibility).
func Visible(a Ask, values map[string]any, f Field) bool {
	c := f.ShowIf
	if c == nil {
		return true
	}
	got := values[c.Field]
	switch c.Op {
	case OpEq:
		return same(got, c.Value)
	case OpNeq:
		return !same(got, c.Value)
	case OpIn:
		list, _ := c.Value.([]any)
		for _, x := range list {
			if same(got, x) {
				return true
			}
		}
		return false
	case OpEmpty:
		return isEmpty(got)
	case OpNotEmpty:
		return !isEmpty(got)
	}
	return true
}

// Normalize returns the values to hand to the model: hidden fields are
// dropped, read-only fields are forced back to their value, and keys that
// name no field are dropped. The input map is not modified.
func Normalize(a Ask, values map[string]any) map[string]any {
	out := map[string]any{}
	for _, f := range a.Fields {
		if !Visible(a, values, f) {
			continue
		}
		if f.ReadOnly {
			if f.Value != nil {
				out[f.Name] = f.Value
			}
			continue
		}
		if v, ok := values[f.Name]; ok {
			out[f.Name] = v
		}
	}
	return out
}

// Validate checks the submitted values against the ask and returns one error
// per failing field (in field order), then rule errors with Field "".
// Hidden fields are ignored; read-only fields must equal their value.
func Validate(a Ask, values map[string]any) []FieldError {
	var errs []FieldError
	live := map[string]any{}
	for _, f := range a.Fields {
		if !Visible(a, values, f) {
			continue
		}
		v, present := values[f.Name]
		if f.ReadOnly {
			if present && !same(v, f.Value) {
				errs = append(errs, FieldError{f.Name, f.label() + " can't be changed"})
			}
			if f.Value != nil {
				live[f.Name] = f.Value
			}
			continue
		}
		if present {
			live[f.Name] = v
		}
		if msg := checkValue(f, v); msg != "" {
			errs = append(errs, FieldError{f.Name, msg})
		}
	}
	return append(errs, checkRuleValues(a, live)...)
}

// checkValue returns the first problem with one visible, writable field.
func checkValue(f Field, v any) string {
	l := f.label()
	if isEmpty(v) || (f.Type == TypeCheckbox && v == false) {
		if f.Required {
			return l + " is required"
		}
		return ""
	}
	switch f.Type {
	case TypeText, TypeLongText:
		return checkText(f, v)
	case TypeNumber:
		return checkNumber(f, v)
	case TypeChoice:
		return checkChoiceValue(f, v)
	case TypeToggle, TypeCheckbox:
		if _, ok := v.(bool); !ok {
			return l + " must be true or false"
		}
	case TypeDate:
		return checkDate(f, v)
	case TypeTime:
		return checkTime(f, v)
	case TypeDateTime:
		return checkDateTime(f, v)
	case TypeDateRange:
		return checkDateRange(f, v)
	case TypeTags:
		return checkTags(f, v)
	case TypeTable:
		return checkTable(f, v)
	}
	return ""
}

func checkRuleValues(a Ask, live map[string]any) []FieldError {
	types := map[string]string{}
	for _, f := range a.Fields {
		types[f.Name] = f.Type
	}
	var errs []FieldError
	for _, r := range a.Rules {
		if !ruleHolds(r, live, types) {
			errs = append(errs, FieldError{"", r.Message})
		}
	}
	return errs
}

func ruleHolds(r Rule, live map[string]any, types map[string]string) bool {
	if r.Type == RuleAtLeastOne {
		for _, n := range r.Fields {
			if v := live[n]; !isEmpty(v) && v != false {
				return true
			}
		}
		return false
	}
	if len(r.Fields) != 2 {
		return true
	}
	x, y := live[r.Fields[0]], live[r.Fields[1]]
	if isEmpty(x) || isEmpty(y) {
		return true // nothing to compare; required handles absence
	}
	switch r.Type {
	case RuleEqual:
		return same(x, y)
	case RuleNotEqual:
		return !same(x, y)
	}
	c, ok := compare(types[r.Fields[0]], x, y)
	if !ok {
		return true // a value error is already reported on its field
	}
	if r.Type == RuleAfter {
		return c < 0
	}
	return c <= 0 // notBefore
}

// same compares two decoded JSON values by their JSON form, so 3 and 3.0 match.
func same(a, b any) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(x) == string(y)
}

func isEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case []any:
		return len(t) == 0
	case []string:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	}
	return 0, false
}

func items(n int, f Field) string {
	l := f.label()
	if f.MinItems > 0 && n < f.MinItems {
		return fmt.Sprintf("%s needs at least %d items", l, f.MinItems)
	}
	if f.MaxItems > 0 && n > f.MaxItems {
		return fmt.Sprintf("%s allows at most %d items", l, f.MaxItems)
	}
	return ""
}
