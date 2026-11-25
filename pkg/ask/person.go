package ask

import (
	"net/url"
	"strings"
	"unicode/utf8"
)

// Person is one picked person (a person field's answer item). Label is
// display only: consumers must re-check ID before acting on it.
type Person struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// PersonValues reads a person field's value: one {"id","label"} object or a
// list of them (decoded JSON, Person or []Person). Items without an id are
// dropped. Nil or anything else gives nil.
func PersonValues(v any) []Person {
	var out []Person
	add := func(x any) {
		switch t := x.(type) {
		case map[string]any:
			id, _ := t["id"].(string)
			label, _ := t["label"].(string)
			if id != "" {
				out = append(out, Person{ID: id, Label: label})
			}
		case Person:
			if t.ID != "" {
				out = append(out, t)
			}
		}
	}
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			add(x)
		}
	case []map[string]any:
		for _, x := range t {
			add(x)
		}
	case []Person:
		for _, x := range t {
			add(x)
		}
	default:
		add(v)
	}
	return out
}

// personOK reports whether x is exactly {"id": non-empty text, "label"?: text}.
func personOK(x any) (string, string, bool) {
	m, ok := x.(map[string]any)
	if !ok {
		return "", "", false
	}
	id, ok := m["id"].(string)
	if !ok || id == "" {
		return "", "", false
	}
	label := ""
	if l, has := m["label"]; has {
		if label, ok = l.(string); !ok {
			return "", "", false
		}
	}
	for k := range m {
		if k != "id" && k != "label" {
			return "", "", false
		}
	}
	return id, label, true
}

func checkPerson(f Field, v any) string {
	l := f.label()
	var people []any
	if f.Multiple {
		arr, ok := v.([]any)
		if !ok {
			return l + " must be a list of people"
		}
		people = arr
	} else {
		people = []any{v}
	}
	seen := map[string]bool{}
	for _, x := range people {
		id, label, ok := personOK(x)
		if !ok {
			if f.Multiple {
				return l + " must be a list of people"
			}
			return l + " must be a person"
		}
		if utf8.RuneCountInString(label) > MaxPersonLabel {
			return l + " has a name longer than 200 characters"
		}
		if seen[id] {
			return l + " lists the same person twice"
		}
		seen[id] = true
	}
	if f.Multiple {
		return items(len(people), f)
	}
	return ""
}

// personPrefill turns a prefill into the answer shape: an id ("u1") or a
// Person becomes {"id","label"} (an id's label from options); a list
// ([]any, []string, []map[string]any, []Person) becomes []any of those.
// Other shapes pass through.
func personPrefill(f Field, v any) any {
	one := func(x any) any {
		switch t := x.(type) {
		case string:
			return map[string]any{"id": t, "label": optionLabel(f, t)}
		case Person:
			return map[string]any{"id": t.ID, "label": t.Label}
		}
		return x
	}
	var arr []any
	switch t := v.(type) {
	case []any:
		arr = t
	case []string:
		for _, x := range t {
			arr = append(arr, x)
		}
	case []map[string]any:
		for _, x := range t {
			arr = append(arr, x)
		}
	case []Person:
		for _, x := range t {
			arr = append(arr, x)
		}
	default:
		return one(v)
	}
	out := make([]any, len(arr))
	for i, x := range arr {
		out[i] = one(x)
	}
	return out
}

// samePeople reports whether two person values name the same ids (order
// and labels ignored; labels are display only). Prefill shapes (ids) are
// expanded first.
func samePeople(f Field, x, y any) bool {
	ids := func(v any) map[string]bool {
		m := map[string]bool{}
		for _, p := range PersonValues(personPrefill(f, v)) {
			m[p.ID] = true
		}
		return m
	}
	a, b := ids(x), ids(y)
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

// sameValue is same, except person values compare by id (samePeople).
func sameValue(f Field, x, y any) bool {
	if f.Type == TypePerson {
		return samePeople(f, x, y)
	}
	return same(x, y)
}

// personDisplay: labels (id when no label) joined by ", ".
func personDisplay(f Field, v any) string {
	var out []string
	for _, x := range list(personPrefill(f, v)) {
		id, label, ok := personOK(x)
		if !ok {
			continue
		}
		if label == "" {
			label = id
		}
		out = append(out, label)
	}
	return joinOrNone(out)
}

// validHref: an app-relative path ("/x", not "//" or "/\\") or an
// https:// URL with a host. No spaces or control characters.
func validHref(h string) bool {
	if h == "" || strings.ContainsAny(h, " \t\r\n") {
		return false
	}
	u, err := url.Parse(h)
	if err != nil {
		return false
	}
	if h[0] == '/' {
		return !strings.HasPrefix(h, "//") && !strings.HasPrefix(h, "/\\") && u.Scheme == "" && u.Host == ""
	}
	// no userinfo (https://bank.com@evil.com): it hides the real host
	return u.Scheme == "https" && u.Host != "" && u.User == nil && strings.HasPrefix(h, "https://") && !strings.ContainsRune(h, '\\')
}
