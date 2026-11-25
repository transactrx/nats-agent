package ask

import (
	"reflect"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/wire"
)

func linkAsk(href string) Ask {
	return Ask{Title: "T", Fields: []Field{
		{Name: "who", Type: TypeText},
		{Name: "open", Type: TypeLink, Label: "Open", Href: href},
	}}
}

func TestCheckLinkHref(t *testing.T) {
	cases := []struct {
		href string
		ok   bool
	}{
		{"/files", true},
		{"/files/a?b=c#d", true},
		{"https://x", true},
		{"https://example.com/path", true},
		{"//evil", false},
		{"javascript:x", false},
		{"http://x", false},
		{"", false},
		{"files", false},
		{"https://", false},
		{"/\\evil", false},
	}
	for _, c := range cases {
		err := CheckAsk(linkAsk(c.href))
		if c.ok && err != nil {
			t.Errorf("href %q: unexpected error %v", c.href, err)
		}
		if !c.ok && err == nil {
			t.Errorf("href %q: want error", c.href)
		}
	}
}

func TestCheckLinkRules(t *testing.T) {
	cases := []struct {
		name string
		f    Field
		want string
	}{
		{"no label", Field{Name: "l", Type: TypeLink, Href: "/x"}, "link needs a label"},
		{"required", Field{Name: "l", Type: TypeLink, Label: "Open", Href: "/x", Required: true}, "link can't be required"},
		{"href on text", Field{Name: "l", Type: TypeText, Href: "/x"}, "href only applies to link fields"},
		{"scope on text", Field{Name: "l", Type: TypeText, Scope: "account"}, "scope only applies to person fields"},
		{"bad scope", Field{Name: "l", Type: TypePerson, Scope: "world"}, `unknown scope "world"`},
	}
	for _, c := range cases {
		err := CheckAsk(Ask{Title: "T", Fields: []Field{c.f}})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %q", c.name, err, c.want)
		}
	}
	if err := CheckAsk(Ask{Title: "T", Fields: []Field{{Name: "p", Type: TypePerson, Scope: "account"}}}); err != nil {
		t.Fatalf("person scope account: %v", err)
	}
}

func TestCheckNoticeOnlyDisplayFields(t *testing.T) {
	ok := []Ask{
		{Title: "Done", Mode: ModeNotice},
		{Title: "Done", Mode: ModeNotice, Fields: []Field{
			{Name: "open", Type: TypeLink, Label: "Open", Href: "/files"},
			{Name: "id", Type: TypeText, ReadOnly: true, Value: "A1"},
			{Name: "who", Type: TypePerson, ReadOnly: true, Value: map[string]any{"id": "u1", "label": "Ann"}},
		}},
	}
	for i, a := range ok {
		if err := CheckAsk(a); err != nil {
			t.Errorf("ok %d: %v", i, err)
		}
	}
	bad := []struct {
		a    Ask
		want string
	}{
		{Ask{Title: "N", Mode: ModeNotice, Fields: []Field{{Name: "x", Type: TypeText}}}, "notice allows only link and read-only fields"},
		{Ask{Title: "N", Mode: ModeNotice, Fields: []Field{{Name: "x", Type: TypeText, ReadOnly: true}},
			Rules: []Rule{{Type: RuleAtLeastOne, Fields: []string{"x"}, Message: "m"}}}, "notice takes no rules"},
	}
	for _, c := range bad {
		err := CheckAsk(c.a)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("err = %v, want %q", err, c.want)
		}
	}
}

func personField(multiple bool) Field {
	return Field{Name: "p", Label: "Person", Type: TypePerson, Multiple: multiple}
}

func TestValidatePersonShapes(t *testing.T) {
	ann := map[string]any{"id": "u1", "label": "Ann"}
	cases := []struct {
		name     string
		multiple bool
		v        any
		want     string
	}{
		{"single ok", false, ann, ""},
		{"single label optional", false, map[string]any{"id": "u1"}, ""},
		{"single as list", false, []any{ann}, "Person must be a person"},
		{"single as text", false, "u1", "Person must be a person"},
		{"no id", false, map[string]any{"label": "Ann"}, "Person must be a person"},
		{"empty id", false, map[string]any{"id": "", "label": "Ann"}, "Person must be a person"},
		{"id not text", false, map[string]any{"id": 3.0, "label": "Ann"}, "Person must be a person"},
		{"label not text", false, map[string]any{"id": "u1", "label": 3.0}, "Person must be a person"},
		{"extra key", false, map[string]any{"id": "u1", "label": "Ann", "email": "a@x"}, "Person must be a person"},
		{"long label", false, map[string]any{"id": "u1", "label": strings.Repeat("é", 201)}, "Person has a name longer than 200 characters"},
		{"label 200 ok", false, map[string]any{"id": "u1", "label": strings.Repeat("é", 200)}, ""},
		{"multi ok", true, []any{ann, map[string]any{"id": "u2", "label": "Bo"}}, ""},
		{"multi as object", true, ann, "Person must be a list of people"},
		{"multi bad item", true, []any{ann, "u2"}, "Person must be a list of people"},
		{"multi duplicate", true, []any{ann, ann}, "Person lists the same person twice"},
	}
	for _, c := range cases {
		a := Ask{Title: "T", Fields: []Field{personField(c.multiple)}}
		errs := Validate(a, map[string]any{"p": c.v})
		got := ""
		if len(errs) > 0 {
			got = errs[0].Message
		}
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestValidatePersonCounts(t *testing.T) {
	f := personField(true)
	f.Required = true
	f.MinItems = 2
	f.MaxItems = 3
	a := Ask{Title: "T", Fields: []Field{f}}
	p := func(id string) any { return map[string]any{"id": id, "label": id} }
	cases := []struct {
		v    any
		want string
	}{
		{nil, "Person is required"},
		{[]any{}, "Person is required"},
		{[]any{p("a")}, "Person needs at least 2 items"},
		{[]any{p("a"), p("b")}, ""},
		{[]any{p("a"), p("b"), p("c"), p("d")}, "Person allows at most 3 items"},
	}
	for i, c := range cases {
		errs := Validate(a, map[string]any{"p": c.v})
		got := ""
		if len(errs) > 0 {
			got = errs[0].Message
		}
		if got != c.want {
			t.Errorf("case %d: got %q want %q", i, got, c.want)
		}
	}
}

func TestFormatPersonLabels(t *testing.T) {
	a := Ask{Title: "Share", Fields: []Field{
		{Name: "to", Label: "With", Type: TypePerson, Multiple: true},
		{Name: "owner", Label: "Owner", Type: TypePerson, ReadOnly: true, Value: "u9",
			Options: []Option{{Value: "u9", Label: "Zoe", Description: "zoe@x"}}},
		{Name: "open", Type: TypeLink, Label: "Open folder", Href: "/files"},
	}}
	if err := CheckAsk(a); err != nil {
		t.Fatal(err)
	}
	vals := map[string]any{
		"to":   []any{map[string]any{"id": "u1", "label": "Ann"}, map[string]any{"id": "u2", "label": "Bo"}},
		"open": "clicked",
	}
	ans := BuildAnswer(a, "a1", vals)
	if _, ok := ans.Values["open"]; ok {
		t.Fatalf("link in values: %v", ans.Values)
	}
	want := map[string]string{"to": "Ann, Bo", "owner": "Zoe"}
	if !reflect.DeepEqual(ans.Display, want) {
		t.Fatalf("display %v", ans.Display)
	}
	got := FormatForModel(a, wire.AskInput{AskID: "a1", Action: wire.InputActionSubmit, Values: vals})
	if got != "[Earlier you asked the user: Share. They answered: With = Ann, Bo; Owner = Zoe]" {
		t.Fatalf("format: %s", got)
	}
	if errs := Validate(a, vals); len(errs) != 0 {
		t.Fatalf("link value must be ignored: %v", errs)
	}
}

func TestPersonValues(t *testing.T) {
	want := []Person{{ID: "u1", Label: "Ann"}, {ID: "u2", Label: "Bo"}}
	got := PersonValues([]any{map[string]any{"id": "u1", "label": "Ann"}, map[string]any{"id": "u2", "label": "Bo"}})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
	if got := PersonValues(map[string]any{"id": "u1", "label": "Ann"}); !reflect.DeepEqual(got, want[:1]) {
		t.Fatalf("%v", got)
	}
	if got := PersonValues(nil); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	if got := PersonValues([]any{map[string]any{"label": "no id"}, "x"}); len(got) != 0 {
		t.Fatalf("bad items must be dropped: %v", got)
	}
}

func TestPersonPrefillIDs(t *testing.T) {
	for _, v := range []any{"u1", []any{"u1", "u2"}, map[string]any{"id": "u1", "label": "Ann"}} {
		_, multi := v.([]any)
		f := Field{Name: "p", Type: TypePerson, Multiple: multi, Value: v}
		if err := CheckAsk(Ask{Title: "T", Fields: []Field{f}}); err != nil {
			t.Errorf("prefill %v: %v", v, err)
		}
	}
	f := Field{Name: "p", Type: TypePerson, Value: []any{"u1"}}
	if err := CheckAsk(Ask{Title: "T", Fields: []Field{f}}); err == nil {
		t.Error("list prefill on single person must fail")
	}
}

func TestToolSchemaListsNewComponents(t *testing.T) {
	s := ToolInputSchema()
	field := s["properties"].(map[string]any)["fields"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	types := field["type"].(map[string]any)["enum"].([]string)
	if !contains(types, TypePerson) || !contains(types, TypeLink) {
		t.Fatalf("types %v", types)
	}
	if _, ok := field["href"]; !ok {
		t.Fatal("no href")
	}
	if _, ok := field["scope"]; !ok {
		t.Fatal("no scope")
	}
	modes := s["properties"].(map[string]any)["mode"].(map[string]any)["enum"].([]string)
	if !contains(modes, ModeNotice) {
		t.Fatalf("modes %v", modes)
	}
	if TypePerson != "person" || TypeLink != "link" || ModeNotice != "notice" {
		t.Fatal("constants")
	}
}

// Fix round 1: a person prefill given as ids is compared by id, not raw.
func TestPersonIDPrefillComparedByID(t *testing.T) {
	opts := []Option{{Value: "u1", Label: "Ann"}, {Value: "u2", Label: "Bo"}}
	ro := Ask{Title: "T", Fields: []Field{{Name: "p", Label: "Person", Type: TypePerson, ReadOnly: true, Value: "u1", Options: opts}}}
	if errs := Validate(ro, map[string]any{"p": map[string]any{"id": "u1", "label": "Ann"}}); len(errs) != 0 {
		t.Fatalf("object echo of id prefill rejected: %v", errs)
	}
	if errs := Validate(ro, map[string]any{"p": "u1"}); len(errs) != 0 {
		t.Fatalf("raw echo rejected: %v", errs)
	}
	if errs := Validate(ro, map[string]any{"p": map[string]any{"id": "u2", "label": "Bo"}}); len(errs) != 1 || errs[0].Message != "Person can't be changed" {
		t.Fatalf("changed person accepted: %v", errs)
	}
	ed := Ask{Title: "T", Mode: ModeEdit, Fields: []Field{
		{Name: "p", Label: "Person", Type: TypePerson, Value: "u1", Options: opts},
		{Name: "m", Label: "People", Type: TypePerson, Multiple: true, Value: []string{"u1", "u2"}, Options: opts},
	}}
	if err := CheckAsk(ed); err != nil {
		t.Fatal(err)
	}
	ans := BuildAnswer(ed, "a", map[string]any{
		"p": map[string]any{"id": "u1", "label": "Ann (renamed)"},
		"m": []any{map[string]any{"id": "u2", "label": "Bo"}, map[string]any{"id": "u1", "label": "Ann"}},
	})
	if len(ans.Changed) != 0 {
		t.Fatalf("unchanged people in changed: %v", ans.Changed)
	}
	ans = BuildAnswer(ed, "a", map[string]any{"p": map[string]any{"id": "u2", "label": "Bo"}, "m": []any{map[string]any{"id": "u1"}}})
	if !reflect.DeepEqual(ans.Changed, []string{"p", "m"}) {
		t.Fatalf("changed = %v", ans.Changed)
	}
}

// Fix round 1: Go-built prefills and values in typed slices.
func TestPersonTypedSlices(t *testing.T) {
	opts := []Option{{Value: "u1", Label: "Ann"}, {Value: "u2", Label: "Bo"}}
	for _, v := range []any{
		[]string{"u1", "u2"},
		[]map[string]any{{"id": "u1", "label": "Ann"}, {"id": "u2", "label": "Bo"}},
		[]Person{{ID: "u1", Label: "Ann"}, {ID: "u2", Label: "Bo"}},
	} {
		a := Ask{Title: "T", Fields: []Field{{Name: "m", Label: "People", Type: TypePerson, Multiple: true, ReadOnly: true, Value: v, Options: opts}}}
		if err := CheckAsk(a); err != nil {
			t.Errorf("prefill %T: %v", v, err)
			continue
		}
		if d := BuildAnswer(a, "a", nil).Display["m"]; d != "Ann, Bo" {
			t.Errorf("prefill %T display %q", v, d)
		}
	}
	want := []Person{{ID: "u1", Label: "Ann"}, {ID: "u2", Label: "Bo"}}
	if got := PersonValues([]map[string]any{{"id": "u1", "label": "Ann"}, {"id": "u2", "label": "Bo"}}); !reflect.DeepEqual(got, want) {
		t.Errorf("[]map: %v", got)
	}
	if got := PersonValues([]Person{{ID: "u1", Label: "Ann"}, {ID: "u2", Label: "Bo"}}); !reflect.DeepEqual(got, want) {
		t.Errorf("[]Person: %v", got)
	}
	if got := PersonValues(Person{ID: "u1", Label: "Ann"}); !reflect.DeepEqual(got, want[:1]) {
		t.Errorf("Person: %v", got)
	}
}
