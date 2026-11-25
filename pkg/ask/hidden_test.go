package ask

import (
	"reflect"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/wire"
)

func hiddenAsk(mode string) Ask {
	return Ask{Title: "Delete file", Mode: mode, Fields: []Field{
		{Name: "fileId", Type: TypeHidden, Value: "f_123"},
		{Name: "ctx", Type: TypeHidden, Value: map[string]any{"version": float64(3), "tags": []any{"a", true}}},
		{Name: "note", Label: "Note", Type: TypeText, Value: "x"},
	}}
}

func TestHiddenFilledWhenMissing(t *testing.T) {
	a := hiddenAsk(ModeEdit)
	if err := CheckAsk(a); err != nil {
		t.Fatal(err)
	}
	vals := map[string]any{"note": "y"}
	if errs := Validate(a, vals); len(errs) != 0 {
		t.Fatalf("missing hidden must validate: %v", errs)
	}
	got := Normalize(a, vals)
	want := map[string]any{"fileId": "f_123", "ctx": map[string]any{"version": float64(3), "tags": []any{"a", true}}, "note": "y"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalize = %v", got)
	}
	// echoed unchanged is fine too
	if errs := Validate(a, map[string]any{"fileId": "f_123", "ctx": map[string]any{"tags": []any{"a", true}, "version": 3}}); len(errs) != 0 {
		t.Fatalf("echo rejected: %v", errs)
	}
}

func TestHiddenChangedRejected(t *testing.T) {
	a := hiddenAsk(ModeAsk)
	ctx := map[string]any{"version": 3.0, "tags": []any{"a", true}}
	cases := []struct {
		vals  map[string]any
		field string
	}{
		{map[string]any{"fileId": "f_999"}, "fileId"},
		{map[string]any{"fileId": nil}, "fileId"},
		{map[string]any{"fileId": 123.0}, "fileId"},
		{map[string]any{"fileId": "f_123", "ctx": map[string]any{"version": 4.0, "tags": []any{"a", true}}}, "ctx"},
		{map[string]any{"ctx": map[string]any{"version": 3.0}}, "ctx"},
		{map[string]any{"fileId": "f_123", "ctx": ctx, "note": "z"}, ""},
	}
	for i, c := range cases {
		errs := Validate(a, c.vals)
		if c.field == "" {
			if len(errs) != 0 {
				t.Errorf("case %d: %v", i, errs)
			}
			continue
		}
		if len(errs) != 1 || errs[0].Field != c.field || errs[0].Message != c.field+" can't be changed" {
			t.Errorf("case %d: errs = %v", i, errs)
		}
	}
}

func TestHiddenNotInDisplayOrChanged(t *testing.T) {
	a := hiddenAsk(ModeEdit)
	ans := BuildAnswer(a, "a1", map[string]any{"note": "y"})
	if ans.Values["fileId"] != "f_123" || ans.Values["ctx"] == nil {
		t.Fatalf("hidden values missing: %v", ans.Values)
	}
	if !reflect.DeepEqual(ans.Display, map[string]string{"note": "y"}) {
		t.Fatalf("display = %v", ans.Display)
	}
	if !reflect.DeepEqual(ans.Changed, []string{"note"}) {
		t.Fatalf("changed = %v", ans.Changed)
	}
	got := FormatForModel(a, wire.AskInput{AskID: "a1", Action: wire.InputActionSubmit, Values: map[string]any{"note": "y"}})
	if strings.Contains(got, "f_123") || strings.Contains(got, "fileId") || strings.Contains(got, "version") {
		t.Fatalf("hidden in model text: %s", got)
	}
	// only hidden fields: nothing to show
	only := Ask{Title: "T", Mode: ModeConfirm, Fields: []Field{{Name: "id", Type: TypeHidden, Value: "x"}}}
	if got := FormatForModel(only, wire.AskInput{AskID: "a", Action: wire.InputActionSubmit}); got != "[Earlier you asked the user: T. They confirmed.]" {
		t.Fatalf("%s", got)
	}
}

func TestCheckHiddenAttrs(t *testing.T) {
	ok := []Field{
		{Name: "h", Type: TypeHidden, Value: "s"},
		{Name: "h", Type: TypeHidden, Value: ""},
		{Name: "h", Type: TypeHidden, Value: 3.5},
		{Name: "h", Type: TypeHidden, Value: false},
		{Name: "h", Type: TypeHidden, Value: []any{"a", 1.0, map[string]any{"b": true}}},
		{Name: "h", Type: TypeHidden, Value: map[string]any{"ids": []string{"a", "b"}, "n": 2}},
	}
	for i, f := range ok {
		if err := CheckAsk(Ask{Title: "T", Fields: []Field{f}}); err != nil {
			t.Errorf("ok %d: %v", i, err)
		}
	}
	bad := []struct {
		f    Field
		want string
	}{
		{Field{Name: "h", Type: TypeHidden}, "hidden needs a value"},
		{Field{Name: "h", Type: TypeHidden, Value: []any{nil}}, "hidden value must be text, a number, true/false, or a list/object of those"},
		{Field{Name: "h", Type: TypeHidden, Value: map[string]any{"x": nil}}, "hidden value must be"},
		{Field{Name: "h", Type: TypeHidden, Value: "s", Label: "L"}, "hidden takes no label"},
		{Field{Name: "h", Type: TypeHidden, Value: "s", Help: "?"}, "hidden takes no help"},
		{Field{Name: "h", Type: TypeHidden, Value: "s", Required: true}, "hidden can't be required"},
		{Field{Name: "h", Type: TypeHidden, Value: "s", Suggested: "t"}, "hidden takes no suggested"},
		{Field{Name: "h", Type: TypeHidden, Value: "s", Options: []Option{{Value: "a"}}}, "hidden takes no options"},
	}
	for _, c := range bad {
		err := CheckAsk(Ask{Title: "T", Fields: []Field{c.f}})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: err = %v, want %q", c.f, err, c.want)
		}
	}
	notice := Ask{Title: "Done", Mode: ModeNotice, Fields: []Field{{Name: "id", Type: TypeHidden, Value: "x"}}}
	if err := CheckAsk(notice); err != nil {
		t.Fatalf("hidden in notice: %v", err)
	}
	if TypeHidden != "hidden" {
		t.Fatal("constant")
	}
}

// C4 ruling: no showIf, readOnly, section or placeholder on hidden.
func TestCheckHiddenNoDisplayAttrs(t *testing.T) {
	base := func() Field { return Field{Name: "h", Type: TypeHidden, Value: "x"} }
	cases := []struct {
		mod  func(*Field)
		want string
	}{
		{func(f *Field) { f.ShowIf = &Condition{Field: "t", Op: OpNotEmpty} }, "hidden can't have showIf"},
		{func(f *Field) { f.ReadOnly = true }, "hidden takes no readOnly"},
		{func(f *Field) { f.Section = "s" }, "hidden takes no section"},
		{func(f *Field) { f.Placeholder = "p" }, "hidden takes no placeholder"},
	}
	for _, c := range cases {
		f := base()
		c.mod(&f)
		a := Ask{Title: "T", Sections: []Section{{ID: "s"}}, Fields: []Field{{Name: "t", Type: TypeText}, f}}
		err := CheckAsk(a)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("err = %v, want %q", err, c.want)
		}
	}
}
