package ask

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/transactrx/nats-agent/pkg/wire"
)

func fmtAsk() Ask {
	return Ask{Title: "Pick", Fields: []Field{
		{Name: "speed", Label: "Speed", Type: TypeChoice, Options: []Option{{Value: "std", Label: "Standard"}, {Value: "fast", Label: "Fast"}}},
		{Name: "period", Label: "Period", Type: TypeDateRange},
		{Name: "ok", Label: "OK", Type: TypeToggle},
	}}
}

func TestBuildAnswerDisplayUsesLabels(t *testing.T) {
	a := BuildAnswer(fmtAsk(), "a1", map[string]any{
		"speed":  "std",
		"period": map[string]any{"from": "2026-11-15", "to": "2026-11-22", "preset": "last7d"},
		"ok":     true,
	})
	if a.Status != "submitted" || a.AskID != "a1" {
		t.Fatalf("%+v", a)
	}
	want := map[string]string{"speed": "Standard", "period": "Last 7 days · Nov 15 – Nov 22", "ok": "Yes"}
	if !reflect.DeepEqual(a.Display, want) {
		t.Fatalf("%v", a.Display)
	}
}

func TestDisplayVariants(t *testing.T) {
	a := Ask{Title: "T", Fields: []Field{
		{Name: "r", Type: TypeDateRange}, {Name: "r2", Type: TypeDateRange},
		{Name: "d", Type: TypeDate},
		{Name: "m", Type: TypeChoice, Multiple: true, Options: []Option{{Value: "a", Label: "A"}, {Value: "b", Label: "B"}}},
		{Name: "o", Type: TypeChoice, Options: []Option{{Value: "a", Label: "A"}}},
		{Name: "tg", Type: TypeTags},
		{Name: "tb", Type: TypeTable, Columns: []Column{{ID: "npi"}, {ID: "n"}}},
		{Name: "off", Type: TypeToggle},
	}}
	got := BuildAnswer(a, "x", map[string]any{
		"r":   map[string]any{"from": "2026-11-15", "to": "2026-11-22"},
		"r2":  map[string]any{"from": "2026-12-28", "to": "2027-01-03"},
		"d":   "2026-11-22",
		"m":   []any{"a", "b"},
		"o":   map[string]any{"other": "custom"},
		"tg":  []any{"x", "y"},
		"tb":  map[string]any{"npi": "123", "n": "Z"},
		"off": false,
	}).Display
	want := map[string]string{
		"r": "Nov 15 – Nov 22", "r2": "Dec 28, 2026 – Jan 3, 2027", "d": "Nov 22, 2026",
		"m": "A, B", "o": "custom", "tg": "x, y", "tb": "123", "off": "No",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
}

func TestBuildAnswerChangedInEditMode(t *testing.T) {
	a := Ask{Title: "E", Mode: ModeEdit, Fields: []Field{
		{Name: "a", Type: TypeText, Value: "x"},
		{Name: "b", Type: TypeText, Value: "y"},
	}}
	got := BuildAnswer(a, "e", map[string]any{"a": "x", "b": "z"})
	if !reflect.DeepEqual(got.Changed, []string{"b"}) {
		t.Fatalf("%v", got.Changed)
	}
	a.Mode = ModeAsk
	if BuildAnswer(a, "e", map[string]any{"a": "x", "b": "z"}).Changed != nil {
		t.Fatal("changed outside edit mode")
	}
}

func TestSkipToolResult(t *testing.T) {
	if got := (Answer{Status: "skipped"}).ToolResult(); got != `{"status":"skipped"}` {
		t.Fatal(got)
	}
	r := BuildAnswer(fmtAsk(), "a", map[string]any{"ok": true}).ToolResult()
	var m map[string]any
	if err := json.Unmarshal([]byte(r), &m); err != nil || m["status"] != "submitted" || m["values"] == nil {
		t.Fatal(r, err)
	}
}

func TestFormatForModel(t *testing.T) {
	in := wire.AskInput{AskID: "a", Action: wire.InputActionSubmit, Values: map[string]any{"ok": true, "speed": "fast", "ghost": 1}}
	got := FormatForModel(fmtAsk(), in)
	want := "[Earlier you asked the user: Pick. They answered: Speed = Fast; OK = Yes]"
	if got != want {
		t.Fatal(got)
	}
	got = FormatForModel(fmtAsk(), wire.AskInput{Action: wire.InputActionSkip})
	if got != "[Earlier you asked the user: Pick. They skipped it.]" {
		t.Fatal(got)
	}
}
