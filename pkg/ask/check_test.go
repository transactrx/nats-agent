package ask

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func okAsk() Ask {
	return Ask{Title: "T", Fields: []Field{{Name: "a", Type: TypeText}}}
}

func TestCheckAskOK(t *testing.T) {
	if err := CheckAsk(okAsk()); err != nil {
		t.Fatal(err)
	}
}

func TestCheckAskConfirm(t *testing.T) {
	if err := CheckAsk(Ask{Title: "Sure?", Mode: ModeConfirm}); err != nil {
		t.Fatalf("confirm with zero fields: %v", err)
	}
	a := okAsk()
	a.Mode = ModeConfirm
	if err := CheckAsk(a); err != nil {
		t.Fatalf("confirm with fields: %v", err)
	}
	if err := CheckAsk(Ask{Title: "T"}); err == nil {
		t.Fatal("zero fields outside confirm must fail")
	}
	if err := CheckAsk(Ask{Title: "T", Mode: ModeEdit}); err == nil {
		t.Fatal("zero fields in edit must fail")
	}
}

func TestCheckAskErrors(t *testing.T) {
	bad := func(f Field) Ask { return Ask{Title: "T", Fields: []Field{f}} }
	f31 := Ask{Title: "T"}
	for i := 0; i < 31; i++ {
		f31.Fields = append(f31.Fields, Field{Name: fmt.Sprintf("f%d", i), Type: TypeText})
	}
	s9 := okAsk()
	for i := 0; i < 9; i++ {
		s9.Sections = append(s9.Sections, Section{ID: fmt.Sprintf("s%d", i)})
	}
	opts201 := bad(Field{Name: "c", Type: TypeChoice})
	for i := 0; i < 201; i++ {
		opts201.Fields[0].Options = append(opts201.Fields[0].Options, Option{Value: fmt.Sprint(i)})
	}
	rows51 := bad(Field{Name: "t", Type: TypeTable})
	for i := 0; i < 51; i++ {
		rows51.Fields[0].Rows = append(rows51.Fields[0].Rows, map[string]any{"i": float64(i)})
	}
	f := false
	big := bad(Field{Name: "a", Type: TypeText, Help: strings.Repeat("x", MaxAskBytes)})
	cases := []struct {
		name string
		ask  Ask
		want string
	}{
		{"empty title", Ask{Fields: okAsk().Fields}, "title is required"},
		{"long title", Ask{Title: strings.Repeat("x", 121), Fields: okAsk().Fields}, "title too long"},
		{"long description", Ask{Title: "T", Description: strings.Repeat("x", 501), Fields: okAsk().Fields}, "description too long"},
		{"bad mode", Ask{Title: "T", Mode: "wizard", Fields: okAsk().Fields}, "invalid mode"},
		{"31 fields", f31, "too many fields"},
		{"9 sections", s9, "too many sections"},
		{"duplicate section", Ask{Title: "T", Sections: []Section{{ID: "s"}, {ID: "s"}}, Fields: okAsk().Fields}, "duplicate section"},
		{"empty field name", bad(Field{Type: TypeText}), "name is required"},
		{"duplicate name", Ask{Title: "T", Fields: []Field{{Name: "a", Type: TypeText}, {Name: "a", Type: TypeText}}}, `field "a": duplicate name`},
		{"unknown type", bad(Field{Name: "a", Type: "wizard"}), `field "a": unknown type`},
		{"choice no options", bad(Field{Name: "c", Type: TypeChoice}), `field "c": choice needs options`},
		{"201 options", opts201, `field "c": too many options`},
		{"duplicate option", bad(Field{Name: "c", Type: TypeChoice, Options: []Option{{Value: "x"}, {Value: "x"}}}), `duplicate option`},
		{"50+ rows", rows51, `field "t": too many rows`},
		{"bad pattern", bad(Field{Name: "code", Type: TypeText, Pattern: "("}), `field "code": invalid pattern`},
		{"bad format", bad(Field{Name: "a", Type: TypeText, Format: "fax"}), `field "a": unknown format`},
		{"format on number", bad(Field{Name: "a", Type: TypeNumber, Format: "email"}), `format only applies to text`},
		{"bad preset", bad(Field{Name: "a", Type: TypeDateRange, Presets: []string{"nope"}}), `unknown preset`},
		{"number min not numeric", bad(Field{Name: "a", Type: TypeNumber, Min: "1"}), `min must be a number`},
		{"date max not date", bad(Field{Name: "a", Type: TypeDate, Max: "soon"}), `max must be a date`},
		{"showIf unknown field", Ask{Title: "T", Fields: []Field{{Name: "a", Type: TypeText, ShowIf: &Condition{Field: "zz", Op: OpEq}}}}, `field "a": showIf names unknown field "zz"`},
		{"showIf self", Ask{Title: "T", Fields: []Field{{Name: "a", Type: TypeText, ShowIf: &Condition{Field: "a", Op: OpEq}}}}, `showIf cannot name itself`},
		{"showIf bad op", Ask{Title: "T", Fields: []Field{{Name: "a", Type: TypeText}, {Name: "b", Type: TypeText, ShowIf: &Condition{Field: "a", Op: "like"}}}}, `unknown showIf op`},
		{"suggested not in options", bad(Field{Name: "c", Type: TypeChoice, AllowOther: &f, Options: []Option{{Value: "x"}}, Suggested: "y"}), `field "c": suggested "y" is not among the options`},
		{"suggested array not in options", bad(Field{Name: "c", Type: TypeChoice, Multiple: true, AllowOther: &f, Options: []Option{{Value: "x"}}, Suggested: []any{"x", "y"}}), `suggested "y" is not among the options`},
		{"unknown section", bad(Field{Name: "a", Type: TypeText, Section: "nope"}), `field "a": unknown section "nope"`},
		{"rule unknown field", Ask{Title: "T", Fields: okAsk().Fields, Rules: []Rule{{Type: RuleAtLeastOne, Fields: []string{"a", "zz"}, Message: "m"}}}, `rule 1: unknown field "zz"`},
		{"rule bad type", Ask{Title: "T", Fields: okAsk().Fields, Rules: []Rule{{Type: "xor", Fields: []string{"a"}, Message: "m"}}}, `rule 1: unknown type`},
		{"rule needs two", Ask{Title: "T", Fields: okAsk().Fields, Rules: []Rule{{Type: RuleAfter, Fields: []string{"a"}, Message: "m"}}}, `rule 1: after needs exactly 2 fields`},
		{"rule no message", Ask{Title: "T", Fields: okAsk().Fields, Rules: []Rule{{Type: RuleAtLeastOne, Fields: []string{"a"}}}}, `rule 1: message is required`},
		{"over 32KB", big, "ask too large"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := CheckAsk(c.ask)
			if err == nil {
				t.Fatal("want error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not contain %q", err, c.want)
			}
		})
	}
}

func TestCheckAskSuggestedAllowed(t *testing.T) {
	// allowOther defaults to true: any suggestion is fine.
	a := Ask{Title: "T", Fields: []Field{{Name: "c", Type: TypeChoice, Options: []Option{{Value: "x"}}, Suggested: "y"}}}
	if err := CheckAsk(a); err != nil {
		t.Fatal(err)
	}
}

func TestCheckAskFromJSON(t *testing.T) {
	var a Ask
	if err := json.Unmarshal([]byte(`{"title":"T","fields":[{"name":"n","type":"number","min":1,"max":5}]}`), &a); err != nil {
		t.Fatal(err)
	}
	if err := CheckAsk(a); err != nil {
		t.Fatal(err)
	}
}
