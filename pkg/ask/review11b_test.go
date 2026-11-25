package ask

import (
	"strings"
	"testing"
)

func TestCheckLinkHrefUserinfo(t *testing.T) {
	for _, h := range []string{"https://a@evil", "https://user:pass@host", "https://bank.com@evil.com/x"} {
		if err := CheckAsk(linkAsk(h)); err == nil {
			t.Errorf("href %q: want error", h)
		}
	}
	if err := CheckAsk(linkAsk("https://host/path?q=a@b")); err != nil {
		t.Errorf("@ in query: %v", err)
	}
}

func TestCheckShowIfLinkAndHiddenRules(t *testing.T) {
	link := Field{Name: "open", Type: TypeLink, Label: "Open", Href: "/files"}
	hid := Field{Name: "id", Type: TypeHidden, Value: "x"}
	txt := Field{Name: "t", Type: TypeText}
	cases := []struct {
		a    Ask
		want string
	}{
		{Ask{Title: "T", Fields: []Field{link, {Name: "t", Type: TypeText, ShowIf: &Condition{Field: "open", Op: OpNotEmpty}}}}, `showIf can't name link field "open"`},
		{Ask{Title: "T", Fields: []Field{hid, txt}, Rules: []Rule{{Type: RuleEqual, Fields: []string{"id", "t"}, Message: "m"}}}, `rule 1: hidden field "id" has no user value`},
		{Ask{Title: "T", Fields: []Field{hid, txt}, Rules: []Rule{{Type: RuleAtLeastOne, Fields: []string{"t", "id"}, Message: "m"}}}, `rule 1: hidden field "id" has no user value`},
		{Ask{Title: "T", Fields: []Field{hid, txt}, Rules: []Rule{{Type: RuleNotEqual, Fields: []string{"t", "id"}, Message: "m"}}}, `rule 1: hidden field "id" has no user value`},
	}
	for i, c := range cases {
		err := CheckAsk(c.a)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("case %d: err = %v, want %q", i, err, c.want)
		}
	}
	// showIf on a hidden field's value stays allowed (server context drives it)
	ok := Ask{Title: "T", Fields: []Field{hid, {Name: "t", Type: TypeText, ShowIf: &Condition{Field: "id", Op: OpEq, Value: "x"}}}}
	if err := CheckAsk(ok); err != nil {
		t.Fatalf("showIf on hidden: %v", err)
	}
}

func TestRuleLivePersonNormalized(t *testing.T) {
	opts := []Option{{Value: "u1", Label: "Ann"}}
	a := Ask{Title: "T", Fields: []Field{
		{Name: "owner", Label: "Owner", Type: TypePerson, ReadOnly: true, Value: "u1", Options: opts},
		{Name: "to", Label: "To", Type: TypePerson},
	}, Rules: []Rule{{Type: RuleNotEqual, Fields: []string{"owner", "to"}, Message: "Pick someone else"}}}
	if err := CheckAsk(a); err != nil {
		t.Fatal(err)
	}
	errs := Validate(a, map[string]any{"to": map[string]any{"id": "u1", "label": "Ann"}})
	if len(errs) != 1 || errs[0].Message != "Pick someone else" {
		t.Fatalf("rule must see owner as {id,label}: %v", errs)
	}
	if errs := Validate(a, map[string]any{"to": map[string]any{"id": "u2", "label": "Bo"}}); len(errs) != 0 {
		t.Fatalf("%v", errs)
	}
}
