package ask

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

type vector struct {
	Name   string            `json:"name"`
	Ask    Ask               `json:"ask"`
	Values map[string]any    `json:"values"`
	Errors map[string]string `json:"errors"`
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	b, err := os.ReadFile("testdata/ask-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vs []vector
	if err := json.Unmarshal(b, &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs) == 0 {
		t.Fatal("no vectors")
	}
	return vs
}

func TestValidateVectors(t *testing.T) {
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			if err := CheckAsk(v.Ask); err != nil {
				t.Fatalf("vector ask is malformed: %v", err)
			}
			got := map[string]string{}
			errs := Validate(v.Ask, v.Values)
			for _, e := range errs {
				if _, dup := got[e.Field]; dup {
					t.Fatalf("duplicate error for %q: %+v", e.Field, errs)
				}
				got[e.Field] = e.Message
			}
			want := v.Errors
			if want == nil {
				want = map[string]string{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("errors\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

func decodeAsk(t *testing.T, s string) Ask {
	t.Helper()
	var a Ask
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		t.Fatal(err)
	}
	return a
}

func decodeMap(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNormalize(t *testing.T) {
	a := decodeAsk(t, `{"title":"T","mode":"edit","fields":[
	 {"name":"temp","type":"toggle"},
	 {"name":"until","type":"date","showIf":{"field":"temp","op":"eq","value":true}},
	 {"name":"id","type":"text","readOnly":true,"value":"A1"},
	 {"name":"ro2","type":"text","readOnly":true},
	 {"name":"reason","type":"text"}]}`)
	got := Normalize(a, decodeMap(t, `{"temp":false,"until":"2026-11-22","id":"HACK","ro2":"x","reason":"r","bogus":1}`))
	want := decodeMap(t, `{"temp":false,"id":"A1","reason":"r"}`)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	got = Normalize(a, decodeMap(t, `{"temp":true,"until":"2026-11-22"}`))
	want = decodeMap(t, `{"temp":true,"until":"2026-11-22","id":"A1"}`)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestNormalizeKeepsDateString(t *testing.T) {
	a := decodeAsk(t, `{"title":"T","fields":[{"name":"d","type":"date"}]}`)
	got := Normalize(a, decodeMap(t, `{"d":"2026-11-22"}`))
	if got["d"] != "2026-11-22" {
		t.Fatalf("got %v", got["d"])
	}
}

func TestVisible(t *testing.T) {
	a := decodeAsk(t, `{"title":"T","fields":[{"name":"k","type":"text"},{"name":"x","type":"text","showIf":{"field":"k","op":"eq","value":"a"}},{"name":"y","type":"text"}]}`)
	if !Visible(a, map[string]any{"k": "a"}, a.Fields[1]) {
		t.Fatal("x should be visible")
	}
	if Visible(a, map[string]any{"k": "b"}, a.Fields[1]) {
		t.Fatal("x should be hidden")
	}
	if !Visible(a, nil, a.Fields[2]) {
		t.Fatal("no showIf is visible")
	}
}
