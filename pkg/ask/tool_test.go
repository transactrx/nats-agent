package ask

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func jsonTags(t reflect.Type) []string {
	var out []string
	for i := 0; i < t.NumField(); i++ {
		out = append(out, strings.Split(t.Field(i).Tag.Get("json"), ",")[0])
	}
	return out
}

func TestToolInputSchemaParsesEveryVectorAsk(t *testing.T) {
	s := ToolInputSchema()
	if s["type"] != "object" {
		t.Fatalf("type = %v", s["type"])
	}
	props := s["properties"].(map[string]any)
	for _, k := range jsonTags(reflect.TypeOf(Ask{})) {
		if _, ok := props[k]; !ok {
			t.Errorf("ask property %q missing from schema", k)
		}
	}
	if !reflect.DeepEqual(s["required"], []string{"title"}) {
		t.Errorf("required = %v", s["required"])
	}
	fields := props["fields"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	for _, k := range jsonTags(reflect.TypeOf(Field{})) {
		if _, ok := fields[k]; !ok {
			t.Errorf("field property %q missing from schema", k)
		}
	}
	if _, err := json.Marshal(s); err != nil {
		t.Fatal(err)
	}
	for _, v := range loadVectors(t) {
		b, _ := json.Marshal(v.Ask)
		var in map[string]any
		_ = json.Unmarshal(b, &in)
		got, err := ParseAsk(in)
		if err != nil {
			t.Errorf("%s: %v", v.Name, err)
			continue
		}
		if !reflect.DeepEqual(got, v.Ask) {
			t.Errorf("%s: round-trip differs", v.Name)
		}
	}
}

func TestParseAskRejectsMalformed(t *testing.T) {
	bad := []map[string]any{
		{},
		{"title": "T", "fields": "nope"},
		{"title": "T", "bogus": 1},
		{"title": "T", "fields": []any{map[string]any{"name": "a", "type": "text", "pattern": "("}}},
		{"title": "T", "fields": []any{map[string]any{"name": "a", "type": "nine"}}},
	}
	for i, in := range bad {
		if _, err := ParseAsk(in); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestToolDescription(t *testing.T) {
	if ToolName != "request_user_input" || !strings.Contains(ToolDescription(), "form") {
		t.Fatal("bad name/description")
	}
}

func TestGuidanceMentionsSuggestNeverDecide(t *testing.T) {
	for _, w := range []string{"suggested", "never", ToolName, "skip", "turn"} {
		if !strings.Contains(strings.ToLower(Guidance), w) {
			t.Errorf("guidance lacks %q", w)
		}
	}
	if len(Guidance) > 1800 {
		t.Fatalf("guidance %d chars", len(Guidance))
	}
}
