package api

import (
	"encoding/json"
	"testing"
)

func TestChatEntryPreservesExtraFields(t *testing.T) {
	for _, input := range []string{
		`{"type":"chatmessage","role":"assistant","content":"","name":"lookup","tool_calls":[{"id":"call-1","arguments":{"exact":9007199254740993}}]}`,
		`{"type":"placeholder","name":"history","extra":{"exact":9007199254740993}}`,
	} {
		var entry ChatEntry
		if err := json.Unmarshal([]byte(input), &entry); err != nil {
			t.Fatal(err)
		}
		if len(entry.Extra) == 0 {
			t.Fatal("additional fields were discarded")
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqual(t, encoded, input)
	}
}

func TestChatEntryRejectsExtraFieldOverrides(t *testing.T) {
	for _, extra := range []string{`null`, `[]`, `"text"`, `{"type":"placeholder"}`, `{"role":"system"}`, `{"content":"changed"}`} {
		entry := MessageEntry("user", "original")
		entry.Extra = json.RawMessage(extra)
		if _, err := json.Marshal(entry); err == nil {
			t.Fatalf("accepted extra=%s", extra)
		}
	}
	entry := PlaceholderEntry("history")
	entry.Extra = json.RawMessage(`{"name":"override"}`)
	if _, err := json.Marshal(entry); err == nil {
		t.Fatal("accepted placeholder name override")
	}
}
