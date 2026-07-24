package langfuseopenai

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/fgn/go-langfuse/contrib/openai/internal/wiretap"
)

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	rendered, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return rendered
}

// TestOutputItemUnionSweep locks a policy for every pinned output
// discriminator plus the unknown case: retained shapes for message,
// function_call, and reasoning; fixed placeholders for the rest; the
// literal "unknown" for unpinned discriminators.
func TestOutputItemUnionSweep(t *testing.T) {
	retained := map[string]bool{"message": true, "function_call": true, "reasoning": true}
	pinnedPlaceholders := []string{
		"image_generation_call", "computer_call", "code_interpreter_call",
		"file_search_call", "web_search_call", "mcp_call", "mcp_list_tools",
		"mcp_approval_request", "local_shell_call",
	}
	for kind := range knownResponsesOutputTypes {
		raw := json.RawMessage(fmt.Sprintf(`{"type":%q,"secret":"SENTINEL"}`, kind))
		sanitized, _, partial := sanitizeResponsesOutputItem(raw)
		rendered := mustMarshal(t, sanitized)
		if strings.Contains(string(rendered), "SENTINEL") {
			t.Errorf("%s: sentinel leaked: %s", kind, rendered)
		}
		if retained[kind] {
			if partial {
				t.Errorf("%s: retained kinds are not partial", kind)
			}
			continue
		}
		if !strings.Contains(string(rendered), `"omitted":true`) || !partial {
			t.Errorf("%s: want fixed placeholder with partial, got %s", kind, rendered)
		}
	}
	for _, kind := range pinnedPlaceholders {
		if !knownResponsesOutputTypes[kind] {
			t.Errorf("pinned discriminator %s missing from the known set", kind)
		}
	}
	// Unpinned discriminators, including the unpinned custom tool
	// calls, become the literal unknown.
	for _, kind := range []string{"custom_tool_call", "banana_call"} {
		raw := json.RawMessage(fmt.Sprintf(`{"type":%q,"secret":"SENTINEL"}`, kind))
		sanitized, _, _ := sanitizeResponsesOutputItem(raw)
		rendered := mustMarshal(t, sanitized)
		if !strings.Contains(string(rendered), `"type":"unknown"`) {
			t.Errorf("%s: unpinned discriminator must render as unknown: %s", kind, rendered)
		}
	}
}

// TestInputItemUnionSweep locks a policy for every pinned input
// discriminator, all four Easy roles, and unpinned forms.
func TestInputItemUnionSweep(t *testing.T) {
	call := newResponsesCall(wiretap.Route{}, 1<<20)
	for kind := range knownResponsesInputTypes {
		var raw json.RawMessage
		switch kind {
		case "message":
			raw = json.RawMessage(`{"type":"message","role":"user","content":[{"type":"input_text","text":"q"}]}`)
		default:
			raw = json.RawMessage(fmt.Sprintf(`{"type":%q,"secret":"SENTINEL"}`, kind))
		}
		sanitized := call.sanitizeInputItem(raw)
		rendered := mustMarshal(t, sanitized)
		if strings.Contains(string(rendered), "SENTINEL") {
			t.Errorf("%s: sentinel leaked: %s", kind, rendered)
		}
	}
	for _, role := range []string{"user", "assistant", "system", "developer"} {
		sanitized := call.sanitizeInputItem(json.RawMessage(fmt.Sprintf(`{"role":%q,"content":"scalar"}`, role)))
		rendered := mustMarshal(t, sanitized)
		if !strings.Contains(string(rendered), "scalar") {
			t.Errorf("Easy role %s must retain scalar content: %s", role, rendered)
		}
	}
	sanitized := call.sanitizeInputItem(json.RawMessage(`{"role":"robot","content":"x"}`))
	rendered := mustMarshal(t, sanitized)
	if !strings.Contains(string(rendered), `"omitted":true`) {
		t.Errorf("unpinned role must tombstone: %s", rendered)
	}
}

// TestUnaryScannerHistories drives the REAL parser's over-cap unary
// contract: UnaryComplete must reflect exactly one complete top-level
// value plus trailing whitespace, and FinishOversizedUnary must apply
// the salvaged status.
func TestUnaryScannerHistories(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		complete bool
	}{
		{"complete", `{"status":"incomplete","model":"m"}`, true},
		{"trailing-whitespace", `{"status":"incomplete"}` + " \n\t", true},
		{"trailing-garbage", `{"status":"completed"}x`, false},
		{"malformed", `{"status":`, false},
		{"second-value", `{"a":1}{"b":2}`, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			call := newResponsesCall(wiretap.Route{}, 1<<20)
			call.BeginOversizedUnary()
			for offset := 0; offset < len(testCase.body); offset += 3 {
				call.FeedOversized([]byte(testCase.body[offset:min(offset+3, len(testCase.body))]))
			}
			if got := call.UnaryComplete(); got != testCase.complete {
				t.Fatalf("UnaryComplete = %v, want %v", got, testCase.complete)
			}
			if testCase.complete {
				call.FinishOversizedUnary(200)
				result := call.Result()
				if !result.TelemetryPartial {
					t.Error("over-cap unary must declare partial telemetry")
				}
				if strings.Contains(testCase.body, `"incomplete"`) && !result.Incomplete {
					t.Error("salvaged incomplete status lost")
				}
			}
		})
	}
}
