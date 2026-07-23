package langfuseopenai

import (
	"testing"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/openai/internal/wiretap"
)

func chatRoute() wiretap.Route {
	return wiretap.Route{Name: "openai.chat.completions", Type: langfuse.TypeGeneration}
}

// TestParallelToolCallsStayDistinct locks review round 2 finding 18:
// interleaved tool-call argument deltas accumulate by wire index and
// export as distinct structured calls.
func TestParallelToolCallsStayDistinct(t *testing.T) {
	call := &call{route: chatRoute(), captureCap: 1 << 16}
	chunks := []string{
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"name":"get_time","arguments":"{\"zone\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Oslo\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"\"CET\"}"}}]}}]}`,
	}
	for _, chunk := range chunks {
		verdict := call.FeedEvent([]byte(chunk))
		if !verdict.Output {
			t.Fatalf("tool delta not output-bearing: %s", chunk)
		}
	}
	output, ok := call.Result().Output.(map[string]any)
	if !ok {
		t.Fatalf("output shape %T", call.Result().Output)
	}
	calls, ok := output["tool_calls"].([]any)
	if !ok || len(calls) != 2 {
		t.Fatalf("tool calls %v", output["tool_calls"])
	}
	first, second := calls[0].(map[string]any), calls[1].(map[string]any)
	if first["name"] != "get_weather" || first["arguments"] != `{"city":"Oslo"}` {
		t.Fatalf("first tool corrupted: %v", first)
	}
	if second["name"] != "get_time" || second["arguments"] != `{"zone":"CET"}` {
		t.Fatalf("second tool corrupted: %v", second)
	}
}

// TestLegacyFunctionCallDeltas locks the deprecated function_call
// stream shape.
func TestLegacyFunctionCallDeltas(t *testing.T) {
	call := &call{route: chatRoute(), captureCap: 1 << 16}
	for _, chunk := range []string{
		`{"choices":[{"index":0,"delta":{"function_call":{"name":"lookup","arguments":"{\"q\":"}}}]}`,
		`{"choices":[{"index":0,"delta":{"function_call":{"arguments":"\"go\"}"}}}]}`,
	} {
		if !call.FeedEvent([]byte(chunk)).Output {
			t.Fatalf("legacy function_call not output-bearing: %s", chunk)
		}
	}
	output, ok := call.Result().Output.(map[string]any)
	if !ok {
		t.Fatalf("output shape %T", call.Result().Output)
	}
	tool := output["tool_calls"].([]any)[0].(map[string]any)
	if tool["name"] != "lookup" || tool["arguments"] != `{"q":"go"}` {
		t.Fatalf("legacy function call corrupted: %v", tool)
	}
}

// TestFinishReasonBytesCharged locks the field-aware budget: an
// over-cap finish reason is dropped and marked partial while valid
// accumulated output is preserved, never erased by metadata overflow.
func TestFinishReasonBytesCharged(t *testing.T) {
	call := &call{route: chatRoute(), captureCap: 8}
	call.FeedEvent([]byte(`{"choices":[{"index":0,"delta":{"content":"seven77"}}]}`))
	call.FeedEvent([]byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"this-reason-exceeds-the-cap"}]}`))
	result := call.Result()
	if !result.TelemetryPartial {
		t.Fatal("over-cap finish reason not reported as partial")
	}
	if len(call.finishReasons) != 0 {
		t.Fatalf("over-cap finish reason retained: %v", call.finishReasons)
	}
	if result.Output != "seven77" {
		t.Fatalf("metadata overflow erased valid output: %v", result.Output)
	}
}

// TestNullErrorAndAudioFieldsAreNotErrors locks the explicit-null
// distinctions.
func TestNullErrorAndAudioFieldsAreNotErrors(t *testing.T) {
	call := &call{route: chatRoute(), captureCap: 1 << 16}
	verdict := call.FeedEvent([]byte(`{"error":null,"choices":[{"index":0,"delta":{"content":"ok","audio":null}}]}`))
	if verdict.Terminal != wiretap.TerminalNone || !verdict.Output {
		t.Fatalf("null error/audio misclassified: %+v", verdict)
	}
	if call.Result().ErrorCategory != "" {
		t.Fatal("null error produced an error category")
	}
}

// TestSameEventFinishReasonCannotStarveOutput locks review round 4
// finding 22: output in the same event charges before the finish
// reason, so metadata can never spend the budget in-cap output needed.
func TestSameEventFinishReasonCannotStarveOutput(t *testing.T) {
	call := &call{route: chatRoute(), captureCap: 8}
	call.FeedEvent([]byte(`{"choices":[{"index":0,"delta":{"content":"seven77"},"finish_reason":"stop"}]}`))
	result := call.Result()
	if result.Output != "seven77" {
		t.Fatalf("same-event finish reason starved output: %v", result.Output)
	}
	if len(call.finishReasons) != 0 || !result.TelemetryPartial {
		t.Fatalf("over-budget reason handling: %v partial=%v", call.finishReasons, result.TelemetryPartial)
	}
}
