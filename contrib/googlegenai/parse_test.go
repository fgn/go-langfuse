package langfusegenai

import (
	"strings"
	"testing"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/googlegenai/internal/wiretap"
)

func generationRoute() wiretap.Route {
	return wiretap.Route{
		Name: "genai.generate_content", Model: "gemini-3.6-flash",
		Type: langfuse.TypeGeneration,
	}
}

func TestParseRequestGenerationConfigAllowlist(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	call.ParseRequest([]byte(`{
		"contents": [{"role":"user","parts":[{"text":"question"}]}],
		"systemInstruction": {"parts":[{"text":"be terse"}]},
		"generationConfig": {
			"temperature": 0.3, "topK": 40, "maxOutputTokens": 512,
			"stopSequences": ["SECRET-STOP"],
			"responseSchema": {"type":"object","description":"SECRET-SCHEMA"}
		}
	}`))
	result := call.Result()
	if result.ModelParameters["temperature"] != 0.3 || result.ModelParameters["topK"] != 40.0 {
		t.Fatalf("allowlisted parameters missing: %v", result.ModelParameters)
	}
	for key := range result.ModelParameters {
		if key == "stopSequences" || key == "responseSchema" {
			t.Fatalf("content-bearing key %q leaked into model parameters", key)
		}
	}
	input, ok := result.Input.(map[string]any)
	if !ok {
		t.Fatalf("input shape %T", result.Input)
	}
	if input["system_instruction"] == nil || input["contents"] == nil {
		t.Fatalf("input missing sections: %v", input)
	}
}

func TestParseRequestMediaPlaceholders(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	call.ParseRequest([]byte(`{
		"contents": [{"role":"user","parts":[
			{"text":"describe"},
			{"inlineData":{"mimeType":"image/png","data":"BASE64SECRET"}},
			{"fileData":{"fileUri":"gs://bucket/SECRET-URI"}}
		]}]
	}`))
	rendered := renderJSON(t, call.Result().Input)
	if strings.Contains(rendered, "BASE64SECRET") || strings.Contains(rendered, "SECRET-URI") {
		t.Fatalf("media leaked: %s", rendered)
	}
	if !strings.Contains(rendered, `"media":"omitted"`) || !strings.Contains(rendered, "describe") {
		t.Fatalf("placeholder or text missing: %s", rendered)
	}
}

func TestStreamAccumulationAndUsageMapping(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	chunks := []string{
		`{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello "}]}}],"modelVersion":"gemini-3.6-flash-002"}`,
		`{"candidates":[{"content":{"role":"model","parts":[{"text":"thinking...","thought":true}]}}]}`,
		`{"candidates":[{"content":{"role":"model","parts":[{"text":"world"}]},"finishReason":"STOP"}],
		  "usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":4,"thoughtsTokenCount":6,"cachedContentTokenCount":2}}`,
		`{"candidates":[{"content":{"role":"model","parts":[{"text":"!"}]}}]}`,
	}
	outputs := 0
	for _, chunk := range chunks {
		verdict := call.FeedEvent([]byte(chunk))
		if verdict.Terminal != wiretap.TerminalNone {
			t.Fatalf("chunk treated as terminal: %q", chunk)
		}
		if verdict.Output {
			outputs++
		}
	}
	// The thought-only chunk is reasoning, not output.
	if outputs != 3 {
		t.Fatalf("output-bearing chunks = %d, want 3", outputs)
	}
	// Content after finishReason STOP must still accumulate (locked by
	// the genai SDK's own behavior), and the thought part is retained
	// marked in the exported content without being output-bearing.
	result := call.Result()
	composite, ok := result.Output.(map[string]any)
	if !ok {
		t.Fatalf("output shape %T: %v", result.Output, result.Output)
	}
	if composite["text"] != "Hello world!" {
		t.Fatalf("accumulated text %q", composite["text"])
	}
	rendered := renderJSON(t, composite["parts"])
	if !strings.Contains(rendered, `"thought":true`) || !strings.Contains(rendered, "thinking...") {
		t.Fatalf("thought part not retained marked: %s", rendered)
	}
	if result.Model != "gemini-3.6-flash-002" {
		t.Fatalf("modelVersion override missing: %q", result.Model)
	}
	usage := result.Usage
	if usage == nil || usage.InputTokens != 10 || usage.OutputTokens != 10 ||
		usage.ReasoningOutputTokens != 6 || usage.CacheReadInputTokens != 2 {
		t.Fatalf("usage mapping %+v; OutputTokens must include thoughts", usage)
	}
	if result.Metadata["finish_reason"] != "STOP" {
		t.Fatalf("finish reason metadata %v", result.Metadata)
	}
	// Provenance split: the transport promotes only the validated
	// response model; the URL-derived request model travels separately.
	if result.RequestModel != "gemini-3.6-flash" {
		t.Fatalf("request model provenance %q", result.RequestModel)
	}
}

func TestStreamCleanEOFProbe(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	if verdict := call.FeedEvent(nil); verdict.Terminal != wiretap.TerminalNone {
		t.Fatal("zero-event stream EOF reported success")
	}
	call.FeedEvent([]byte(`{"candidates":[{"content":{"parts":[{"text":"x"}]}}]}`))
	if verdict := call.FeedEvent(nil); verdict.Terminal != wiretap.TerminalSuccess {
		t.Fatal("clean EOF after events did not report success")
	}
}

func TestStreamErrorEvent(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	verdict := call.FeedEvent([]byte(`{"error":{"code":429,"message":"SECRET quota"}}`))
	if verdict.Terminal != wiretap.TerminalError {
		t.Fatal("error event not terminal")
	}
	if call.Result().ErrorCategory != "provider error" {
		t.Fatalf("error category %q", call.Result().ErrorCategory)
	}
}

func TestUnaryFunctionCallOutput(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	call.FinishUnary([]byte(`{
		"candidates":[{"content":{"role":"model","parts":[
			{"functionCall":{"name":"lookup","args":{"q":"go"}}}
		]},"finishReason":"STOP"}],
		"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}
	}`), 200)
	rendered := renderJSON(t, call.Result().Output)
	if !strings.Contains(rendered, `"functionCall"`) || !strings.Contains(rendered, `"lookup"`) {
		t.Fatalf("function call output missing: %s", rendered)
	}
}

func TestUnaryEmbeddingCounts(t *testing.T) {
	batch := &call{route: wiretap.Route{
		Name: "genai.batch_embed_contents",
		Type: langfuse.TypeEmbedding,
	}, captureCap: 1 << 16}
	batch.FinishUnary([]byte(`{"embeddings":[{"values":[0.1]},{"values":[0.2]},{"values":[0.3]}]}`), 200)
	if batch.Result().Metadata["embeddings"] != 3 {
		t.Fatalf("batch embedding count %v", batch.Result().Metadata)
	}
	predict := &call{route: wiretap.Route{
		Name: "genai.predict",
		Type: langfuse.TypeEmbedding,
	}, captureCap: 1 << 16}
	predict.FinishUnary([]byte(`{"predictions":[{"embeddings":{"values":[0.1]}}]}`), 200)
	if predict.Result().Metadata["embeddings"] != 1 {
		t.Fatalf("predict embedding count %v", predict.Result().Metadata)
	}
}

func TestOverCapDeltaDropsOutputKeepsUsage(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 16}
	call.FeedEvent([]byte(`{"candidates":[{"content":{"parts":[{"text":"0123456789ABCDEF-overflow"}]}}]}`))
	call.FeedEvent([]byte(`{"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1}}`))
	result := call.Result()
	if result.Output != nil {
		t.Fatalf("over-cap output not dropped: %v", result.Output)
	}
	if !result.TelemetryPartial {
		t.Fatal("over-cap drop not reported as partial telemetry")
	}
	if result.Usage == nil || result.Usage.InputTokens != 2 {
		t.Fatalf("usage lost after over-cap drop: %+v", result.Usage)
	}
}

// TestToolOnlyStreamExportsOutput locks that a function-call-only
// stream still exports Output (sanitized parts) while stamping
// completion start via its output-bearing verdicts.
func TestToolOnlyStreamExportsOutput(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	verdict := call.FeedEvent([]byte(`{"candidates":[{"content":{"role":"model","parts":[
		{"functionCall":{"name":"lookup","args":{"q":"go"}}}
	]}}]}`))
	if !verdict.Output {
		t.Fatal("function-call delta not output-bearing")
	}
	rendered := renderJSON(t, call.Result().Output)
	if !strings.Contains(rendered, `"functionCall"`) {
		t.Fatalf("tool-only stream lost output: %s", rendered)
	}
}

// TestNullErrorFieldIsNotAnError locks the explicit-null distinction.
func TestNullErrorFieldIsNotAnError(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	verdict := call.FeedEvent([]byte(`{"error":null,"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`))
	if verdict.Terminal != wiretap.TerminalNone {
		t.Fatal("explicit null error treated as provider error")
	}
}

// TestToolUsePromptTokensJoinInclusiveInput locks the v1.59.0 usage
// bucket: toolUsePromptTokenCount joins the inclusive input side with
// its own detail bucket.
func TestToolUsePromptTokensJoinInclusiveInput(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 1 << 16}
	call.FinishUnary([]byte(`{
		"candidates":[{"content":{"parts":[{"text":"x"}]}}],
		"usageMetadata":{"promptTokenCount":10,"toolUsePromptTokenCount":4,"candidatesTokenCount":2}
	}`), 200)
	usage := call.Result().Usage
	if usage.InputTokens != 14 || usage.Details["input_tool_use_tokens"] != 4 {
		t.Fatalf("tool-use usage mapping %+v", usage)
	}
}

// TestFinishReasonBudgetFieldAware locks the Google counterpart of the
// field-aware metadata budget: over-cap finish reasons are dropped and
// marked partial without erasing accumulated output.
func TestFinishReasonBudgetFieldAware(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 8}
	call.FeedEvent([]byte(`{"candidates":[{"content":{"parts":[{"text":"seven77"}]}}]}`))
	call.FeedEvent([]byte(`{"candidates":[{"finishReason":"REASON_EXCEEDING_THE_CAP","content":{"parts":[]}}]}`))
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

// TestSameCandidateFinishReasonCannotStarveOutput locks review round 4
// finding 22 for the established Gemini shape combining final text
// with finishReason in one candidate.
func TestSameCandidateFinishReasonCannotStarveOutput(t *testing.T) {
	call := &call{route: generationRoute(), captureCap: 8}
	call.FeedEvent([]byte(`{"candidates":[{"content":{"parts":[{"text":"seven77"}]},"finishReason":"STOP"}]}`))
	result := call.Result()
	if result.Output != "seven77" {
		t.Fatalf("same-candidate finish reason starved output: %v", result.Output)
	}
	if len(call.finishReasons) != 0 || !result.TelemetryPartial {
		t.Fatalf("over-budget reason handling: %v partial=%v", call.finishReasons, result.TelemetryPartial)
	}
}
