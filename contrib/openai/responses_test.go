package langfuseopenai_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

func postResponses(t *testing.T, httpClient *http.Client, baseURL, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		baseURL+"/v1/responses", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func drainAndClose(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return string(body)
}

const responsesUnaryBody = `{
  "id": "resp-1", "status": "completed", "model": "gpt-5-mini-2025",
  "output": [
    {"type": "reasoning", "id": "rs-1", "encrypted_content": "SECRET-ENCRYPTED",
     "summary": [{"type": "summary_text", "text": "thinking aloud"}]},
    {"type": "message", "id": "msg-1", "status": "completed", "role": "assistant",
     "content": [
       {"type": "output_text", "text": "SECRET-OUTPUT hello",
        "logprobs": [{"token": "SECRET-LOGPROB"}],
        "annotations": [{"type": "url_citation", "url": "https://SECRET-URL"}]},
       {"type": "refusal", "refusal": "cannot do that"}]},
    {"type": "function_call", "id": "fc-1", "name": "get_weather",
     "arguments": "{\"city\":\"Oslo\"}", "call_id": "call-1"},
    {"type": "image_generation_call", "id": "ig-1", "result": "SECRET-BASE64"},
    {"type": "wild_future_item", "payload": "SECRET-FUTURE"}
  ],
  "usage": {"input_tokens": 20, "output_tokens": 10, "total_tokens": 30,
    "input_tokens_details": {"cached_tokens": 4},
    "output_tokens_details": {"reasoning_tokens": 3}}
}`

const responsesRequestBody = `{
  "model": "gpt-5-mini",
  "instructions": "be terse",
  "input": [
    {"role": "user", "content": [
      {"type": "input_text", "text": "SECRET-INPUT question"},
      {"type": "input_image", "image_url": "data:image/png;base64,SECRET-IMAGE"},
      {"type": "input_file", "file_data": "SECRET-FILEDATA", "filename": "s.pdf"}]},
    {"role": "assistant", "content": "earlier answer"},
    {"type": "function_call_output", "call_id": "call-0", "output": "SECRET-TOOLOUT ok"},
    {"type": "item_reference", "id": "msg-0"},
    {"type": "computer_call", "action": {"type": "screenshot"}}
  ],
  "prompt": {"id": "tpl-1", "version": "3", "variables": {
    "city": "Oslo",
    "styled": {"type": "input_text", "text": "formal"},
    "photo": {"type": "input_image", "image_url": "data:SECRET-VARIMAGE"}}},
  "temperature": 0.2, "top_p": 0.9, "max_output_tokens": 128,
  "max_tool_calls": 2, "top_logprobs": 3, "parallel_tool_calls": false,
  "background": false, "store": true, "service_tier": "auto",
  "truncation": "auto", "reasoning": {"effort": "high"},
  "include": ["message.output_text.logprobs"],
  "metadata": {"secret_meta": "SECRET-METADATA"},
  "previous_response_id": "resp-0", "prompt_cache_key": "SECRET-CACHEKEY",
  "safety_identifier": "SECRET-SAFETY", "user": "SECRET-USER",
  "tools": [{"type": "function", "function": {"name": "f", "description": "SECRET-TOOLDEF"}}],
  "tool_choice": "auto", "text": {"format": {"type": "json_schema", "schema": {"x": "SECRET-SCHEMA"}}}
}`

func TestResponsesUnaryExportsGeneration(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, responsesUnaryBody)
	})

	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postResponses(t, httpClient, provider.URL, responsesRequestBody)
	if body := drainAndClose(t, resp); body != responsesUnaryBody {
		t.Fatalf("application bytes altered: %q", body)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := span.GetName(); got != "openai.responses" {
		t.Fatalf("span name %q", got)
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "gpt-5-mini-2025" {
		t.Fatalf("model %q", got)
	}

	input := attrString(t, span, "langfuse.observation.input")
	if !strings.Contains(input, "SECRET-INPUT question") || !strings.Contains(input, "be terse") {
		t.Fatalf("input missing text/instructions: %q", input)
	}
	if !strings.Contains(input, "earlier answer") || !strings.Contains(input, "SECRET-TOOLOUT ok") {
		t.Fatalf("assistant message or tool output missing: %q", input)
	}
	if !strings.Contains(input, `"city":"Oslo"`) && !strings.Contains(input, `"city": "Oslo"`) {
		t.Fatalf("scalar prompt variable missing: %q", input)
	}
	for _, secret := range []string{
		"SECRET-IMAGE", "SECRET-FILEDATA", "SECRET-VARIMAGE", "SECRET-METADATA",
		"SECRET-CACHEKEY", "SECRET-SAFETY", "SECRET-USER", "SECRET-TOOLDEF", "SECRET-SCHEMA",
	} {
		if strings.Contains(input, secret) {
			t.Fatalf("excluded or media field leaked %s into input: %q", secret, input)
		}
	}

	parameters := attrString(t, span, "langfuse.observation.model.parameters")
	for _, want := range []string{"temperature", "top_p", "max_output_tokens", "max_tool_calls", "top_logprobs", "parallel_tool_calls"} {
		if !strings.Contains(parameters, want) {
			t.Fatalf("parameter %s missing: %q", want, parameters)
		}
	}
	for _, excluded := range []string{"store", "background", "service_tier", "truncation", "reasoning"} {
		if strings.Contains(parameters, excluded) {
			t.Fatalf("excluded field %s in parameters: %q", excluded, parameters)
		}
	}

	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "SECRET-OUTPUT hello") || !strings.Contains(output, "cannot do that") {
		t.Fatalf("message content missing: %q", output)
	}
	if !strings.Contains(output, "get_weather") || !strings.Contains(output, "thinking aloud") {
		t.Fatalf("function call or reasoning summary missing: %q", output)
	}
	for _, secret := range []string{"SECRET-ENCRYPTED", "SECRET-LOGPROB", "SECRET-URL", "SECRET-BASE64", "SECRET-FUTURE", "msg-1", "wild_future_item"} {
		if strings.Contains(output, secret) {
			t.Fatalf("output leaked %s: %q", secret, output)
		}
	}
	if !strings.Contains(output, `"omitted":true`) && !strings.Contains(output, `"omitted": true`) {
		t.Fatalf("placeholder items missing: %q", output)
	}

	var usageMap map[string]int64
	if err := json.Unmarshal([]byte(attrString(t, span, "langfuse.observation.usage_details")), &usageMap); err != nil {
		t.Fatal(err)
	}
	if usageMap["input"]+usageMap["input_cached_tokens"] != 20 ||
		usageMap["output"]+usageMap["output_reasoning_tokens"] != 10 {
		t.Fatalf("usage buckets %v do not reconstruct 20/10", usageMap)
	}
}

func TestResponsesRetrievalRoutesPassThrough(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp-1","status":"completed"}`)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	for _, path := range []string{"/v1/responses/resp-1", "/v1/responses/resp-1/input_items"} {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, provider.URL+path, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		drainAndClose(t, resp)
	}
	flush(t, lf)
	receiver.expectNone(t)
}

func TestResponsesUnaryStatusMapping(t *testing.T) {
	cases := []struct {
		status     string
		wantLevel  string
		wantStatus string
	}{
		{"completed", "", ""},
		{"failed", "ERROR", "provider error"},
		{"incomplete", "WARNING", "incomplete"},
		{"in_progress", "WARNING", "incomplete"},
		{"queued", "WARNING", "incomplete"},
		{"cancelled", "WARNING", "incomplete"},
		{"someday_new", "WARNING", "incomplete"},
	}
	for _, testCase := range cases {
		t.Run(testCase.status, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			lf := newTestClient(t, receiver, nil)
			body := fmt.Sprintf(`{"id":"r","status":%q,"model":"m","output":[]}`, testCase.status)
			provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			})
			httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
			resp := postResponses(t, httpClient, provider.URL, `{"model":"m","input":"q"}`)
			drainAndClose(t, resp)
			flush(t, lf)
			span := receiver.nextSpan(t)
			if got := attrString(t, span, "langfuse.observation.level"); got != testCase.wantLevel {
				t.Fatalf("level %q, want %q", got, testCase.wantLevel)
			}
			if got := attrString(t, span, "langfuse.observation.status_message"); got != testCase.wantStatus {
				t.Fatalf("status %q, want %q", got, testCase.wantStatus)
			}
		})
	}
}

func TestResponsesUnaryDuplicateControlKeysAreSchemaInvalid(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed","status":"failed","model":"m"}`)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postResponses(t, httpClient, provider.URL, `{"model":"m","input":"q"}`)
	drainAndClose(t, resp)
	flush(t, lf)
	span := receiver.nextSpan(t)
	// Malformed-body rule: no status extracted, partial telemetry, the
	// clean wire lifecycle stands.
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "telemetry_partial" {
		t.Fatalf("status %q, want telemetry_partial", got)
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "" {
		t.Fatalf("duplicate-key body must yield no control fields, got model %q", got)
	}
}

func sseBody(events ...string) string {
	var builder strings.Builder
	for _, event := range events {
		builder.WriteString("data: ")
		builder.WriteString(event)
		builder.WriteString("\n\n")
	}
	return builder.String()
}

func runResponsesStream(t *testing.T, body string) (*otlpReceiver, func()) {
	t.Helper()
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postResponses(t, httpClient, provider.URL, `{"model":"m","input":"q","stream":true}`)
	drainAndClose(t, resp)
	return receiver, func() { flush(t, lf) }
}

const responsesTerminal = `{"type":"response.completed","response":{"id":"r","status":"completed","model":"gpt-5-mini-2025",` +
	`"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"final answer"}]}],` +
	`"usage":{"input_tokens":5,"output_tokens":7,"total_tokens":12}}}`

func TestResponsesStreamTerminalOutputIsAuthoritative(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.created","response":{"id":"r","status":"in_progress"}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg-1"}}`,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"partial "}`,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"text"}`,
		responsesTerminal,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"IGNORED-TRAILING"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "final answer") {
		t.Fatalf("terminal output missing: %q", output)
	}
	if strings.Contains(output, "partial ") || strings.Contains(output, "IGNORED-TRAILING") {
		t.Fatalf("terminal output must replace accumulation and freeze: %q", output)
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "gpt-5-mini-2025" {
		t.Fatalf("model %q", got)
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "" {
		t.Fatalf("status %q, want clean", got)
	}
}

func TestResponsesStreamEOFBeforeTerminalIsIncompleteWithFallback(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"partial answer"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q, want incomplete", got)
	}
	if output := attrString(t, span, "langfuse.observation.output"); !strings.Contains(output, "partial answer") {
		t.Fatalf("fallback output missing: %q", output)
	}
}

func TestResponsesStreamFailedAndErrorEvents(t *testing.T) {
	for name, events := range map[string][]string{
		"failed": {
			`{"type":"response.failed","response":{"id":"r","status":"failed","error":{"code":"sk-SECRET-CODE","message":"boom"}}}`,
		},
		"error": {
			`{"type":"error","code":"sk-SECRET-CODE","message":"boom","param":"input"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			receiver, flushFn := runResponsesStream(t, sseBody(events...))
			flushFn()
			span := receiver.nextSpan(t)
			if got := attrString(t, span, "langfuse.observation.status_message"); got != "provider error" {
				t.Fatalf("status %q, want the fixed provider error category", got)
			}
			// The provider code may appear only inside the Mask-governed
			// output channel, never in status or events.
			if strings.Contains(attrString(t, span, "langfuse.observation.status_message"), "SECRET-CODE") {
				t.Fatal("provider code leaked into status")
			}
			for _, event := range span.GetEvents() {
				for _, attribute := range event.GetAttributes() {
					if strings.Contains(attribute.GetValue().GetStringValue(), "SECRET-CODE") {
						t.Fatal("provider code leaked into the exception event")
					}
				}
			}
			if output := attrString(t, span, "langfuse.observation.output"); !strings.Contains(output, "boom") {
				t.Fatalf("sanitized error object missing from output: %q", output)
			}
		})
	}
}

func TestResponsesStreamIncompleteTerminalFreezes(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"cut "}`,
		`{"type":"response.incomplete","response":{"id":"r","status":"incomplete","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"cut short"}]}]}}`,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"IGNORED"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q, want incomplete", got)
	}
	if output := attrString(t, span, "langfuse.observation.output"); !strings.Contains(output, "cut short") || strings.Contains(output, "IGNORED") {
		t.Fatalf("incomplete terminal output wrong: %q", output)
	}
}

func TestResponsesStreamDoneSentinelIsNotSuccess(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"x"}`,
		`[DONE]`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q, want incomplete ([DONE] is not a Responses terminal)", got)
	}
}

func TestResponsesStreamDoneEventsReplaceDeltas(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"dup "}`,
		`{"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"m1","text":"clean text"}`,
		`{"type":"response.function_call_arguments.delta","output_index":1,"item_id":"f1","delta":"{\"ci"}`,
		`{"type":"response.function_call_arguments.done","output_index":1,"item_id":"f1","arguments":"{\"city\":\"Oslo\"}"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "clean text") || strings.Contains(output, "dup ") {
		t.Fatalf("done must replace deltas: %q", output)
	}
	if !strings.Contains(output, `{\"city\":\"Oslo\"}`) && !strings.Contains(output, `{"city":"Oslo"}`) {
		if !strings.Contains(output, "Oslo") {
			t.Fatalf("done arguments missing: %q", output)
		}
	}
	if strings.Contains(output, `{\"ci`) && !strings.Contains(output, "Oslo") {
		t.Fatalf("stale partial arguments exported: %q", output)
	}
}

func TestResponsesStreamItemIdentityConflictsRejected(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"kept"}`,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"DIFFERENT","delta":"REJECTED"}`,
		`{"type":"response.output_text.delta","output_index":-1,"content_index":0,"item_id":"x","delta":"NEGATIVE"}`,
		`{"type":"response.output_text.delta","output_index":9999999,"content_index":0,"item_id":"y","delta":"HUGE"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "kept") {
		t.Fatalf("valid content missing: %q", output)
	}
	for _, rejected := range []string{"REJECTED", "NEGATIVE", "HUGE"} {
		if strings.Contains(output, rejected) {
			t.Fatalf("hostile event retained %s: %q", rejected, output)
		}
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q", got)
	}
}

func TestResponsesStreamReasoningAndAudioPlaceholders(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.reasoning_summary_text.delta","output_index":0,"item_id":"r1","delta":"quiet thoughts"}`,
		`{"type":"response.audio.transcript.delta","delta":"spoken words"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "quiet thoughts") {
		t.Fatalf("reasoning summary fallback missing: %q", output)
	}
	if strings.Contains(output, "spoken words") {
		t.Fatalf("transcript text must not be retained: %q", output)
	}
	if !strings.Contains(output, `"audio"`) {
		t.Fatalf("audio placeholder missing: %q", output)
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q", got)
	}
}

func TestResponsesOversizedTerminalSalvagesControlPlane(t *testing.T) {
	// A terminal event far beyond the 256 KiB cap: the scanner must
	// salvage status/model/usage while the accumulated delta output
	// serves as fallback, with partial telemetry declared.
	jumboText := strings.Repeat("x", 400<<10)
	jumbo := `{"type":"response.completed","response":{"id":"r","status":"completed","model":"gpt-5-mini-2025",` +
		`"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + jumboText + `"}]}],` +
		`"usage":{"input_tokens":5,"output_tokens":7,"total_tokens":12}}}`
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"fallback text"}`,
		jumbo,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "gpt-5-mini-2025" {
		t.Fatalf("salvaged model %q", got)
	}
	var usageMap map[string]int64
	if err := json.Unmarshal([]byte(attrString(t, span, "langfuse.observation.usage_details")), &usageMap); err != nil {
		t.Fatal(err)
	}
	if usageMap["input"] != 5 {
		t.Fatalf("salvaged usage %v", usageMap)
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "telemetry_partial" {
		t.Fatalf("status %q, want telemetry_partial (over-cap output omitted)", got)
	}
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "fallback text") {
		t.Fatalf("fallback output missing: %q", output)
	}
	if strings.Contains(output, "xxxxxxxxxx") {
		t.Fatalf("over-cap output leaked: %q", output)
	}
}

func TestResponsesOversizedNonTerminalDeltaKeepsStream(t *testing.T) {
	jumboDelta := `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"` +
		strings.Repeat("y", 300<<10) + `"}`
	receiver, flushFn := runResponsesStream(t, sseBody(
		jumboDelta,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"tail"}`,
		responsesTerminal,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "telemetry_partial" {
		t.Fatalf("status %q, want telemetry_partial", got)
	}
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "final answer") {
		t.Fatalf("terminal output missing after oversized delta: %q", output)
	}
	if strings.Contains(output, "yyyyyyyy") {
		t.Fatalf("oversized delta content leaked: %q", output)
	}
	if span.GetEndTimeUnixNano() == 0 {
		t.Fatal("span must have ended")
	}
}

func TestResponsesOversizedUnaryBodySalvagesStatus(t *testing.T) {
	pad := strings.Repeat("z", 600<<10)
	body := `{"id":"r","status":"incomplete","model":"gpt-5-mini-2025",` +
		`"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + pad + `"}]}],` +
		`"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}`
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postResponses(t, httpClient, provider.URL, `{"model":"m","input":"q"}`)
	drainAndClose(t, resp)
	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q, want the salvaged incomplete", got)
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "gpt-5-mini-2025" {
		t.Fatalf("salvaged model %q", got)
	}
	output := attrString(t, span, "langfuse.observation.output")
	if strings.Contains(output, "zzzzzz") {
		t.Fatalf("over-cap unary output leaked: %q", output)
	}
	if !strings.Contains(output, "omitted") {
		t.Fatalf("placeholder missing: %q", output)
	}
}

func TestResponsesTombstoneForOversizedDoneItem(t *testing.T) {
	bigItem := `{"type":"message","id":"m1","role":"assistant","content":[{"type":"output_text","text":"` +
		strings.Repeat("w", 80<<10) + `"}]}`
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"stale delta"}`,
		`{"type":"response.output_item.done","output_index":0,"item_id":"m1","item":`+bigItem+`}`,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"resurrected"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if strings.Contains(output, "stale delta") || strings.Contains(output, "resurrected") || strings.Contains(output, "wwww") {
		t.Fatalf("tombstone must bar stale and oversized content: %q", output)
	}
	if !strings.Contains(output, "omitted") {
		t.Fatalf("tombstone missing: %q", output)
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q", got)
	}
}

func TestResponsesTerminalFirstOutputStampsCompletionStart(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.created","response":{"id":"r","status":"in_progress"}}`,
		responsesTerminal,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if !hasAttr(span, "langfuse.observation.completion_start_time") {
		t.Fatal("a terminal carrying the first visible output must stamp completion start")
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "" {
		t.Fatalf("status %q, want clean", got)
	}
}

func TestResponsesRejectedContentNeverStampsCompletionStart(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":-1,"content_index":0,"item_id":"x","delta":"REJECTED"}`,
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"a","delta":""}`,
		`{"type":"response.reasoning_summary_text.delta","output_index":0,"item_id":"a","delta":"thoughts"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if hasAttr(span, "langfuse.observation.completion_start_time") {
		t.Fatal("rejected, empty, and reasoning content must never stamp completion start")
	}
}

func TestResponsesTerminalSeverityMismatch(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.completed","response":{"id":"r","status":"failed","model":"m","output":[]}}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	// The embedded class outranks the milder event type; the mismatch
	// itself degrades telemetry but failed wins the status.
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "provider error" {
		t.Fatalf("status %q, want the more severe provider error", got)
	}
}

func TestResponsesDuplicateResponseMemberIsNotATerminal(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.completed","response":{"status":"completed"},"response":{"status":"failed"}}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q, want incomplete (duplicate terminal response is schema-invalid)", got)
	}
}

func TestResponsesSchemaInvalidTerminalFieldsAreNotTerminals(t *testing.T) {
	for name, terminal := range map[string]string{
		"status-number":    `{"type":"response.completed","response":{"status":123}}`,
		"response-string":  `{"type":"response.completed","response":"done"}`,
		"missing-response": `{"type":"response.completed"}`,
		"usage-array":      `{"type":"response.completed","response":{"status":"completed","usage":[1]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			receiver, flushFn := runResponsesStream(t, sseBody(terminal))
			flushFn()
			span := receiver.nextSpan(t)
			if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
				t.Fatalf("status %q, want incomplete (schema-invalid payloads yield no hard verdict)", got)
			}
		})
	}
}

func TestResponsesBufferedFieldCapDropMatchesSalvage(t *testing.T) {
	// A under-cap event whose usage exceeds its 4 KiB field cap: the
	// buffered parse must omit exactly what salvage would, with partial
	// telemetry, while status still lands.
	hugeUsage := `{"input_tokens":1,"output_tokens":2,"total_tokens":3,"pad":"` + strings.Repeat("u", 8<<10) + `"}`
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.completed","response":{"id":"r","status":"completed","model":"m","usage":`+hugeUsage+`,"output":[]}}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "telemetry_partial" {
		t.Fatalf("status %q, want telemetry_partial", got)
	}
	if hasAttr(span, "langfuse.observation.usage_details") {
		t.Fatal("an over-cap usage field must be dropped whole from the buffered parse too")
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "m" {
		t.Fatalf("later fields must survive the drop; model %q", got)
	}
}

func TestResponsesNestedItemIdentityConflictRejected(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m1","delta":"kept"}`,
		// The real done event carries its ID inside the item; a
		// conflicting nested identity must not replace state.
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m2","role":"assistant","content":[{"type":"output_text","text":"HIJACKED"}]}}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if strings.Contains(output, "HIJACKED") || !strings.Contains(output, "kept") {
		t.Fatalf("conflicting nested identity replaced state: %q", output)
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
		t.Fatalf("status %q", got)
	}
}

func TestResponsesNoDeltaOversizedDoneKeepsDiscriminator(t *testing.T) {
	bigItem := `{"type":"message","id":"m1","role":"assistant","content":[{"type":"output_text","text":"` +
		strings.Repeat("w", 80<<10) + `"}]}`
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_item.done","output_index":0,"item":`+bigItem+`}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, `"message"`) || !strings.Contains(output, "omitted") {
		t.Fatalf("tombstone must keep the real discriminator without prior stream state: %q", output)
	}
	if strings.Contains(output, "wwww") {
		t.Fatalf("tombstoned content leaked: %q", output)
	}
}

func TestResponsesEasyAssistantMessageIsNotTombstoned(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"r","status":"completed","model":"m","output":[]}`)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	// An EasyInputMessage assistant item with explicit type and scalar
	// content is a pinned wire shape and must survive sanitization.
	resp := postResponses(t, httpClient, provider.URL,
		`{"model":"m","input":[
		  {"type":"message","role":"assistant","content":"earlier scalar answer"},
		  {"type":"message","role":"developer","content":[{"type":"input_text","text":"dev note"}]},
		  {"type":"message","role":"invalid-role","content":"x"}]}`)
	drainAndClose(t, resp)
	flush(t, lf)
	span := receiver.nextSpan(t)
	input := attrString(t, span, "langfuse.observation.input")
	if !strings.Contains(input, "earlier scalar answer") || !strings.Contains(input, "dev note") {
		t.Fatalf("pinned Easy message forms lost: %q", input)
	}
	if strings.Contains(input, "invalid-role") {
		t.Fatalf("an unpinned role must tombstone, not copy through: %q", input)
	}
}

func TestResponsesOversizedSchemaInvalidTerminalsAreNotTerminals(t *testing.T) {
	pad := `,"pad":"` + strings.Repeat("p", 300<<10) + `"`
	for name, terminal := range map[string]string{
		"status-number":    `{"type":"response.completed","response":{"status":123` + pad + `}}`,
		"missing-response": `{"type":"response.completed"` + pad + `}`,
		"response-string":  `{"type":"response.completed","response":"done"` + pad + `}`,
		"usage-array":      `{"type":"response.completed","response":{"status":"completed","usage":[1]` + pad + `}}`,
	} {
		t.Run(name, func(t *testing.T) {
			receiver, flushFn := runResponsesStream(t, sseBody(terminal))
			flushFn()
			span := receiver.nextSpan(t)
			got := attrString(t, span, "langfuse.observation.status_message")
			if got != "incomplete" && got != "telemetry_partial" {
				t.Fatalf("status %q; an oversized schema-invalid terminal must not freeze the stream", got)
			}
			if got != "incomplete" {
				t.Fatalf("status %q, want incomplete (EOF without a valid terminal)", got)
			}
		})
	}
}

func TestResponsesBufferedDecodedModelCapMatchesSalvage(t *testing.T) {
	longModel := strings.Repeat("m", 257)
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.completed","response":{"id":"r","status":"completed","model":"`+longModel+`","output":[]}}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "" {
		t.Fatalf("a 257-byte model must be dropped on the buffered path too; got %d bytes", len(got))
	}
	if got := attrString(t, span, "langfuse.observation.status_message"); got != "telemetry_partial" {
		t.Fatalf("status %q, want telemetry_partial for the dropped field", got)
	}
}

func TestResponsesTerminalMediaResultBearsOutput(t *testing.T) {
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.completed","response":{"id":"r","status":"completed","model":"m",`+
			`"output":[{"type":"image_generation_call","id":"ig","result":"BASE64"}]}}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if !hasAttr(span, "langfuse.observation.completion_start_time") {
		t.Fatal("a terminal-first media placeholder with a present media field must stamp completion start")
	}
	output := attrString(t, span, "langfuse.observation.output")
	if strings.Contains(output, "BASE64") || !strings.Contains(output, "omitted") {
		t.Fatalf("media must be a placeholder: %q", output)
	}
}

func TestResponsesStalePartialImageAfterTombstoneIsNotBearing(t *testing.T) {
	bigItem := `{"type":"image_generation_call","id":"ig","result":"` + strings.Repeat("b", 80<<10) + `"}`
	receiver, flushFn := runResponsesStream(t, sseBody(
		`{"type":"response.output_item.done","output_index":0,"item":`+bigItem+`}`,
		`{"type":"response.image_generation_call.partial_image","output_index":0,"item_id":"ig","partial_image_b64":"STALE"}`,
	))
	flushFn()
	span := receiver.nextSpan(t)
	if hasAttr(span, "langfuse.observation.completion_start_time") {
		t.Fatal("a stale media chunk on a tombstoned identity must not stamp completion start")
	}
}

// TestResponsesClosedEnvelopeTable drives every closed-envelope member
// through wrong-kind, null, and valid cases on BOTH the buffered and
// salvage paths, plus duplicates and a malformed tail after an early
// terminal type: the two paths must classify identically.
func TestResponsesClosedEnvelopeTable(t *testing.T) {
	pad := `,"pad":"` + strings.Repeat("p", 300<<10) + `"`
	cases := []struct {
		name     string
		envelope string // response object contents
		valid    bool
	}{
		{"valid-minimal", `{"status":"completed"}`, true},
		{"valid-null-members", `{"status":"completed","usage":null,"error":null,"incomplete_details":null,"output":null}`, true},
		{"valid-full", `{"status":"completed","model":"m","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3},"output":[]}`, true},
		{"status-number", `{"status":123}`, false},
		{"model-object", `{"status":"completed","model":{}}`, false},
		{"usage-array", `{"status":"completed","usage":[1]}`, false},
		{"error-string", `{"status":"completed","error":"boom"}`, false},
		{"incomplete-details-array", `{"status":"completed","incomplete_details":[]}`, false},
		{"output-object", `{"status":"completed","output":{}}`, false},
	}
	for _, testCase := range cases {
		for _, oversized := range []bool{false, true} {
			suffix := "buffered"
			body := `{"type":"response.completed","response":` + testCase.envelope + `}`
			if oversized {
				suffix = "salvaged"
				body = `{"type":"response.completed","response":` + testCase.envelope + `` + pad + `}`
			}
			t.Run(testCase.name+"-"+suffix, func(t *testing.T) {
				receiver, flushFn := runResponsesStream(t, sseBody(body))
				flushFn()
				span := receiver.nextSpan(t)
				got := attrString(t, span, "langfuse.observation.status_message")
				if testCase.valid && got == "incomplete" {
					t.Fatalf("%s: valid envelope classified incomplete", suffix)
				}
				if !testCase.valid && got != "incomplete" {
					t.Fatalf("%s: schema-invalid envelope yielded status %q, want incomplete", suffix, got)
				}
			})
		}
	}

	t.Run("malformed-after-early-type-salvaged", func(t *testing.T) {
		receiver, flushFn := runResponsesStream(t, sseBody(
			`{"type":"response.completed","response":{"status":"completed"`+pad+`},MALFORMED`))
		flushFn()
		span := receiver.nextSpan(t)
		if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
			t.Fatalf("status %q, want incomplete for a malformed oversized tail", got)
		}
	})
	t.Run("duplicate-response-salvaged", func(t *testing.T) {
		receiver, flushFn := runResponsesStream(t, sseBody(
			`{"type":"response.completed","response":{"status":"completed"},"response":{"status":"failed"`+pad+`}}`))
		flushFn()
		span := receiver.nextSpan(t)
		if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
			t.Fatalf("status %q, want incomplete for a duplicate response member", got)
		}
	})
}

// TestResponsesDecodedCapBoundaryTable locks the exact decoded caps at
// 255/256/257 for plain and escaped spellings on the buffered and
// salvage paths, SSE and unary alike.
func TestResponsesDecodedCapBoundaryTable(t *testing.T) {
	pad := `,"pad":"` + strings.Repeat("p", 300<<10) + `"`
	unaryPad := `,"pad":"` + strings.Repeat("p", 600<<10) + `"`
	// Models beyond the transport's 128-char shape gate export as the
	// Mask-governed unvalidated_model metadata; the scanner's 256-byte
	// decoded cap decides whether the value survives AT ALL, and both
	// paths must agree exactly at the boundary. Escaped spellings decode
	// to the same lengths.
	spellings := map[string]func(int) string{
		"plain":   func(n int) string { return strings.Repeat("m", n) },
		"escaped": func(n int) string { return strings.Repeat(`\u006d`, n) },
	}

	for spelling, build := range spellings {
		for _, length := range []int{255, 256, 257} {
			kept := length <= 256
			model := build(length)
			t.Run(fmt.Sprintf("%s-%d-sse-buffered", spelling, length), func(t *testing.T) {
				receiver, flushFn := runResponsesStream(t, sseBody(
					`{"type":"response.completed","response":{"status":"completed","model":"`+model+`","output":[]}}`))
				flushFn()
				span := receiver.nextSpan(t)
				if got := hasAttr(span, "langfuse.observation.metadata.unvalidated_model"); got != kept {
					t.Fatalf("model retained = %v, want %v", got, kept)
				}
			})
			t.Run(fmt.Sprintf("%s-%d-sse-salvaged", spelling, length), func(t *testing.T) {
				receiver, flushFn := runResponsesStream(t, sseBody(
					`{"type":"response.completed","response":{"status":"completed","model":"`+model+`","output":[]`+pad+`}}`))
				flushFn()
				span := receiver.nextSpan(t)
				if got := hasAttr(span, "langfuse.observation.metadata.unvalidated_model"); got != kept {
					t.Fatalf("model retained = %v, want %v", got, kept)
				}
			})
			t.Run(fmt.Sprintf("%s-%d-unary-buffered", spelling, length), func(t *testing.T) {
				receiver := newOTLPReceiver(t)
				lf := newTestClient(t, receiver, nil)
				body := `{"status":"completed","model":"` + model + `","output":[]}`
				provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, body)
				})
				httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
				drainAndClose(t, postResponses(t, httpClient, provider.URL, `{"model":"m","input":"q"}`))
				flush(t, lf)
				span := receiver.nextSpan(t)
				if got := hasAttr(span, "langfuse.observation.metadata.unvalidated_model"); got != kept {
					t.Fatalf("model retained = %v, want %v", got, kept)
				}
			})
			t.Run(fmt.Sprintf("%s-%d-unary-salvaged", spelling, length), func(t *testing.T) {
				receiver := newOTLPReceiver(t)
				lf := newTestClient(t, receiver, nil)
				body := `{"status":"completed","model":"` + model + `","output":[]` + unaryPad + `}`
				provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, body)
				})
				httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
				drainAndClose(t, postResponses(t, httpClient, provider.URL, `{"model":"m","input":"q"}`))
				flush(t, lf)
				span := receiver.nextSpan(t)
				if got := hasAttr(span, "langfuse.observation.metadata.unvalidated_model"); got != kept {
					t.Fatalf("model retained = %v, want %v", got, kept)
				}
			})
		}
	}
}
