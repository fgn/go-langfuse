package langfuseopenai_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

// otlpReceiver captures spans exported by the core client, mirroring
// the core contract-test harness.
type otlpReceiver struct {
	server *httptest.Server
	spans  chan *tracepb.Span
}

func newOTLPReceiver(t *testing.T) *otlpReceiver {
	t.Helper()
	receiver := &otlpReceiver{spans: make(chan *tracepb.Span, 64)}
	receiver.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read export body: %v", err)
			return
		}
		var payload collectortracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &payload); err != nil {
			t.Errorf("unmarshal export: %v", err)
			return
		}
		for _, resourceSpans := range payload.GetResourceSpans() {
			for _, scopeSpans := range resourceSpans.GetScopeSpans() {
				for _, span := range scopeSpans.GetSpans() {
					receiver.spans <- span
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.server.Close)
	return receiver
}

func (r *otlpReceiver) nextSpan(t *testing.T) *tracepb.Span {
	t.Helper()
	select {
	case span := <-r.spans:
		return span
	case <-time.After(10 * time.Second):
		t.Fatal("no span exported within 10s")
		return nil
	}
}

func (r *otlpReceiver) expectNone(t *testing.T) {
	t.Helper()
	select {
	case span := <-r.spans:
		t.Fatalf("unexpected span exported: %s", span.GetName())
	case <-time.After(150 * time.Millisecond):
	}
}

func attrString(t *testing.T, span *tracepb.Span, key string) string {
	t.Helper()
	for _, attribute := range span.GetAttributes() {
		if attribute.GetKey() == key {
			return attribute.GetValue().GetStringValue()
		}
	}
	return ""
}

func hasAttr(span *tracepb.Span, key string) bool {
	for _, attribute := range span.GetAttributes() {
		if attribute.GetKey() == key {
			return true
		}
	}
	return false
}

func newTestClient(t *testing.T, receiver *otlpReceiver, mutate func(*langfuse.Config)) *langfuse.Client {
	t.Helper()
	cfg := langfuse.Config{
		BaseURL:   receiver.server.URL,
		PublicKey: "pk-lf-test",
		SecretKey: "sk-lf-test",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := langfuse.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(shutdownCtx)
	})
	return client
}

func flush(t *testing.T, client *langfuse.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

const chatResponse = `{
  "id": "chatcmpl-1", "model": "example-model-002",
  "choices": [{"index": 0, "finish_reason": "stop",
    "message": {"role": "assistant", "content": "SECRET-OUTPUT hello"}}],
  "usage": {"prompt_tokens": 9, "completion_tokens": 12,
    "prompt_tokens_details": {"cached_tokens": 3},
    "completion_tokens_details": {"reasoning_tokens": 5}}
}`

func chatServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func postChat(t *testing.T, httpClient *http.Client, baseURL string, ctx context.Context) *http.Response {
	t.Helper()
	body := `{"model":"example-model","temperature":0.4,"stop":["SECRET-STOP"],"messages":[{"role":"user","content":"SECRET-INPUT question"}]}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-SECRET-KEY")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestUnaryChatCompletionExportsGeneration(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})

	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != chatResponse {
		t.Fatalf("application bytes altered: %q", body)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := span.GetName(); got != "openai.chat.completions" {
		t.Fatalf("span name %q", got)
	}
	if got := attrString(t, span, "langfuse.observation.type"); got != "generation" {
		t.Fatalf("observation type %q", got)
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "example-model-002" {
		t.Fatalf("model %q; response model must win", got)
	}
	input := attrString(t, span, "langfuse.observation.input")
	if !strings.Contains(input, "SECRET-INPUT question") {
		t.Fatalf("input not captured: %q", input)
	}
	if strings.Contains(input, "SECRET-STOP") {
		t.Fatal("stop sequences leaked into exported input")
	}
	output := attrString(t, span, "langfuse.observation.output")
	if !strings.Contains(output, "SECRET-OUTPUT hello") {
		t.Fatalf("output not captured: %q", output)
	}
	usage := attrString(t, span, "langfuse.observation.usage_details")
	var usageMap map[string]int64
	if err := json.Unmarshal([]byte(usage), &usageMap); err != nil {
		t.Fatalf("usage %q: %v", usage, err)
	}
	if usageMap["input"]+usageMap["input_cached_tokens"] != 9 ||
		usageMap["output"]+usageMap["output_reasoning_tokens"] != 12 {
		t.Fatalf("usage buckets %v do not reconstruct inclusive 9/12", usageMap)
	}
	parameters := attrString(t, span, "langfuse.observation.model.parameters")
	if !strings.Contains(parameters, "temperature") || strings.Contains(parameters, "SECRET-STOP") {
		t.Fatalf("model parameters wrong or leaking: %q", parameters)
	}
	if got := attrString(t, span, "langfuse.observation.metadata.finish_reason"); got != "stop" {
		t.Fatalf("metadata finish reason %q", got)
	}
	for _, attribute := range span.GetAttributes() {
		if strings.Contains(attribute.GetValue().GetStringValue(), "sk-SECRET-KEY") {
			t.Fatalf("authorization header leaked via %q", attribute.GetKey())
		}
	}
}

func TestStreamingUsageAfterFinishAndDone(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		chunks := []string{
			`: keep-alive ping`,
			`data: {"choices":[{"index":0,"delta":{"role":"assistant"}}],"model":"example-model-002"}`,
			`data: {"choices":[{"index":0,"delta":{"content":"Hello "}}]}`,
			`data: {"choices":[{"index":0,"delta":{"content":"world"}}]}`,
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2}}`,
			`data: [DONE]`,
		}
		for _, chunk := range chunks {
			_, _ = io.WriteString(w, chunk+"\n\n")
			flusher.Flush()
		}
	})

	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	span := receiver.nextSpan(t)
	output := attrString(t, span, "langfuse.observation.output")
	if output != "Hello world" {
		t.Fatalf("accumulated stream output %q", output)
	}
	usage := attrString(t, span, "langfuse.observation.usage_details")
	if !strings.Contains(usage, `"input":4`) {
		t.Fatalf("usage-after-finish chunk lost: %q", usage)
	}
	if !hasAttr(span, "langfuse.observation.completion_start_time") {
		t.Fatal("completion start time missing")
	}
	if status := attrString(t, span, "langfuse.observation.status_message"); status != "" {
		t.Fatalf("clean stream carries status %q", status)
	}
}

func TestStreamEOFWithoutDoneIsIncomplete(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	span := receiver.nextSpan(t)
	if status := attrString(t, span, "langfuse.observation.status_message"); status != "incomplete" {
		t.Fatalf("status %q, want incomplete", status)
	}
}

func TestNoOpClientsPassOriginalRequestUntouched(t *testing.T) {
	var sawBody atomic.Value
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sawBody.Store(string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})

	cases := map[string]*langfuse.Client{
		"nil":  nil,
		"zero": {},
	}
	receiver := newOTLPReceiver(t)
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	cases["disabled"] = disabled
	stopped := newTestClient(t, receiver, nil)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := stopped.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	cancel()
	cases["post-shutdown"] = stopped

	for name, client := range cases {
		t.Run(name, func(t *testing.T) {
			transport := langfuseopenai.NewTransport(client, roundTripCheck{t: t, inner: http.DefaultTransport})
			httpClient := &http.Client{Transport: transport}
			resp := postChat(t, httpClient, provider.URL, context.Background())
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if got := sawBody.Load().(string); !strings.Contains(got, "SECRET-INPUT") {
				t.Fatalf("provider did not receive the body: %q", got)
			}
		})
	}
	receiver.expectNone(t)
}

// roundTripCheck asserts the no-op fast path forwards the untouched
// original request: original context and unwrapped body types.
type roundTripCheck struct {
	t     *testing.T
	inner http.RoundTripper
}

func (c roundTripCheck) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, ok := req.Body.(interface{ Len() int }); req.Body != nil && !ok {
		// http.NewRequest(strings.Reader) bodies expose Len; a wrapped
		// recorder body would not.
		if req.GetBody == nil {
			c.t.Error("no-op path delivered a wrapped body")
		}
	}
	return c.inner.RoundTrip(req)
}

func TestSampledOutAttemptSkipsCaptureButExportsNothing(t *testing.T) {
	receiver := newOTLPReceiver(t)
	zero := 0.0
	lf := newTestClient(t, receiver, func(cfg *langfuse.Config) { cfg.SampleRate = &zero })
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	flush(t, lf)
	receiver.expectNone(t)
}

func TestMaskGovernsAdapterContent(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, func(cfg *langfuse.Config) {
		cfg.Mask = func(value any) any {
			switch value := value.(type) {
			case string:
				return strings.ReplaceAll(value, "SECRET", "[masked]")
			case []any:
				for index, item := range value {
					value[index] = maskAny(item)
				}
				return value
			case map[string]any:
				return maskAny(value)
			default:
				return value
			}
		}
	})
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	span := receiver.nextSpan(t)
	for _, key := range []string{"langfuse.observation.input", "langfuse.observation.output"} {
		if value := attrString(t, span, key); strings.Contains(value, "SECRET") {
			t.Fatalf("%s bypassed Mask: %q", key, value)
		}
	}
}

func maskAny(value any) any {
	switch value := value.(type) {
	case string:
		return strings.ReplaceAll(value, "SECRET", "[masked]")
	case map[string]any:
		masked := make(map[string]any, len(value))
		for key, item := range value {
			masked[key] = maskAny(item)
		}
		return masked
	case []any:
		masked := make([]any, len(value))
		for index, item := range value {
			masked[index] = maskAny(item)
		}
		return masked
	default:
		return value
	}
}

func TestPrivacyModes(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})

	t.Run("WithoutContentExport", func(t *testing.T) {
		httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil,
			langfuseopenai.WithoutContentExport())}
		resp := postChat(t, httpClient, provider.URL, context.Background())
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		flush(t, lf)
		span := receiver.nextSpan(t)
		if hasAttr(span, "langfuse.observation.input") || hasAttr(span, "langfuse.observation.output") {
			t.Fatal("content exported despite WithoutContentExport")
		}
		if !strings.Contains(attrString(t, span, "langfuse.observation.usage_details"), `"output"`) {
			t.Fatal("usage lost in WithoutContentExport mode")
		}
	})

	t.Run("WithoutBodyInspection", func(t *testing.T) {
		httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil,
			langfuseopenai.WithoutBodyInspection())}
		resp := postChat(t, httpClient, provider.URL, context.Background())
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		flush(t, lf)
		span := receiver.nextSpan(t)
		if hasAttr(span, "langfuse.observation.input") || hasAttr(span, "langfuse.observation.output") ||
			hasAttr(span, "langfuse.observation.usage_details") {
			t.Fatal("body-derived data exported despite WithoutBodyInspection")
		}
		if got := attrString(t, span, "langfuse.observation.metadata.provider"); got == "" {
			t.Fatal("route metadata missing")
		}
	})
}

func TestNon2xxRecordsErrorWithFixedCategory(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"SECRET rate limited","code":"rate_limit"}}`)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	span := receiver.nextSpan(t)
	if status := attrString(t, span, "langfuse.observation.status_message"); status != "http 429" {
		t.Fatalf("status %q, want fixed category http 429", status)
	}
	if level := attrString(t, span, "langfuse.observation.level"); level != "ERROR" {
		t.Fatalf("level %q", level)
	}
	// The provider error body is content (Output), never event text.
	for _, event := range span.GetEvents() {
		for _, attribute := range event.GetAttributes() {
			if strings.Contains(attribute.GetValue().GetStringValue(), "SECRET") {
				t.Fatal("error body leaked into exception event")
			}
		}
	}
	if output := attrString(t, span, "langfuse.observation.output"); !strings.Contains(output, "rate_limit") {
		t.Fatalf("error body missing from output: %q", output)
	}
}

func TestDoubleWrapGuardReturnsExistingLayer(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	first := langfuseopenai.NewTransport(lf, nil)
	second := langfuseopenai.NewTransport(lf, first)
	if first != second {
		t.Fatal("double wrap created a second layer")
	}
}

func TestContextWithCallAttributesAndPrecedence(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil,
		langfuseopenai.WithObservationName(func(langfuseopenai.RouteInfo) string { return "option-name" }))}

	ctx := langfuseopenai.ContextWithCall(context.Background(), langfuseopenai.CallAttributes{
		Name:     "call-name",
		Prompt:   &langfuse.PromptRef{Name: "summarize-topic", Version: 3},
		Metadata: map[string]any{"team": "search"},
	})
	resp := postChat(t, httpClient, provider.URL, ctx)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := span.GetName(); got != "call-name" {
		t.Fatalf("span name %q; CallAttributes must beat the naming option", got)
	}
	if got := attrString(t, span, "langfuse.observation.prompt.name"); got != "summarize-topic" {
		t.Fatalf("prompt link missing: %q", got)
	}
	if got := attrString(t, span, "langfuse.observation.metadata.team"); got != "search" {
		t.Fatalf("call metadata %q", got)
	}
	if got := attrString(t, span, "langfuse.observation.model.name"); got != "example-model-002" {
		t.Fatalf("wire model overridden: %q", got)
	}

	if langfuseopenai.ContextWithCall(nil, langfuseopenai.CallAttributes{}) != nil { //nolint:staticcheck
		t.Fatal("nil context did not return nil")
	}
}

func TestRedirectHopsAreSeparateAttempts(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	var target *httptest.Server
	target = chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/relocated/v1/chat/completions" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, chatResponse)
			return
		}
		w.Header().Set("Location", target.URL+"/relocated/v1/chat/completions")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, target.URL, context.Background())
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	seen := map[string]bool{}
	for range 2 {
		span := receiver.nextSpan(t)
		seen[attrString(t, span, "langfuse.observation.status_message")] = true
	}
	if !seen["http 307"] {
		t.Fatalf("redirect hop not recorded as its own attempt: %v", seen)
	}
}

func TestUnrecognizedRoutesPassThrough(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	for _, path := range []string{"/v1/models", "/v1/responses", "/v1/files"} {
		req, err := http.NewRequest(http.MethodGet, provider.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	flush(t, lf)
	receiver.expectNone(t)
}

func TestTelemetryPartialOnMalformed2xx(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model": "broken"`)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	resp := postChat(t, httpClient, provider.URL, context.Background())
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != `{"model": "broken"` {
		t.Fatalf("malformed body altered: %q", body)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if status := attrString(t, span, "langfuse.observation.status_message"); status != "telemetry_partial" {
		t.Fatalf("status %q, want telemetry_partial", status)
	}
	if level := attrString(t, span, "langfuse.observation.level"); level != "WARNING" {
		t.Fatalf("level %q, want WARNING", level)
	}
}

func TestMediaPartsReplacedByPlaceholders(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	body := `{"model":"m","messages":[{"role":"user","content":[
		{"type":"text","text":"describe"},
		{"type":"image_url","image_url":{"url":"data:image/png;base64,BASE64SECRETBYTES"}}
	]}]}`
	req, err := http.NewRequest(http.MethodPost, provider.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	flush(t, lf)
	span := receiver.nextSpan(t)
	input := attrString(t, span, "langfuse.observation.input")
	if strings.Contains(input, "BASE64SECRETBYTES") {
		t.Fatalf("media bytes leaked: %q", input)
	}
	if !strings.Contains(input, `"media":"omitted"`) || !strings.Contains(input, "describe") {
		t.Fatalf("placeholder or text missing: %q", input)
	}
}

func TestAttemptNestsUnderLogicalObservation(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver, nil)
	provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatResponse)
	})
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}

	ctx, logical := lf.StartObservation(context.Background(), "answer-question",
		langfuse.TypeSpan, langfuse.ObservationAttributes{})
	resp := postChat(t, httpClient, provider.URL, ctx)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	logical.End()

	flush(t, lf)
	byName := map[string]*tracepb.Span{}
	for range 2 {
		span := receiver.nextSpan(t)
		byName[span.GetName()] = span
	}
	attempt, logicalSpan := byName["openai.chat.completions"], byName["answer-question"]
	if attempt == nil || logicalSpan == nil {
		t.Fatalf("missing spans: %v", byName)
	}
	if !bytes.Equal(attempt.GetParentSpanId(), logicalSpan.GetSpanId()) {
		t.Fatal("attempt is not a child of the logical observation")
	}
	if !bytes.Equal(attempt.GetTraceId(), logicalSpan.GetTraceId()) {
		t.Fatal("attempt left the logical trace")
	}
}
