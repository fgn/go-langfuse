package integrationtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
	openaigo "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type applicationTransport struct {
	tracer oteltrace.Tracer
	base   http.RoundTripper
	spans  chan sdktrace.ReadOnlySpan
}

func (a applicationTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, span := a.tracer.Start(r.Context(), "application-http")
	defer span.End()
	a.spans <- span.(sdktrace.ReadOnlySpan)
	return a.base.RoundTrip(r.WithContext(ctx))
}

// TestWithParentNestsTransportGenerationsUnderARun is the separate-provider
// use case end to end: an application span exports elsewhere, the Langfuse
// run lives on a detached path, and the official client's call is recorded
// under the run while application HTTP spans, outside and inside the
// Langfuse transport, keep the application parent.
func TestWithParentNestsTransportGenerationsUnderARun(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","model":"example-model-002",`+
			`"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer"}}],`+
			`"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	}))
	t.Cleanup(provider.Close)
	application := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	tracer := application.Tracer("application")
	requestCtx, request := tracer.Start(context.Background(), "request")
	defer request.End()

	spans := make(chan sdktrace.ReadOnlySpan, 2)
	inner := applicationTransport{tracer: tracer, base: http.DefaultTransport, spans: spans}
	outer := applicationTransport{tracer: tracer, base: langfuseopenai.NewTransport(lf, inner), spans: spans}
	_, run := lf.StartObservation(oteltrace.ContextWithSpanContext(requestCtx, oteltrace.SpanContext{}), "run",
		langfuse.TypeAgent, langfuse.ObservationAttributes{})
	client := officialClient(provider.URL+"/v1", &http.Client{Transport: outer}, option.WithMaxRetries(0))
	_, err := client.Chat.Completions.New(lf.WithParent(requestCtx, run), openaigo.ChatCompletionNewParams{
		Model:    openaigo.ChatModel("example-model"),
		Messages: []openaigo.ChatCompletionMessageParamUnion{openaigo.UserMessage("synthetic question")},
	})
	if err != nil {
		t.Fatal(err)
	}
	run.End()

	outerSpan, innerSpan := <-spans, <-spans
	if outerSpan.Parent().SpanID() != request.SpanContext().SpanID() ||
		innerSpan.Parent().SpanID() != outerSpan.SpanContext().SpanID() {
		t.Fatalf("application spans left the application trace: outer parent %v, inner parent %v",
			outerSpan.Parent().SpanID(), innerSpan.Parent().SpanID())
	}
	flush(t, lf)
	exported := map[string]*tracepb.Span{}
	for range 2 {
		span := receiver.nextSpan(t)
		exported[span.GetName()] = span
	}
	runSpan, generation := exported["run"], exported["openai.chat.completions"]
	if runSpan == nil || generation == nil {
		t.Fatalf("exported spans %v, want the run and its generation", exported)
	}
	if string(generation.GetParentSpanId()) != string(runSpan.GetSpanId()) ||
		string(generation.GetTraceId()) != string(runSpan.GetTraceId()) {
		t.Fatal("the generation is not a child of the run")
	}
	if got := attrString(generation, "langfuse.observation.metadata.response_id"); got != "chatcmpl-1" {
		t.Fatalf("response_id %q", got)
	}
}

// TestWithParentUnderADroppedRunKeepsTheApplicationTrace pairs a kept
// application parent with a sampled-out Langfuse run: the generation is not
// exported, and application HTTP spans inside the Langfuse transport still
// record under the application parent.
func TestWithParentUnderADroppedRunKeepsTheApplicationTrace(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","model":"example-model-002",`+
			`"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer"}}]}`)
	}))
	t.Cleanup(provider.Close)
	application := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	tracer := application.Tracer("application")
	requestCtx, request := tracer.Start(context.Background(), "request")
	defer request.End()

	spans := make(chan sdktrace.ReadOnlySpan, 1)
	inner := applicationTransport{tracer: tracer, base: http.DefaultTransport, spans: spans}
	runCtx := lf.WithSampleRate(oteltrace.ContextWithSpanContext(requestCtx, oteltrace.SpanContext{}), 0)
	_, run := lf.StartObservation(runCtx, "run", langfuse.TypeAgent, langfuse.ObservationAttributes{})
	client := officialClient(provider.URL+"/v1",
		&http.Client{Transport: langfuseopenai.NewTransport(lf, inner)}, option.WithMaxRetries(0))
	_, err := client.Chat.Completions.New(lf.WithParent(requestCtx, run), openaigo.ChatCompletionNewParams{
		Model:    openaigo.ChatModel("example-model"),
		Messages: []openaigo.ChatCompletionMessageParamUnion{openaigo.UserMessage("synthetic question")},
	})
	if err != nil {
		t.Fatal(err)
	}
	run.End()

	innerSpan := <-spans
	if !innerSpan.SpanContext().IsSampled() || innerSpan.Parent().SpanID() != request.SpanContext().SpanID() {
		t.Fatalf("inner application span sampled=%v parent=%v, want sampled under the request span",
			innerSpan.SpanContext().IsSampled(), innerSpan.Parent().SpanID())
	}
	_, marker := lf.StartObservation(context.Background(), "marker", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	marker.End()
	flush(t, lf)
	if span := receiver.nextSpan(t); span.GetName() != "marker" {
		t.Fatalf("exported %q, want only the marker: the dropped run's generation must not export", span.GetName())
	}
}
