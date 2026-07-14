package langfuse

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestNewSpanProcessorValidatesConfiguration(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name   string
		ctx    context.Context
		config Config
		want   string
	}{
		{name: "nil context", want: "langfuse: context is nil"},
		{name: "canceled context", ctx: canceled, want: "context canceled"},
		{name: "missing public key", ctx: context.Background(), config: Config{SecretKey: "secret"}, want: "langfuse: public key is required"},
		{name: "missing secret key", ctx: context.Background(), config: Config{PublicKey: "public"}, want: "langfuse: secret key is required"},
		{name: "wrong scheme", ctx: context.Background(), config: Config{BaseURL: "ftp://example.com", PublicKey: "public", SecretKey: "secret"}, want: "langfuse: base URL must use http or https"},
		{name: "missing host", ctx: context.Background(), config: Config{BaseURL: "https:///otel", PublicKey: "public", SecretKey: "secret"}, want: "langfuse: base URL must include a host"},
		{name: "unsupported path", ctx: context.Background(), config: Config{BaseURL: "https://example.com/something", PublicKey: "public", SecretKey: "secret"}, want: "langfuse: base URL path must be empty, /api/public/otel, or /api/public/otel/v1/traces"},
		{name: "credentials in URL", ctx: context.Background(), config: Config{BaseURL: "https://user@example.com", PublicKey: "public", SecretKey: "secret"}, want: "langfuse: base URL must not include credentials, query, or fragment"},
		{name: "query in URL", ctx: context.Background(), config: Config{BaseURL: "https://example.com?debug=true", PublicKey: "public", SecretKey: "secret"}, want: "langfuse: base URL must not include credentials, query, or fragment"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSpanProcessor(test.ctx, test.config)
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTracesEndpointAcceptsDocumentedURLForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		base string
		want string
	}{
		{"", "https://cloud.langfuse.com/api/public/otel/v1/traces"},
		{" https://example.com/ ", "https://example.com/api/public/otel/v1/traces"},
		{"https://example.com/api/public/otel", "https://example.com/api/public/otel/v1/traces"},
		{"https://example.com/api/public/otel/", "https://example.com/api/public/otel/v1/traces"},
		{"https://example.com/api/public/otel/v1/traces", "https://example.com/api/public/otel/v1/traces"},
	}
	for _, test := range tests {
		t.Run(test.base, func(t *testing.T) {
			t.Parallel()
			got, err := tracesEndpoint(test.base)
			if err != nil {
				t.Fatalf("tracesEndpoint: %v", err)
			}
			if got != test.want {
				t.Fatalf("endpoint = %q, want %q", got, test.want)
			}
		})
	}
}

func TestOTLPWireContractAndSharedProvider(t *testing.T) {
	t.Parallel()

	request := make(chan capturedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		body, err := io.ReadAll(incoming.Body)
		request <- capturedRequest{
			method:           incoming.Method,
			path:             incoming.URL.Path,
			authorization:    incoming.Header.Get("Authorization"),
			ingestionVersion: incoming.Header.Get(ingestionVersionKey),
			sdkName:          incoming.Header.Get("x-langfuse-sdk-name"),
			publicKey:        incoming.Header.Get("x-langfuse-public-key"),
			contentType:      incoming.Header.Get("Content-Type"),
			body:             body,
			err:              err,
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	processor, err := NewSpanProcessor(
		context.Background(),
		Config{BaseURL: server.URL, PublicKey: "public", SecretKey: "secret"},
		sdktrace.WithBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatalf("NewSpanProcessor: %v", err)
	}

	generic := &recordingExporter{}
	res := resource.NewSchemaless(attribute.String("service.name", "wire-test"))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(generic)),
	)

	parentContext, parent := provider.Tracer("example.app").Start(context.Background(), "request")
	_, child := provider.Tracer("example.llm").Start(
		parentContext,
		"chat",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	child.SetAttributes(attribute.String("gen_ai.response.model", "gpt-5-2026-07-01"))
	childContext := child.SpanContext()
	parentSpanID := parent.SpanContext().SpanID()
	child.End()
	parent.End()

	flushContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.ForceFlush(flushContext); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	var got capturedRequest
	select {
	case got = <-request:
	case <-flushContext.Done():
		t.Fatal("timed out waiting for OTLP request")
	}
	if got.err != nil {
		t.Fatalf("read OTLP request: %v", got.err)
	}
	if got.method != http.MethodPost {
		t.Errorf("request method = %q, want POST", got.method)
	}
	if got.path != "/api/public/otel/v1/traces" {
		t.Errorf("request path = %q", got.path)
	}
	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("public:secret"))
	if got.authorization != wantAuthorization {
		t.Errorf("Authorization = %q, want %q", got.authorization, wantAuthorization)
	}
	if got.ingestionVersion != "4" {
		t.Errorf("%s = %q, want 4", ingestionVersionKey, got.ingestionVersion)
	}
	if got.sdkName != "go" {
		t.Errorf("x-langfuse-sdk-name = %q, want go", got.sdkName)
	}
	if got.publicKey != "public" {
		t.Errorf("x-langfuse-public-key = %q, want public", got.publicKey)
	}
	if got.contentType != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", got.contentType)
	}

	var exportRequest collectortracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(got.body, &exportRequest); err != nil {
		t.Fatalf("decode OTLP request: %v", err)
	}
	spans := exportedProtoSpans(&exportRequest)
	if len(spans) != 1 {
		t.Fatalf("Langfuse exported %d spans, want 1", len(spans))
	}
	span := spans[0]
	traceID := childContext.TraceID()
	spanID := childContext.SpanID()
	if !bytes.Equal(span.TraceId, traceID[:]) {
		t.Errorf("trace ID changed: got %x, want %x", span.TraceId, traceID)
	}
	if !bytes.Equal(span.SpanId, spanID[:]) {
		t.Errorf("span ID changed: got %x, want %x", span.SpanId, spanID)
	}
	if !bytes.Equal(span.ParentSpanId, parentSpanID[:]) {
		t.Errorf("parent span ID changed: got %x, want %x", span.ParentSpanId, parentSpanID)
	}
	if value, ok := protoBoolAttribute(span, appRootKey); !ok || !value {
		t.Errorf("%s = %v, %v; want true", appRootKey, value, ok)
	}
	if value, ok := protoStringAttribute(span, "gen_ai.response.model"); !ok || value != "gpt-5-2026-07-01" {
		t.Errorf("late streaming attribute = %q, %v", value, ok)
	}
	if got := protoResourceAttribute(&exportRequest, "service.name"); got != "wire-test" {
		t.Errorf("service.name = %q, want wire-test", got)
	}

	if names := generic.names(); !slices.Equal(names, []string{"chat", "request"}) {
		t.Errorf("generic exporter spans = %v, want [chat request]", names)
	}

	if err := provider.Shutdown(flushContext); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !generic.isShutdown() {
		t.Fatal("provider did not shut down the existing exporter")
	}
}

func TestDefaultFilterIncludesLateAttributesAndKnownScopes(t *testing.T) {
	t.Parallel()

	recorder := &recordingProcessor{}
	processor := &spanProcessor{
		next:    recorder,
		claimed: make(map[spanKey]struct{}),
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))

	startAndEnd := func(scope, name string, initial ...attribute.KeyValue) {
		_, span := provider.Tracer(scope).Start(context.Background(), name, trace.WithAttributes(initial...))
		span.End()
	}
	startAndEnd("example.app", "plain")
	startAndEnd("example.app", "explicit", attribute.String(observationTypeKey, "generation"))

	_, late := provider.Tracer("example.app").Start(context.Background(), "late")
	late.SetAttributes(attribute.String("gen_ai.response.model", "gpt-5"))
	late.End()

	startAndEnd("opentelemetry.instrumentation.openai.v2", "openai")
	startAndEnd("ai", "ai")
	startAndEnd("ai.extra", "ai-extra")

	if got := recorder.names(); !slices.Equal(got, []string{"explicit", "late", "openai", "ai"}) {
		t.Fatalf("exported spans = %v", got)
	}
}

func TestAppRootClaimCrossesFilteredSpansAndBaggage(t *testing.T) {
	t.Parallel()

	recorder := &recordingProcessor{}
	processor := &spanProcessor{next: recorder, claimed: make(map[spanKey]struct{})}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))

	rootContext, root := provider.Tracer("example.app").Start(
		context.Background(),
		"root",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	middleContext, middle := provider.Tracer("example.app").Start(rootContext, "filtered-middle")
	_, child := provider.Tracer("example.app").Start(
		middleContext,
		"child",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	child.End()
	middle.End()
	root.End()

	remoteTraceID := trace.TraceID{1}
	remoteParent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    remoteTraceID,
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	member, err := baggage.NewMember(traceIDBaggageKey, remoteTraceID.String())
	if err != nil {
		t.Fatalf("NewMember: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("New baggage: %v", err)
	}
	remoteContext := trace.ContextWithRemoteSpanContext(context.Background(), remoteParent)
	remoteContext = baggage.ContextWithBaggage(remoteContext, bag)
	_, remoteChild := provider.Tracer("example.app").Start(
		remoteContext,
		"remote-child",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	remoteChild.End()

	spans := recorder.byName()
	if !spans["root"].appRoot {
		t.Fatal("first exported span was not marked as app root")
	}
	if spans["child"].appRoot {
		t.Fatal("exported child below a filtered span was marked as another app root")
	}
	if spans["remote-child"].appRoot {
		t.Fatal("same-trace baggage claim did not suppress a second app root")
	}
}

func TestRecordOnlySpanCannotClaimAppRoot(t *testing.T) {
	t.Parallel()

	recorder := &recordingProcessor{}
	processor := &spanProcessor{next: recorder, claimed: make(map[spanKey]struct{})}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(spanNameSampler{}),
		sdktrace.WithSpanProcessor(processor),
	)
	parentContext, parent := provider.Tracer("example.app").Start(
		context.Background(),
		"record-only",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	_, child := provider.Tracer("example.app").Start(
		parentContext,
		"sampled",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	child.End()
	parent.End()

	spans := recorder.byName()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %v, want only sampled", recorder.names())
	}
	if !spans["sampled"].appRoot {
		t.Fatal("sampled child of record-only span was not marked as app root")
	}
}

func TestCallbacksAreIgnoredAfterShutdown(t *testing.T) {
	t.Parallel()

	recorder := &recordingProcessor{}
	processor := &spanProcessor{next: recorder, claimed: make(map[spanKey]struct{})}
	if err := processor.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	provider := sdktrace.NewTracerProvider()
	_, span := provider.Tracer("example.app").Start(
		context.Background(),
		"after-shutdown",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	readWrite, ok := span.(sdktrace.ReadWriteSpan)
	if !ok {
		t.Fatal("recording span does not implement sdktrace.ReadWriteSpan")
	}
	readOnly, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		t.Fatal("recording span does not implement sdktrace.ReadOnlySpan")
	}
	processor.OnStart(context.Background(), readWrite)
	processor.OnEnd(readOnly)
	if err := processor.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush after shutdown: %v", err)
	}

	starts, ends, flushes, shutdowns := recorder.counts()
	if starts != 0 || ends != 0 || flushes != 0 || shutdowns != 1 {
		t.Fatalf("delegate calls = start:%d end:%d flush:%d shutdown:%d", starts, ends, flushes, shutdowns)
	}
	span.End()
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("provider Shutdown: %v", err)
	}
}

func TestProviderShutdownFlushesLangfuse(t *testing.T) {
	t.Parallel()

	request := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		_, _ = io.Copy(io.Discard, incoming.Body)
		request <- struct{}{}
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	processor, err := NewSpanProcessor(
		context.Background(),
		Config{BaseURL: server.URL, PublicKey: "public", SecretKey: "secret"},
		sdktrace.WithBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatalf("NewSpanProcessor: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	_, span := provider.Tracer("example.app").Start(
		context.Background(),
		"chat",
		trace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	span.End()

	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownContext); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-request:
	case <-shutdownContext.Done():
		t.Fatal("provider shutdown did not flush Langfuse")
	}
}

func TestNewSpanProcessorDoesNotReplaceGlobalProvider(t *testing.T) {
	t.Parallel()

	before := otel.GetTracerProvider()
	processor, err := NewSpanProcessor(context.Background(), Config{
		BaseURL:   "http://127.0.0.1:1",
		PublicKey: "public",
		SecretKey: "secret",
	})
	if err != nil {
		t.Fatalf("NewSpanProcessor: %v", err)
	}
	if after := otel.GetTracerProvider(); after != before {
		t.Fatal("NewSpanProcessor replaced the global tracer provider")
	}
	if err := processor.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

type capturedRequest struct {
	method           string
	path             string
	authorization    string
	ingestionVersion string
	sdkName          string
	publicKey        string
	contentType      string
	body             []byte
	err              error
}

type spanSnapshot struct {
	name    string
	appRoot bool
}

func snapshot(span sdktrace.ReadOnlySpan) spanSnapshot {
	result := spanSnapshot{name: span.Name()}
	for _, attr := range span.Attributes() {
		if string(attr.Key) == appRootKey {
			result.appRoot = attr.Value.AsBool()
		}
	}
	return result
}

type recordingExporter struct {
	mu       sync.Mutex
	spans    []spanSnapshot
	shutdown bool
}

func (e *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, span := range spans {
		e.spans = append(e.spans, snapshot(span))
	}
	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	return nil
}

func (e *recordingExporter) names() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	names := make([]string, len(e.spans))
	for index, span := range e.spans {
		names[index] = span.name
	}
	return names
}

func (e *recordingExporter) isShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

type recordingProcessor struct {
	mu        sync.Mutex
	spans     []spanSnapshot
	starts    int
	flushes   int
	shutdowns int
}

func (p *recordingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	p.mu.Lock()
	p.starts++
	p.mu.Unlock()
}

func (p *recordingProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	p.spans = append(p.spans, snapshot(span))
	p.mu.Unlock()
}

func (p *recordingProcessor) Shutdown(context.Context) error {
	p.mu.Lock()
	p.shutdowns++
	p.mu.Unlock()
	return nil
}

func (p *recordingProcessor) ForceFlush(context.Context) error {
	p.mu.Lock()
	p.flushes++
	p.mu.Unlock()
	return nil
}

func (p *recordingProcessor) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	names := make([]string, len(p.spans))
	for index, span := range p.spans {
		names[index] = span.name
	}
	return names
}

func (p *recordingProcessor) byName() map[string]spanSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	spans := make(map[string]spanSnapshot, len(p.spans))
	for _, span := range p.spans {
		spans[span.name] = span
	}
	return spans
}

func (p *recordingProcessor) counts() (starts, ends, flushes, shutdowns int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts, len(p.spans), p.flushes, p.shutdowns
}

type spanNameSampler struct{}

func (spanNameSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	decision := sdktrace.RecordAndSample
	if parameters.Name == "record-only" {
		decision = sdktrace.RecordOnly
	}
	return sdktrace.SamplingResult{Decision: decision}
}

func (spanNameSampler) Description() string { return "spanNameSampler" }

func exportedProtoSpans(request *collectortracepb.ExportTraceServiceRequest) []*tracepb.Span {
	var spans []*tracepb.Span
	for _, resourceSpans := range request.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			spans = append(spans, scopeSpans.Spans...)
		}
	}
	return spans
}

func protoStringAttribute(span *tracepb.Span, key string) (string, bool) {
	for _, attr := range span.Attributes {
		if attr.Key == key {
			return attr.Value.GetStringValue(), true
		}
	}
	return "", false
}

func protoBoolAttribute(span *tracepb.Span, key string) (bool, bool) {
	for _, attr := range span.Attributes {
		if attr.Key == key {
			return attr.Value.GetBoolValue(), true
		}
	}
	return false, false
}

func protoResourceAttribute(request *collectortracepb.ExportTraceServiceRequest, key string) string {
	for _, resourceSpans := range request.ResourceSpans {
		if resourceSpans.Resource == nil {
			continue
		}
		for _, attr := range resourceSpans.Resource.Attributes {
			if attr.Key == key {
				return attr.Value.GetStringValue()
			}
		}
	}
	return ""
}
