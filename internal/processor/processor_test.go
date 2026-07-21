package processor

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"

	otelattr "go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

func TestNewRequiresNextProcessor(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{}); err == nil {
		t.Fatal("New(Config{}) succeeded, want an error")
	}
}

func TestOnStartFillsOnlyMissingAttributes(t *testing.T) {
	t.Parallel()

	next := newRecordingProcessor()
	processor, err := New(Config{
		Next:        next,
		Environment: "default-environment",
		Release:     "default-release",
		ContextAttributes: func(context.Context) []otelattr.KeyValue {
			return []otelattr.KeyValue{
				otelattr.String(lfattr.EnvironmentKey, "propagated-environment"),
				otelattr.String(lfattr.ReleaseKey, "propagated-release"),
				otelattr.String(lfattr.TraceUserIDKey, "propagated-user"),
				otelattr.String(lfattr.TraceSessionIDKey, "propagated-session"),
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	_, span := provider.Tracer(lfattr.TracerName).Start(
		context.Background(),
		"explicit-values",
		oteltrace.WithAttributes(
			otelattr.String(lfattr.EnvironmentKey, "explicit-environment"),
			otelattr.String(lfattr.TraceUserIDKey, "explicit-user"),
		),
	)
	span.End()

	started := next.startedByName()["explicit-values"]
	assertStringAttribute(t, started, lfattr.EnvironmentKey, "explicit-environment")
	assertStringAttribute(t, started, lfattr.ReleaseKey, "propagated-release")
	assertStringAttribute(t, started, lfattr.TraceUserIDKey, "explicit-user")
	assertStringAttribute(t, started, lfattr.TraceSessionIDKey, "propagated-session")
	assertBoolAttribute(t, started, lfattr.AppRootKey, true)
}

func TestSmartFilterUsesFinalAttributesAndExactModernSemconvPrefix(t *testing.T) {
	t.Parallel()

	next := newRecordingProcessor()
	processor, err := New(Config{Next: next})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	startEnd := func(scope, name string, attrs ...otelattr.KeyValue) {
		_, span := provider.Tracer(scope).Start(
			context.Background(),
			name,
			oteltrace.WithAttributes(attrs...),
		)
		span.End()
	}

	startEnd("unknown", "observation", otelattr.String(lfattr.ObservationTypeKey, "generation"))
	startEnd("unknown", "gen-ai", otelattr.String("gen_ai.request.model", "gpt-5"))
	startEnd("unknown", "gen-ai-no-dot", otelattr.String("gen_ai", "value"))
	startEnd("unknown", "gen-aix", otelattr.String("gen_aix.request.model", "value"))
	startEnd("unknown", "other-ai", otelattr.String("other_ai.request", "sensitive"))
	startEnd("unknown", "plain", otelattr.String("http.request.method", "GET"))
	startEnd("ai.extra", "python-prefix-parity")

	_, late := provider.Tracer("unknown").Start(context.Background(), "late")
	late.SetAttributes(otelattr.String("gen_ai.response.model", "gpt-5"))
	late.End()

	got := next.endedNames()
	want := []string{"gen-ai", "late", "observation", "python-prefix-parity"}
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("exported names = %v, want %v", got, want)
	}

	lateSpan := next.endedByName()["late"]
	if _, found := lateSpan.attributes[lfattr.AppRootKey]; found {
		t.Fatal("span that became exportable late was marked as an application root")
	}
}

func TestOwnScopeIsIsolatedByPublicKey(t *testing.T) {
	t.Parallel()

	next := newRecordingProcessor()
	processor, err := New(Config{Next: next, PublicKey: "pk-target"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	for _, test := range []struct {
		name      string
		publicKey string
	}{
		{name: "matching-project", publicKey: "pk-target"},
		{name: "different-project", publicKey: "pk-other"},
		{name: "missing-project"},
	} {
		options := []oteltrace.TracerOption{}
		if test.publicKey != "" {
			options = append(options, oteltrace.WithInstrumentationAttributes(
				otelattr.String("public_key", test.publicKey),
			))
		}
		_, span := provider.Tracer(lfattr.TracerName, options...).Start(context.Background(), test.name)
		span.End()
	}

	got := next.endedNames()
	if !slices.Equal(got, []string{"matching-project"}) {
		t.Fatalf("exported names = %v, want only matching-project", got)
	}
}

func TestKnownLLMInstrumentationScopesIncludeOwnAndPythonSnapshot(t *testing.T) {
	t.Parallel()

	want := []string{
		lfattr.TracerName,
		"langfuse-sdk",
		"agent_framework",
		"autogen-core",
		"ai",
		"haystack",
		"langsmith",
		"litellm",
		"openinference",
		"opentelemetry.instrumentation.agno",
		"opentelemetry.instrumentation.alephalpha",
		"opentelemetry.instrumentation.anthropic",
		"opentelemetry.instrumentation.bedrock",
		"opentelemetry.instrumentation.cohere",
		"opentelemetry.instrumentation.crewai",
		"opentelemetry.instrumentation.google_generativeai",
		"opentelemetry.instrumentation.groq",
		"opentelemetry.instrumentation.haystack",
		"opentelemetry.instrumentation.langchain",
		"opentelemetry.instrumentation.llamaindex",
		"opentelemetry.instrumentation.mistralai",
		"opentelemetry.instrumentation.ollama",
		"opentelemetry.instrumentation.openai",
		"opentelemetry.instrumentation.openai_agents",
		"opentelemetry.instrumentation.openai_v2",
		"opentelemetry.instrumentation.replicate",
		"opentelemetry.instrumentation.sagemaker",
		"opentelemetry.instrumentation.together",
		"opentelemetry.instrumentation.transformers",
		"opentelemetry.instrumentation.vertexai",
		"opentelemetry.instrumentation.voyageai",
		"opentelemetry.instrumentation.watsonx",
		"opentelemetry.instrumentation.writer",
		"pydantic-ai",
		"strands-agents",
		"vllm",
	}

	if !slices.Equal(knownLLMInstrumentationScopePrefixes[:], want) {
		t.Fatalf("known scopes changed:\n got: %v\nwant: %v", knownLLMInstrumentationScopePrefixes, want)
	}
}

func TestApplicationRootUsesDirectParentExpectation(t *testing.T) {
	t.Parallel()

	next := newRecordingProcessor()
	processor, err := New(Config{Next: next})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	exportedTracer := provider.Tracer(lfattr.TracerName)
	filteredTracer := provider.Tracer("http.server")

	directCtx, directParent := exportedTracer.Start(context.Background(), "direct-parent")
	_, directChild := exportedTracer.Start(directCtx, "direct-child")
	directChild.End()
	directParent.End()

	rootCtx, root := exportedTracer.Start(context.Background(), "exported-root")
	middleCtx, middle := filteredTracer.Start(rootCtx, "filtered-middle")
	_, grandchild := exportedTracer.Start(middleCtx, "exported-grandchild")
	grandchild.End()
	middle.End()
	root.End()

	filteredCtx, filteredParent := filteredTracer.Start(context.Background(), "filtered-parent")
	_, siblingA := exportedTracer.Start(filteredCtx, "sibling-a")
	_, siblingB := exportedTracer.Start(filteredCtx, "sibling-b")
	siblingA.End()
	siblingB.End()
	filteredParent.End()

	endedCtx, endedParent := exportedTracer.Start(context.Background(), "ended-parent")
	endedParent.End()
	_, childAfterEnd := exportedTracer.Start(endedCtx, "child-after-parent-end")
	childAfterEnd.End()

	spans := next.endedByName()
	assertBoolAttribute(t, spans["direct-parent"], lfattr.AppRootKey, true)
	assertMissingAttribute(t, spans["direct-child"], lfattr.AppRootKey)
	assertBoolAttribute(t, spans["exported-root"], lfattr.AppRootKey, true)
	assertBoolAttribute(t, spans["exported-grandchild"], lfattr.AppRootKey, true)
	assertBoolAttribute(t, spans["sibling-a"], lfattr.AppRootKey, true)
	assertBoolAttribute(t, spans["sibling-b"], lfattr.AppRootKey, true)
	assertBoolAttribute(t, spans["ended-parent"], lfattr.AppRootKey, true)
	assertBoolAttribute(t, spans["child-after-parent-end"], lfattr.AppRootKey, true)

	processor.expectationsMu.Lock()
	remaining := len(processor.expected)
	processor.expectationsMu.Unlock()
	if remaining != 0 {
		t.Fatalf("application-root state has %d entries after all spans ended", remaining)
	}
}

func TestActiveExpectationStateIsBoundedWhenSpansNeverEnd(t *testing.T) {
	t.Parallel()

	next := &discardingProcessor{}
	processor, err := New(Config{Next: next})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := provider.Tracer(lfattr.TracerName)
	spans := make([]oteltrace.Span, maxActiveExpectations+1)
	for index := range spans {
		_, spans[index] = tracer.Start(context.Background(), "never-ended")
	}
	if got := processor.expectedCount(); got != maxActiveExpectations {
		t.Fatalf("active expectation count = %d, want bounded %d", got, maxActiveExpectations)
	}
	if !processor.expectationLimitReported.Load() {
		t.Fatal("expectation limit diagnostic was not marked as reported")
	}
	for _, span := range spans {
		span.End()
	}
	if got := processor.expectedCount(); got != 0 {
		t.Fatalf("active expectation count after ends = %d, want 0", got)
	}
	_ = provider.Shutdown(context.Background())
}

func TestClientScopedClaimSuppressesRootAcrossFilteredParent(t *testing.T) {
	t.Parallel()

	type claim struct {
		owner   string
		traceID oteltrace.TraceID
	}
	type claimContextKey struct{}

	next := newRecordingProcessor()
	processor, err := New(Config{
		Next: next,
		HasTraceClaim: func(ctx context.Context, traceID oteltrace.TraceID) bool {
			value, _ := ctx.Value(claimContextKey{}).(claim)
			return value.owner == "this-client" && value.traceID == traceID
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	exportedTracer := provider.Tracer(lfattr.TracerName)
	filteredTracer := provider.Tracer("http.server")

	rootCtx, root := exportedTracer.Start(context.Background(), "claimed-root")
	claimedCtx := context.WithValue(rootCtx, claimContextKey{}, claim{
		owner:   "this-client",
		traceID: root.SpanContext().TraceID(),
	})
	middleCtx, middle := filteredTracer.Start(claimedCtx, "claimed-filtered-middle")
	_, child := exportedTracer.Start(middleCtx, "claimed-child")
	child.End()
	middle.End()
	root.End()

	remoteTraceID := oteltrace.TraceID{1}
	remoteParent := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    remoteTraceID,
		SpanID:     oteltrace.SpanID{2},
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})
	remoteCtx := oteltrace.ContextWithRemoteSpanContext(context.Background(), remoteParent)
	wrongOwnerCtx := context.WithValue(remoteCtx, claimContextKey{}, claim{
		owner:   "another-client",
		traceID: remoteTraceID,
	})
	_, wrongOwner := exportedTracer.Start(wrongOwnerCtx, "wrong-owner")
	wrongOwner.End()

	spans := next.endedByName()
	assertBoolAttribute(t, spans["claimed-root"], lfattr.AppRootKey, true)
	assertMissingAttribute(t, spans["claimed-child"], lfattr.AppRootKey)
	assertBoolAttribute(t, spans["wrong-owner"], lfattr.AppRootKey, true)
}

func TestLiteLLMRawRequestApplicationRootEligibility(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		withParent bool
		wantRoot   bool
	}{
		{name: "ended parent", withParent: true, wantRoot: false},
		{name: "no parent", wantRoot: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := newRecordingProcessor()
			processor, err := New(Config{Next: next})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			litellm := provider.Tracer("litellm")

			ctx := context.Background()
			if test.withParent {
				parentCtx, parent := litellm.Start(ctx, "litellm-request")
				parent.End()
				ctx = parentCtx
			}
			_, raw := litellm.Start(ctx, "raw_gen_ai_request")
			raw.End()

			span := next.endedByName()["raw_gen_ai_request"]
			if test.wantRoot {
				assertBoolAttribute(t, span, lfattr.AppRootKey, true)
			} else {
				assertMissingAttribute(t, span, lfattr.AppRootKey)
			}
		})
	}
}

func TestRecordOnlyParentDoesNotSuppressSampledChildRoot(t *testing.T) {
	t.Parallel()

	next := newRecordingProcessor()
	processor, err := New(Config{Next: next})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(nameSampler{}),
		sdktrace.WithSpanProcessor(processor),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := provider.Tracer(lfattr.TracerName)

	parentCtx, parent := tracer.Start(context.Background(), "record-only")
	_, child := tracer.Start(parentCtx, "sampled-child")
	child.End()
	parent.End()

	spans := next.endedByName()
	if _, found := spans["record-only"]; found {
		t.Fatal("record-only parent reached the wrapped exporting processor")
	}
	assertBoolAttribute(t, spans["sampled-child"], lfattr.AppRootKey, true)
}

func TestShutdownIsIdempotentClearsStateAndIgnoresLaterCallbacks(t *testing.T) {
	t.Parallel()

	next := newRecordingProcessor()
	processor, err := New(Config{Next: next})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := provider.Tracer(lfattr.TracerName)

	_, active := tracer.Start(context.Background(), "active-at-shutdown")
	if got := processor.expectedCount(); got != 1 {
		t.Fatalf("start-time state count = %d, want 1", got)
	}

	const concurrency = 32
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for index := range concurrency {
		go func() {
			defer wg.Done()
			if index%2 == 0 {
				_ = processor.ForceFlush(context.Background())
				return
			}
			_ = processor.Shutdown(context.Background())
		}()
	}
	wg.Wait()

	if got := processor.expectedCount(); got != 0 {
		t.Fatalf("state count after shutdown = %d, want 0", got)
	}
	active.End()
	_, after := tracer.Start(context.Background(), "after-shutdown")
	after.End()
	_ = processor.ForceFlush(context.Background())
	_ = processor.Shutdown(context.Background())
	_ = provider.Shutdown(context.Background())

	starts, ends, _, shutdowns := next.counts()
	if starts != 1 || ends != 0 || shutdowns != 1 {
		t.Fatalf("wrapped calls = starts:%d ends:%d shutdowns:%d, want 1,0,1", starts, ends, shutdowns)
	}
}

type nameSampler struct{}

func (nameSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	decision := sdktrace.RecordAndSample
	if parameters.Name == "record-only" {
		decision = sdktrace.RecordOnly
	}
	return sdktrace.SamplingResult{Decision: decision}
}

func (nameSampler) Description() string { return "nameSampler" }

type discardingProcessor struct{}

func (*discardingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (*discardingProcessor) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (*discardingProcessor) ForceFlush(context.Context) error                { return nil }
func (*discardingProcessor) Shutdown(context.Context) error                  { return nil }

type recordedSpan struct {
	name       string
	attributes map[string]any
}

func recordSpan(span sdktrace.ReadOnlySpan) recordedSpan {
	result := recordedSpan{
		name:       span.Name(),
		attributes: make(map[string]any),
	}
	for _, item := range span.Attributes() {
		result.attributes[string(item.Key)] = item.Value.AsInterface()
	}
	return result
}

type recordingProcessor struct {
	mu        sync.Mutex
	started   []recordedSpan
	ended     []recordedSpan
	flushes   int
	shutdowns int
}

func newRecordingProcessor() *recordingProcessor { return &recordingProcessor{} }

func (p *recordingProcessor) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	p.mu.Lock()
	p.started = append(p.started, recordSpan(span))
	p.mu.Unlock()
}

func (p *recordingProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	p.ended = append(p.ended, recordSpan(span))
	p.mu.Unlock()
}

func (p *recordingProcessor) ForceFlush(context.Context) error {
	p.mu.Lock()
	p.flushes++
	p.mu.Unlock()
	return nil
}

func (p *recordingProcessor) Shutdown(context.Context) error {
	p.mu.Lock()
	p.shutdowns++
	p.mu.Unlock()
	return nil
}

func (p *recordingProcessor) startedByName() map[string]recordedSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[string]recordedSpan, len(p.started))
	for _, span := range p.started {
		result[span.name] = span
	}
	return result
}

func (p *recordingProcessor) endedByName() map[string]recordedSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[string]recordedSpan, len(p.ended))
	for _, span := range p.ended {
		result[span.name] = span
	}
	return result
}

func (p *recordingProcessor) endedNames() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]string, len(p.ended))
	for index, span := range p.ended {
		result[index] = span.name
	}
	return result
}

func (p *recordingProcessor) counts() (starts, ends, flushes, shutdowns int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.started), len(p.ended), p.flushes, p.shutdowns
}

func (p *Processor) expectedCount() int {
	p.expectationsMu.Lock()
	defer p.expectationsMu.Unlock()
	return len(p.expected)
}

func assertStringAttribute(t *testing.T, span recordedSpan, key, want string) {
	t.Helper()
	got, found := span.attributes[key]
	if !found || got != want {
		t.Fatalf("%s = %#v, found %v; want %q", key, got, found, want)
	}
}

func assertBoolAttribute(t *testing.T, span recordedSpan, key string, want bool) {
	t.Helper()
	got, found := span.attributes[key]
	if !found || got != want {
		t.Fatalf("%s = %#v, found %v; want %v", key, got, found, want)
	}
}

func assertMissingAttribute(t *testing.T, span recordedSpan, key string) {
	t.Helper()
	if got, found := span.attributes[key]; found {
		t.Fatalf("%s = %#v, want attribute absent", key, got)
	}
}

func TestAdmitRunsOnlyForSDKScopeSpansWhileActive(t *testing.T) {
	t.Parallel()

	var admitted []string
	processor, err := New(Config{
		Next: newRecordingProcessor(),
		Admit: func(ctx context.Context) {
			name, _ := ctx.Value(admitProbeKey{}).(string)
			admitted = append(admitted, name)
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	// A foreign span whose context carries an admission value must not be
	// admitted: only the SDK tracer's own spans confirm admission.
	foreignCtx := context.WithValue(context.Background(), admitProbeKey{}, "foreign")
	_, foreign := provider.Tracer("gen_ai.instrumentor").Start(foreignCtx, "foreign")
	foreign.End()

	sdkCtx := context.WithValue(context.Background(), admitProbeKey{}, "sdk")
	_, sdk := provider.Tracer(lfattr.TracerName).Start(sdkCtx, "sdk")
	sdk.End()

	if len(admitted) != 1 || admitted[0] != "sdk" {
		t.Fatalf("admitted contexts = %v, want exactly the SDK-scope span's context", admitted)
	}

	if err := processor.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	lateCtx := context.WithValue(context.Background(), admitProbeKey{}, "late")
	_, late := provider.Tracer(lfattr.TracerName).Start(lateCtx, "late")
	late.End()
	if len(admitted) != 1 {
		t.Fatalf("admitted contexts after shutdown = %v, want admission closed", admitted)
	}
}

type admitProbeKey struct{}
