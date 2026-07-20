package langfuse

import (
	"bytes"
	"context"
	"encoding/hex"
	"reflect"
	"sort"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
)

func TestOwnedProviderAlwaysSamplesForeignUnsampledParentsAndPreservesIDs(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	foreignProvider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	t.Cleanup(func() { shutdownProvider(t, foreignProvider) })
	localParentCtx, localParent := foreignProvider.Tracer("foreign.local").Start(
		context.Background(),
		"unsampled-local-parent",
	)
	localParentContext := localParent.SpanContext()
	if !localParentContext.IsValid() || localParentContext.IsSampled() || localParentContext.IsRemote() {
		t.Fatalf("local parent context = %v, want valid local unsampled context", localParentContext)
	}

	_, localChild := client.StartObservation(
		localParentCtx,
		"owned-child-local",
		TypeGeneration,
		ObservationAttributes{},
	)
	localChild.End()
	localParent.End()

	remoteTraceID := mustInteropTraceID(t, "0123456789abcdef0123456789abcdef")
	remoteSpanID := mustInteropSpanID(t, "0123456789abcdef")
	remoteParentContext := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    remoteTraceID,
		SpanID:     remoteSpanID,
		TraceFlags: 0,
		Remote:     true,
	})
	remoteParentCtx := oteltrace.ContextWithRemoteSpanContext(context.Background(), remoteParentContext)
	_, remoteChild := client.StartObservation(
		remoteParentCtx,
		"owned-child-remote",
		TypeGeneration,
		ObservationAttributes{},
	)
	remoteChild.End()

	flushClient(t, client)
	spans := interopSpanMap(t, receiver)
	if len(spans) != 2 {
		t.Fatalf("exported span count = %d, want 2; names = %v", len(spans), sortedInteropSpanNames(spans))
	}
	assertInteropParentage(t, spans["owned-child-local"], localParentContext, localChild)
	assertInteropParentage(t, spans["owned-child-remote"], remoteParentContext, remoteChild)
	assertInteropBoolAttribute(t, spans["owned-child-local"], lfattr.AppRootKey, true)
	assertInteropBoolAttribute(t, spans["owned-child-remote"], lfattr.AppRootKey, true)
}

func TestBorrowedProviderHonorsCallerSampler(t *testing.T) {
	tests := []struct {
		name       string
		sampler    sdktrace.Sampler
		wantExport bool
	}{
		{name: "sampled", sampler: sdktrace.AlwaysSample(), wantExport: true},
		{name: "record-only", sampler: interopRecordOnlySampler{}, wantExport: false},
		{name: "dropped", sampler: sdktrace.NeverSample(), wantExport: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receiver := otlpreceiver.New()
			t.Cleanup(receiver.Close)
			provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(test.sampler))
			t.Cleanup(func() { shutdownProvider(t, provider) })
			client := newInteropClient(t, receiver, Config{TracerProvider: provider})
			t.Cleanup(func() { shutdownClient(t, client) })

			_, observation := client.StartObservation(
				context.Background(),
				"sampler-"+test.name,
				TypeGeneration,
				ObservationAttributes{},
			)
			observation.End()
			flushClient(t, client)

			spans := interopAllSpans(receiver)
			if test.wantExport {
				if len(spans) != 1 || spans[0].GetName() != "sampler-"+test.name {
					t.Fatalf("exported spans = %v, want one sampled observation", interopProtoSpanNames(spans))
				}
			} else if len(spans) != 0 {
				t.Fatalf("exported spans = %v, want none for %s sampler", interopProtoSpanNames(spans), test.name)
			}
		})
	}
}

func TestBorrowedProviderFiltersAtEndForThirdPartySpans(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { shutdownProvider(t, provider) })
	client := newInteropClient(t, receiver, Config{TracerProvider: provider})
	t.Cleanup(func() { shutdownClient(t, client) })

	tracer := provider.Tracer("third.party")
	_, late := tracer.Start(context.Background(), "late-gen-ai")
	late.SetAttributes(attribute.String("gen_ai.response.model", "gemini-3"))
	late.End()

	_, unrelated := tracer.Start(
		context.Background(),
		"unrelated-http",
		oteltrace.WithAttributes(attribute.String("http.request.method", "GET")),
	)
	unrelated.End()

	_, prefixNearMiss := tracer.Start(
		context.Background(),
		"prefix-near-miss",
		oteltrace.WithAttributes(attribute.String("gen_ai_other.request.model", "example")),
	)
	prefixNearMiss.End()

	flushClient(t, client)
	spans := interopSpanMap(t, receiver)
	if len(spans) != 1 || spans["late-gen-ai"] == nil {
		t.Fatalf("exported spans = %v, want only late-gen-ai", sortedInteropSpanNames(spans))
	}
	assertInteropMissingAttribute(t, spans["late-gen-ai"], lfattr.AppRootKey)
}

func TestBorrowedProviderCoexistsWithGenericExporterAndAnnotatesEverySpan(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	genericExporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(genericExporter)),
	)
	t.Cleanup(func() { shutdownProvider(t, provider) })
	client := newInteropClient(t, receiver, Config{
		TracerProvider: provider,
		Environment:    "integration",
		Release:        "2026.07",
	})
	t.Cleanup(func() { shutdownClient(t, client) })

	ctx := client.WithTraceAttributes(context.Background(), TraceAttributes{
		Name:      "checkout",
		UserID:    "context-user",
		SessionID: "session-1",
		Tags:      []string{"integration", "checkout"},
		Metadata:  map[string]any{"tenant": "acme"},
		Version:   "request-v1",
	})
	tracer := provider.Tracer("application")
	_, modernModelSpan := tracer.Start(
		ctx,
		"third-party-gen-ai",
		oteltrace.WithAttributes(attribute.String("gen_ai.request.model", "gpt-5")),
	)
	modernModelSpan.End()
	_, httpSpan := tracer.Start(
		ctx,
		"ordinary-http",
		oteltrace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String(lfattr.EnvironmentKey, "instrumentation-owned"),
			attribute.String(lfattr.TraceUserIDKey, "instrumentation-user"),
		),
	)
	httpSpan.End()

	flushClient(t, client)
	langfuseSpans := interopSpanMap(t, receiver)
	if len(langfuseSpans) != 1 || langfuseSpans["third-party-gen-ai"] == nil {
		t.Fatalf("Langfuse spans = %v, want only third-party-gen-ai", sortedInteropSpanNames(langfuseSpans))
	}

	genericSpans := interopStubMap(t, genericExporter.GetSpans())
	if len(genericSpans) != 2 {
		t.Fatalf("generic span count = %d, want 2; names = %v", len(genericSpans), sortedInteropStubNames(genericSpans))
	}
	for _, name := range []string{"third-party-gen-ai", "ordinary-http"} {
		span := genericSpans[name]
		assertInteropStubStringAttribute(t, span, lfattr.ReleaseKey, "2026.07")
		assertInteropStubStringAttribute(t, span, lfattr.TraceNameKey, "checkout")
		assertInteropStubStringAttribute(t, span, lfattr.TraceSessionIDKey, "session-1")
		assertInteropStubStringSliceAttribute(t, span, lfattr.TraceTagsKey, []string{"integration", "checkout"})
		assertInteropStubStringAttribute(t, span, lfattr.TraceMetadataKey+".tenant", "acme")
		assertInteropStubStringAttribute(t, span, lfattr.VersionKey, "request-v1")
	}
	assertInteropStubStringAttribute(t, genericSpans["third-party-gen-ai"], lfattr.EnvironmentKey, "integration")
	assertInteropStubStringAttribute(t, genericSpans["third-party-gen-ai"], lfattr.TraceUserIDKey, "context-user")
	assertInteropStubStringAttribute(t, genericSpans["ordinary-http"], lfattr.EnvironmentKey, "instrumentation-owned")
	assertInteropStubStringAttribute(t, genericSpans["ordinary-http"], lfattr.TraceUserIDKey, "instrumentation-user")
}

func TestTraceAttributesAreNestedImmutableAndUpdateTheActiveObservation(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	outer := client.WithTraceAttributes(context.Background(), TraceAttributes{
		Name:      "outer-trace",
		UserID:    "outer-user",
		SessionID: "session-outer",
		Tags:      []string{"base", "shared"},
		Metadata:  map[string]any{"region": "eu", "layer": "outer"},
		Version:   "outer-version",
	})
	inner := client.WithTraceAttributes(outer, TraceAttributes{
		UserID:   "inner-user",
		Tags:     []string{"shared", "inner"},
		Metadata: map[string]any{"layer": "inner", "attempt": 2},
		Version:  "inner-version",
	})

	_, innerObservation := client.StartObservation(inner, "inner-observation", TypeSpan, ObservationAttributes{})
	innerObservation.End()
	_, restoredOuter := client.StartObservation(outer, "restored-outer", TypeSpan, ObservationAttributes{})
	restoredOuter.End()

	activeCtx, active := client.StartObservation(
		outer,
		"active-observation",
		TypeSpan,
		ObservationAttributes{Version: "explicit-observation-version"},
	)
	updatedCtx := client.WithTraceAttributes(activeCtx, TraceAttributes{
		UserID:  "updated-user",
		Tags:    []string{"active"},
		Version: "updated-context-version",
	})
	_, child := client.StartObservation(updatedCtx, "active-child", TypeSpan, ObservationAttributes{})
	child.End()
	active.End()

	flushClient(t, client)
	spans := interopSpanMap(t, receiver)
	if len(spans) != 4 {
		t.Fatalf("exported span count = %d, want 4; names = %v", len(spans), sortedInteropSpanNames(spans))
	}

	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.TraceNameKey, "outer-trace")
	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.TraceUserIDKey, "inner-user")
	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.TraceSessionIDKey, "session-outer")
	assertInteropStringSliceAttribute(t, spans["inner-observation"], lfattr.TraceTagsKey, []string{"base", "shared", "inner"})
	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.TraceMetadataKey+".region", "eu")
	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.TraceMetadataKey+".layer", "inner")
	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.TraceMetadataKey+".attempt", "2")
	assertInteropStringAttribute(t, spans["inner-observation"], lfattr.VersionKey, "inner-version")

	assertInteropStringAttribute(t, spans["restored-outer"], lfattr.TraceUserIDKey, "outer-user")
	assertInteropStringSliceAttribute(t, spans["restored-outer"], lfattr.TraceTagsKey, []string{"base", "shared"})
	assertInteropStringAttribute(t, spans["restored-outer"], lfattr.TraceMetadataKey+".layer", "outer")
	assertInteropStringAttribute(t, spans["restored-outer"], lfattr.VersionKey, "outer-version")

	assertInteropStringAttribute(t, spans["active-observation"], lfattr.TraceUserIDKey, "updated-user")
	assertInteropStringSliceAttribute(t, spans["active-observation"], lfattr.TraceTagsKey, []string{"base", "shared", "active"})
	assertInteropStringAttribute(t, spans["active-observation"], lfattr.VersionKey, "explicit-observation-version")
	assertInteropStringAttribute(t, spans["active-child"], lfattr.TraceUserIDKey, "updated-user")
	assertInteropStringAttribute(t, spans["active-child"], lfattr.VersionKey, "updated-context-version")
	assertInteropMissingAttribute(t, spans["active-child"], lfattr.AppRootKey)
}

func TestContextsAndTraceClaimsAreClientScoped(t *testing.T) {
	receiverA := otlpreceiver.New()
	receiverB := otlpreceiver.New()
	t.Cleanup(receiverA.Close)
	t.Cleanup(receiverB.Close)
	clientA := newInteropClient(t, receiverA, Config{Environment: "project_a"})
	clientB := newInteropClient(t, receiverB, Config{Environment: "project_b"})
	t.Cleanup(func() { shutdownClient(t, clientA) })
	t.Cleanup(func() { shutdownClient(t, clientB) })

	ctxA := clientA.WithTraceAttributes(context.Background(), TraceAttributes{
		UserID: "a-user",
		Tags:   []string{"a-tag"},
	})
	ctxA, rootA := clientA.StartObservation(ctxA, "project-a-root", TypeSpan, ObservationAttributes{})
	ctxB := clientB.WithTraceAttributes(ctxA, TraceAttributes{
		UserID: "b-user",
		Tags:   []string{"b-tag"},
	})
	_, childB := clientB.StartObservation(ctxB, "project-b-child", TypeSpan, ObservationAttributes{})
	childB.End()
	rootA.End()

	flushClient(t, clientA)
	flushClient(t, clientB)
	spanA := interopSpanMap(t, receiverA)["project-a-root"]
	spanB := interopSpanMap(t, receiverB)["project-b-child"]
	if spanA == nil || spanB == nil {
		t.Fatalf("missing isolated spans: project A = %v, project B = %v", spanA != nil, spanB != nil)
	}
	if !bytes.Equal(spanA.GetTraceId(), spanB.GetTraceId()) {
		t.Fatalf("project B trace ID = %x, want continued project A trace ID %x", spanB.GetTraceId(), spanA.GetTraceId())
	}
	if !bytes.Equal(spanB.GetParentSpanId(), spanA.GetSpanId()) {
		t.Fatalf("project B parent ID = %x, want project A span ID %x", spanB.GetParentSpanId(), spanA.GetSpanId())
	}
	assertInteropStringAttribute(t, spanA, lfattr.TraceUserIDKey, "a-user")
	assertInteropStringSliceAttribute(t, spanA, lfattr.TraceTagsKey, []string{"a-tag"})
	assertInteropStringAttribute(t, spanB, lfattr.TraceUserIDKey, "b-user")
	assertInteropStringSliceAttribute(t, spanB, lfattr.TraceTagsKey, []string{"b-tag"})
	assertInteropStringAttribute(t, spanB, lfattr.EnvironmentKey, "project_b")
	assertInteropBoolAttribute(t, spanB, lfattr.AppRootKey, true)
}

func TestWithDetachedTraceStartsNewApplicationRoot(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	ctx := client.WithTraceAttributes(context.Background(), TraceAttributes{
		UserID:    "user-7",
		SessionID: "session-3",
	})
	requestCtx, request := client.StartObservation(ctx, "request", TypeSpan, ObservationAttributes{})
	// The request observation ends before the background work does, like an
	// HTTP handler that spawns a goroutine and returns.
	request.End()

	detached := client.WithDetachedTrace(requestCtx)
	jobCtx, job := client.StartObservation(detached, "background-job", TypeChain, ObservationAttributes{})
	_, child := client.StartObservation(jobCtx, "job-child", TypeGeneration, ObservationAttributes{})
	child.End()
	job.End()

	flushClient(t, client)
	spans := interopSpanMap(t, receiver)
	requestSpan, jobSpan, childSpan := spans["request"], spans["background-job"], spans["job-child"]
	if requestSpan == nil || jobSpan == nil || childSpan == nil {
		t.Fatalf("missing spans: request %v, job %v, child %v", requestSpan != nil, jobSpan != nil, childSpan != nil)
	}

	if bytes.Equal(jobSpan.GetTraceId(), requestSpan.GetTraceId()) {
		t.Fatalf("detached job trace ID = %x, want a new trace distinct from the request trace", jobSpan.GetTraceId())
	}
	if len(jobSpan.GetParentSpanId()) != 0 {
		t.Fatalf("detached job parent span ID = %x, want none", jobSpan.GetParentSpanId())
	}
	assertInteropBoolAttribute(t, jobSpan, lfattr.AppRootKey, true)
	if !bytes.Equal(childSpan.GetTraceId(), jobSpan.GetTraceId()) {
		t.Fatalf("job child trace ID = %x, want the detached job trace ID %x", childSpan.GetTraceId(), jobSpan.GetTraceId())
	}
	if !bytes.Equal(childSpan.GetParentSpanId(), jobSpan.GetSpanId()) {
		t.Fatalf("job child parent ID = %x, want the detached job span ID %x", childSpan.GetParentSpanId(), jobSpan.GetSpanId())
	}
	assertInteropMissingAttribute(t, childSpan, lfattr.AppRootKey)

	// Trace attributes set before the detach continue to propagate onto the
	// new trace, so session and user grouping survive the handoff.
	assertInteropStringAttribute(t, jobSpan, lfattr.TraceUserIDKey, "user-7")
	assertInteropStringAttribute(t, jobSpan, lfattr.TraceSessionIDKey, "session-3")
	assertInteropStringAttribute(t, childSpan, lfattr.TraceUserIDKey, "user-7")
	assertInteropBoolAttribute(t, requestSpan, lfattr.AppRootKey, true)
}

func TestApplicationRootUsesClaimsAndDirectParentExpectations(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { shutdownProvider(t, provider) })
	client := newInteropClient(t, receiver, Config{TracerProvider: provider})
	t.Cleanup(func() { shutdownClient(t, client) })

	sdkRootCtx, sdkRoot := client.StartObservation(context.Background(), "sdk-root", TypeSpan, ObservationAttributes{})
	_, sdkDirectChild := client.StartObservation(sdkRootCtx, "sdk-direct-child", TypeSpan, ObservationAttributes{})
	filteredSDKCtx, filteredSDK := provider.Tracer("http.server").Start(sdkRootCtx, "filtered-under-sdk")
	_, sdkGrandchild := client.StartObservation(filteredSDKCtx, "sdk-grandchild", TypeSpan, ObservationAttributes{})
	sdkGrandchild.End()
	filteredSDK.End()
	sdkDirectChild.End()
	sdkRoot.End()

	externalCtx, externalRoot := provider.Tracer("external.instrumentation").Start(
		context.Background(),
		"external-root",
		oteltrace.WithAttributes(attribute.String("gen_ai.request.model", "gemini")),
	)
	_, externalDirectChild := client.StartObservation(externalCtx, "external-direct-child", TypeSpan, ObservationAttributes{})
	filteredExternalCtx, filteredExternal := provider.Tracer("database").Start(externalCtx, "filtered-under-external")
	_, externalGrandchild := client.StartObservation(filteredExternalCtx, "external-grandchild", TypeSpan, ObservationAttributes{})
	externalGrandchild.End()
	filteredExternal.End()
	externalDirectChild.End()
	externalRoot.End()

	flushClient(t, client)
	spans := interopSpanMap(t, receiver)
	if len(spans) != 6 {
		t.Fatalf("exported span count = %d, want 6; names = %v", len(spans), sortedInteropSpanNames(spans))
	}
	if spans["filtered-under-sdk"] != nil || spans["filtered-under-external"] != nil {
		t.Fatalf("filtered middle spans leaked: names = %v", sortedInteropSpanNames(spans))
	}

	assertInteropBoolAttribute(t, spans["sdk-root"], lfattr.AppRootKey, true)
	assertInteropMissingAttribute(t, spans["sdk-direct-child"], lfattr.AppRootKey)
	assertInteropMissingAttribute(t, spans["sdk-grandchild"], lfattr.AppRootKey)
	assertInteropBoolAttribute(t, spans["external-root"], lfattr.AppRootKey, true)
	assertInteropMissingAttribute(t, spans["external-direct-child"], lfattr.AppRootKey)
	assertInteropBoolAttribute(t, spans["external-grandchild"], lfattr.AppRootKey, true)
}

func TestShutdownOrderingAndIdempotence(t *testing.T) {
	t.Run("borrowed client first leaves provider and generic exporter alive", func(t *testing.T) {
		receiver := otlpreceiver.New()
		t.Cleanup(receiver.Close)
		genericExporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(genericExporter)),
		)
		client := newInteropClient(t, receiver, Config{TracerProvider: provider})

		_, observation := client.StartObservation(context.Background(), "before-client-shutdown", TypeSpan, ObservationAttributes{})
		observation.End()
		shutdownClient(t, client)
		if got := len(interopAllSpans(receiver)); got != 1 {
			t.Fatalf("Langfuse span count after client shutdown = %d, want 1", got)
		}
		shutdownClient(t, client)

		tracer := provider.Tracer("post-client")
		_, post := tracer.Start(
			context.Background(),
			"after-client-shutdown",
			oteltrace.WithAttributes(attribute.String("gen_ai.request.model", "still-generic")),
		)
		post.End()
		genericSpans := interopStubMap(t, genericExporter.GetSpans())
		if genericSpans["before-client-shutdown"].Name == "" || genericSpans["after-client-shutdown"].Name == "" {
			t.Fatalf("generic exporter lost spans across client shutdown: names = %v", sortedInteropStubNames(genericSpans))
		}
		if got := len(interopAllSpans(receiver)); got != 1 {
			t.Fatalf("Langfuse span count after processor unregister = %d, want 1", got)
		}
		assertProviderUnreserved(t, provider)
		shutdownProvider(t, provider)
	})

	t.Run("borrowed provider first remains safe", func(t *testing.T) {
		receiver := otlpreceiver.New()
		t.Cleanup(receiver.Close)
		genericExporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(genericExporter)),
		)
		client := newInteropClient(t, receiver, Config{TracerProvider: provider})

		_, observation := client.StartObservation(context.Background(), "before-provider-shutdown", TypeSpan, ObservationAttributes{})
		observation.End()
		if got := interopStubMap(t, genericExporter.GetSpans())["before-provider-shutdown"].Name; got == "" {
			t.Fatal("generic exporter did not receive span before provider shutdown")
		}
		shutdownProvider(t, provider)
		if got := len(interopAllSpans(receiver)); got != 1 {
			t.Fatalf("Langfuse span count after provider shutdown = %d, want 1", got)
		}
		shutdownClient(t, client)
		shutdownClient(t, client)
		assertProviderUnreserved(t, provider)
		if got := len(interopAllSpans(receiver)); got != 1 {
			t.Fatalf("Langfuse span count after repeated client shutdown = %d, want 1", got)
		}
	})

	t.Run("owned client shutdown is idempotent", func(t *testing.T) {
		receiver := otlpreceiver.New()
		t.Cleanup(receiver.Close)
		client := newInteropClient(t, receiver, Config{})

		_, observation := client.StartObservation(context.Background(), "owned-before-shutdown", TypeSpan, ObservationAttributes{})
		observation.End()
		shutdownClient(t, client)
		shutdownClient(t, client)
		spans := interopAllSpans(receiver)
		if len(spans) != 1 || spans[0].GetName() != "owned-before-shutdown" {
			t.Fatalf("owned shutdown exported spans = %v, want exactly one", interopProtoSpanNames(spans))
		}
	})
}

type interopRecordOnlySampler struct{}

func (interopRecordOnlySampler) ShouldSample(sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{Decision: sdktrace.RecordOnly}
}

func (interopRecordOnlySampler) Description() string { return "interop-record-only" }

func newInteropClient(t *testing.T, receiver *otlpreceiver.Receiver, overrides Config) *Client {
	t.Helper()
	overrides.BaseURL = receiver.URL()
	if overrides.PublicKey == "" {
		overrides.PublicKey = "pk-interop"
	}
	if overrides.SecretKey == "" {
		overrides.SecretKey = "sk-interop"
	}
	client, err := New(context.Background(), overrides)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return client
}

func mustInteropTraceID(t *testing.T, value string) oteltrace.TraceID {
	t.Helper()
	id, err := oteltrace.TraceIDFromHex(value)
	if err != nil {
		t.Fatalf("TraceIDFromHex(%q): %v", value, err)
	}
	return id
}

func mustInteropSpanID(t *testing.T, value string) oteltrace.SpanID {
	t.Helper()
	id, err := oteltrace.SpanIDFromHex(value)
	if err != nil {
		t.Fatalf("SpanIDFromHex(%q): %v", value, err)
	}
	return id
}

func interopAllSpans(receiver *otlpreceiver.Receiver) []*tracepb.Span {
	var spans []*tracepb.Span
	for _, request := range receiver.Requests() {
		spans = append(spans, otlpreceiver.Spans(request)...)
	}
	return spans
}

func interopSpanMap(t *testing.T, receiver *otlpreceiver.Receiver) map[string]*tracepb.Span {
	t.Helper()
	result := make(map[string]*tracepb.Span)
	for _, span := range interopAllSpans(receiver) {
		if previous := result[span.GetName()]; previous != nil {
			t.Fatalf("duplicate exported span name %q", span.GetName())
		}
		result[span.GetName()] = span
	}
	return result
}

func interopStubMap(t *testing.T, spans tracetest.SpanStubs) map[string]tracetest.SpanStub {
	t.Helper()
	result := make(map[string]tracetest.SpanStub, len(spans))
	for _, span := range spans {
		if previous, found := result[span.Name]; found {
			t.Fatalf("duplicate generic span name %q (previous span ID %s)", span.Name, previous.SpanContext.SpanID())
		}
		result[span.Name] = span
	}
	return result
}

func sortedInteropSpanNames(spans map[string]*tracepb.Span) []string {
	names := make([]string, 0, len(spans))
	for name := range spans {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedInteropStubNames(spans map[string]tracetest.SpanStub) []string {
	names := make([]string, 0, len(spans))
	for name := range spans {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func interopProtoSpanNames(spans []*tracepb.Span) []string {
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.GetName())
	}
	sort.Strings(names)
	return names
}

func assertInteropParentage(t *testing.T, span *tracepb.Span, parent oteltrace.SpanContext, observation *Observation) {
	t.Helper()
	if span == nil {
		t.Fatal("exported child span is missing")
	}
	if got, want := hex.EncodeToString(span.GetTraceId()), parent.TraceID().String(); got != want {
		t.Fatalf("trace ID = %q, want parent trace ID %q", got, want)
	}
	if got, want := hex.EncodeToString(span.GetParentSpanId()), parent.SpanID().String(); got != want {
		t.Fatalf("parent span ID = %q, want %q", got, want)
	}
	if got, want := hex.EncodeToString(span.GetSpanId()), observation.ID(); got != want {
		t.Fatalf("span ID = %q, want observation ID %q", got, want)
	}
	if span.GetFlags()&uint32(oteltrace.FlagsSampled) == 0 {
		t.Fatal("owned-provider child is not sampled")
	}
}

func interopProtoAttribute(span *tracepb.Span, key string) (*commonpb.AnyValue, bool) {
	if span == nil {
		return nil, false
	}
	for _, item := range span.GetAttributes() {
		if item.GetKey() == key {
			return item.GetValue(), true
		}
	}
	return nil, false
}

func assertInteropStringAttribute(t *testing.T, span *tracepb.Span, key, want string) {
	t.Helper()
	value, found := interopProtoAttribute(span, key)
	if !found || value.GetStringValue() != want {
		t.Fatalf("span %q attribute %q = (%q, %v), want %q", span.GetName(), key, value.GetStringValue(), found, want)
	}
}

func assertInteropBoolAttribute(t *testing.T, span *tracepb.Span, key string, want bool) {
	t.Helper()
	value, found := interopProtoAttribute(span, key)
	if !found || value.GetBoolValue() != want {
		t.Fatalf("span %q attribute %q = (%v, %v), want %v", span.GetName(), key, value.GetBoolValue(), found, want)
	}
}

func assertInteropStringSliceAttribute(t *testing.T, span *tracepb.Span, key string, want []string) {
	t.Helper()
	value, found := interopProtoAttribute(span, key)
	var got []string
	if found && value.GetArrayValue() != nil {
		for _, item := range value.GetArrayValue().GetValues() {
			got = append(got, item.GetStringValue())
		}
	}
	if !found || !reflect.DeepEqual(got, want) {
		t.Fatalf("span %q attribute %q = (%v, %v), want %v", span.GetName(), key, got, found, want)
	}
}

func assertInteropMissingAttribute(t *testing.T, span *tracepb.Span, key string) {
	t.Helper()
	if _, found := interopProtoAttribute(span, key); found {
		t.Fatalf("span %q unexpectedly has attribute %q", span.GetName(), key)
	}
}

func interopStubAttribute(span tracetest.SpanStub, key string) (attribute.Value, bool) {
	for _, item := range span.Attributes {
		if string(item.Key) == key {
			return item.Value, true
		}
	}
	return attribute.Value{}, false
}

func assertInteropStubStringAttribute(t *testing.T, span tracetest.SpanStub, key, want string) {
	t.Helper()
	value, found := interopStubAttribute(span, key)
	if !found || value.AsString() != want {
		t.Fatalf("generic span %q attribute %q = (%q, %v), want %q", span.Name, key, value.AsString(), found, want)
	}
}

func assertInteropStubStringSliceAttribute(t *testing.T, span tracetest.SpanStub, key string, want []string) {
	t.Helper()
	value, found := interopStubAttribute(span, key)
	if !found || !reflect.DeepEqual(value.AsStringSlice(), want) {
		t.Fatalf("generic span %q attribute %q = (%v, %v), want %v", span.Name, key, value.AsStringSlice(), found, want)
	}
}
