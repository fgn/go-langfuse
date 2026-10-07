package langfuse_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/fgn/go-langfuse"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestWithParentNestsUnderAnObservationFromAnotherContextPath(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	application := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	requestCtx, request := application.Tracer("application").Start(context.Background(), "request")
	defer request.End()

	runCtx := oteltrace.ContextWithSpanContext(requestCtx, oteltrace.SpanContext{})
	_, run := client.StartObservation(runCtx, "run", langfuse.TypeAgent, langfuse.ObservationAttributes{})
	callCtx := client.WithParent(requestCtx, run)
	if !oteltrace.SpanFromContext(callCtx).SpanContext().Equal(request.SpanContext()) {
		t.Fatal("WithParent changed the active span seen by other tracers")
	}
	generationCtx, generation := client.StartObservation(callCtx, "generation", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{})
	_, tool := client.StartObservation(generationCtx, "tool", langfuse.TypeTool, langfuse.ObservationAttributes{})
	_, second := client.StartObservation(callCtx, "second-generation", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{})
	_, applicationChild := application.Tracer("application").Start(callCtx, "application-child")
	applicationChild.End()
	tool.End()
	generation.End()
	second.End()
	run.End()

	spans := exportObservationWireSpans(t, client, receiver, 4)
	runSpan := observationWireSpanNamed(t, spans, "run").span
	for child, parent := range map[string][]byte{
		"generation":        runSpan.SpanId,
		"second-generation": runSpan.SpanId,
		"tool":              observationWireSpanNamed(t, spans, "generation").span.SpanId,
	} {
		span := observationWireSpanNamed(t, spans, child).span
		if !bytes.Equal(span.TraceId, runSpan.TraceId) || !bytes.Equal(span.ParentSpanId, parent) {
			t.Fatalf("%s parent = %x in trace %x, want %x in trace %x",
				child, span.ParentSpanId, span.TraceId, parent, runSpan.TraceId)
		}
	}
	requestTraceID := request.SpanContext().TraceID()
	if bytes.Equal(runSpan.TraceId, requestTraceID[:]) {
		t.Fatal("the run joined the application trace instead of starting its own")
	}
	if applicationChild.(sdktrace.ReadOnlySpan).Parent().SpanID() != request.SpanContext().SpanID() {
		t.Fatal("an application span started under WithParent lost its application parent")
	}
}

func TestWithParentInheritsTheParentSamplingDecision(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	_, run := client.StartObservation(client.WithSampleRate(context.Background(), 0), "dropped-run",
		langfuse.TypeAgent, langfuse.ObservationAttributes{})
	_, generation := client.StartObservation(client.WithParent(context.Background(), run), "dropped-generation",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{})
	generation.End()
	run.End()
	_, marker := client.StartObservation(context.Background(), "marker", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	marker.End()

	for _, span := range exportObservationWireSpans(t, client, receiver, 1) {
		if span.span.Name != "marker" {
			t.Fatalf("exported %q, want only the marker: a sampled-out parent drops its children", span.span.Name)
		}
	}
}

func TestWithParentIgnoresAParentFromAnotherClient(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	other, _ := newObservationWireClient(t, nil)
	_, foreign := other.StartObservation(context.Background(), "other-run", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	defer foreign.End()

	_, root := client.StartObservation(client.WithParent(context.Background(), foreign), "root",
		langfuse.TypeSpan, langfuse.ObservationAttributes{})
	root.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "root").span
	if len(span.ParentSpanId) != 0 {
		t.Fatalf("root parent = %x, want a new trace root", span.ParentSpanId)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "parent observation is missing or from another client", 1)
}

func TestWithParentKeepsTheParentRootClaimAfterItEnds(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	_, middle := client.StartObservation(rootCtx, "middle", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	middle.End()
	root.End()
	_, normal := client.StartObservation(rootCtx, "normal", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	normal.End()
	for name, parent := range map[string]*langfuse.Observation{"bridge-root": root, "bridge-middle": middle} {
		_, bridge := client.StartObservation(client.WithParent(context.Background(), parent), name, langfuse.TypeSpan,
			langfuse.ObservationAttributes{})
		bridge.End()
	}

	spans := exportObservationWireSpans(t, client, receiver, 5)
	for _, name := range []string{"middle", "normal", "bridge-root", "bridge-middle"} {
		attributes := observationWireAttributeMap(t, observationWireSpanNamed(t, spans, name).span.Attributes)
		if attributes["langfuse.internal.is_app_root"] == true {
			t.Fatalf("%s became a second application root of its trace", name)
		}
	}
}

func TestWithParentDoesNotRestoreLostScoreAuthority(t *testing.T) {
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) { config.SampleRate = rate(0) })
	application := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	defer root.End()
	foreignCtx, foreign := application.Tracer("application").Start(rootCtx, "foreign")
	defer foreign.End()
	downgradedCtx, downgraded := client.StartObservation(foreignCtx, "downgraded", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	defer downgraded.End()

	for name, ctx := range map[string]context.Context{"downgraded path": downgradedCtx, "foreign ambient span": foreignCtx} {
		bridgeCtx, bridge := client.StartObservation(client.WithParent(ctx, root), "bridge", langfuse.TypeSpan,
			langfuse.ObservationAttributes{})
		recordDeliveredScore(t, client, receiver, bridgeCtx, langfuse.Score{TraceID: root.TraceID()}, name)
		bridge.End()
	}
}

func TestWithParentFromAnotherTraceKeepsScoreAuthority(t *testing.T) {
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) { config.SampleRate = rate(0) })
	application := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	requestCtx, request := application.Tracer("application").Start(context.Background(), "request")
	defer request.End()
	_, root := client.StartObservation(oteltrace.ContextWithSpanContext(requestCtx, oteltrace.SpanContext{}), "root",
		langfuse.TypeAgent, langfuse.ObservationAttributes{})
	defer root.End()

	bridgeCtx, bridge := client.StartObservation(client.WithParent(requestCtx, root), "bridge", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	defer bridge.End()
	value := 1.0
	if err := client.RecordScore(bridgeCtx, langfuse.Score{Name: "quality", TraceID: root.TraceID(), NumericValue: &value}); err != nil {
		t.Fatal(err)
	}
	flushClient(t, client)
	if got := ingestionRequestCount(receiver); got != 0 {
		t.Fatalf("ingestion requests = %d, want 0: the application trace is not a hop in the sampled-out Langfuse trace", got)
	}
}

func TestWithParentKeepsScoreAuthorityAcrossSDKDescendants(t *testing.T) {
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) { config.SampleRate = rate(0) })
	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	defer root.End()
	childCtx, child := client.StartObservation(client.WithParent(rootCtx, root), "child", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	defer child.End()
	grandchildCtx, grandchild := client.StartObservation(childCtx, "grandchild", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	defer grandchild.End()

	value := 1.0
	for _, ctx := range []context.Context{childCtx, grandchildCtx} {
		if err := client.RecordScore(ctx, langfuse.Score{Name: "quality", TraceID: root.TraceID(), NumericValue: &value}); err != nil {
			t.Fatal(err)
		}
	}
	flushClient(t, client)
	if got := ingestionRequestCount(receiver); got != 0 {
		t.Fatalf("ingestion requests = %d, want 0: SDK-only descendants keep the sampled-out trace's authority", got)
	}
}

func TestWithParentTakesContentCaptureFromTheContext(t *testing.T) {
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) { config.DisableContentCapture = true })
	_, parent := client.StartObservation(client.WithContentCapture(context.Background(), true), "parent",
		langfuse.TypeAgent, langfuse.ObservationAttributes{Input: "parent input"})
	_, child := client.StartObservation(client.WithParent(context.Background(), parent), "child",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{Input: "child input"})
	child.End()
	parent.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	if got := observationWireAttributeMap(t, observationWireSpanNamed(t, spans, "child").span.Attributes)["langfuse.observation.input"]; got != nil {
		t.Fatalf("child input = %#v, want none: capture comes from the child's context", got)
	}
}

type dropByName string

func (d dropByName) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Name == string(d) {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}
func (d dropByName) Description() string { return "dropByName" }

func TestWithParentLeavesABorrowedSamplerAuthoritative(t *testing.T) {
	application := sdktrace.NewTracerProvider(sdktrace.WithSampler(dropByName("dropped-parent")))
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) { config.TracerProvider = application })
	_, parent := client.StartObservation(context.Background(), "dropped-parent", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	_, child := client.StartObservation(client.WithParent(context.Background(), parent), "kept-child",
		langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	parent.End()

	spans := exportObservationWireSpans(t, client, receiver, 1)
	if spans[0].span.Name != "kept-child" {
		t.Fatalf("exported %q, want the child the application sampler kept", spans[0].span.Name)
	}
}
