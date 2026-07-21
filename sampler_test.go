package langfuse_test

import (
	"context"
	"encoding/hex"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
)

// rate returns a pointer for Config.SampleRate literals.
func rate(fraction float64) *float64 { return &fraction }

func randomTraceIDHex(rng *rand.Rand) string {
	var raw [16]byte
	for {
		rng.Read(raw[:])
		if raw != [16]byte{} {
			return hex.EncodeToString(raw[:])
		}
	}
}

func mustTraceSampledAt(t *testing.T, traceID string, fraction float64) bool {
	t.Helper()
	sampled, err := langfuse.TraceSampledAt(traceID, fraction)
	if err != nil {
		t.Fatalf("TraceSampledAt(%q, %v) error = %v", traceID, fraction, err)
	}
	return sampled
}

func TestTraceSampledAtMatchesOTelRatioSampler(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1))
	fractions := []float64{0.001, 0.1, 0.25, 0.5, 0.75, 0.999}
	for range 2000 {
		id := randomTraceIDHex(rng)
		otelID, err := oteltrace.TraceIDFromHex(id)
		if err != nil {
			t.Fatalf("TraceIDFromHex(%q) error = %v", id, err)
		}
		for _, fraction := range fractions {
			want := sdktrace.TraceIDRatioBased(fraction).ShouldSample(sdktrace.SamplingParameters{
				ParentContext: context.Background(),
				TraceID:       otelID,
			}).Decision == sdktrace.RecordAndSample
			if got := mustTraceSampledAt(t, id, fraction); got != want {
				t.Fatalf("TraceSampledAt(%q, %v) = %v, want the TraceIDRatioBased decision %v", id, fraction, got, want)
			}
		}
	}
}

func TestTraceSampledAtNestsAcrossFractions(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(2))
	for range 2000 {
		id := randomTraceIDHex(rng)
		smaller := rng.Float64()
		larger := smaller + (1-smaller)*rng.Float64()
		if mustTraceSampledAt(t, id, smaller) && !mustTraceSampledAt(t, id, larger) {
			t.Fatalf("trace %q selected at %v but not at larger fraction %v", id, smaller, larger)
		}
	}
}

func TestTraceSampledAtBoundaryFractions(t *testing.T) {
	t.Parallel()
	id := randomTraceIDHex(rand.New(rand.NewSource(3)))
	if got := mustTraceSampledAt(t, id, 0); got {
		t.Fatal("TraceSampledAt(id, 0) = true, want false: fraction 0 selects nothing")
	}
	if got := mustTraceSampledAt(t, id, 1); !got {
		t.Fatal("TraceSampledAt(id, 1) = false, want true: fraction 1 selects everything")
	}
}

func TestTraceSampledAtRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	valid := randomTraceIDHex(rand.New(rand.NewSource(4)))

	for name, fraction := range map[string]float64{
		"NaN":              math.NaN(),
		"positiveInf":      math.Inf(1),
		"negativeInf":      math.Inf(-1),
		"negative":         -0.1,
		"twoPercentAsTwo":  2,
		"justAboveOne":     1.0000001,
		"largeMisreadRate": 50,
	} {
		if _, err := langfuse.TraceSampledAt(valid, fraction); err == nil {
			t.Errorf("TraceSampledAt(valid, %s %v) error = nil, want the strict fraction error", name, fraction)
		}
	}

	for name, id := range map[string]string{
		"empty":     "",
		"short":     "abc123",
		"long":      valid + "00",
		"nonHex":    strings.Repeat("zz", 16),
		"uppercase": strings.ToUpper(valid),
		"allZero":   strings.Repeat("0", 32),
	} {
		if _, err := langfuse.TraceSampledAt(id, 0.5); err == nil {
			t.Errorf("TraceSampledAt(%s %q, 0.5) error = nil, want the malformed trace ID error", name, id)
		}
	}
}

func FuzzTraceSampledAt(f *testing.F) {
	f.Add("0af7651916cd43dd8448eb211c80319c", 0.5)
	f.Add("", math.NaN())
	f.Add(strings.Repeat("0", 32), -1.0)
	f.Fuzz(func(t *testing.T, traceID string, fraction float64) {
		sampled, err := langfuse.TraceSampledAt(traceID, fraction)
		if err != nil && sampled {
			t.Fatal("TraceSampledAt reported sampled together with an error")
		}
		if err == nil {
			smaller, smallerErr := langfuse.TraceSampledAt(traceID, fraction/2)
			if smallerErr != nil {
				t.Fatalf("halved fraction became invalid: %v", smallerErr)
			}
			if smaller && !sampled {
				t.Fatal("trace selected at a smaller fraction but not at the larger one")
			}
		}
	})
}

func TestSampleRateZeroExportsNoTraces(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0)
	})

	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{Input: "question"})
	_, child := client.StartObservation(rootCtx, "child", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Model: "m"})
	client.Event(rootCtx, "event", langfuse.ObservationAttributes{})
	child.End()
	root.End()

	if root.Sampled() || child.Sampled() {
		t.Fatal("Sampled() = true at rate 0, want false for the root and every descendant")
	}
	if root.TraceID() == "" || root.ID() == "" {
		t.Fatal("sampled-out observation lost its identifiers, want valid trace and span IDs")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Client.Flush() error = %v", err)
	}
	if requests := receiver.Requests(); len(requests) != 0 {
		t.Fatalf("OTLP request count = %d at rate 0, want 0", len(requests))
	}
}

func TestSampleRateOneExportsEverything(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(1)
	})
	_, root := client.StartObservation(context.Background(), "root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if !root.Sampled() {
		t.Fatal("Sampled() = false at rate 1, want true")
	}
	root.End()
	exportObservationWireSpans(t, client, receiver, 1)
}

func TestFractionalRateExportsExactlyTheDeterministicSubset(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0.5)
	})

	kept := make(map[string]bool)
	for range 64 {
		_, root := client.StartObservation(context.Background(), "root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
		want := mustTraceSampledAt(t, root.TraceID(), 0.5)
		if got := root.Sampled(); got != want {
			t.Fatalf("Sampled() = %v for trace %s, want the deterministic decision %v", got, root.TraceID(), want)
		}
		kept[root.TraceID()] = want
		root.End()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Client.Flush() error = %v", err)
	}
	exported := make(map[string]bool)
	for _, request := range receiver.Requests() {
		for _, resourceSpans := range request.Export.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					exported[hex.EncodeToString(span.TraceId)] = true
				}
			}
		}
	}
	for traceID, want := range kept {
		if exported[traceID] != want {
			t.Fatalf("trace %s exported = %v, want %v", traceID, exported[traceID], want)
		}
	}
}

func TestWithSampleRateDecidesOncePerPath(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, nil)

	// A dropped root pins its path: a later override cannot resurrect it.
	droppedCtx := client.WithSampleRate(context.Background(), 0)
	droppedRootCtx, droppedRoot := client.StartObservation(droppedCtx, "dropped-root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	resurrectCtx := client.WithSampleRate(droppedRootCtx, 1)
	_, droppedChild := client.StartObservation(resurrectCtx, "child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if droppedRoot.Sampled() || droppedChild.Sampled() {
		t.Fatal("a rate-1 override resurrected part of a dropped trace")
	}
	droppedChild.End()
	droppedRoot.End()

	// A kept root pins its path: a later rate-0 override cannot drop children.
	keptRootCtx, keptRoot := client.StartObservation(context.Background(), "kept-root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	dropCtx := client.WithSampleRate(keptRootCtx, 0)
	_, keptChild := client.StartObservation(dropCtx, "child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if !keptRoot.Sampled() || !keptChild.Sampled() {
		t.Fatal("a rate-0 override dropped a descendant of a kept trace")
	}
	keptChild.End()
	keptRoot.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	for _, item := range spans {
		if got := hex.EncodeToString(item.span.TraceId); got != keptRoot.TraceID() {
			t.Fatalf("exported span belongs to trace %s, want only the kept trace %s", got, keptRoot.TraceID())
		}
	}
}

func TestDecisionInheritedAcrossForeignMiddleSpan(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, nil)
	foreign := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = foreign.Shutdown(ctx)
	})

	// Dropped SDK root -> foreign middle span -> rate-1 override: the
	// grandchild must inherit the drop, not re-decide (round-1 blocker).
	rootCtx, root := client.StartObservation(client.WithSampleRate(context.Background(), 0),
		"root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	middleCtx, middle := foreign.Tracer("foreign.middle").Start(rootCtx, "middle")
	_, grandchild := client.StartObservation(client.WithSampleRate(middleCtx, 1),
		"grandchild", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if grandchild.Sampled() {
		t.Fatal("grandchild re-decided across a foreign middle span and escaped its dropped trace")
	}
	grandchild.End()
	middle.End()
	root.End()

	// The reverse direction: kept root, foreign middle, rate-0 override.
	keptRootCtx, keptRoot := client.StartObservation(context.Background(),
		"kept-root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	keptMiddleCtx, keptMiddle := foreign.Tracer("foreign.middle").Start(keptRootCtx, "middle")
	_, keptGrandchild := client.StartObservation(client.WithSampleRate(keptMiddleCtx, 0),
		"kept-grandchild", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if !keptGrandchild.Sampled() {
		t.Fatal("grandchild of a kept trace was dropped by a later rate-0 override")
	}
	keptGrandchild.End()
	keptMiddle.End()
	keptRoot.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	for _, item := range spans {
		if got := hex.EncodeToString(item.span.TraceId); got != keptRoot.TraceID() {
			t.Fatalf("exported span belongs to trace %s, want only the kept trace %s", got, keptRoot.TraceID())
		}
	}
}

func TestUnsampledForeignParentKeepsSDKChildrenAtDefaultRate(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, nil)
	foreign := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = foreign.Shutdown(ctx)
	})

	parentCtx, parent := foreign.Tracer("foreign.local").Start(context.Background(), "unsampled-parent")
	_, child := client.StartObservation(parentCtx, "child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if !child.Sampled() {
		t.Fatal("SDK child of an unsampled foreign parent was dropped at the default rate; v0.1 behavior must be preserved")
	}
	child.End()
	parent.End()
	spans := exportObservationWireSpans(t, client, receiver, 1)
	if got := hex.EncodeToString(spans[0].span.TraceId); got != parent.SpanContext().TraceID().String() {
		t.Fatalf("exported child trace = %s, want the foreign trace %s retained", got, parent.SpanContext().TraceID())
	}
}

func TestSiblingsFromPreDecisionContextAgreeUnderEqualRates(t *testing.T) {
	t.Parallel()
	client, _ := newObservationWireClient(t, nil)
	foreign := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = foreign.Shutdown(ctx)
	})

	parentCtx, parent := foreign.Tracer("foreign.local").Start(context.Background(), "parent")
	defer parent.End()
	rated := client.WithSampleRate(parentCtx, 0.5)
	want := mustTraceSampledAt(t, parent.SpanContext().TraceID().String(), 0.5)

	_, first := client.StartObservation(rated, "first", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	first.End()
	_, second := client.StartObservation(rated, "second", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	second.End()
	var observed bool
	_ = client.Observe(rated, "third", langfuse.TypeSpan, langfuse.ObservationAttributes{},
		func(_ context.Context, observation *langfuse.Observation) error {
			observed = observation.Sampled()
			return nil
		})

	if first.Sampled() != want || second.Sampled() != want || observed != want {
		t.Fatalf("sibling decisions = %v/%v/%v, want all equal to the deterministic %v",
			first.Sampled(), second.Sampled(), observed, want)
	}
}

func TestDetachedContextRedecidesWithSurvivingRate(t *testing.T) {
	t.Parallel()
	client, _ := newObservationWireClient(t, nil)

	rated := client.WithSampleRate(context.Background(), 0)
	rootCtx, root := client.StartObservation(rated, "root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	defer root.End()

	detached := oteltrace.ContextWithSpanContext(rootCtx, oteltrace.SpanContext{})
	_, background := client.StartObservation(detached, "background", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	defer background.End()

	if background.TraceID() == root.TraceID() {
		t.Fatal("detached observation stayed in the request trace, want a fresh root trace")
	}
	if background.Sampled() {
		t.Fatal("detached root ignored the surviving rate-0 override")
	}
}

func TestSamplerPreservesForeignTraceState(t *testing.T) {
	t.Parallel()
	traceState, err := oteltrace.ParseTraceState("vendor=abc123")
	if err != nil {
		t.Fatalf("ParseTraceState() error = %v", err)
	}
	remoteParent := func(rng *rand.Rand) oteltrace.SpanContext {
		var traceID oteltrace.TraceID
		var spanID oteltrace.SpanID
		rng.Read(traceID[:])
		rng.Read(spanID[:])
		return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: oteltrace.FlagsSampled,
			TraceState: traceState,
			Remote:     true,
		})
	}
	rng := rand.New(rand.NewSource(5))

	// Kept: the exported wire span must carry the vendor trace state.
	keptClient, receiver := newObservationWireClient(t, nil)
	keptCtx := oteltrace.ContextWithSpanContext(context.Background(), remoteParent(rng))
	_, kept := keptClient.StartObservation(keptCtx, "kept", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	kept.End()
	spans := exportObservationWireSpans(t, keptClient, receiver, 1)
	if got := spans[0].span.TraceState; got != "vendor=abc123" {
		t.Fatalf("exported trace_state = %q, want the preserved parent state", got)
	}

	// Dropped: no wire span exists, so assert on the returned span context.
	droppedClient, _ := newObservationWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0)
	})
	droppedCtx := oteltrace.ContextWithSpanContext(context.Background(), remoteParent(rng))
	childCtx, dropped := droppedClient.StartObservation(droppedCtx, "dropped", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	defer dropped.End()
	if got := oteltrace.SpanFromContext(childCtx).SpanContext().TraceState().Get("vendor"); got != "abc123" {
		t.Fatalf("dropped span trace state vendor = %q, want abc123 preserved", got)
	}
}

type countingError struct{ calls *atomic.Int64 }

func (e countingError) Error() string {
	e.calls.Add(1)
	return "counting error"
}

func TestSampledOutObservationSkipsMaskAndErrorCalls(t *testing.T) {
	t.Parallel()
	var maskCalls atomic.Int64
	client, _ := newObservationWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0)
		config.Mask = func(value any) any {
			maskCalls.Add(1)
			return value
		}
	})

	_, root := client.StartObservation(context.Background(), "root", langfuse.TypeSpan,
		langfuse.ObservationAttributes{Input: "start input"})
	startCalls := maskCalls.Load()

	root.Update(langfuse.ObservationAttributes{Output: "expensive payload"})
	if got := maskCalls.Load(); got != startCalls {
		t.Fatalf("Update on a sampled-out observation called Mask %d more times, want 0", got-startCalls)
	}

	var errorCalls atomic.Int64
	root.RecordError(countingError{calls: &errorCalls})
	if got := errorCalls.Load(); got != 0 {
		t.Fatalf("RecordError on a sampled-out observation called Error() %d times, want 0", got)
	}
	root.End()
}

func TestRecordOnlySpanKeepsFullUpdatePath(t *testing.T) {
	t.Parallel()
	var maskCalls atomic.Int64
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(recordOnlySampler{}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:        receiver.URL(),
		PublicKey:      "pk-lf-record-only",
		SecretKey:      "sk-lf-record-only",
		TracerProvider: provider,
		Mask: func(value any) any {
			maskCalls.Add(1)
			return value
		},
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})

	_, observation := client.StartObservation(context.Background(), "record-only", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	before := maskCalls.Load()
	observation.Update(langfuse.ObservationAttributes{Output: "still recorded"})
	if got := maskCalls.Load(); got != before+1 {
		t.Fatalf("Update on a RecordOnly span called Mask %d times, want 1: recording spans keep the full path", got-before)
	}
	observation.End()
}

type recordOnlySampler struct{}

func (recordOnlySampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{
		Decision:   sdktrace.RecordOnly,
		Tracestate: oteltrace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (recordOnlySampler) Description() string { return "recordOnly" }

func TestNewRejectsInvalidSampleRates(t *testing.T) {
	t.Parallel()
	for name, fraction := range map[string]float64{
		"NaN":         math.NaN(),
		"negative":    -0.5,
		"aboveOne":    1.5,
		"positiveInf": math.Inf(1),
	} {
		_, err := langfuse.New(context.Background(), langfuse.Config{
			BaseURL:    "https://cloud.langfuse.com",
			PublicKey:  "pk-lf-x",
			SecretKey:  "sk-lf-x",
			SampleRate: rate(fraction),
		})
		if err == nil {
			t.Errorf("New with SampleRate %s = nil error, want the sample-rate validation error", name)
		}
	}
}

func TestConfigFromEnvRejectsInvalidSampleRate(t *testing.T) {
	for _, raw := range []string{"x", "2", "-0.1", "NaN", "Inf"} {
		t.Setenv("LANGFUSE_SAMPLE_RATE", raw)
		cfg := langfuse.ConfigFromEnv()
		cfg.BaseURL, cfg.PublicKey, cfg.SecretKey = "https://cloud.langfuse.com", "pk-lf-x", "sk-lf-x"
		if _, err := langfuse.New(context.Background(), cfg); err == nil ||
			!strings.Contains(err.Error(), "LANGFUSE_SAMPLE_RATE") {
			t.Errorf("New after LANGFUSE_SAMPLE_RATE=%q error = %v, want the environment validation error", raw, err)
		}
	}
}

func TestConfigFromEnvAcceptsZeroAndOneSampleRate(t *testing.T) {
	for raw, want := range map[string]float64{"0": 0, "1": 1, "0.25": 0.25} {
		t.Setenv("LANGFUSE_SAMPLE_RATE", raw)
		cfg := langfuse.ConfigFromEnv()
		if cfg.SampleRate == nil || *cfg.SampleRate != want {
			t.Errorf("ConfigFromEnv() with LANGFUSE_SAMPLE_RATE=%q SampleRate = %v, want %v", raw, cfg.SampleRate, want)
		}
	}
}

func TestWithSampleRateGuards(t *testing.T) {
	var nilClient *langfuse.Client
	ctx := context.Background()
	if got := nilClient.WithSampleRate(ctx, 0.5); got != ctx {
		t.Fatal("nil client WithSampleRate changed the context")
	}

	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatalf("langfuse.New(disabled) error = %v", err)
	}
	if got := disabled.WithSampleRate(ctx, 0.5); got != ctx {
		t.Fatal("disabled client WithSampleRate changed the context")
	}

	var diagnostics atomic.Int64
	restore := langfuse.SetTestErrorHandler(func(error) { diagnostics.Add(1) })
	defer restore()

	client, _ := newObservationWireClient(t, nil)
	if got := client.WithSampleRate(ctx, math.NaN()); got != ctx {
		t.Fatal("invalid fraction WithSampleRate changed the context")
	}
	if got := client.WithSampleRate(ctx, 1.5); got != ctx {
		t.Fatal("out-of-range fraction WithSampleRate changed the context")
	}
	if diagnostics.Load() != 2 {
		t.Fatalf("invalid WithSampleRate diagnostics = %d, want one per rejected call", diagnostics.Load())
	}
	if got := client.WithSampleRate(ctx, 0); got == ctx {
		t.Fatal("valid boundary fraction 0 was rejected")
	}
	if got := client.WithSampleRate(ctx, 1); got == ctx {
		t.Fatal("valid boundary fraction 1 was rejected")
	}
}

func TestWithSampleRateOnBorrowedClientDiagnosesOnce(t *testing.T) {
	var diagnostics atomic.Int64
	restore := langfuse.SetTestErrorHandler(func(error) { diagnostics.Add(1) })
	defer restore()

	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:        receiver.URL(),
		PublicKey:      "pk-lf-borrowed-rate",
		SecretKey:      "sk-lf-borrowed-rate",
		TracerProvider: provider,
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})

	ctx := context.Background()
	if got := client.WithSampleRate(ctx, 0.5); got != ctx {
		t.Fatal("borrowed-mode WithSampleRate changed the context, want it ignored")
	}
	_ = client.WithSampleRate(ctx, 0.5)
	if diagnostics.Load() != 1 {
		t.Fatalf("borrowed-mode WithSampleRate diagnostics = %d, want exactly 1", diagnostics.Load())
	}
}

func TestBorrowedConfigSampleRateIsIgnoredWithDiagnostic(t *testing.T) {
	var diagnostics atomic.Int64
	restore := langfuse.SetTestErrorHandler(func(error) { diagnostics.Add(1) })
	defer restore()

	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:        receiver.URL(),
		PublicKey:      "pk-lf-borrowed-config-rate",
		SecretKey:      "sk-lf-borrowed-config-rate",
		TracerProvider: provider,
		SampleRate:     rate(0),
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	if diagnostics.Load() == 0 {
		t.Fatal("borrowed provider with SampleRate produced no diagnostic")
	}

	// The application's AlwaysSample stays authoritative despite rate 0.
	_, observation := client.StartObservation(context.Background(), "kept", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if !observation.Sampled() {
		t.Fatal("borrowed-mode observation dropped, want the application sampler to remain authoritative")
	}
	observation.End()
}

func TestStartObservationAfterShutdownIsNoop(t *testing.T) {
	t.Parallel()
	client, _ := newObservationWireClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Client.Shutdown() error = %v", err)
	}

	childCtx, observation := client.StartObservation(context.Background(), "late", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	if observation.Sampled() {
		t.Fatal("post-shutdown observation reports Sampled() = true, want the no-op observation")
	}
	if observation.TraceID() != "" || observation.ID() != "" {
		t.Fatal("post-shutdown observation carries identifiers, want the no-op observation")
	}
	if childCtx == nil {
		t.Fatal("post-shutdown StartObservation returned a nil context")
	}
	observation.Update(langfuse.ObservationAttributes{Output: "ignored"})
	observation.End()
}

// countingReentrantProcessor counts its callbacks and, on the first SDK-span
// OnStart, re-enters Client.Shutdown so teardown completes inside
// tracer.Start (the round-3 R3-F1 interleaving).
type countingReentrantProcessor struct {
	client *langfuse.Client
	starts atomic.Int64
	ends   atomic.Int64
	once   atomic.Bool
}

func (p *countingReentrantProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	p.starts.Add(1)
	if p.once.CompareAndSwap(false, true) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.client.Shutdown(ctx)
	}
}

func (p *countingReentrantProcessor) OnEnd(sdktrace.ReadOnlySpan) { p.ends.Add(1) }

func (*countingReentrantProcessor) ForceFlush(context.Context) error { return nil }

func (*countingReentrantProcessor) Shutdown(context.Context) error { return nil }

func newBorrowedAdmissionClient(t *testing.T, provider *sdktrace.TracerProvider, key string) *langfuse.Client {
	t.Helper()
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:        receiver.URL(),
		PublicKey:      key,
		SecretKey:      "sk-lf-admission",
		TracerProvider: provider,
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	return client
}

func assertNoopStartResult(t *testing.T, observation *langfuse.Observation, why string) {
	t.Helper()
	if observation.Sampled() {
		t.Fatalf("Sampled() = true %s, want the no-op observation", why)
	}
	if observation.TraceID() != "" || observation.ID() != "" {
		t.Fatalf("observation carries IDs %s/%s %s, want the no-op observation",
			observation.TraceID(), observation.ID(), why)
	}
}

func TestReentrantShutdownAfterAdmissionYieldsNoopWithBalancedCallbacks(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client := newBorrowedAdmissionClient(t, provider, "pk-lf-reentrant-after")
	// Registered after the client: the Langfuse processor admits the token
	// first, then this processor's OnStart completes Shutdown, so only the
	// stopped post-check can refuse the start.
	reentrant := &countingReentrantProcessor{client: client}
	provider.RegisterSpanProcessor(reentrant)

	_, observation := client.StartObservation(context.Background(), "raced", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	assertNoopStartResult(t, observation, "after a reentrant shutdown admitted mid-start")
	observation.End()

	if starts, ends := reentrant.starts.Load(), reentrant.ends.Load(); starts != 1 || ends != 1 {
		t.Fatalf("later processor callbacks = %d starts / %d ends, want 1/1: the refused span must still be ended", starts, ends)
	}
}

func TestReentrantShutdownBeforeAdmissionYieldsNoopWithBalancedCallbacks(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	// Registered before the client: this processor's OnStart completes
	// Shutdown before the Langfuse processor runs, so the token is never
	// admitted and the token check refuses the start.
	reentrant := &countingReentrantProcessor{}
	provider.RegisterSpanProcessor(reentrant)
	client := newBorrowedAdmissionClient(t, provider, "pk-lf-reentrant-before")
	reentrant.client = client

	_, observation := client.StartObservation(context.Background(), "raced", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	assertNoopStartResult(t, observation, "after a reentrant shutdown preempted admission")
	observation.End()

	if starts, ends := reentrant.starts.Load(), reentrant.ends.Load(); starts != 1 || ends != 1 {
		t.Fatalf("earlier processor callbacks = %d starts / %d ends, want 1/1: the refused span must still be ended", starts, ends)
	}
}

// gateProcessor blocks OnStart until released, so a test can hold a start
// inside tracer.Start while Shutdown runs on another goroutine.
type gateProcessor struct {
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (p *gateProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	if p.once.CompareAndSwap(false, true) {
		close(p.entered)
		<-p.release
	}
}

func (*gateProcessor) OnEnd(sdktrace.ReadOnlySpan) {}

func (*gateProcessor) ForceFlush(context.Context) error { return nil }

func (*gateProcessor) Shutdown(context.Context) error { return nil }

func TestConcurrentShutdownDuringHeldStartRefusesTheStart(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client := newBorrowedAdmissionClient(t, provider, "pk-lf-held-start")
	gate := &gateProcessor{entered: make(chan struct{}), release: make(chan struct{})}
	provider.RegisterSpanProcessor(gate)

	type startResult struct{ observation *langfuse.Observation }
	results := make(chan startResult, 1)
	go func() {
		_, observation := client.StartObservation(context.Background(), "held", langfuse.TypeSpan,
			langfuse.ObservationAttributes{})
		results <- startResult{observation: observation}
	}()

	<-gate.entered // the start is now parked inside tracer.Start, token admitted
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Client.Shutdown() error = %v", err)
	}
	close(gate.release)

	select {
	case result := <-results:
		assertNoopStartResult(t, result.observation, "for a start held across a completed shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("held StartObservation did not return")
	}
}

func TestRecordOnlySpanKeepsAfterEndDiagnostics(t *testing.T) {
	var diagnostics atomic.Int64
	restore := langfuse.SetTestErrorHandler(func(error) { diagnostics.Add(1) })
	defer restore()

	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(recordOnlySampler{}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client := newBorrowedAdmissionClient(t, provider, "pk-lf-record-only-ended")

	_, observation := client.StartObservation(context.Background(), "record-only", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	observation.End()

	// An ended RecordOnly span is non-recording and unsampled, exactly the
	// shape a naive recomputation would misread as sampled-out. The
	// established after-end diagnostics must still fire, and RecordError
	// must not reach err.Error once the ended guard wins.
	observation.Update(langfuse.ObservationAttributes{Output: "late"})
	if got := diagnostics.Load(); got != 1 {
		t.Fatalf("after-end Update diagnostics = %d, want 1", got)
	}
	var errorCalls atomic.Int64
	observation.RecordError(countingError{calls: &errorCalls})
	if got := diagnostics.Load(); got != 2 {
		t.Fatalf("after-end RecordError diagnostics = %d, want 2", got)
	}
	if errorCalls.Load() != 0 {
		t.Fatal("RecordError called err.Error() after End, want the ended guard to win first")
	}
}

func TestSiblingsWithDifferentRatesAreSubtreeScoped(t *testing.T) {
	t.Parallel()
	client, receiver := newObservationWireClient(t, nil)
	foreign := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = foreign.Shutdown(ctx)
	})

	parentCtx, parent := foreign.Tracer("foreign.local").Start(context.Background(), "parent")
	defer parent.End()

	// Two siblings from the same pre-decision foreign context at rates 0 and
	// 1: membership becomes subtree-scoped, and each sibling's descendants —
	// including Event, which discards its child context internally — must
	// stay inside their subtree's decision.
	droppedCtx, droppedSibling := client.StartObservation(client.WithSampleRate(parentCtx, 0),
		"dropped-sibling", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	keptCtx, keptSibling := client.StartObservation(client.WithSampleRate(parentCtx, 1),
		"kept-sibling", langfuse.TypeSpan, langfuse.ObservationAttributes{})

	_, droppedChild := client.StartObservation(droppedCtx, "dropped-child", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	client.Event(droppedCtx, "dropped-event", langfuse.ObservationAttributes{})
	_, keptChild := client.StartObservation(keptCtx, "kept-child", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	client.Event(keptCtx, "kept-event", langfuse.ObservationAttributes{})

	if droppedSibling.Sampled() || droppedChild.Sampled() {
		t.Fatal("dropped subtree leaked a sampled observation")
	}
	if !keptSibling.Sampled() || !keptChild.Sampled() {
		t.Fatal("kept subtree lost an observation")
	}
	droppedChild.End()
	droppedSibling.End()
	keptChild.End()
	keptSibling.End()

	spans := exportObservationWireSpans(t, client, receiver, 3)
	for _, item := range spans {
		if strings.HasPrefix(item.span.Name, "dropped-") {
			t.Fatalf("span %q from the dropped subtree was exported", item.span.Name)
		}
		if got := hex.EncodeToString(item.span.TraceId); got != parent.SpanContext().TraceID().String() {
			t.Fatalf("exported span trace = %s, want the shared foreign trace %s", got, parent.SpanContext().TraceID())
		}
	}
}
