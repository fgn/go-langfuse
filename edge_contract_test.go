package langfuse_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/fgn/go-langfuse"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestEdgeContractInvalidAndEmptyEnums(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	_, empty := client.StartObservation(context.Background(), "empty-enums", "",
		langfuse.ObservationAttributes{})
	empty.End()

	const invalidType = "invalid-type-PAYLOAD-79f32"
	const invalidLevel = "invalid-level-PAYLOAD-82dd1"
	_, invalid := client.StartObservation(context.Background(), "invalid-enums",
		langfuse.ObservationType(invalidType), langfuse.ObservationAttributes{
			Level:         langfuse.Level(invalidLevel),
			StatusMessage: "safe status survives",
		})
	invalid.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	assertObservationWireAttributes(t, observationWireSpanNamed(t, spans, "empty-enums").span.Attributes,
		edgeBaseAttributes("span"))
	wantInvalid := edgeBaseAttributes("span")
	wantInvalid["langfuse.observation.status_message"] = "safe status survives"
	assertObservationWireAttributes(t, observationWireSpanNamed(t, spans, "invalid-enums").span.Attributes,
		wantInvalid)

	assertEdgeDiagnosticCount(t, diagnostics, "unsupported observation type; using span", 1)
	assertEdgeDiagnosticCount(t, diagnostics, "unsupported observation level omitted", 1)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, invalidType, invalidLevel)
}

func TestEdgeContractInvalidPromptIsOmitted(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	_, emptyName := client.StartObservation(context.Background(), "prompt-empty-name",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Model:  "model-survives",
			Prompt: &langfuse.PromptRef{Name: "", Version: 3},
		})
	emptyName.End()

	const invalidPromptName = "prompt-name-PAYLOAD-a4981"
	_, invalidVersion := client.StartObservation(context.Background(), "prompt-invalid-version",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Model:  "model-survives",
			Prompt: &langfuse.PromptRef{Name: invalidPromptName, Version: 0},
		})
	invalidVersion.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	want := edgeBaseAttributes("generation")
	want["langfuse.observation.model.name"] = "model-survives"
	for _, name := range []string{"prompt-empty-name", "prompt-invalid-version"} {
		assertObservationWireAttributes(t, observationWireSpanNamed(t, spans, name).span.Attributes, want)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "invalid prompt reference omitted", 2)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, invalidPromptName)
}

func TestEdgeContractTypedNilValuesAreAbsent(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	type request struct{ Secret string }
	var input *request
	var output []string
	var metadata map[string]any
	var modelParameters map[string]any
	var costs map[string]float64
	var prompt *langfuse.PromptRef

	_, observation := client.StartObservation(context.Background(), "typed-nil",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Input:           input,
			Output:          output,
			Metadata:        metadata,
			Model:           "model-survives",
			ModelParameters: modelParameters,
			CostDetails:     costs,
			Prompt:          prompt,
		})
	observation.End()

	want := edgeBaseAttributes("generation")
	want["langfuse.observation.model.name"] = "model-survives"
	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "typed-nil")
	assertObservationWireAttributes(t, span.span.Attributes, want)
	if got := diagnostics.snapshot(); len(got) != 0 {
		t.Fatalf("typed nil values emitted diagnostics: %v", got)
	}
}

func TestEdgeContractCyclicAndOversizePayloadsAreIsolated(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	cyclicInput := map[string]any{"label": "cyclic-input-PAYLOAD-611e"}
	cyclicInput["self"] = cyclicInput
	cyclicMetadata := map[string]any{"label": "cyclic-metadata-PAYLOAD-214f"}
	cyclicMetadata["self"] = cyclicMetadata
	oversize := strings.Repeat("oversize-PAYLOAD-23fb", 1<<16)
	if len(oversize) <= 1<<20 {
		t.Fatalf("test payload length = %d, want over 1 MiB", len(oversize))
	}

	_, observation := client.StartObservation(context.Background(), "malformed-payloads",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Input:  cyclicInput,
			Output: oversize,
			Metadata: map[string]any{
				"bad":  cyclicMetadata,
				"safe": "metadata-survives",
			},
			Model: "model-survives",
			Usage: &langfuse.Usage{InputTokens: 5, OutputTokens: 3},
		})
	observation.End()

	want := edgeBaseAttributes("generation")
	want["langfuse.observation.metadata.safe"] = "metadata-survives"
	want["langfuse.observation.model.name"] = "model-survives"
	want["langfuse.observation.usage_details"] = `{"input":5,"output":3,"total":8}`
	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "malformed-payloads")
	assertObservationWireAttributes(t, span.span.Attributes, want)
	assertEdgeDiagnosticCount(t, diagnostics, "observation input could not be serialized; field omitted", 1)
	assertEdgeDiagnosticCount(t, diagnostics, "observation output exceeds the internal size limit; field omitted", 1)
	assertEdgeDiagnosticCount(t, diagnostics, "observation metadata value could not be serialized; field omitted", 1)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics,
		"cyclic-input-PAYLOAD-611e", "cyclic-metadata-PAYLOAD-214f", "oversize-PAYLOAD-23fb")
}

func TestEdgeContractPanickingUserMethodsAreContained(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	_, observation := client.StartObservation(context.Background(), "panicking-user-methods",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Input: edgePanickingJSON{},
			Metadata: map[string]any{
				"panic": edgePanickingJSON{},
				"safe":  "survives",
			},
			Model: "model-survives",
		})
	observation.RecordError(edgePanickingError{})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "panicking-user-methods")
	want := edgeBaseAttributes("generation")
	want["langfuse.observation.level"] = "ERROR"
	want["langfuse.observation.metadata.safe"] = "survives"
	want["langfuse.observation.model.name"] = "model-survives"
	want["langfuse.observation.status_message"] = "error"
	assertObservationWireAttributes(t, span.span.Attributes, want)
	assertEdgeDiagnosticCount(t, diagnostics, "serializer panicked; field omitted", 2)
	assertEdgeDiagnosticCount(t, diagnostics, "error string method panicked; generic error recorded", 1)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, "PANIC-PAYLOAD-marshal", "PANIC-PAYLOAD-error")
}

func TestEdgeContractPanickingOTelErrorHandlerIsContained(t *testing.T) {
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(edgePanickingErrorHandler{})
	t.Cleanup(func() { otel.SetErrorHandler(previous) })

	client, receiver := newObservationWireClient(t, nil)
	_, observation := client.StartObservation(context.Background(), "handler-panic",
		langfuse.ObservationType("invalid-type"), langfuse.ObservationAttributes{
			Input: edgePanickingJSON{},
		})
	observation.End()
	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "handler-panic")
	assertObservationWireAttributes(t, span.span.Attributes, edgeBaseAttributes("span"))
}

func TestEdgeContractUpdateAfterEndIsNoop(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	_, observation := client.StartObservation(context.Background(), "ended-update", langfuse.TypeSpan,
		langfuse.ObservationAttributes{Input: "before"})
	observation.End()
	const afterEndPayload = "after-end-PAYLOAD-c46d"
	observation.Update(langfuse.ObservationAttributes{
		Output:        afterEndPayload,
		Level:         langfuse.LevelError,
		StatusMessage: afterEndPayload,
	})
	observation.End()

	want := edgeBaseAttributes("span")
	want["langfuse.observation.input"] = "before"
	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "ended-update")
	assertObservationWireAttributes(t, span.span.Attributes, want)
	assertEdgeDiagnosticCount(t, diagnostics, "update ignored after observation end", 1)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, afterEndPayload)
}

func TestEdgeContractStartTimeIsIgnoredByUpdate(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	start := time.Date(2026, 7, 16, 14, 15, 16, 123456789, time.UTC)
	ignored := time.Date(1999, 1, 2, 3, 4, 5, 6, time.UTC)

	_, observation := client.StartObservation(context.Background(), "start-time-update",
		langfuse.TypeSpan, langfuse.ObservationAttributes{StartTime: start})
	observation.Update(langfuse.ObservationAttributes{
		Output:    "update-survives",
		StartTime: ignored,
	})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "start-time-update")
	if got, want := span.span.StartTimeUnixNano, uint64(start.UnixNano()); got != want {
		t.Fatalf("wire start time after Update = %d, want initial StartTime %d (ignored Update was %d)",
			got, want, ignored.UnixNano())
	}
	want := edgeBaseAttributes("span")
	want["langfuse.observation.output"] = "update-survives"
	assertObservationWireAttributes(t, span.span.Attributes, want)
	assertEdgeDiagnosticCount(t, diagnostics, "update start time ignored", 1)
}

func TestEdgeContractEventIgnoresExplicitStartTime(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	historical := time.Now().Add(-24 * time.Hour)
	before := time.Now()
	client.Event(context.Background(), "instant-event", langfuse.ObservationAttributes{StartTime: historical})

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "instant-event")
	start := time.Unix(0, int64(span.span.StartTimeUnixNano))
	end := time.Unix(0, int64(span.span.EndTimeUnixNano))
	if start.Before(before.Add(-time.Second)) {
		t.Fatalf("event start = %v, want recording time near %v (explicit historical time was %v)", start, before, historical)
	}
	if duration := end.Sub(start); duration < 0 || duration > time.Second {
		t.Fatalf("event duration = %v, want an instantaneous recording", duration)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "event start time ignored", 1)
}

func TestEdgeContractObservationMetadataBudgetAppliesAcrossUpdates(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	metadata := make(map[string]any, 32)
	for index := range 32 {
		metadata[fmt.Sprintf("z%02d", index)] = "initial"
	}
	_, observation := client.StartObservation(context.Background(), "metadata-lifetime",
		langfuse.TypeSpan, langfuse.ObservationAttributes{Metadata: metadata})
	update := make(map[string]any, 33)
	for index := range 32 {
		update[fmt.Sprintf("a%02d", index)] = "must-be-omitted"
	}
	update["z00"] = "updated"
	observation.Update(langfuse.ObservationAttributes{Metadata: update})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "metadata-lifetime")
	attributes := observationWireAttributeMap(t, span.span.Attributes)
	count := 0
	for key := range attributes {
		if strings.HasPrefix(key, "langfuse.observation.metadata.") {
			count++
		}
	}
	if count != 32 {
		t.Fatalf("observation metadata entries = %d, want 32", count)
	}
	if got := attributes["langfuse.observation.metadata.z00"]; got != "updated" {
		t.Fatalf("existing metadata update = %#v, want updated", got)
	}
	if _, found := attributes["langfuse.observation.metadata.a00"]; found {
		t.Fatal("new metadata key exceeded lifetime budget but was exported")
	}
	assertEdgeDiagnosticCount(t, diagnostics, "observation metadata exceeds the lifetime entry limit", 1)
}

func TestEdgeContractTraceMetadataReplacementWinsAtFullBudget(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	initial := make(map[string]any, 32)
	for index := range 32 {
		initial[fmt.Sprintf("z%02d", index)] = "initial"
	}
	ctx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{Metadata: initial})
	update := make(map[string]any, 33)
	for index := range 32 {
		update[fmt.Sprintf("a%02d", index)] = "must-be-omitted"
	}
	update["z00"] = "updated"
	ctx = client.WithTraceAttributes(ctx, langfuse.TraceAttributes{Metadata: update})
	_, observation := client.StartObservation(ctx, "trace-metadata-replacement",
		langfuse.TypeSpan, langfuse.ObservationAttributes{})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "trace-metadata-replacement")
	attributes := observationWireAttributeMap(t, span.span.Attributes)
	if got := attributes["langfuse.trace.metadata.z00"]; got != "updated" {
		t.Fatalf("existing trace metadata replacement = %#v, want updated", got)
	}
	if _, found := attributes["langfuse.trace.metadata.a00"]; found {
		t.Fatal("new trace metadata key exceeded lifetime budget but was exported")
	}
	assertEdgeDiagnosticCount(t, diagnostics, "trace metadata exceeds the lifetime entry limit", 1)
}

func TestEdgeContractAggregateObservationByteBudgetPreservesPriorityFields(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	large := strings.Repeat("x", 900<<10)
	_, observation := client.StartObservation(context.Background(), "aggregate-byte-budget",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Model:  "priority-model",
			Usage:  &langfuse.Usage{InputTokens: 3, OutputTokens: 2},
			Input:  large,
			Output: large,
			Metadata: map[string]any{
				"too_large_in_aggregate": large,
			},
		})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "aggregate-byte-budget")
	attributes := observationWireAttributeMap(t, span.span.Attributes)
	for key, want := range map[string]any{
		"langfuse.observation.type":          "generation",
		"langfuse.observation.model.name":    "priority-model",
		"langfuse.observation.usage_details": `{"input":3,"output":2,"total":5}`,
		"langfuse.observation.input":         large,
		"langfuse.observation.output":        large,
	} {
		if got := attributes[key]; got != want {
			t.Fatalf("priority attribute %q was not preserved", key)
		}
	}
	if _, found := attributes["langfuse.observation.metadata.too_large_in_aggregate"]; found {
		t.Fatal("metadata exceeding aggregate observation budget was exported")
	}
	if span.span.DroppedAttributesCount != 0 {
		t.Fatalf("OTel dropped attributes = %d, want SDK-side deterministic omission", span.span.DroppedAttributesCount)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "observation attributes exceed the aggregate size limit", 1)
}

func TestEdgeContractCallerMutationAfterCallIsRaceSafe(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)

	tags := []string{"stable-tag"}
	traceMetadata := map[string]any{"stable": "trace-before"}
	ctx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:     "stable-trace",
		Tags:     tags,
		Metadata: traceMetadata,
	})

	input := map[string]any{"message": "input-before"}
	metadata := map[string]any{"stable": "metadata-before"}
	parameters := map[string]any{"temperature": 0.1}
	details := map[string]int64{"input_audio_tokens": 2}
	usage := &langfuse.Usage{InputTokens: 20, OutputTokens: 10, Details: details}
	costs := map[string]float64{"input": 0.01}
	rootContext, root := client.StartObservation(ctx, "mutation-root", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{
			Input:           input,
			Metadata:        metadata,
			ModelParameters: parameters,
			Usage:           usage,
			CostDetails:     costs,
		})

	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for range 2_000 {
			tags[0] = "caller-mutated-tag"
			traceMetadata["stable"] = "caller-mutated-trace"
			input["message"] = "caller-mutated-input"
			metadata["stable"] = "caller-mutated-metadata"
			parameters["temperature"] = 9.9
			details["input_audio_tokens"] = 19
			usage.InputTokens = 99
			usage.OutputTokens = 88
			costs["input"] = 42
		}
	})
	const childCount = 8
	for index := range childCount {
		workers.Go(func() {
			<-start
			_, child := client.StartObservation(rootContext, "mutation-child-"+string(rune('a'+index)),
				langfuse.TypeSpan, langfuse.ObservationAttributes{})
			child.End()
		})
	}
	close(start)
	workers.Wait()
	root.End()

	spans := exportObservationWireSpans(t, client, receiver, childCount+1)
	rootSpan := observationWireSpanNamed(t, spans, "mutation-root")
	wantRoot := edgeBaseAttributes("generation")
	wantRoot["langfuse.observation.input"] = `{"message":"input-before"}`
	wantRoot["langfuse.observation.metadata.stable"] = "metadata-before"
	wantRoot["langfuse.observation.model.parameters"] = `{"temperature":0.1}`
	wantRoot["langfuse.observation.usage_details"] = `{"input":18,"input_audio_tokens":2,"output":10,"total":30}`
	wantRoot["langfuse.observation.cost_details"] = `{"input":0.01}`
	wantRoot["langfuse.trace.metadata.stable"] = "trace-before"
	wantRoot["langfuse.trace.name"] = "stable-trace"
	wantRoot["langfuse.trace.tags"] = []any{"stable-tag"}
	assertObservationWireAttributes(t, rootSpan.span.Attributes, wantRoot)
	for index := range childCount {
		span := observationWireSpanNamed(t, spans, "mutation-child-"+string(rune('a'+index)))
		wantChild := map[string]any{
			"langfuse.environment":           wireEnv,
			"langfuse.observation.type":      "span",
			"langfuse.release":               wireRelease,
			"langfuse.trace.metadata.stable": "trace-before",
			"langfuse.trace.name":            "stable-trace",
			"langfuse.trace.tags":            []any{"stable-tag"},
		}
		assertObservationWireAttributes(t, span.span.Attributes, wantChild)
	}
}

func TestEdgeContractShutdownRejectsNewWork(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Client.Shutdown() error = %v", err)
	}

	original := context.WithValue(context.Background(), edgeContextKey{}, "preserved")
	const afterShutdownPayload = "after-shutdown-PAYLOAD-350a"
	returned, observation := client.StartObservation(original, afterShutdownPayload,
		langfuse.TypeGeneration, langfuse.ObservationAttributes{Input: afterShutdownPayload})
	if returned != original {
		t.Fatal("StartObservation after shutdown returned a different context")
	}
	if observation.TraceID() != "" || observation.ID() != "" {
		t.Fatalf("StartObservation after shutdown returned recording IDs (%q, %q)", observation.TraceID(), observation.ID())
	}
	observation.Update(langfuse.ObservationAttributes{Output: afterShutdownPayload})
	observation.RecordError(errors.New(afterShutdownPayload))
	observation.End()
	client.Event(original, afterShutdownPayload, langfuse.ObservationAttributes{Input: afterShutdownPayload})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after shutdown error = %v", err)
	}
	if got := len(receiver.Requests()); got != 0 {
		t.Fatalf("OTLP requests after shutdown = %d, want zero", got)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "observation ignored after client shutdown", 1)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, afterShutdownPayload)
}

func TestEdgeContractShutdownLinearizesAgainstStartingObservation(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	providerExporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(providerExporter)),
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})

	maskEntered := make(chan struct{})
	releaseMask := make(chan struct{})
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
		config.Mask = func(value any) any {
			close(maskEntered)
			<-releaseMask
			return value
		}
	})

	observationResult := make(chan *langfuse.Observation, 1)
	go func() {
		_, observation := client.StartObservation(context.Background(), "racing-start",
			langfuse.TypeSpan, langfuse.ObservationAttributes{Input: "synthetic"})
		observationResult <- observation
	}()
	<-maskEntered

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := client.Shutdown(shutdownCtx); err != nil {
		cancel()
		t.Fatalf("Shutdown() error = %v", err)
	}
	cancel()
	close(releaseMask)
	observation := <-observationResult
	if observation.TraceID() != "" || observation.ID() != "" {
		t.Fatalf("racing StartObservation returned recording IDs (%q, %q)", observation.TraceID(), observation.ID())
	}
	observation.End()

	if got := len(providerExporter.GetSpans()); got != 0 {
		t.Fatalf("generic exporter received %d post-shutdown SDK spans, want zero", got)
	}
	if got := len(receiver.Requests()); got != 0 {
		t.Fatalf("Langfuse receiver received %d post-shutdown requests, want zero", got)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "observation ignored after client shutdown", 1)
}

func TestEdgeContractUnicodePropagationUsesCharacterBoundary(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	valid := strings.Repeat("界", 200)
	invalid := strings.Repeat("界", 201)
	if utf8.RuneCountInString(valid) != 200 || len(valid) <= 200 {
		t.Fatalf("valid test string has %d characters and %d bytes, want 200 characters and >200 bytes",
			utf8.RuneCountInString(valid), len(valid))
	}

	validContext := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:      valid,
		UserID:    valid,
		SessionID: valid,
		Tags:      []string{valid},
		Metadata:  map[string]any{"unicode": valid},
		Version:   valid,
	})
	_, validObservation := client.StartObservation(validContext, "unicode-200", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	validObservation.End()

	invalidContext := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:      invalid,
		UserID:    invalid,
		SessionID: invalid,
		Tags:      []string{invalid},
		Metadata:  map[string]any{"unicode": invalid},
		Version:   invalid,
	})
	_, invalidObservation := client.StartObservation(invalidContext, "unicode-201", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	invalidObservation.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	wantValid := edgeBaseAttributes("span")
	wantValid["langfuse.trace.name"] = valid
	wantValid["user.id"] = valid
	wantValid["session.id"] = valid
	wantValid["langfuse.trace.tags"] = []any{valid}
	wantValid["langfuse.trace.metadata.unicode"] = valid
	wantValid["langfuse.version"] = valid
	assertObservationWireAttributes(t,
		observationWireSpanNamed(t, spans, "unicode-200").span.Attributes, wantValid)
	assertObservationWireAttributes(t,
		observationWireSpanNamed(t, spans, "unicode-201").span.Attributes, edgeBaseAttributes("span"))
	assertEdgeDiagnosticCount(t, diagnostics, "exceeds 200 characters", 6)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, valid, invalid)
}

func TestEdgeContractTraceTagLifetimeBudgets(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	shortTags := make([]string, 100)
	for index := range shortTags {
		shortTags[index] = fmt.Sprintf("tag-%03d", index)
	}
	shortCtx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{Tags: shortTags})
	_, shortObservation := client.StartObservation(shortCtx, "tag-count-budget", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	shortObservation.End()

	longTags := make([]string, 100)
	wantLongCount := 0
	longBytes := 0
	for index := range longTags {
		longTags[index] = fmt.Sprintf("%02d", index) + strings.Repeat("界", 198)
		if wantLongCount < 64 && longBytes+len(longTags[index]) <= 16<<10 {
			wantLongCount++
			longBytes += len(longTags[index])
		}
	}
	longCtx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{Tags: longTags})
	_, longObservation := client.StartObservation(longCtx, "tag-byte-budget", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	longObservation.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	shortAttributes := observationWireAttributeMap(t, observationWireSpanNamed(t, spans, "tag-count-budget").span.Attributes)
	shortWireTags, ok := shortAttributes["langfuse.trace.tags"].([]any)
	if !ok || len(shortWireTags) != 64 {
		t.Fatalf("count-budget tags = %#v, want 64 tags", shortAttributes["langfuse.trace.tags"])
	}
	longAttributes := observationWireAttributeMap(t, observationWireSpanNamed(t, spans, "tag-byte-budget").span.Attributes)
	longWireTags, ok := longAttributes["langfuse.trace.tags"].([]any)
	if !ok || len(longWireTags) != wantLongCount {
		t.Fatalf("byte-budget tag count = %d (type ok %v), want %d", len(longWireTags), ok, wantLongCount)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "trace tags exceed the lifetime count or byte limit", 2)
}

type edgeContextKey struct{}

type edgePanickingJSON struct{}

func (edgePanickingJSON) MarshalJSON() ([]byte, error) {
	panic("PANIC-PAYLOAD-marshal")
}

type edgePanickingError struct{}

func (edgePanickingError) Error() string { panic("PANIC-PAYLOAD-error") }

type edgePanickingErrorHandler struct{}

func (edgePanickingErrorHandler) Handle(error) { panic("handler panic") }

func edgeBaseAttributes(observationType string) map[string]any {
	return map[string]any{
		"langfuse.environment":          wireEnv,
		"langfuse.internal.is_app_root": true,
		"langfuse.observation.type":     observationType,
		"langfuse.release":              wireRelease,
	}
}

type edgeDiagnosticRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (r *edgeDiagnosticRecorder) Handle(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, err.Error())
}

func (r *edgeDiagnosticRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.messages...)
}

func captureEdgeDiagnostics(t *testing.T) *edgeDiagnosticRecorder {
	t.Helper()
	previous := otel.GetErrorHandler()
	recorder := &edgeDiagnosticRecorder{}
	otel.SetErrorHandler(recorder)
	t.Cleanup(func() { otel.SetErrorHandler(previous) })
	return recorder
}

func assertEdgeDiagnosticCount(t *testing.T, recorder *edgeDiagnosticRecorder, text string, want int) {
	t.Helper()
	count := 0
	for _, message := range recorder.snapshot() {
		if strings.Contains(message, text) {
			count++
		}
	}
	if count != want {
		t.Fatalf("diagnostics containing %q = %d, want %d; all diagnostics: %v",
			text, count, want, recorder.snapshot())
	}
}

func assertEdgeDiagnosticsPayloadFree(t *testing.T, recorder *edgeDiagnosticRecorder, payloads ...string) {
	t.Helper()
	for _, message := range recorder.snapshot() {
		for _, payload := range payloads {
			if payload != "" && strings.Contains(message, payload) {
				t.Fatalf("diagnostic disclosed caller payload %q: %q", payload, message)
			}
		}
	}
}
