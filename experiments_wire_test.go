package langfuse_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	experimentIDKey      = "langfuse.experiment.id"
	experimentNameKey    = "langfuse.experiment.name"
	experimentDescKey    = "langfuse.experiment.description"
	experimentDatasetKey = "langfuse.experiment.dataset.id"
	experimentMetaKey    = "langfuse.experiment.metadata"
	itemIDKey            = "langfuse.experiment.item.id"
	itemVersionKey       = "langfuse.experiment.item.version"
	itemMetaKey          = "langfuse.experiment.item.metadata"
	itemRootKey          = "langfuse.experiment.item.root_observation_id"
	itemExpectedKey      = "langfuse.experiment.item.expected_output"
	environmentKey       = "langfuse.environment"
)

var experimentKeys = []string{
	experimentIDKey, experimentNameKey, experimentDescKey, experimentDatasetKey, experimentMetaKey,
	itemIDKey, itemVersionKey, itemMetaKey, itemRootKey, itemExpectedKey,
}

func wireExperiment() langfuse.Experiment {
	return langfuse.Experiment{
		ID:          "run-2026-10-01",
		Name:        "triage-v2",
		Description: "prompt v2 against the curated set",
		Metadata:    map[string]any{"model": "m-1", "nested": map[string]any{"depth": 2}},
	}
}

func wireExperimentItem() langfuse.ExperimentItem {
	return langfuse.ExperimentItem{
		ID:             "item-1",
		DatasetID:      "dataset-1",
		Version:        time.Date(2026, 9, 30, 12, 0, 0, 123456789, time.UTC),
		ExpectedOutput: "Paris",
		Metadata:       map[string]any{"difficulty": "easy"},
	}
}

func wireAttributes(t *testing.T, spans []wireSpan, name string) map[string]any {
	t.Helper()
	return observationWireAttributeMap(t, observationWireSpanNamed(t, spans, name).span.Attributes)
}

func newBorrowedExperimentClient(t *testing.T, sampler sdktrace.Sampler) (
	*langfuse.Client, *sdktrace.TracerProvider, *otlpreceiver.Receiver,
) {
	t.Helper()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
		config.ServiceName = ""
	})
	return client, provider, receiver
}

func TestExperimentWireItemTraceCarriesIdentity(t *testing.T) {
	client, provider, receiver := newBorrowedExperimentClient(t, sdktrace.AlwaysSample())

	ambientCtx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Environment: "request_env", Metadata: map[string]any{"tenant": "acme"},
	})
	ambientCtx, ambient := client.StartObservation(ambientCtx, "ambient", langfuse.TypeSpan, langfuse.ObservationAttributes{})

	itemCtx, root, err := client.StartExperimentItem(ambientCtx, wireExperiment(), wireExperimentItem(),
		"item-task", langfuse.ObservationAttributes{Input: "What is the capital of France?"})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	// Re-applies the ambient trace state, including its environment.
	itemCtx = client.WithTraceAttributes(itemCtx, langfuse.TraceAttributes{UserID: "user-1"})
	generationCtx, generation := client.StartObservation(itemCtx, "item-generation", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Model: "m-1", Output: "Paris"})
	client.Event(generationCtx, "item-event", langfuse.ObservationAttributes{})
	_, foreign := provider.Tracer("instrumentation.example").Start(generationCtx, "foreign-generation",
		oteltrace.WithAttributes(
			attribute.String("gen_ai.operation.name", "chat"),
			attribute.String(experimentIDKey, "stale-run"),
			attribute.String(environmentKey, "production"),
		))
	foreign.End()
	generation.End()
	root.Update(langfuse.ObservationAttributes{Output: "Paris"})
	root.End()
	_, evaluator := client.StartObservation(itemCtx, "item-evaluator", langfuse.TypeEvaluator, langfuse.ObservationAttributes{})
	evaluator.End()
	ambient.End()

	spans := exportObservationWireSpans(t, client, receiver, 6)
	if root.TraceID() == ambient.TraceID() {
		t.Fatal("the item root joined the ambient trace")
	}
	assertObservationWireIdentity(t, observationWireSpanNamed(t, spans, "item-task").span, root.TraceID(), root.ID(), "")
	shared := map[string]any{
		experimentIDKey:      "run-2026-10-01",
		experimentNameKey:    "triage-v2",
		experimentDatasetKey: "dataset-1",
		experimentMetaKey:    `{"model":"m-1","nested":{"depth":2}}`,
		itemIDKey:            "item-1",
		itemVersionKey:       "2026-09-30T12:00:00.123Z",
		itemMetaKey:          `{"difficulty":"easy"}`,
		itemRootKey:          root.ID(),
		environmentKey:       "sdk-experiment",
	}
	for _, name := range []string{"item-task", "item-generation", "item-event", "foreign-generation", "item-evaluator"} {
		attributes := wireAttributes(t, spans, name)
		for key, want := range shared {
			if got := attributes[key]; got != want {
				t.Errorf("span %q attribute %s = %#v, want %#v", name, key, got, want)
			}
		}
		if hex.EncodeToString(observationWireSpanNamed(t, spans, name).span.TraceId) != root.TraceID() {
			t.Errorf("span %q left the item trace", name)
		}
		if name == "item-task" {
			continue
		}
		for _, key := range []string{experimentDescKey, itemExpectedKey} {
			if _, found := attributes[key]; found {
				t.Errorf("span %q carries root-only %s", name, key)
			}
		}
	}
	rootAttributes := wireAttributes(t, spans, "item-task")
	if rootAttributes[experimentDescKey] != "prompt v2 against the curated set" || rootAttributes[itemExpectedKey] != "Paris" ||
		rootAttributes["langfuse.trace.metadata.tenant"] != "acme" {
		t.Fatalf("root attributes = %#v", rootAttributes)
	}
	ambientAttributes := wireAttributes(t, spans, "ambient")
	for _, key := range experimentKeys {
		if _, found := ambientAttributes[key]; found {
			t.Fatalf("ambient span carries %s", key)
		}
	}
	if ambientAttributes[environmentKey] != "request_env" {
		t.Fatalf("ambient environment = %#v, want the request override", ambientAttributes[environmentKey])
	}
}

func TestExperimentWireIdentityIsScopedToTheItemTrace(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	second, secondReceiver := newObservationWireClient(t, nil)
	experiment := wireExperiment()

	outerCtx, outer, err := client.StartExperimentItem(context.Background(), experiment,
		langfuse.ExperimentItem{ID: "outer"}, "outer", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("outer StartExperimentItem() error = %v", err)
	}
	detached := oteltrace.ContextWithSpanContext(outerCtx, oteltrace.SpanContext{})
	_, background := client.StartObservation(detached, "detached", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	background.End()

	innerCtx, inner, err := client.StartExperimentItem(outerCtx, experiment,
		langfuse.ExperimentItem{ID: "inner"}, "inner", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("inner StartExperimentItem() error = %v", err)
	}
	_, innerChild := client.StartObservation(innerCtx, "inner-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	innerChild.End()
	inner.End()

	failedCtx, failed, err := client.StartExperimentItem(outerCtx, experiment,
		langfuse.ExperimentItem{ID: ""}, "failed", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrInvalidExperiment) || failed.TraceID() != "" {
		t.Fatalf("invalid nested start = (%q, %v), want a no-op and ErrInvalidExperiment", failed.TraceID(), err)
	}
	_, fallback := client.StartObservation(failedCtx, "after-failed", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	fallback.End()
	_, other := second.StartObservation(outerCtx, "second-client", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	other.End()
	_, outerChild := client.StartObservation(outerCtx, "outer-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	outerChild.End()
	outer.End()

	spans := append(exportObservationWireSpans(t, client, receiver, 6),
		exportObservationWireSpans(t, second, secondReceiver, 1)...)
	for name, want := range map[string]struct {
		itemID, root, traceID any
	}{
		"outer-child":   {"outer", outer.ID(), outer.TraceID()},
		"inner":         {"inner", inner.ID(), inner.TraceID()},
		"inner-child":   {"inner", inner.ID(), inner.TraceID()},
		"detached":      {nil, nil, nil},
		"after-failed":  {nil, nil, nil},
		"second-client": {nil, nil, outer.TraceID()},
	} {
		attributes := wireAttributes(t, spans, name)
		if attributes[itemIDKey] != want.itemID || attributes[itemRootKey] != want.root {
			t.Errorf("span %q identity = (%#v, %#v), want (%#v, %#v)",
				name, attributes[itemIDKey], attributes[itemRootKey], want.itemID, want.root)
		}
		traceID := hex.EncodeToString(observationWireSpanNamed(t, spans, name).span.TraceId)
		if want.traceID != nil && traceID != want.traceID || want.traceID == nil && traceID == outer.TraceID() {
			t.Errorf("span %q trace = %s, want %v", name, traceID, want.traceID)
		}
		if want.itemID == nil && attributes[environmentKey] != wireEnv {
			t.Errorf("span %q environment = %#v, want the client environment", name, attributes[environmentKey])
		}
	}
	if span := observationWireSpanNamed(t, spans, "after-failed").span; len(span.ParentSpanId) != 0 {
		t.Fatal("work after a failed nested start kept the outer span as its parent")
	}
}

func TestExperimentWireItemRootsIgnoreWithParent(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	_, run := client.StartObservation(context.Background(), "run", langfuse.TypeAgent, langfuse.ObservationAttributes{})
	parentCtx := client.WithParent(context.Background(), run)

	itemCtx, root, err := client.StartExperimentItem(parentCtx, wireExperiment(), langfuse.ExperimentItem{ID: "item"},
		"item", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	_, child := client.StartObservation(itemCtx, "item-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	root.End()
	failedCtx, _, err := client.StartExperimentItem(parentCtx, wireExperiment(), langfuse.ExperimentItem{},
		"failed", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrInvalidExperiment) {
		t.Fatalf("invalid start error = %v, want ErrInvalidExperiment", err)
	}
	_, afterFailed := client.StartObservation(failedCtx, "after-failed", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	afterFailed.End()
	_, underRun := client.StartObservation(client.WithParent(itemCtx, run), "under-run", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	underRun.End()
	run.End()

	spans := exportObservationWireSpans(t, client, receiver, 5)
	runSpan := observationWireSpanNamed(t, spans, "run").span
	itemSpan := observationWireSpanNamed(t, spans, "item").span
	if len(itemSpan.ParentSpanId) != 0 || bytes.Equal(itemSpan.TraceId, runSpan.TraceId) {
		t.Fatal("the item root joined the WithParent parent instead of starting its own trace")
	}
	if span := observationWireSpanNamed(t, spans, "item-child").span; !bytes.Equal(span.ParentSpanId, itemSpan.SpanId) {
		t.Fatalf("item child parent = %x, want the item root %x", span.ParentSpanId, itemSpan.SpanId)
	}
	if span := observationWireSpanNamed(t, spans, "after-failed").span; len(span.ParentSpanId) != 0 ||
		bytes.Equal(span.TraceId, runSpan.TraceId) {
		t.Fatal("work after a failed start kept the WithParent parent")
	}
	if span := observationWireSpanNamed(t, spans, "under-run").span; !bytes.Equal(span.ParentSpanId, runSpan.SpanId) {
		t.Fatalf("WithParent from an item context parent = %x, want the run %x", span.ParentSpanId, runSpan.SpanId)
	}
	attributes := wireAttributes(t, spans, "under-run")
	for _, key := range experimentKeys {
		if _, found := attributes[key]; found {
			t.Errorf("a span outside the item trace carries %s", key)
		}
	}
	if attributes[environmentKey] != wireEnv {
		t.Errorf("span outside the item trace environment = %#v, want the client environment", attributes[environmentKey])
	}
}

func TestExperimentWireAlwaysSamplesOnlyTheItemTrace(t *testing.T) {
	zero := 0.0
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) { config.SampleRate = &zero })

	ctx := client.WithSampleRate(context.Background(), 0)
	itemCtx, root, err := client.StartExperimentItem(ctx, wireExperiment(), wireExperimentItem(),
		"sampled-item", langfuse.ObservationAttributes{})
	if err != nil || !root.Sampled() {
		t.Fatalf("StartExperimentItem() = (sampled %t, %v), want a sampled item at rate 0", root.Sampled(), err)
	}
	_, child := client.StartObservation(itemCtx, "sampled-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	root.End()

	detached := oteltrace.ContextWithSpanContext(itemCtx, oteltrace.SpanContext{})
	_, later := client.StartObservation(detached, "later-trace", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	_, unrelated := client.StartObservation(ctx, "unrelated", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	if later.Sampled() || unrelated.Sampled() {
		t.Fatalf("later trace sampled %t, unrelated sampled %t; want both dropped at rate 0",
			later.Sampled(), unrelated.Sampled())
	}
	later.End()
	unrelated.End()
	spans := exportObservationWireSpans(t, client, receiver, 2)
	observationWireSpanNamed(t, spans, "sampled-item")
	observationWireSpanNamed(t, spans, "sampled-child")
}

func TestExperimentWireRejectsSampledOutRoots(t *testing.T) {
	for name, sampler := range map[string]sdktrace.Sampler{
		"drop":        sdktrace.NeverSample(),
		"record-only": recordOnlySampler{},
	} {
		t.Run(name, func(t *testing.T) {
			client, _, _ := newBorrowedExperimentClient(t, sampler)
			ctx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(),
				wireExperimentItem(), "rejected-item", langfuse.ObservationAttributes{})
			if !errors.Is(err, langfuse.ErrExperimentItemNotExported) || root.TraceID() != "" {
				t.Fatalf("StartExperimentItem() = (%q, %v), want a no-op and ErrExperimentItemNotExported",
					root.TraceID(), err)
			}
			if oteltrace.SpanContextFromContext(ctx).IsValid() {
				t.Fatal("the rejected start returned a context with an ambient span")
			}
		})
	}
}

type experimentNullJSON struct{}

func (experimentNullJSON) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

func TestExperimentWireExpectedOutputEncoding(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	cases := map[string]struct {
		value any
		want  any // nil means absent
	}{
		"empty string":        {value: "", want: ""},
		"JSON-looking string": {value: `{"a":1}`, want: `{"a":1}`},
		"structure":           {value: map[string]any{"b": 2, "a": []any{1, "x"}}, want: `{"a":[1,"x"],"b":2}`},
		"number":              {value: 3.5, want: "3.5"},
		"raw string":          {value: json.RawMessage(`"Paris"`), want: "Paris"},
		"raw object":          {value: json.RawMessage(" {\"n\": 12345678901234567890} "), want: `{"n":12345678901234567890}`},
		"raw null":            {value: json.RawMessage(`null`), want: nil},
		"raw empty":           {value: json.RawMessage(" "), want: nil},
		"null marshaler":      {value: experimentNullJSON{}, want: nil},
		"nil":                 {value: nil, want: nil},
		"typed nil":           {value: map[string]any(nil), want: nil},
	}
	for name, test := range cases {
		item := wireExperimentItem()
		item.ExpectedOutput = test.value
		_, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), item, name,
			langfuse.ObservationAttributes{})
		if err != nil {
			t.Fatalf("%s: StartExperimentItem() error = %v", name, err)
		}
		root.End()
	}
	spans := exportObservationWireSpans(t, client, receiver, len(cases))
	for name, test := range cases {
		got, found := wireAttributes(t, spans, name)[itemExpectedKey]
		if test.want == nil && found || test.want != nil && got != test.want {
			t.Errorf("%s: expected output = %#v (present %t), want %#v", name, got, found, test.want)
		}
	}
}

func TestExperimentWireExpectedOutputFollowsContentCapture(t *testing.T) {
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.DisableContentCapture = true
	})
	_, hidden, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"capture-off", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	hidden.End()
	optedIn := client.WithContentCapture(context.Background(), true)
	_, shown, err := client.StartExperimentItem(optedIn, wireExperiment(), wireExperimentItem(),
		"capture-on", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	shown.End()
	spans := exportObservationWireSpans(t, client, receiver, 2)
	if _, found := wireAttributes(t, spans, "capture-off")[itemExpectedKey]; found {
		t.Fatal("expected output exported with content capture disabled")
	}
	if got := wireAttributes(t, spans, "capture-off")[itemIDKey]; got != "item-1" {
		t.Fatalf("identity must not depend on content capture; item ID = %#v", got)
	}
	if got := wireAttributes(t, spans, "capture-on")[itemExpectedKey]; got != "Paris" {
		t.Fatalf("opted-in expected output = %#v", got)
	}
}

func TestExperimentWireMasksEachValueOnce(t *testing.T) {
	var mu sync.Mutex
	calls := map[langfuse.MaskField]int{}
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.Mask = func(field langfuse.MaskField, value any) any {
			mu.Lock()
			calls[field]++
			mu.Unlock()
			switch field {
			case langfuse.MaskExperimentMetadata:
				return map[string]any{"model": "[redacted]"}
			case langfuse.MaskExperimentItemMetadata:
				return nil
			case langfuse.MaskExperimentItemExpectedOutput:
				return "[expected]"
			default:
				return value
			}
		}
	})
	itemCtx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"masked-item", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	_, child := client.StartObservation(itemCtx, "masked-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	root.End()
	spans := exportObservationWireSpans(t, client, receiver, 2)
	for _, span := range spans {
		attributes := observationWireAttributeMap(t, span.span.Attributes)
		if attributes[experimentMetaKey] != `{"model":"[redacted]"}` {
			t.Fatalf("span %q experiment metadata = %#v", span.span.Name, attributes[experimentMetaKey])
		}
		if _, found := attributes[itemMetaKey]; found {
			t.Fatalf("span %q kept masked-out item metadata", span.span.Name)
		}
	}
	if got := wireAttributes(t, spans, "masked-item")[itemExpectedKey]; got != "[expected]" {
		t.Fatalf("expected output = %#v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, field := range []langfuse.MaskField{
		langfuse.MaskExperimentMetadata, langfuse.MaskExperimentItemMetadata, langfuse.MaskExperimentItemExpectedOutput,
	} {
		if calls[field] != 1 {
			t.Errorf("Mask(%q) calls = %d, want 1", field, calls[field])
		}
	}
}

func TestExperimentRejectsInvalidInputAtomically(t *testing.T) {
	run := langfuse.Experiment{ID: "run-1", Name: "triage"}
	item := langfuse.ExperimentItem{ID: "item-1"}
	expected := func(value any) langfuse.ExperimentItem {
		return langfuse.ExperimentItem{ID: "item-1", ExpectedOutput: value}
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	for name, test := range map[string]struct {
		experiment langfuse.Experiment
		item       langfuse.ExperimentItem
		mask       func(langfuse.MaskField, any) any
	}{
		"empty experiment ID": {experiment: langfuse.Experiment{Name: "triage"}, item: item},
		"empty name":          {experiment: langfuse.Experiment{ID: "run-1"}, item: item},
		"empty item ID":       {experiment: run, item: langfuse.ExperimentItem{}},
		"long ID":             {experiment: run, item: langfuse.ExperimentItem{ID: strings.Repeat("x", 256)}},
		"control character":   {experiment: langfuse.Experiment{ID: "run-1", Name: "a\nb"}, item: item},
		"invalid UTF-8 ID":    {experiment: run, item: langfuse.ExperimentItem{ID: "item-1", DatasetID: "\xff"}},
		"long description": {
			experiment: langfuse.Experiment{ID: "run-1", Name: "triage", Description: strings.Repeat("d", 16<<10+1)},
			item:       item,
		},
		"version out of range": {
			experiment: run,
			item:       langfuse.ExperimentItem{ID: "item-1", Version: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		"oversized metadata": {
			experiment: langfuse.Experiment{ID: "run-1", Name: "triage", Metadata: map[string]any{"blob": strings.Repeat("m", 16<<10)}},
			item:       item,
		},
		"cyclic metadata":               {experiment: run, item: langfuse.ExperimentItem{ID: "item-1", Metadata: cycle}},
		"oversized expected output":     {experiment: run, item: expected(strings.Repeat("e", 256<<10+1))},
		"invalid UTF-8 expected output": {experiment: run, item: expected("\xff")},
		"invalid raw expected output":   {experiment: run, item: expected(json.RawMessage(`{"a":`))},
		"unsupported expected output":   {experiment: run, item: expected(func() {})},
		"panicking marshaler":           {experiment: run, item: expected(edgePanickingJSON{})},
		"mask panic": {experiment: run, item: expected("Paris"), mask: func(langfuse.MaskField, any) any {
			panic("PANIC-PAYLOAD")
		}},
		"mask changes metadata type": {
			experiment: langfuse.Experiment{ID: "run-1", Name: "triage", Metadata: map[string]any{"k": "v"}},
			item:       item,
			mask:       func(langfuse.MaskField, any) any { return "not a map" },
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, receiver := newObservationWireClient(t, func(config *langfuse.Config) { config.Mask = test.mask })
			ctx, root, err := client.StartExperimentItem(context.Background(), test.experiment, test.item, "invalid",
				langfuse.ObservationAttributes{})
			if !errors.Is(err, langfuse.ErrInvalidExperiment) {
				t.Fatalf("StartExperimentItem() error = %v, want ErrInvalidExperiment", err)
			}
			if ctx == nil || root == nil || root.TraceID() != "" {
				t.Fatalf("rejected start returned ctx %v and observation %q", ctx, root.TraceID())
			}
			for _, secret := range []string{"PANIC-PAYLOAD", "run-1", "triage", "item-1", "\xff"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error %q leaks %q", err, secret)
				}
			}
			flushClient(t, client)
			if got := len(receiver.Requests()); got != 0 {
				t.Fatalf("a rejected start exported %d requests", got)
			}
		})
	}
}

func TestExperimentUnavailableClients(t *testing.T) {
	masked := false
	mask := func(langfuse.MaskField, any) any { masked = true; return nil }
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true, Mask: mask})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stopped, _ := newObservationWireClient(t, func(config *langfuse.Config) { config.Mask = mask })
	if err := stopped.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	type callerKey struct{}
	ctx := context.WithValue(context.Background(), callerKey{}, "caller")
	for name, test := range map[string]struct {
		client  *langfuse.Client
		wantErr bool
	}{
		"nil":      {client: nil},
		"disabled": {client: disabled},
		"stopped":  {client: stopped, wantErr: true},
	} {
		got, root, err := test.client.StartExperimentItem(ctx, wireExperiment(), wireExperimentItem(), "x",
			langfuse.ObservationAttributes{})
		if (err != nil) != test.wantErr || root == nil || root.TraceID() != "" || !test.wantErr && got != ctx {
			t.Fatalf("%s client = (%v, %q, %v), want ctx unchanged unless failing, a no-op, and error %t",
				name, got, root.TraceID(), err, test.wantErr)
		}
		invalid := wireExperimentItem()
		invalid.ID = ""
		if _, _, err := test.client.StartExperimentItem(ctx, wireExperiment(), invalid, "x",
			langfuse.ObservationAttributes{}); !errors.Is(err, langfuse.ErrInvalidExperiment) {
			t.Fatalf("%s client accepted invalid input: %v", name, err)
		}
		//nolint:staticcheck // A nil context is the input under test.
		if got, _, err := test.client.StartExperimentItem(nil, wireExperiment(), wireExperimentItem(), "x",
			langfuse.ObservationAttributes{}); err == nil || got != nil {
			t.Fatalf("%s client accepted a nil context", name)
		}
	}
	if masked {
		t.Fatal("an unavailable client called Mask")
	}
}

func TestExperimentWireMaximalItemRootExportsWhole(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	large := func(size int) string { return strings.Repeat("x", size) }
	traceMetadata := make(map[string]any, 32)
	observationMetadata := make(map[string]any, 32)
	for index := range 32 {
		key := string(rune('a'+index%26)) + strings.Repeat("k", index)
		traceMetadata[key] = large(190)
		observationMetadata[key] = large(1024)
	}
	tags := make([]string, 64)
	for index := range tags {
		tags[index] = large(190) + strings.Repeat("t", index%10) + string(rune('A'+index))
	}
	ctx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name: large(200), UserID: large(200), SessionID: large(200), Version: large(200),
		Tags: tags, Metadata: traceMetadata,
	})
	metadataBlob := large(16<<10 - 32)
	experiment := wireExperiment()
	experiment.Description = large(16 << 10)
	experiment.Metadata = map[string]any{"blob": metadataBlob}
	item := wireExperimentItem()
	item.ID = large(255)
	item.ExpectedOutput = large(256 << 10)
	item.Metadata = map[string]any{"blob": metadataBlob}
	input, output := large(1<<20-1), large(900<<10)
	itemCtx, root, err := client.StartExperimentItem(ctx, experiment, item, "maximal-item", langfuse.ObservationAttributes{
		Input: input, Metadata: observationMetadata,
	})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	root.Update(langfuse.ObservationAttributes{Output: output})
	_, child := client.StartObservation(itemCtx, "maximal-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	root.End()
	spans := exportObservationWireSpans(t, client, receiver, 2)
	rootWire := observationWireSpanNamed(t, spans, "maximal-item").span
	if rootWire.DroppedAttributesCount != 0 {
		t.Fatalf("maximal root dropped %d attributes", rootWire.DroppedAttributesCount)
	}
	attributes := observationWireAttributeMap(t, rootWire.Attributes)
	metadataJSON := `{"blob":"` + metadataBlob + `"}`
	for key, want := range map[string]string{
		experimentIDKey:               experiment.ID,
		itemIDKey:                     item.ID,
		itemRootKey:                   root.ID(),
		itemExpectedKey:               item.ExpectedOutput.(string),
		experimentDescKey:             experiment.Description,
		experimentMetaKey:             metadataJSON,
		itemMetaKey:                   metadataJSON,
		"langfuse.observation.input":  input,
		"langfuse.observation.output": output,
	} {
		if got, _ := attributes[key].(string); got != want {
			t.Errorf("maximal root %s has %d bytes, want the complete %d-byte value", key, len(got), len(want))
		}
	}
}

func TestExperimentScoresUseTheItemEnvironment(t *testing.T) {
	client, receiver := newScoreWireClient(t, nil)
	itemCtx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"scored", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	root.End()
	value := 1.0
	cases := map[string]struct {
		itemContext bool
		traceID     string
		want        string
	}{
		"item":        {itemContext: true, traceID: root.TraceID(), want: "sdk-experiment"},
		"other-trace": {itemContext: true, traceID: "0123456789abcdef0123456789abcdef", want: "score_wire"},
		"no-context":  {traceID: root.TraceID(), want: "score_wire"},
	}
	for name, test := range cases {
		ctx := context.Background()
		if test.itemContext {
			ctx = itemCtx
		}
		if err := client.RecordScore(ctx, langfuse.Score{
			Name: name, TraceID: test.traceID, ObservationID: root.ID(), NumericValue: &value,
		}); err != nil {
			t.Fatalf("%s: RecordScore() error = %v", name, err)
		}
	}
	flushClient(t, client)
	got := map[string]any{}
	for _, request := range receiver.all() {
		if strings.HasSuffix(request.path, "/api/public/ingestion") {
			_, body := scoreWireEvent(t, request)
			name, _ := body["name"].(string)
			got[name] = body["environment"]
		}
	}
	for name, test := range cases {
		if got[name] != test.want {
			t.Errorf("score %q environment = %#v, want %q", name, got[name], test.want)
		}
	}
}

func TestExperimentWireRejectsRootsTheExportFilterDrops(t *testing.T) {
	generationsOnly := func(span sdktrace.ReadOnlySpan) bool {
		for _, kv := range span.Attributes() {
			if kv.Key == "langfuse.observation.type" {
				return kv.Value.AsString() == "generation"
			}
		}
		return false
	}
	for name, filter := range map[string]func(sdktrace.ReadOnlySpan) bool{
		"generations only": generationsOnly,
		"panicking":        func(sdktrace.ReadOnlySpan) bool { panic("filter") },
	} {
		t.Run(name, func(t *testing.T) {
			client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
				config.ShouldExportSpan = filter
			})
			ctx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(),
				wireExperimentItem(), "filtered-item", langfuse.ObservationAttributes{})
			if !errors.Is(err, langfuse.ErrExperimentItemNotExported) || root.TraceID() != "" ||
				oteltrace.SpanContextFromContext(ctx).IsValid() {
				t.Fatalf("StartExperimentItem() = (%q, %v), want a no-op and ErrExperimentItemNotExported",
					root.TraceID(), err)
			}
			_, generation := client.StartObservation(ctx, "detached", langfuse.TypeGeneration, langfuse.ObservationAttributes{})
			generation.End()
			flushClient(t, client)
			for _, request := range receiver.Requests() {
				for _, span := range otlpreceiver.Spans(request) {
					attributes := observationWireAttributeMap(t, span.Attributes)
					if _, linked := attributes[itemRootKey]; linked || span.Name == "filtered-item" {
						t.Fatalf("exported %q with attributes %v after a rejected start", span.Name, attributes)
					}
				}
			}
		})
	}
}

func TestExperimentWireRejectsIdentityDroppedBySpanLimits(t *testing.T) {
	limits := sdktrace.NewSpanLimits()
	limits.AttributeCountLimit = 6
	provider := sdktrace.NewTracerProvider(sdktrace.WithRawSpanLimits(limits))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client, _ := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
		config.ServiceName = ""
	})
	ctx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"limited-item", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrExperimentItemNotExported) || root.TraceID() != "" ||
		oteltrace.SpanContextFromContext(ctx).IsValid() {
		t.Fatalf("StartExperimentItem() = (%q, %v), want a no-op and ErrExperimentItemNotExported", root.TraceID(), err)
	}

	limits.AttributeCountLimit = 128
	limits.AttributeValueLengthLimit = 8
	truncating := sdktrace.NewTracerProvider(sdktrace.WithRawSpanLimits(limits))
	t.Cleanup(func() { _ = truncating.Shutdown(context.Background()) })
	client, _ = newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = truncating
		config.ServiceName = ""
	})
	if _, _, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"truncated-item", langfuse.ObservationAttributes{}); !errors.Is(err, langfuse.ErrExperimentItemNotExported) {
		t.Fatalf("truncated identity error = %v, want ErrExperimentItemNotExported", err)
	}

	// Truncated content is the provider's policy, as for observation input.
	limits.AttributeValueLengthLimit = 64
	contentLimited := sdktrace.NewTracerProvider(sdktrace.WithRawSpanLimits(limits))
	t.Cleanup(func() { _ = contentLimited.Shutdown(context.Background()) })
	client, _ = newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = contentLimited
		config.ServiceName = ""
	})
	item := wireExperimentItem()
	item.ExpectedOutput = strings.Repeat("long expected output ", 10)
	if _, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), item,
		"content-limited-item", langfuse.ObservationAttributes{}); err != nil || root.TraceID() == "" {
		t.Fatalf("content-limited start = (%q, %v), want a started item", root.TraceID(), err)
	}
}

func TestExperimentWireEnvironmentHelpersKeepItemSpansInTheExperiment(t *testing.T) {
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = nil
	})
	itemCtx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"env-item", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	itemCtx = client.WithTraceAttributes(itemCtx, langfuse.TraceAttributes{Environment: "production"})
	member, err := baggage.NewMember("langfuse_environment", "staging")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	importedCtx, imported, err := client.StartExperimentItem(context.Background(), wireExperiment(),
		wireExperimentItem(), "env-imported", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	client.WithTraceAttributesFromBaggage(baggage.ContextWithBaggage(importedCtx, bag))
	imported.End()
	_, child := client.StartObservation(itemCtx, "env-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	childCtx := client.WithTraceAttributes(itemCtx, langfuse.TraceAttributes{Environment: "production"})
	child.End()
	root.End()
	_, evaluator := client.StartObservation(childCtx, "env-evaluator", langfuse.TypeEvaluator, langfuse.ObservationAttributes{})
	evaluator.End()

	// Outside an item the helpers still set the environment.
	plainCtx, plain := client.StartObservation(context.Background(), "env-plain", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	client.WithTraceAttributes(plainCtx, langfuse.TraceAttributes{Environment: "production"})
	plain.End()

	spans := exportObservationWireSpans(t, client, receiver, 5)
	for _, name := range []string{"env-item", "env-imported", "env-child", "env-evaluator"} {
		if got := wireAttributes(t, spans, name)[environmentKey]; got != "sdk-experiment" {
			t.Errorf("%s environment = %v, want sdk-experiment", name, got)
		}
	}
	if got := wireAttributes(t, spans, "env-plain")[environmentKey]; got != "production" {
		t.Errorf("plain span environment = %v, want production", got)
	}
}

func TestExperimentWireItemMetadataShapes(t *testing.T) {
	for name, test := range map[string]struct {
		metadata any
		mask     func(langfuse.MaskField, any) any
		want     any
		fails    bool
	}{
		"map":             {metadata: map[string]any{"a": 1}, want: `{"a":1}`},
		"raw object":      {metadata: json.RawMessage(` {"n": 12345678901234567890} `), want: `{"n":12345678901234567890}`},
		"struct":          {metadata: struct{ Region string }{"eu"}, want: `{"Region":"eu"}`},
		"array":           {metadata: json.RawMessage(`["x", 1]`)},
		"large string":    {metadata: strings.Repeat("x", 20000)},
		"large slice":     {metadata: []string{strings.Repeat("x", 20000)}},
		"large raw array": {metadata: json.RawMessage(`["` + strings.Repeat("x", 20000) + `"]`)},
		"time":            {metadata: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		"pointer to map":  {metadata: &map[string]any{"p": true}, want: `{"p":true}`},
		"string":          {metadata: "label"},
		"empty object":    {metadata: json.RawMessage(`{}`)},
		"invalid raw":     {metadata: json.RawMessage(`{"a":`), fails: true},
		"masked to array": {metadata: map[string]any{"a": 1}, mask: func(langfuse.MaskField, any) any { return []any{1} }, fails: true},
		"masked away":     {metadata: map[string]any{"a": 1}, mask: func(langfuse.MaskField, any) any { return nil }},
	} {
		t.Run(name, func(t *testing.T) {
			client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
				if test.mask != nil {
					config.Mask = func(field langfuse.MaskField, value any) any {
						if field == langfuse.MaskExperimentItemMetadata {
							return test.mask(field, value)
						}
						return value
					}
				}
			})
			_, root, err := client.StartExperimentItem(context.Background(), langfuse.Experiment{ID: "run", Name: "run"},
				langfuse.ExperimentItem{ID: "item", Metadata: test.metadata}, "shape", langfuse.ObservationAttributes{})
			if test.fails {
				if !errors.Is(err, langfuse.ErrInvalidExperiment) {
					t.Fatalf("StartExperimentItem() error = %v, want ErrInvalidExperiment", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("StartExperimentItem() error = %v", err)
			}
			root.End()
			attributes := wireAttributes(t, exportObservationWireSpans(t, client, receiver, 1), "shape")
			if got := attributes[itemMetaKey]; got != test.want {
				t.Fatalf("item metadata attribute = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestExperimentWireFiltersSeeTheItemIdentity(t *testing.T) {
	attributeIs := func(span sdktrace.ReadOnlySpan, key, want string) bool {
		for _, kv := range span.Attributes() {
			if string(kv.Key) == key {
				return want == "" && kv.Value.AsString() != "" || kv.Value.AsString() == want
			}
		}
		return false
	}
	for name, filter := range map[string]func(sdktrace.ReadOnlySpan) bool{
		"experiment environment": func(span sdktrace.ReadOnlySpan) bool {
			return attributeIs(span, environmentKey, "sdk-experiment")
		},
		"experiment ID": func(span sdktrace.ReadOnlySpan) bool { return attributeIs(span, experimentIDKey, "") },
	} {
		t.Run(name, func(t *testing.T) {
			client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
				config.ShouldExportSpan = filter
			})
			itemCtx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
				"filtered-item", langfuse.ObservationAttributes{})
			if err != nil {
				t.Fatalf("StartExperimentItem() error = %v", err)
			}
			_, child := client.StartObservation(itemCtx, "filtered-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
			child.End()
			root.End()
			_, outside := client.StartObservation(context.Background(), "outside", langfuse.TypeSpan, langfuse.ObservationAttributes{})
			outside.End()
			spans := exportObservationWireSpans(t, client, receiver, 2)
			if got := wireAttributes(t, spans, "filtered-item")[itemRootKey]; got != root.ID() {
				t.Fatalf("root identity = %v, want %s", got, root.ID())
			}
			wireAttributes(t, spans, "filtered-child")
		})
	}

	// A linked run gets its experiment ID only after the start.
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, func(config *langfuse.Config) {
		config.ShouldExportSpan = func(span sdktrace.ReadOnlySpan) bool { return attributeIs(span, experimentIDKey, "") }
	})
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name: "filtered", RunName: "filtered", Items: []langfuse.ExperimentItem{{ID: "a", DatasetID: "ds", Input: 1}},
		Task: func(context.Context, langfuse.ExperimentItem) (any, error) { return "out", nil },
	})
	if err != nil || result.ItemResults[0].Err != nil {
		t.Fatalf("RunExperiment() = %+v, %v; want the item to run", result.ItemResults, err)
	}
}

func TestExperimentWireRejectedRootsExportNoItemIdentity(t *testing.T) {
	limits := sdktrace.NewSpanLimits()
	limits.AttributeValueLengthLimit = 32
	provider := sdktrace.NewTracerProvider(sdktrace.WithRawSpanLimits(limits))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
		config.ServiceName = ""
	})
	_, _, err := client.StartExperimentItem(context.Background(), langfuse.Experiment{ID: "run-1", Name: "run"},
		langfuse.ExperimentItem{ID: strings.Repeat("i", 80)}, "rejected-root", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrExperimentItemNotExported) {
		t.Fatalf("StartExperimentItem() error = %v, want ErrExperimentItemNotExported", err)
	}
	attributes := wireAttributes(t, exportObservationWireSpans(t, client, receiver, 1), "rejected-root")
	for _, key := range []string{experimentIDKey, experimentNameKey, itemIDKey, itemRootKey} {
		if value, ok := attributes[key]; ok && value != "" {
			t.Errorf("rejected root exports %s = %v", key, value)
		}
	}
	// The provider's 32-byte limit also truncates the status text.
	full, status := "langfuse: experiment item root rejected", fmt.Sprint(attributes["langfuse.observation.status_message"])
	if attributes["langfuse.observation.level"] != "ERROR" || status == "" || !strings.HasPrefix(full, status) {
		t.Errorf("rejected root status = %v", attributes)
	}
}

func TestExperimentWireLateAcceptedRootsAreTheOnlyApplicationRoot(t *testing.T) {
	nonEmpty := func(key string) func(sdktrace.ReadOnlySpan) bool {
		return func(span sdktrace.ReadOnlySpan) bool {
			for _, kv := range span.Attributes() {
				if string(kv.Key) == key {
					return kv.Value.AsString() != ""
				}
			}
			return false
		}
	}
	for name, test := range map[string]struct {
		key   string
		items []langfuse.ExperimentItem
	}{
		"linked experiment ID": {experimentIDKey, []langfuse.ExperimentItem{{ID: "a", DatasetID: "ds", Input: 1}}},
		"root observation ID":  {itemRootKey, []langfuse.ExperimentItem{{ID: "a", Input: 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			server := newRunnerServer(t)
			client := newRunnerClient(t, server, func(config *langfuse.Config) { config.ShouldExportSpan = nonEmpty(test.key) })
			result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
				Name: "late", RunName: "late", Items: test.items,
				Task: func(ctx context.Context, _ langfuse.ExperimentItem) (any, error) {
					_, child := client.StartObservation(ctx, "child", langfuse.TypeGeneration, langfuse.ObservationAttributes{})
					child.End()
					return "out", nil
				},
				Evaluators: []langfuse.Evaluator{exactMatch},
			})
			if err != nil || result.ItemResults[0].Err != nil {
				t.Fatalf("RunExperiment() = %+v, %v", result.ItemResults, err)
			}
			spans, _, _ := server.recorded()
			roots := map[string]bool{}
			for _, span := range spans {
				if spanAttributes(t, span)["langfuse.internal.is_app_root"] == true {
					roots[span.Name] = true
				}
			}
			if len(spans) != 3 || !reflect.DeepEqual(roots, map[string]bool{"experiment-item-run": true}) {
				t.Fatalf("%d spans; application roots = %v, want only the item root", len(spans), roots)
			}
		})
	}
}
