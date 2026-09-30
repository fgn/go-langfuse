package langfuse_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
	"go.opentelemetry.io/otel/attribute"
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
		DatasetID:   "dataset-1",
		Metadata:    map[string]any{"model": "m-1", "nested": map[string]any{"depth": 2}},
	}
}

func wireExperimentItem() langfuse.ExperimentItem {
	return langfuse.ExperimentItem{
		ID:             "item-1",
		Version:        time.Date(2026, 9, 30, 12, 0, 0, 123456789, time.UTC),
		ExpectedOutput: "Paris",
		Metadata:       map[string]any{"difficulty": "easy"},
	}
}

// wireAttributes returns one exported span's attributes by span name.
func wireAttributes(t *testing.T, spans []wireSpan, name string) map[string]any {
	t.Helper()
	return observationWireAttributeMap(t, observationWireSpanNamed(t, spans, name).span.Attributes)
}

func TestExperimentWireItemTraceCarriesAuthoritativeIdentity(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)

	// An ambient request trace with a request-scoped environment override:
	// the item must leave both behind.
	ambientCtx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name: "request", Environment: "request_env", Metadata: map[string]any{"tenant": "acme"},
	})
	ambientCtx, ambient := client.StartObservation(ambientCtx, "ambient", langfuse.TypeSpan, langfuse.ObservationAttributes{})

	itemCtx, root, err := client.StartExperimentItem(ambientCtx, wireExperiment(), wireExperimentItem(),
		"item-task", langfuse.ObservationAttributes{Input: "What is the capital of France?"})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	if root.TraceID() == "" || root.TraceID() == ambient.TraceID() {
		t.Fatalf("item trace %q must be a new trace, not the ambient %q", root.TraceID(), ambient.TraceID())
	}
	// A request-scoped environment set inside the item cannot move it out of
	// sdk-experiment, neither on the root nor on later children.
	itemCtx = client.WithTraceAttributes(itemCtx, langfuse.TraceAttributes{Environment: "late_env"})
	generationCtx, generation := client.StartObservation(itemCtx, "item-generation", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Model: "m-1", Output: "Paris"})
	client.Event(generationCtx, "item-event", langfuse.ObservationAttributes{})
	generation.End()
	root.Update(langfuse.ObservationAttributes{Output: "Paris"})
	root.End()
	// An evaluator after End still belongs to the item.
	_, evaluator := client.StartObservation(itemCtx, "item-evaluator", langfuse.TypeEvaluator, langfuse.ObservationAttributes{})
	evaluator.End()
	ambient.End()

	spans := exportObservationWireSpans(t, client, receiver, 5)
	rootWire := observationWireSpanNamed(t, spans, "item-task")
	assertObservationWireIdentity(t, rootWire.span, root.TraceID(), root.ID(), "")
	// Leaves are normalized to strings, as Python's dotted attributes are.
	metadataJSON := `{"model":"m-1","nested":{"depth":"2"}}`
	shared := map[string]any{
		experimentIDKey:      "run-2026-10-01",
		experimentNameKey:    "triage-v2",
		experimentDatasetKey: "dataset-1",
		experimentMetaKey:    metadataJSON,
		itemIDKey:            "item-1",
		itemVersionKey:       "2026-09-30T12:00:00.123Z",
		itemMetaKey:          `{"difficulty":"easy"}`,
		itemRootKey:          root.ID(),
		environmentKey:       "sdk-experiment",
	}
	for _, name := range []string{"item-task", "item-generation", "item-event", "item-evaluator"} {
		attributes := wireAttributes(t, spans, name)
		for key, want := range shared {
			if got := attributes[key]; got != want {
				t.Errorf("span %q attribute %s = %#v, want %#v", name, key, got, want)
			}
		}
		// Request-scoped trace fields other than the environment still apply.
		if got := attributes["langfuse.trace.metadata.tenant"]; got != "acme" {
			t.Errorf("span %q trace metadata = %#v, want the propagated value", name, got)
		}
		wantRootOnly := name == "item-task"
		for _, key := range []string{experimentDescKey, itemExpectedKey} {
			if _, found := attributes[key]; found != wantRootOnly {
				t.Errorf("span %q has %s = %t, want %t", name, key, found, wantRootOnly)
			}
		}
		if span := observationWireSpanNamed(t, spans, name).span; hex.EncodeToString(span.TraceId) != root.TraceID() {
			t.Errorf("span %q left the item trace", name)
		}
	}
	rootAttributes := wireAttributes(t, spans, "item-task")
	if rootAttributes[experimentDescKey] != "prompt v2 against the curated set" ||
		rootAttributes[itemExpectedKey] != "Paris" ||
		rootAttributes["langfuse.observation.input"] != "What is the capital of France?" ||
		rootAttributes["langfuse.observation.type"] != "span" {
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
	experiment := wireExperiment()

	outerCtx, outer, err := client.StartExperimentItem(context.Background(), experiment,
		langfuse.ExperimentItem{ID: "outer"}, "outer", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("outer StartExperimentItem() error = %v", err)
	}

	// A context detached into a new trace carries no item identity.
	detached := oteltrace.ContextWithSpanContext(outerCtx, oteltrace.SpanContext{})
	_, background := client.StartObservation(detached, "detached", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	background.End()

	// A nested item replaces the identity for its own trace only.
	innerCtx, inner, err := client.StartExperimentItem(outerCtx, experiment,
		langfuse.ExperimentItem{ID: "inner"}, "inner", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("inner StartExperimentItem() error = %v", err)
	}
	_, innerChild := client.StartObservation(innerCtx, "inner-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	innerChild.End()
	inner.End()

	// A failed nested start rejects atomically and returns a context without
	// the enclosing identity or span, so the caller's fallback work is not
	// attributed to the outer item.
	failedCtx, failed, err := client.StartExperimentItem(outerCtx, experiment,
		langfuse.ExperimentItem{ID: ""}, "failed", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrInvalidExperiment) || failed.TraceID() != "" {
		t.Fatalf("invalid nested start = (%q, %v), want a no-op and ErrInvalidExperiment", failed.TraceID(), err)
	}
	_, fallback := client.StartObservation(failedCtx, "after-failed", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	fallback.End()
	_, outerChild := client.StartObservation(outerCtx, "outer-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	outerChild.End()
	outer.End()

	spans := exportObservationWireSpans(t, client, receiver, 6)
	for name, want := range map[string]map[string]any{
		"outer-child":  {itemIDKey: "outer", itemRootKey: outer.ID()},
		"inner":        {itemIDKey: "inner", itemRootKey: inner.ID()},
		"inner-child":  {itemIDKey: "inner", itemRootKey: inner.ID()},
		"detached":     {itemIDKey: nil, itemRootKey: nil},
		"after-failed": {itemIDKey: nil, itemRootKey: nil},
	} {
		attributes := wireAttributes(t, spans, name)
		for key, value := range want {
			if got := attributes[key]; got != value {
				t.Errorf("span %q %s = %#v, want %#v", name, key, got, value)
			}
		}
	}
	if got := wireAttributes(t, spans, "detached")[environmentKey]; got != wireEnv {
		t.Fatalf("detached environment = %#v, want the client environment", got)
	}
	if inner.TraceID() == outer.TraceID() {
		t.Fatal("nested item shares the outer trace")
	}
	// Work continued on the error context forms its own unlinked trace: it
	// neither joins nor is attributed to the outer item.
	afterFailed := observationWireSpanNamed(t, spans, "after-failed").span
	if hex.EncodeToString(afterFailed.TraceId) == outer.TraceID() || len(afterFailed.ParentSpanId) != 0 {
		t.Fatal("work after a failed nested start joined the outer item trace")
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

	// The bypass must not leak into a later trace started from the item
	// context, nor into the caller's own context.
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

func newBorrowedExperimentClient(
	t *testing.T,
	sampler sdktrace.Sampler,
	change func(*langfuse.Config),
) (*langfuse.Client, *sdktrace.TracerProvider, *otlpreceiver.Receiver) {
	t.Helper()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
		config.ServiceName = ""
		if change != nil {
			change(config)
		}
	})
	return client, provider, receiver
}

func TestExperimentWireBorrowedProviderStampsForeignSpans(t *testing.T) {
	client, provider, receiver := newBorrowedExperimentClient(t, sdktrace.AlwaysSample(), nil)
	itemCtx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"borrowed-item", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	// A GenAI instrumentor span on the same provider, started with stale
	// identity: every reserved key the SDK sets must win.
	tracer := provider.Tracer("instrumentation.example")
	middleCtx, middle := tracer.Start(itemCtx, "foreign-generation", oteltrace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String(experimentIDKey, "stale-run"),
		attribute.String(itemRootKey, "0000000000000001"),
		attribute.String(environmentKey, "production"),
	))
	_, child := client.StartObservation(middleCtx, "sdk-under-foreign", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	middle.End()
	root.End()

	spans := exportObservationWireSpans(t, client, receiver, 3)
	for _, name := range []string{"foreign-generation", "sdk-under-foreign"} {
		attributes := wireAttributes(t, spans, name)
		if attributes[experimentIDKey] != "run-2026-10-01" || attributes[itemRootKey] != root.ID() ||
			attributes[environmentKey] != "sdk-experiment" || attributes[itemIDKey] != "item-1" {
			t.Fatalf("span %q identity = %#v", name, attributes)
		}
	}
}

func TestExperimentWireRejectsRootsThatWouldNotExport(t *testing.T) {
	for name, setup := range map[string]struct {
		sampler sdktrace.Sampler
		filter  func(sdktrace.ReadOnlySpan) bool
	}{
		"borrowed drop":        {sampler: sdktrace.NeverSample()},
		"borrowed record-only": {sampler: recordOnlySampler{}},
		"filter at start": {sampler: sdktrace.AlwaysSample(), filter: func(span sdktrace.ReadOnlySpan) bool {
			return span.Name() != "rejected-item"
		}},
	} {
		t.Run(name, func(t *testing.T) {
			client, _, receiver := newBorrowedExperimentClient(t, setup.sampler, func(config *langfuse.Config) {
				config.ShouldExportSpan = setup.filter
			})
			itemCtx, root, err := client.StartExperimentItem(context.Background(), wireExperiment(),
				wireExperimentItem(), "rejected-item", langfuse.ObservationAttributes{})
			if !errors.Is(err, langfuse.ErrExperimentItemNotExported) || root.TraceID() != "" {
				t.Fatalf("StartExperimentItem() = (%q, %v), want a no-op and ErrExperimentItemNotExported",
					root.TraceID(), err)
			}
			_, child := client.StartObservation(itemCtx, "unlinked", langfuse.TypeSpan, langfuse.ObservationAttributes{})
			child.End()
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.Flush(flushCtx); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			for _, request := range receiver.Requests() {
				for _, span := range otlpreceiver.Spans(request) {
					for _, attribute := range span.Attributes {
						if strings.HasPrefix(attribute.Key, "langfuse.experiment.") {
							t.Fatalf("span %q exported experiment attribute %s", span.Name, attribute.Key)
						}
					}
				}
			}
		})
	}
}

func TestExperimentWireTwoClientsShareAContext(t *testing.T) {
	first, firstReceiver := newObservationWireClient(t, nil)
	second, secondReceiver := newObservationWireClient(t, nil)
	itemCtx, root, err := first.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"first-item", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	_, other := second.StartObservation(itemCtx, "second-client", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	other.End()
	root.End()
	exportObservationWireSpans(t, first, firstReceiver, 1)
	spans := exportObservationWireSpans(t, second, secondReceiver, 1)
	for _, key := range experimentKeys {
		if _, found := wireAttributes(t, spans, "second-client")[key]; found {
			t.Fatalf("another client's span carries %s", key)
		}
	}
}

func TestExperimentWireExpectedOutputEncoding(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	cases := map[string]struct {
		value any
		want  any // nil means absent
	}{
		"plain string":        {value: "Paris", want: "Paris"},
		"empty string":        {value: "", want: ""},
		"JSON-looking string": {value: `{"a":1}`, want: `{"a":1}`},
		"structure":           {value: map[string]any{"b": 2, "a": []any{1, "x"}}, want: `{"a":[1,"x"],"b":2}`},
		"number":              {value: 3.5, want: "3.5"},
		"raw string":          {value: json.RawMessage(`"Paris"`), want: "Paris"},
		"raw object":          {value: json.RawMessage(" {\"n\": 12345678901234567890} "), want: `{"n":12345678901234567890}`},
		"raw null":            {value: json.RawMessage(`null`), want: nil},
		"nil":                 {value: nil, want: nil},
		"typed nil":           {value: map[string]any(nil), want: nil},
	}
	for name, test := range cases {
		item := wireExperimentItem()
		item.ID = name
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
				return nil // deliberate redaction omits the attribute
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
	for range 3 {
		_, child := client.StartObservation(itemCtx, "masked-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
		child.End()
	}
	root.End()
	spans := exportObservationWireSpans(t, client, receiver, 4)
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
			t.Errorf("Mask(%q) calls = %d, want exactly 1 for the whole item trace", field, calls[field])
		}
	}
}

func TestExperimentRejectsInvalidInputAtomically(t *testing.T) {
	longID := strings.Repeat("x", 256)
	for name, test := range map[string]struct {
		mask       func(langfuse.MaskField, any) any
		experiment func(*langfuse.Experiment)
		item       func(*langfuse.ExperimentItem)
	}{
		"empty experiment ID":  {experiment: func(e *langfuse.Experiment) { e.ID = "" }},
		"empty name":           {experiment: func(e *langfuse.Experiment) { e.Name = "" }},
		"empty item ID":        {item: func(i *langfuse.ExperimentItem) { i.ID = "" }},
		"long ID":              {item: func(i *langfuse.ExperimentItem) { i.ID = longID }},
		"control character":    {experiment: func(e *langfuse.Experiment) { e.Name = "a\nb" }},
		"invalid UTF-8":        {experiment: func(e *langfuse.Experiment) { e.DatasetID = "\xff" }},
		"long description":     {experiment: func(e *langfuse.Experiment) { e.Description = strings.Repeat("d", 16<<10+1) }},
		"version out of range": {item: func(i *langfuse.ExperimentItem) { i.Version = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		"colliding metadata paths": {experiment: func(e *langfuse.Experiment) {
			e.Metadata = map[string]any{"a.b": 1, "a": map[string]any{"b": 2}}
		}},
		"reserved metadata segment": {item: func(i *langfuse.ExperimentItem) {
			i.Metadata = map[string]any{"x": map[string]any{"__proto__": 1}}
		}},
		"oversized metadata": {experiment: func(e *langfuse.Experiment) {
			e.Metadata = map[string]any{"blob": strings.Repeat("m", 16<<10)}
		}},
		"cyclic metadata": {item: func(i *langfuse.ExperimentItem) {
			cycle := map[string]any{}
			cycle["self"] = cycle
			i.Metadata = cycle
		}},
		"oversized expected output": {item: func(i *langfuse.ExperimentItem) {
			i.ExpectedOutput = strings.Repeat("e", 256<<10+1)
		}},
		"invalid raw expected output": {item: func(i *langfuse.ExperimentItem) {
			i.ExpectedOutput = json.RawMessage(`{"a":`)
		}},
		"mask panic": {mask: func(field langfuse.MaskField, value any) any {
			if field == langfuse.MaskExperimentItemExpectedOutput {
				panic("PANIC-PAYLOAD")
			}
			return value
		}},
		"mask changes metadata type": {mask: func(field langfuse.MaskField, value any) any {
			if field == langfuse.MaskExperimentMetadata {
				return "not a map"
			}
			return value
		}},
	} {
		t.Run(name, func(t *testing.T) {
			client, receiver := newObservationWireClient(t, func(config *langfuse.Config) { config.Mask = test.mask })
			experiment, item := wireExperiment(), wireExperimentItem()
			if test.experiment != nil {
				test.experiment(&experiment)
			}
			if test.item != nil {
				test.item(&item)
			}
			ctx, root, err := client.StartExperimentItem(context.Background(), experiment, item, "invalid",
				langfuse.ObservationAttributes{})
			if !errors.Is(err, langfuse.ErrInvalidExperiment) {
				t.Fatalf("StartExperimentItem() error = %v, want ErrInvalidExperiment", err)
			}
			if ctx == nil || root == nil || root.TraceID() != "" {
				t.Fatalf("rejected start returned ctx %v and observation %q", ctx, root.TraceID())
			}
			for _, secret := range []string{"PANIC-PAYLOAD", "run-2026-10-01", "triage-v2", "item-1", "\xff"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error %q leaks %q", err, secret)
				}
			}
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.Flush(flushCtx); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if got := len(receiver.Requests()); got != 0 {
				t.Fatalf("a rejected start exported %d requests", got)
			}
		})
	}
}

func TestExperimentUnavailableClients(t *testing.T) {
	t.Parallel()
	masked := false
	disabled, err := langfuse.New(context.Background(), langfuse.Config{
		Disabled: true,
		Mask:     func(langfuse.MaskField, any) any { masked = true; return nil },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	type callerKey struct{}
	ctx := context.WithValue(context.Background(), callerKey{}, "caller")
	for name, client := range map[string]*langfuse.Client{"nil": nil, "disabled": disabled} {
		got, root, err := client.StartExperimentItem(ctx, wireExperiment(), wireExperimentItem(), "x",
			langfuse.ObservationAttributes{})
		if err != nil || got != ctx || root == nil || root.TraceID() != "" {
			t.Fatalf("%s client = (%v, %q, %v), want (ctx, no-op, nil)", name, got, root.TraceID(), err)
		}
		invalid := wireExperimentItem()
		invalid.ID = ""
		if _, _, err := client.StartExperimentItem(ctx, wireExperiment(), invalid, "x",
			langfuse.ObservationAttributes{}); !errors.Is(err, langfuse.ErrInvalidExperiment) {
			t.Fatalf("%s client accepted invalid input: %v", name, err)
		}
		//nolint:staticcheck // A nil context is the input under test.
		if got, _, err := client.StartExperimentItem(nil, wireExperiment(), wireExperimentItem(), "x",
			langfuse.ObservationAttributes{}); err == nil || got != nil {
			t.Fatalf("%s client accepted a nil context", name)
		}
	}
	if masked {
		t.Fatal("a disabled client called Mask")
	}
}

func TestExperimentStoppedClientReturnsError(t *testing.T) {
	client, _ := newObservationWireClient(t, nil)
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	_, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(), "x",
		langfuse.ObservationAttributes{})
	if err == nil || root.TraceID() != "" {
		t.Fatalf("stopped client = (%q, %v), want a no-op and an error", root.TraceID(), err)
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
	experiment := wireExperiment()
	experiment.Description = large(16 << 10)
	experiment.Metadata = map[string]any{"blob": large(16<<10 - 32)}
	item := wireExperimentItem()
	item.ID = large(255)
	item.ExpectedOutput = large(256 << 10)
	item.Metadata = map[string]any{"blob": large(16<<10 - 32)}
	itemCtx, root, err := client.StartExperimentItem(ctx, experiment, item, "maximal-item", langfuse.ObservationAttributes{
		Input: large(1<<20 - 1), Metadata: observationMetadata,
	})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	root.Update(langfuse.ObservationAttributes{Output: large(900 << 10)})
	for range 8 {
		root.RecordError(errors.New(large(64 << 10)))
	}
	_, child := client.StartObservation(itemCtx, "maximal-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	root.End()
	spans := exportObservationWireSpans(t, client, receiver, 2)
	rootWire := observationWireSpanNamed(t, spans, "maximal-item").span
	if rootWire.DroppedAttributesCount != 0 {
		t.Fatalf("maximal root dropped %d attributes", rootWire.DroppedAttributesCount)
	}
	attributes := observationWireAttributeMap(t, rootWire.Attributes)
	for _, key := range []string{
		experimentIDKey, itemIDKey, itemRootKey, itemExpectedKey, experimentDescKey,
		experimentMetaKey, itemMetaKey,
	} {
		if _, found := attributes[key]; !found {
			t.Fatalf("maximal root lost %s", key)
		}
	}
}

func TestExperimentWireOwnsTheReservedNamespaceAtExport(t *testing.T) {
	client, provider, receiver := newBorrowedExperimentClient(t, sdktrace.AlwaysSample(), nil)
	experiment := wireExperiment()
	experiment.DatasetID = "" // local data: no dataset link may appear
	experiment.Metadata = map[string]any{"email": "[redacted]"}
	itemCtx, root, err := client.StartExperimentItem(context.Background(), experiment, wireExperimentItem(),
		"owned-item", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	tracer := provider.Tracer("instrumentation.example")
	foreignCtx, foreign := tracer.Start(itemCtx, "foreign", oteltrace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String(experimentMetaKey+".email", "patient@example.test"),
		attribute.String(experimentDatasetKey, "someone-elses-dataset"),
		attribute.String(itemExpectedKey, "child expected output"),
		attribute.String("gen_ai.request.model", "kept"),
	))
	_, child := client.StartObservation(foreignCtx, "late-mutated", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	// Mutations after start, through the raw OpenTelemetry span.
	oteltrace.SpanFromContext(foreignCtx).SetAttributes(attribute.String(experimentIDKey, "late-run"))
	child.End()
	foreign.SetAttributes(attribute.String(environmentKey, "production"), attribute.String(itemRootKey, "0000000000000001"))
	foreign.End()
	root.End()

	spans := exportObservationWireSpans(t, client, receiver, 3)
	for _, name := range []string{"owned-item", "foreign", "late-mutated"} {
		attributes := wireAttributes(t, spans, name)
		if attributes[experimentIDKey] != "run-2026-10-01" || attributes[itemRootKey] != root.ID() ||
			attributes[environmentKey] != "sdk-experiment" || attributes[experimentMetaKey] != `{"email":"[redacted]"}` {
			t.Fatalf("span %q identity = %#v", name, attributes)
		}
		for key := range attributes {
			if key == experimentMetaKey+".email" || key == experimentDatasetKey {
				t.Fatalf("span %q exported a foreign reserved key %s", name, key)
			}
		}
		if _, found := attributes[itemExpectedKey]; found != (name == "owned-item") {
			t.Fatalf("span %q expected-output presence is wrong", name)
		}
	}
	if got := wireAttributes(t, spans, "foreign")["gen_ai.request.model"]; got != "kept" {
		t.Fatalf("unrelated foreign attributes must survive; got %#v", got)
	}
}

func TestExperimentWireAbortedRootIsNeverExported(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	client, _, receiver := newBorrowedExperimentClient(t, sdktrace.AlwaysSample(), func(config *langfuse.Config) {
		// Rejects a span on its first evaluation (start) and accepts it on
		// its second (end), as a filter keyed on late attributes might.
		config.ShouldExportSpan = func(span sdktrace.ReadOnlySpan) bool {
			mu.Lock()
			defer mu.Unlock()
			calls[span.Name()]++
			return calls[span.Name()] > 1
		}
	})
	_, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"flip-flop", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrExperimentItemNotExported) || root.TraceID() != "" {
		t.Fatalf("StartExperimentItem() = (%q, %v), want ErrExperimentItemNotExported", root.TraceID(), err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := len(receiver.Requests()); got != 0 {
		t.Fatalf("an aborted item root was exported in %d requests", got)
	}
}

func TestExperimentWireBorrowedLimitsThatDropIdentityReject(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{
		AttributeCountLimit: 3, AttributeValueLengthLimit: -1, EventCountLimit: 8,
		LinkCountLimit: 8, AttributePerEventCountLimit: 8, AttributePerLinkCountLimit: 8,
	}))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
		config.ServiceName = ""
	})
	_, root, err := client.StartExperimentItem(context.Background(), wireExperiment(), wireExperimentItem(),
		"limited", langfuse.ObservationAttributes{})
	if !errors.Is(err, langfuse.ErrExperimentItemNotExported) || root.TraceID() != "" {
		t.Fatalf("StartExperimentItem() = (%q, %v), want ErrExperimentItemNotExported", root.TraceID(), err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := len(receiver.Requests()); got != 0 {
		t.Fatalf("an item root without complete identity was exported in %d requests", got)
	}
}

type deepMetadata struct{}

func (deepMetadata) MarshalJSON() ([]byte, error) {
	return []byte(strings.Repeat(`{"a":`, 40) + "1" + strings.Repeat("}", 40)), nil
}

func TestExperimentWireMetadataNormalization(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	experiment := wireExperiment()
	experiment.Metadata = map[string]any{
		"big":   json.Number("9007199254740993"),
		"null":  nil,
		"flag":  true,
		"list":  []any{1, "a<b", nil},
		"raw":   json.RawMessage(`{"x": 1.50, "y": null}`),
		"empty": map[string]any{},
		"text":  "Paris",
	}
	_, root, err := client.StartExperimentItem(context.Background(), experiment, wireExperimentItem(),
		"normalized", langfuse.ObservationAttributes{})
	if err != nil {
		t.Fatalf("StartExperimentItem() error = %v", err)
	}
	root.End()
	spans := exportObservationWireSpans(t, client, receiver, 1)
	want := `{"big":"9007199254740993","flag":"true","list":"[1,\"a<b\",null]","raw":{"x":"1.50"},"text":"Paris"}`
	if got := wireAttributes(t, spans, "normalized")[experimentMetaKey]; got != want {
		t.Fatalf("normalized metadata = %s\nwant %s", got, want)
	}

	deep := wireExperiment()
	deep.Metadata = map[string]any{"deep": deepMetadata{}}
	if _, _, err := client.StartExperimentItem(context.Background(), deep, wireExperimentItem(), "deep",
		langfuse.ObservationAttributes{}); !errors.Is(err, langfuse.ErrInvalidExperiment) {
		t.Fatalf("metadata nested past the depth limit through a custom marshaler = %v", err)
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
	record := func(ctx context.Context, traceID string) {
		t.Helper()
		if err := client.RecordScore(ctx, langfuse.Score{
			Name: "accuracy", TraceID: traceID, ObservationID: root.ID(), NumericValue: &value,
		}); err != nil {
			t.Fatalf("RecordScore() error = %v", err)
		}
	}
	record(itemCtx, root.TraceID())
	record(itemCtx, "0123456789abcdef0123456789abcdef")
	record(context.Background(), root.TraceID())
	flushClient(t, client)
	var environments []string
	for _, request := range receiver.all() {
		if !strings.HasSuffix(request.path, "/api/public/ingestion") {
			continue
		}
		_, body := scoreWireEvent(t, request)
		environment, _ := body["environment"].(string)
		environments = append(environments, environment)
	}
	if strings.Join(environments, ",") != "sdk-experiment,score_wire,score_wire" {
		t.Fatalf("score environments = %v, want the item environment only for the item trace in its context",
			environments)
	}
}
