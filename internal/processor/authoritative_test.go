package processor

import (
	"context"
	"testing"

	otelattr "go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

type authoritativeContextKey struct{}

func newAuthoritativeTestProcessor(t *testing.T, filter func(sdktrace.ReadOnlySpan) bool) (
	*Processor, *recordingProcessor, oteltrace.Tracer,
) {
	t.Helper()
	next := newRecordingProcessor()
	processor, err := New(Config{
		Next:        next,
		Environment: "client-environment",
		AuthoritativeAttributes: func(ctx context.Context, _ sdktrace.ReadOnlySpan) []otelattr.KeyValue {
			attributes, _ := ctx.Value(authoritativeContextKey{}).([]otelattr.KeyValue)
			return attributes
		},
		ShouldExportSpan: filter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return processor, next, provider.Tracer(lfattr.TracerName)
}

func TestAuthoritativeAttributesOwnTheNamespaceAtStartAndEnd(t *testing.T) {
	t.Parallel()
	var startSeen []otelattr.KeyValue
	_, next, tracer := newAuthoritativeTestProcessor(t, func(span sdktrace.ReadOnlySpan) bool {
		if startSeen == nil {
			startSeen = span.Attributes()
		}
		return true
	})
	owned := []otelattr.KeyValue{
		otelattr.String(lfattr.ExperimentIDKey, "run"),
		otelattr.String(lfattr.EnvironmentKey, lfattr.ExperimentEnvironment),
	}
	ctx := context.WithValue(context.Background(), authoritativeContextKey{}, owned)
	_, span := tracer.Start(ctx, "owned", oteltrace.WithAttributes(
		otelattr.String(lfattr.ExperimentIDKey, "stale"),
		otelattr.String(lfattr.ExperimentDatasetIDKey, "foreign"),
		otelattr.String("unrelated", "kept"),
	))
	span.SetAttributes(otelattr.String(lfattr.ExperimentIDKey, "late"), otelattr.String(lfattr.EnvironmentKey, "late"))
	span.End()

	// The start-time filter already saw the authoritative value.
	assertAttribute(t, startSeen, lfattr.ExperimentIDKey, "run")
	exported := next.endedByName()["owned"].attributes
	if exported[lfattr.ExperimentIDKey] != "run" || exported[lfattr.EnvironmentKey] != lfattr.ExperimentEnvironment ||
		exported["unrelated"] != "kept" {
		t.Fatalf("exported attributes = %#v", exported)
	}
	if _, found := exported[lfattr.ExperimentDatasetIDKey]; found {
		t.Fatal("a reserved key outside the authoritative set was exported")
	}
}

func TestSpansWithoutAuthoritativeAttributesAreUnchanged(t *testing.T) {
	t.Parallel()
	_, next, tracer := newAuthoritativeTestProcessor(t, nil)
	_, span := tracer.Start(context.Background(), "plain", oteltrace.WithAttributes(
		otelattr.String(lfattr.ExperimentIDKey, "caller-owned"),
	))
	span.End()
	if got := next.endedByName()["plain"].attributes[lfattr.ExperimentIDKey]; got != "caller-owned" {
		t.Fatalf("unowned span attribute = %#v, want the caller's value", got)
	}
}

func TestAbortedSpanIsNeverExported(t *testing.T) {
	t.Parallel()
	processor, next, tracer := newAuthoritativeTestProcessor(t, func(sdktrace.ReadOnlySpan) bool { return true })
	_, span := tracer.Start(context.Background(), "aborted")
	processor.Abort(span.SpanContext())
	span.End()
	if _, exported := next.endedByName()["aborted"]; exported {
		t.Fatal("an aborted span was exported")
	}
	// The abort is consumed by the span's end and affects nothing else.
	_, other := tracer.Start(context.Background(), "after")
	other.End()
	if _, exported := next.endedByName()["after"]; !exported {
		t.Fatal("a later span was suppressed")
	}
}

func TestAuthoritativeCallbackPanicIsContained(t *testing.T) {
	t.Parallel()
	next := newRecordingProcessor()
	processor, err := New(Config{
		Next: next,
		AuthoritativeAttributes: func(context.Context, sdktrace.ReadOnlySpan) []otelattr.KeyValue {
			panic("PANIC-PAYLOAD")
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer(lfattr.TracerName).Start(context.Background(), "survives")
	span.End()
	if _, exported := next.endedByName()["survives"]; !exported {
		t.Fatal("a panicking callback prevented export")
	}
}

func assertAttribute(t *testing.T, attributes []otelattr.KeyValue, key otelattr.Key, want string) {
	t.Helper()
	for _, item := range attributes {
		if item.Key == key {
			if got := item.Value.AsString(); got != want {
				t.Fatalf("attribute %s = %q, want %q", key, got, want)
			}
			return
		}
	}
	t.Fatalf("attribute %s is missing", key)
}
