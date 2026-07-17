package langfuse_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestObserveEndsObservationAndParentsChildren(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)

	err := client.Observe(context.Background(), "observe-root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{Input: "question"},
		func(ctx context.Context, observation *langfuse.Observation) error {
			_, child := client.StartObservation(ctx, "observe-child", langfuse.TypeSpan,
				langfuse.ObservationAttributes{})
			child.End()
			observation.Update(langfuse.ObservationAttributes{Output: "answer"})
			return nil
		})
	if err != nil {
		t.Fatalf("Observe() error = %v, want nil", err)
	}

	spans := exportObservationWireSpans(t, client, receiver, 2)
	root := observationWireSpanNamed(t, spans, "observe-root")
	child := observationWireSpanNamed(t, spans, "observe-child")
	if len(root.span.ParentSpanId) != 0 {
		t.Fatalf("Observe span parent = %x, want a root span", root.span.ParentSpanId)
	}
	if !bytes.Equal(child.span.ParentSpanId, root.span.SpanId) {
		t.Fatalf("child parent span ID = %x, want the Observe span %x", child.span.ParentSpanId, root.span.SpanId)
	}
	want := edgeBaseAttributes("agent")
	want["langfuse.observation.input"] = "question"
	want["langfuse.observation.output"] = "answer"
	assertObservationWireAttributes(t, root.span.Attributes, want)
	assertObservationWireSpanShape(t, root.span, time.Time{}, tracepb.Status_STATUS_CODE_UNSET, "", 0)
}

func TestObserveRecordsReturnedError(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)

	wantErr := errors.New("model exploded")
	err := client.Observe(context.Background(), "observe-error", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Model: "model-survives"},
		func(ctx context.Context, observation *langfuse.Observation) error {
			return wantErr
		})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Observe() error = %v, want the callback error %v unchanged", err, wantErr)
	}

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "observe-error")
	want := edgeBaseAttributes("generation")
	want["langfuse.observation.level"] = "ERROR"
	want["langfuse.observation.model.name"] = "model-survives"
	want["langfuse.observation.status_message"] = wantErr.Error()
	assertObservationWireAttributes(t, span.span.Attributes, want)
	assertObservationWireSpanShape(t, span.span, time.Time{}, tracepb.Status_STATUS_CODE_ERROR, wantErr.Error(), 1)
	if got := span.span.Events[0].Name; got != "exception" {
		t.Fatalf("error event name = %q, want exception", got)
	}
}

func TestObservePanicEndsObservationWithoutCapturingPanicValue(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	const panicPayload = "PANIC-PAYLOAD-observe-51ce"
	recovered := func() (recovered any) {
		defer func() { recovered = recover() }()
		_ = client.Observe(context.Background(), "observe-panic", langfuse.TypeSpan,
			langfuse.ObservationAttributes{},
			func(ctx context.Context, observation *langfuse.Observation) error {
				panic(panicPayload)
			})
		return nil
	}()
	if recovered != panicPayload {
		t.Fatalf("recovered panic = %v, want the original panic value %q", recovered, panicPayload)
	}

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "observe-panic")
	want := edgeBaseAttributes("span")
	want["langfuse.observation.level"] = "ERROR"
	want["langfuse.observation.status_message"] = "panic"
	assertObservationWireAttributes(t, span.span.Attributes, want)
	assertObservationWireSpanShape(t, span.span, time.Time{}, tracepb.Status_STATUS_CODE_ERROR, "panic", 0)
	assertEdgeDiagnosticsPayloadFree(t, diagnostics, panicPayload)
}

func TestObserveNilCallbackStartsNoObservation(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)

	if err := client.Observe(context.Background(), "observe-nil-callback", langfuse.TypeSpan,
		langfuse.ObservationAttributes{}, nil); err != nil {
		t.Fatalf("Observe(nil callback) error = %v, want nil", err)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "observe callback is nil", 1)

	// A follow-up observation proves the nil-callback call exported nothing.
	_, observation := client.StartObservation(context.Background(), "observe-follow-up",
		langfuse.TypeSpan, langfuse.ObservationAttributes{})
	observation.End()
	exportObservationWireSpans(t, client, receiver, 1)
}

func TestObserveRunsCallbackOnNilDisabledAndStoppedClients(t *testing.T) {
	captureEdgeDiagnostics(t)
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatalf("langfuse.New(disabled) error = %v", err)
	}
	stopped, _ := newObservationWireClient(t, nil)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stopped.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Client.Shutdown() error = %v", err)
	}

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "caller-value")
	wantErr := errors.New("callback error passes through")
	for name, client := range map[string]*langfuse.Client{
		"nil":      nil,
		"disabled": disabled,
		"stopped":  stopped,
	} {
		ran := false
		err := client.Observe(ctx, "observe-noop", langfuse.TypeSpan, langfuse.ObservationAttributes{},
			func(callbackCtx context.Context, observation *langfuse.Observation) error {
				ran = true
				if callbackCtx != ctx {
					t.Errorf("%s client: callback context differs from the caller context", name)
				}
				if observation == nil {
					t.Errorf("%s client: observation handle is nil, want a no-op handle", name)
				}
				if id := observation.ID(); id != "" {
					t.Errorf("%s client: no-op observation ID = %q, want empty", name, id)
				}
				return wantErr
			})
		if !ran {
			t.Fatalf("%s client: callback did not run", name)
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("%s client: Observe() error = %v, want the callback error unchanged", name, err)
		}
	}
}
