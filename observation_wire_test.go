package langfuse_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	wirePublicKey = "pk-lf-observation-wire"
	wireSecretKey = "sk-lf-observation-wire"
	wireService   = "langfuse-wire-tests"
	wireRelease   = "2026.07.16"
	wireEnv       = "wire_test"
)

type wireSpan struct {
	span              *tracepb.Span
	resource          *resourcepb.Resource
	resourceSchemaURL string
	scope             *commonpb.InstrumentationScope
	scopeSchemaURL    string
}

type wireProviderError struct{ message string }

func (e wireProviderError) Error() string { return e.message }

func TestObservationWireRootAndNestedGolden(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)

	traceContext := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:      "customer-turn",
		UserID:    "user-42",
		SessionID: "session-9",
		Tags:      []string{"chat", "production"},
		Metadata: map[string]any{
			"attempt": 2,
			"tenant":  "acme",
		},
		Version: "trace-v1",
	})

	rootStart := time.Date(2026, 7, 16, 10, 20, 30, 123456789, time.UTC)
	rootContext, root := client.StartObservation(traceContext, "chat-turn", langfuse.TypeAgent,
		langfuse.ObservationAttributes{
			Input:         map[string]any{"question": "hello"},
			Metadata:      map[string]any{"stage": "start"},
			Level:         langfuse.LevelDebug,
			StatusMessage: "working",
			Version:       "root-v2",
			StartTime:     rootStart,
		})
	rootTraceID, rootSpanID := root.TraceID(), root.ID()

	childStart := rootStart.Add(250 * time.Millisecond)
	_, child := client.StartObservation(rootContext, "call-tool", langfuse.TypeTool,
		langfuse.ObservationAttributes{
			Input:         []any{"alpha", int64(7)},
			Output:        map[string]any{"ok": true},
			Metadata:      map[string]any{"tool": "search"},
			Level:         langfuse.LevelError,
			StatusMessage: "tool failed",
			StartTime:     childStart,
		})
	childTraceID, childSpanID := child.TraceID(), child.ID()
	child.End()

	root.Update(langfuse.ObservationAttributes{
		Output:   "final answer",
		Metadata: map[string]any{"stage": "done"},
	})
	root.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	rootWire := observationWireSpanNamed(t, spans, "chat-turn")
	childWire := observationWireSpanNamed(t, spans, "call-tool")

	assertObservationWireEnvelope(t, rootWire)
	assertObservationWireEnvelope(t, childWire)
	assertObservationWireIdentity(t, rootWire.span, rootTraceID, rootSpanID, "")
	assertObservationWireIdentity(t, childWire.span, childTraceID, childSpanID, rootSpanID)
	if childTraceID != rootTraceID {
		t.Fatalf("nested TraceID() = %q, root TraceID() = %q", childTraceID, rootTraceID)
	}
	if childSpanID == rootSpanID {
		t.Fatalf("nested ID() unexpectedly equals root ID() %q", rootSpanID)
	}

	assertObservationWireSpanShape(t, rootWire.span, rootStart, tracepb.Status_STATUS_CODE_UNSET, "", 0)
	assertObservationWireSpanShape(t, childWire.span, childStart, tracepb.Status_STATUS_CODE_ERROR, "tool failed", 0)
	assertObservationWireAttributes(t, rootWire.span.Attributes, map[string]any{
		"langfuse.environment":                wireEnv,
		"langfuse.internal.is_app_root":       true,
		"langfuse.observation.input":          `{"question":"hello"}`,
		"langfuse.observation.level":          "DEBUG",
		"langfuse.observation.metadata.stage": "done",
		"langfuse.observation.output":         "final answer",
		"langfuse.observation.status_message": "working",
		"langfuse.observation.type":           "agent",
		"langfuse.release":                    wireRelease,
		"langfuse.trace.metadata.attempt":     "2",
		"langfuse.trace.metadata.tenant":      "acme",
		"langfuse.trace.name":                 "customer-turn",
		"langfuse.trace.tags":                 []any{"chat", "production"},
		"langfuse.version":                    "root-v2",
		"session.id":                          "session-9",
		"user.id":                             "user-42",
	})
	assertObservationWireAttributes(t, childWire.span.Attributes, map[string]any{
		"langfuse.environment":                wireEnv,
		"langfuse.observation.input":          `["alpha",7]`,
		"langfuse.observation.level":          "ERROR",
		"langfuse.observation.metadata.tool":  "search",
		"langfuse.observation.output":         `{"ok":true}`,
		"langfuse.observation.status_message": "tool failed",
		"langfuse.observation.type":           "tool",
		"langfuse.release":                    wireRelease,
		"langfuse.trace.metadata.attempt":     "2",
		"langfuse.trace.metadata.tenant":      "acme",
		"langfuse.trace.name":                 "customer-turn",
		"langfuse.trace.tags":                 []any{"chat", "production"},
		"langfuse.version":                    "trace-v1",
		"session.id":                          "session-9",
		"user.id":                             "user-42",
	})
	for _, span := range []*tracepb.Span{rootWire.span, childWire.span} {
		for _, key := range []string{"langfuse.trace.input", "langfuse.trace.output"} {
			if _, exists := observationWireAttributeMap(t, span.Attributes)[key]; exists {
				t.Errorf("span %q exported deprecated %s", span.Name, key)
			}
		}
	}
}

func TestObservationWireAllTenTypesAndPromptRestriction(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	types := []langfuse.ObservationType{
		langfuse.TypeSpan,
		langfuse.TypeGeneration,
		langfuse.TypeEvent,
		langfuse.TypeEmbedding,
		langfuse.TypeAgent,
		langfuse.TypeTool,
		langfuse.TypeChain,
		langfuse.TypeRetriever,
		langfuse.TypeEvaluator,
		langfuse.TypeGuardrail,
	}
	for _, observationType := range types {
		name := "type-" + string(observationType)
		if observationType == langfuse.TypeEvent {
			client.Event(context.Background(), name, langfuse.ObservationAttributes{
				Prompt: &langfuse.PromptRef{Name: "wire-prompt", Version: 3},
			})
			continue
		}
		_, observation := client.StartObservation(context.Background(), name, observationType,
			langfuse.ObservationAttributes{
				Prompt: &langfuse.PromptRef{Name: "wire-prompt", Version: 3},
			})
		observation.End()
	}

	spans := exportObservationWireSpans(t, client, receiver, len(types))
	for _, observationType := range types {
		span := observationWireSpanNamed(t, spans, "type-"+string(observationType))
		assertObservationWireEnvelope(t, span)
		want := map[string]any{
			"langfuse.environment":          wireEnv,
			"langfuse.internal.is_app_root": true,
			"langfuse.observation.type":     string(observationType),
			"langfuse.release":              wireRelease,
		}
		if observationType == langfuse.TypeGeneration {
			want["langfuse.observation.prompt.name"] = "wire-prompt"
			want["langfuse.observation.prompt.version"] = int64(3)
		}
		assertObservationWireAttributes(t, span.span.Attributes, want)
		assertObservationWireSpanShape(t, span.span, time.Time{}, tracepb.Status_STATUS_CODE_UNSET, "", 0)
	}
}

func TestObservationWireOwnedModeUsesStandardBatchGeometry(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	const total = 600 // above one 512-span batch, below the 2048-span queue
	for index := range total {
		_, observation := client.StartObservation(context.Background(), fmt.Sprintf("batch-%03d", index),
			langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()
	}
	_ = exportObservationWireSpans(t, client, receiver, total)
	requests := receiver.Requests()
	if len(requests) < 2 {
		t.Fatalf("OTLP batch requests = %d, want at least 2 for %d observations", len(requests), total)
	}
	sawBatchedRequest := false
	for index, request := range requests {
		spans := len(otlpreceiver.Spans(request))
		if spans > 512 {
			t.Fatalf("OTLP request %d contains %d spans, want at most the standard batch size 512", index, spans)
		}
		if spans > 1 {
			sawBatchedRequest = true
		}
	}
	if !sawBatchedRequest {
		t.Fatalf("no OTLP request contained more than one span across %d requests", len(requests))
	}
}

func TestObservationWireBorrowedProviderBatchesSpansPerRequest(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
		config.TracerProvider = provider
	})
	const total = 10
	for index := range total {
		_, observation := client.StartObservation(context.Background(), fmt.Sprintf("borrowed-batch-%d", index),
			langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()
	}
	_ = exportObservationWireSpans(t, client, receiver, total)
	requests := receiver.Requests()
	if len(requests) >= total {
		t.Fatalf("borrowed OTLP request count = %d, want fewer requests than the %d spans", len(requests), total)
	}
	largest := 0
	for _, request := range requests {
		if spans := len(otlpreceiver.Spans(request)); spans > largest {
			largest = spans
		}
	}
	if largest <= 1 {
		t.Fatalf("largest borrowed OTLP request contains %d span(s), want batching with more than one", largest)
	}
}

func TestObservationWireBatchProcessorIgnoresHostileEnvironmentSizing(t *testing.T) {
	t.Setenv("OTEL_BSP_MAX_QUEUE_SIZE", "-1")
	t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "999999999")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "-1")
	t.Setenv("OTEL_BSP_EXPORT_TIMEOUT", "0")
	t.Setenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "0")
	t.Setenv("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", "1")
	t.Setenv("OTEL_SPAN_EVENT_COUNT_LIMIT", "0")
	t.Setenv("OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT", "0")
	t.Setenv("OTEL_SPAN_LINK_COUNT_LIMIT", "0")
	t.Setenv("OTEL_LINK_ATTRIBUTE_COUNT_LIMIT", "0")

	client, receiver := newObservationWireClient(t, nil)
	_, observation := client.StartObservation(context.Background(), "hostile-bsp-environment",
		langfuse.TypeTool, langfuse.ObservationAttributes{Input: "input-survives"})
	observation.RecordError(wireProviderError{message: "error-survives"})
	observation.End()
	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "hostile-bsp-environment")
	assertObservationWireEnvelope(t, span)
	assertObservationWireAttributes(t, span.span.Attributes, map[string]any{
		"langfuse.environment":                wireEnv,
		"langfuse.internal.is_app_root":       true,
		"langfuse.observation.input":          "input-survives",
		"langfuse.observation.level":          "ERROR",
		"langfuse.observation.status_message": "error-survives",
		"langfuse.observation.type":           "tool",
		"langfuse.release":                    wireRelease,
	})
	assertObservationWireSpanShape(t, span.span, time.Time{}, tracepb.Status_STATUS_CODE_ERROR, "error-survives", 1)
}

func TestObservationWireGenerationUsageGolden(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	completion := time.Date(2026, 7, 16, 11, 12, 13, 987654321, time.FixedZone("wire", -7*60*60))

	_, observation := client.StartObservation(context.Background(), "generate", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{
			Input:  "hello",
			Output: "world",
			Model:  "gemini-2.5-flash",
			ModelParameters: map[string]any{
				"max_tokens":  64,
				"temperature": 0.2,
			},
			Usage: &langfuse.Usage{
				InputTokens:              200,
				OutputTokens:             100,
				CacheReadInputTokens:     30,
				CacheCreationInputTokens: 20,
				ReasoningOutputTokens:    25,
				Details: map[string]int64{
					"input_audio_tokens":  10,
					"output_audio_tokens": 5,
				},
			},
			CostDetails: map[string]float64{
				"input":  0.00125,
				"output": 0.0045,
			},
			Prompt:              &langfuse.PromptRef{Name: "support-answer", Version: 7},
			CompletionStartTime: completion,
		})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "generate")
	assertObservationWireEnvelope(t, span)
	assertObservationWireAttributes(t, span.span.Attributes, map[string]any{
		"langfuse.environment":                       wireEnv,
		"langfuse.internal.is_app_root":              true,
		"langfuse.observation.completion_start_time": "2026-07-16T18:12:13.987654321Z",
		"langfuse.observation.cost_details":          `{"input":0.00125,"output":0.0045}`,
		"langfuse.observation.input":                 "hello",
		"langfuse.observation.model.name":            "gemini-2.5-flash",
		"langfuse.observation.model.parameters":      `{"max_tokens":64,"temperature":0.2}`,
		"langfuse.observation.output":                "world",
		"langfuse.observation.prompt.name":           "support-answer",
		"langfuse.observation.prompt.version":        int64(7),
		"langfuse.observation.type":                  "generation",
		"langfuse.observation.usage_details":         `{"input":140,"input_audio_tokens":10,"input_cache_creation":20,"input_cached_tokens":30,"output":70,"output_audio_tokens":5,"output_reasoning_tokens":25,"total":300}`,
		"langfuse.release":                           wireRelease,
	})
	for key := range observationWireAttributeMap(t, span.span.Attributes) {
		if strings.HasPrefix(key, "gen_ai.usage.") {
			t.Errorf("SDK generation emitted forbidden usage attribute %q", key)
		}
	}
	assertObservationWireSpanShape(t, span.span, time.Time{}, tracepb.Status_STATUS_CODE_UNSET, "", 0)
}

func TestObservationWireMetadataAndUpdateMerge(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	outer := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:     "outer",
		UserID:   "user-before",
		Tags:     []string{"one", "two"},
		Metadata: map[string]any{"keep": 1, "replace": "before"},
		Version:  "trace-before",
	})
	rootContext, root := client.StartObservation(outer, "merge-root", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{
			Input:           "original-input",
			Metadata:        map[string]any{"keep": "old", "replace": "old"},
			ModelParameters: map[string]any{"old": 1},
			Version:         "observation-version",
		})
	root.Update(langfuse.ObservationAttributes{
		Output:          false,
		Metadata:        map[string]any{"replace": "new", "added": map[string]any{"ok": true}},
		ModelParameters: map[string]any{"new": 2},
	})

	inner := client.WithTraceAttributes(rootContext, langfuse.TraceAttributes{
		Name:     "inner",
		Tags:     []string{"two", "three"},
		Metadata: map[string]any{"replace": "after", "added": 2},
		Version:  "trace-after",
	})
	_, child := client.StartObservation(inner, "merge-child", langfuse.TypeSpan, langfuse.ObservationAttributes{})
	child.End()
	root.End()

	spans := exportObservationWireSpans(t, client, receiver, 2)
	rootSpan := observationWireSpanNamed(t, spans, "merge-root")
	childSpan := observationWireSpanNamed(t, spans, "merge-child")
	assertObservationWireAttributes(t, rootSpan.span.Attributes, map[string]any{
		"langfuse.environment":                  wireEnv,
		"langfuse.internal.is_app_root":         true,
		"langfuse.observation.input":            "original-input",
		"langfuse.observation.metadata.added":   `{"ok":true}`,
		"langfuse.observation.metadata.keep":    "old",
		"langfuse.observation.metadata.replace": "new",
		"langfuse.observation.model.parameters": `{"new":2}`,
		"langfuse.observation.output":           "false",
		"langfuse.observation.type":             "generation",
		"langfuse.release":                      wireRelease,
		"langfuse.trace.metadata.added":         "2",
		"langfuse.trace.metadata.keep":          "1",
		"langfuse.trace.metadata.replace":       "after",
		"langfuse.trace.name":                   "inner",
		"langfuse.trace.tags":                   []any{"one", "two", "three"},
		"langfuse.version":                      "observation-version",
		"user.id":                               "user-before",
	})
	assertObservationWireAttributes(t, childSpan.span.Attributes, map[string]any{
		"langfuse.environment":            wireEnv,
		"langfuse.observation.type":       "span",
		"langfuse.release":                wireRelease,
		"langfuse.trace.metadata.added":   "2",
		"langfuse.trace.metadata.keep":    "1",
		"langfuse.trace.metadata.replace": "after",
		"langfuse.trace.name":             "inner",
		"langfuse.trace.tags":             []any{"one", "two", "three"},
		"langfuse.version":                "trace-after",
		"user.id":                         "user-before",
	})
	if !bytes.Equal(childSpan.span.ParentSpanId, rootSpan.span.SpanId) {
		t.Fatalf("merge-child parent = %x, want merge-root span ID %x", childSpan.span.ParentSpanId, rootSpan.span.SpanId)
	}
}

func TestObservationWireContentCaptureAndMask(t *testing.T) {
	t.Run("mask before serialization", func(t *testing.T) {
		var calls atomic.Int32
		client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
			config.Mask = func(value any) any {
				calls.Add(1)
				switch typed := value.(type) {
				case string:
					return "[masked:" + typed + "]"
				case map[string]any:
					if _, isInput := typed["content"]; isInput {
						return map[string]any{"content": "masked"}
					}
					return map[string]any{"safe": "yes"}
				default:
					return nil
				}
			}
		})
		_, observation := client.StartObservation(context.Background(), "masked", langfuse.TypeSpan,
			langfuse.ObservationAttributes{
				Input:    map[string]any{"content": "secret"},
				Output:   "secret-output",
				Metadata: map[string]any{"secret": "metadata"},
			})
		observation.End()
		span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "masked")
		assertObservationWireAttributes(t, span.span.Attributes, map[string]any{
			"langfuse.environment":               wireEnv,
			"langfuse.internal.is_app_root":      true,
			"langfuse.observation.input":         `{"content":"masked"}`,
			"langfuse.observation.metadata.safe": "yes",
			"langfuse.observation.output":        "[masked:secret-output]",
			"langfuse.observation.type":          "span",
			"langfuse.release":                   wireRelease,
		})
		if got := calls.Load(); got != 3 {
			t.Fatalf("Mask calls = %d, want exactly input, output, and complete metadata (3)", got)
		}
	})

	t.Run("disabled drops only content before mask", func(t *testing.T) {
		var calls atomic.Int32
		client, receiver := newObservationWireClient(t, func(config *langfuse.Config) {
			config.DisableContentCapture = true
			config.Mask = func(value any) any {
				calls.Add(1)
				return map[string]any{"safe": "masked"}
			}
		})
		_, observation := client.StartObservation(context.Background(), "content-disabled", langfuse.TypeGeneration,
			langfuse.ObservationAttributes{
				Input:    "must-not-export-input",
				Output:   "must-not-export-output",
				Metadata: map[string]any{"secret": "must-be-masked"},
				Model:    "gemini-2.5-flash",
				Usage:    &langfuse.Usage{InputTokens: 9, OutputTokens: 4},
			})
		observation.End()
		span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "content-disabled")
		assertObservationWireAttributes(t, span.span.Attributes, map[string]any{
			"langfuse.environment":               wireEnv,
			"langfuse.internal.is_app_root":      true,
			"langfuse.observation.metadata.safe": "masked",
			"langfuse.observation.model.name":    "gemini-2.5-flash",
			"langfuse.observation.type":          "generation",
			"langfuse.observation.usage_details": `{"input":9,"output":4,"total":13}`,
			"langfuse.release":                   wireRelease,
		})
		if got := calls.Load(); got != 1 {
			t.Fatalf("Mask calls with content disabled = %d, want metadata only (1)", got)
		}
		attributes := observationWireAttributeMap(t, span.span.Attributes)
		for _, key := range []string{"langfuse.observation.input", "langfuse.observation.output"} {
			if _, exists := attributes[key]; exists {
				t.Errorf("content-disabled observation exported %s", key)
			}
		}
	})
}

func TestObservationWireRecordError(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	_, observation := client.StartObservation(context.Background(), "record-error", langfuse.TypeTool,
		langfuse.ObservationAttributes{
			Level:         langfuse.LevelWarning,
			StatusMessage: "before",
		})
	observation.RecordError(nil)
	wantError := wireProviderError{message: "provider exploded"}
	observation.RecordError(wantError)
	observation.Update(langfuse.ObservationAttributes{Output: "continued after error"})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "record-error")
	assertObservationWireAttributes(t, span.span.Attributes, map[string]any{
		"langfuse.environment":                wireEnv,
		"langfuse.internal.is_app_root":       true,
		"langfuse.observation.level":          "ERROR",
		"langfuse.observation.output":         "continued after error",
		"langfuse.observation.status_message": wantError.Error(),
		"langfuse.observation.type":           "tool",
		"langfuse.release":                    wireRelease,
	})
	assertObservationWireSpanShape(t, span.span, time.Time{}, tracepb.Status_STATUS_CODE_ERROR, wantError.Error(), 1)
	event := span.span.Events[0]
	if event.Name != "exception" {
		t.Fatalf("error event name = %q, want exception", event.Name)
	}
	if event.TimeUnixNano < span.span.StartTimeUnixNano || event.TimeUnixNano > span.span.EndTimeUnixNano {
		t.Fatalf("error event timestamp %d outside span [%d, %d]", event.TimeUnixNano, span.span.StartTimeUnixNano, span.span.EndTimeUnixNano)
	}
	if event.DroppedAttributesCount != 0 {
		t.Fatalf("error event dropped attributes = %d, want 0", event.DroppedAttributesCount)
	}
	assertObservationWireAttributes(t, event.Attributes, map[string]any{
		"exception.message": "provider exploded",
		"exception.type":    reflect.TypeFor[wireProviderError]().PkgPath() + "." + reflect.TypeFor[wireProviderError]().Name(),
	})
}

func TestObservationWireRecordErrorBudgetsPreserveFinalStatusAndRequestHeadroom(t *testing.T) {
	diagnostics := captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	largeContent := strings.Repeat("c", 900<<10)
	_, observation := client.StartObservation(context.Background(), "record-error-budget",
		langfuse.TypeTool, langfuse.ObservationAttributes{
			Input:  largeContent,
			Output: largeContent,
		})
	firstPrefix := "error-0-"
	firstMessage := firstPrefix + strings.Repeat("e", (64<<10)-len(firstPrefix))
	observation.RecordError(wireProviderError{message: firstMessage})
	observation.Update(langfuse.ObservationAttributes{
		StatusMessage: "x",
		Metadata: map[string]any{
			"would_consume_error_reserve": strings.Repeat("m", 220<<10),
		},
	})

	finalMessage := firstMessage
	for index := 1; index < 8; index++ {
		prefix := fmt.Sprintf("error-%d-", index)
		finalMessage = prefix + strings.Repeat("e", (64<<10)-len(prefix))
		observation.RecordError(wireProviderError{message: finalMessage})
	}
	observation.RecordError(wireProviderError{message: "ninth error must be omitted"})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "record-error-budget")
	attributes := observationWireAttributeMap(t, span.span.Attributes)
	gotStatus, statusOK := attributes["langfuse.observation.status_message"].(string)
	if !statusOK || gotStatus != finalMessage {
		t.Fatalf("final Langfuse status message length/value = (%d, equal %v), want (%d, true)",
			len(gotStatus), gotStatus == finalMessage, len(finalMessage))
	}
	if got := attributes["langfuse.observation.level"]; got != "ERROR" {
		t.Fatalf("final Langfuse level = %#v, want ERROR", got)
	}
	if got := len(span.span.Events); got != 8 {
		t.Fatalf("exception event count = %d, want 8", got)
	}
	if got := span.span.Status.GetMessage(); got != finalMessage {
		t.Fatalf("OTel status message length/value = (%d, equal %v), want (%d, true)",
			len(got), got == finalMessage, len(finalMessage))
	}
	// A fully budgeted observation must fit the 4 MiB transport request cap on
	// its own, or splitting could never deliver it.
	requestBytes := proto.Size(receiver.Requests()[0].Export)
	if requestBytes >= 4<<20 {
		t.Fatalf("fully budgeted single-span request = %d bytes, want below the 4 MiB request cap", requestBytes)
	}
	assertEdgeDiagnosticCount(t, diagnostics, "observation error-event limit reached", 1)
	assertEdgeDiagnosticCount(t, diagnostics, "observation attributes exceed the aggregate size limit", 1)
}

func TestObservationWireMetadataBudgetPreservesRequiredFields(t *testing.T) {
	captureEdgeDiagnostics(t)
	client, receiver := newObservationWireClient(t, nil)
	traceMetadata := make(map[string]any, 100)
	observationMetadata := make(map[string]any, 100)
	for index := range 100 {
		traceMetadata[fmt.Sprintf("trace-%03d", index)] = index
		observationMetadata[fmt.Sprintf("observation-%03d", index)] = index
	}

	ctx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:      "budgeted-trace",
		UserID:    "budgeted-user",
		SessionID: "budgeted-session",
		Tags:      []string{"budgeted"},
		Metadata:  traceMetadata,
		Version:   "budgeted-version",
	})
	completion := time.Now().UTC().Add(-time.Second).Truncate(time.Nanosecond)
	_, observation := client.StartObservation(ctx, "metadata-budget", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{
			Input:               "input",
			Output:              "output",
			Metadata:            observationMetadata,
			Model:               "budgeted-model",
			ModelParameters:     map[string]any{"temperature": 0.2},
			Usage:               &langfuse.Usage{InputTokens: 10, OutputTokens: 5},
			CostDetails:         map[string]float64{"input": 0.01},
			Prompt:              &langfuse.PromptRef{Name: "budgeted-prompt", Version: 2},
			CompletionStartTime: completion,
		})
	observation.End()

	span := observationWireSpanNamed(t, exportObservationWireSpans(t, client, receiver, 1), "metadata-budget")
	attributes := observationWireAttributeMap(t, span.span.Attributes)
	for key, want := range map[string]any{
		"langfuse.environment":                       wireEnv,
		"langfuse.release":                           wireRelease,
		"langfuse.internal.is_app_root":              true,
		"langfuse.trace.name":                        "budgeted-trace",
		"user.id":                                    "budgeted-user",
		"session.id":                                 "budgeted-session",
		"langfuse.trace.tags":                        []any{"budgeted"},
		"langfuse.version":                           "budgeted-version",
		"langfuse.observation.type":                  "generation",
		"langfuse.observation.input":                 "input",
		"langfuse.observation.output":                "output",
		"langfuse.observation.model.name":            "budgeted-model",
		"langfuse.observation.model.parameters":      `{"temperature":0.2}`,
		"langfuse.observation.usage_details":         `{"input":10,"output":5,"total":15}`,
		"langfuse.observation.cost_details":          `{"input":0.01}`,
		"langfuse.observation.prompt.name":           "budgeted-prompt",
		"langfuse.observation.prompt.version":        int64(2),
		"langfuse.observation.completion_start_time": completion.Format(time.RFC3339Nano),
	} {
		if got := attributes[key]; !reflect.DeepEqual(got, want) {
			t.Errorf("attribute %q = %#v, want %#v", key, got, want)
		}
	}

	var traceCount, observationCount int
	for key := range attributes {
		if strings.HasPrefix(key, "langfuse.trace.metadata.") {
			traceCount++
		}
		if strings.HasPrefix(key, "langfuse.observation.metadata.") {
			observationCount++
		}
	}
	if traceCount != 32 || observationCount != 32 {
		t.Fatalf("metadata counts = trace:%d observation:%d, want 32 each", traceCount, observationCount)
	}
	if span.span.DroppedAttributesCount != 0 {
		t.Fatalf("dropped attributes = %d, want zero after SDK metadata budgeting", span.span.DroppedAttributesCount)
	}
}

func TestObservationWireClientSendsExactLangfuseHeaders(t *testing.T) {
	client, receiver := newObservationWireClient(t, nil)
	_, observation := client.StartObservation(context.Background(), "header-check", langfuse.TypeSpan,
		langfuse.ObservationAttributes{})
	observation.End()
	_ = exportObservationWireSpans(t, client, receiver, 1)

	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(wirePublicKey+":"+wireSecretKey))
	for _, request := range receiver.Requests() {
		for header, want := range map[string]string{
			"Authorization":                wantAuth,
			"x-langfuse-ingestion-version": "4",
			"x-langfuse-sdk-name":          "go",
			"x-langfuse-sdk-version":       langfuse.SDKVersion,
			"x-langfuse-public-key":        wirePublicKey,
		} {
			if got := request.Header.Get(header); got != want {
				t.Errorf("%s = %q, want %q", header, got, want)
			}
		}
	}
}

func newObservationWireClient(t *testing.T, change func(*langfuse.Config)) (*langfuse.Client, *otlpreceiver.Receiver) {
	t.Helper()
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	config := langfuse.Config{
		BaseURL:     receiver.URL(),
		PublicKey:   wirePublicKey,
		SecretKey:   wireSecretKey,
		Environment: wireEnv,
		Release:     wireRelease,
		ServiceName: wireService,
	}
	if change != nil {
		change(&config)
	}
	client, err := langfuse.New(context.Background(), config)
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Client.Shutdown() error = %v", err)
		}
	})
	return client, receiver
}

func exportObservationWireSpans(t *testing.T, client *langfuse.Client, receiver *otlpreceiver.Receiver, want int) []wireSpan {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Client.Flush() error = %v", err)
	}
	requests := receiver.Requests()
	if len(requests) == 0 {
		t.Fatal("OTLP request count = 0, want at least one")
	}

	var result []wireSpan
	for _, request := range requests {
		if request.Method != http.MethodPost || request.Path != "/api/public/otel/v1/traces" {
			t.Fatalf("OTLP request = %s %s, want POST /api/public/otel/v1/traces", request.Method, request.Path)
		}
		if got := request.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Fatalf("OTLP Content-Type = %q, want application/x-protobuf", got)
		}
		for _, resourceSpans := range request.Export.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					result = append(result, wireSpan{
						span:              span,
						resource:          resourceSpans.Resource,
						resourceSchemaURL: resourceSpans.SchemaUrl,
						scope:             scopeSpans.Scope,
						scopeSchemaURL:    scopeSpans.SchemaUrl,
					})
				}
			}
		}
	}
	if len(result) != want {
		t.Fatalf("exported span count = %d, want %d", len(result), want)
	}
	return result
}

func observationWireSpanNamed(t *testing.T, spans []wireSpan, name string) wireSpan {
	t.Helper()
	for _, span := range spans {
		if span.span.Name == name {
			return span
		}
	}
	t.Fatalf("exported spans do not contain %q", name)
	return wireSpan{}
}

func assertObservationWireEnvelope(t *testing.T, span wireSpan) {
	t.Helper()
	if span.resource == nil {
		t.Fatal("OTLP ResourceSpans.resource is nil")
	}
	if span.resource.DroppedAttributesCount != 0 {
		t.Fatalf("resource dropped attributes = %d, want 0", span.resource.DroppedAttributesCount)
	}
	assertObservationWireAttributes(t, span.resource.Attributes, map[string]any{
		"service.name":           wireService,
		"telemetry.sdk.language": "go",
		"telemetry.sdk.name":     "opentelemetry",
		"telemetry.sdk.version":  "1.44.0",
	})
	if span.resourceSchemaURL != "https://opentelemetry.io/schemas/1.41.0" {
		t.Fatalf("resource schema URL = %q, want OTel 1.41.0 schema", span.resourceSchemaURL)
	}
	if span.scope == nil {
		t.Fatal("OTLP ScopeSpans.scope is nil")
	}
	if span.scope.Name != "langfuse-sdk.go" || span.scope.Version != "0.1.0" {
		t.Fatalf("instrumentation scope = (%q, %q), want (langfuse-sdk.go, 0.1.0)", span.scope.Name, span.scope.Version)
	}
	if span.scope.DroppedAttributesCount != 0 {
		t.Fatalf("scope dropped attributes = %d, want 0", span.scope.DroppedAttributesCount)
	}
	assertObservationWireAttributes(t, span.scope.Attributes, map[string]any{"public_key": wirePublicKey})
	if span.scopeSchemaURL != "" {
		t.Fatalf("scope schema URL = %q, want empty", span.scopeSchemaURL)
	}
}

func assertObservationWireIdentity(t *testing.T, span *tracepb.Span, traceID, spanID, parentSpanID string) {
	t.Helper()
	wantTraceID := decodeObservationWireID(t, traceID, 16)
	wantSpanID := decodeObservationWireID(t, spanID, 8)
	wantParentID := decodeObservationWireID(t, parentSpanID, 8)
	if !bytes.Equal(span.TraceId, wantTraceID) {
		t.Errorf("span %q trace ID = %x, want %x", span.Name, span.TraceId, wantTraceID)
	}
	if !bytes.Equal(span.SpanId, wantSpanID) {
		t.Errorf("span %q span ID = %x, want %x", span.Name, span.SpanId, wantSpanID)
	}
	if !bytes.Equal(span.ParentSpanId, wantParentID) {
		t.Errorf("span %q parent span ID = %x, want %x", span.Name, span.ParentSpanId, wantParentID)
	}
}

func decodeObservationWireID(t *testing.T, value string, size int) []byte {
	t.Helper()
	if value == "" {
		return nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode ID %q: %v", value, err)
	}
	if len(decoded) != size {
		t.Fatalf("decoded ID %q length = %d, want %d", value, len(decoded), size)
	}
	return decoded
}

func assertObservationWireSpanShape(t *testing.T, span *tracepb.Span, start time.Time, status tracepb.Status_StatusCode, message string, events int) {
	t.Helper()
	if span.Kind != tracepb.Span_SPAN_KIND_INTERNAL {
		t.Errorf("span %q kind = %v, want INTERNAL", span.Name, span.Kind)
	}
	// OTel Go-generated trace IDs carry both the sampled bit (0x1) and the
	// W3C random trace-ID flag (0x100) in OTLP.
	if span.Flags != 0x101 {
		t.Errorf("span %q flags = %#x, want sampled+random flags 0x101", span.Name, span.Flags)
	}
	if span.TraceState != "" {
		t.Errorf("span %q trace state = %q, want empty", span.Name, span.TraceState)
	}
	if !start.IsZero() && span.StartTimeUnixNano != uint64(start.UnixNano()) {
		t.Errorf("span %q start = %d, want %d", span.Name, span.StartTimeUnixNano, start.UnixNano())
	}
	if span.EndTimeUnixNano < span.StartTimeUnixNano {
		t.Errorf("span %q end %d precedes start %d", span.Name, span.EndTimeUnixNano, span.StartTimeUnixNano)
	}
	if span.DroppedAttributesCount != 0 || span.DroppedEventsCount != 0 || span.DroppedLinksCount != 0 {
		t.Errorf("span %q dropped counts = attributes:%d events:%d links:%d, want all zero",
			span.Name, span.DroppedAttributesCount, span.DroppedEventsCount, span.DroppedLinksCount)
	}
	if len(span.Links) != 0 {
		t.Errorf("span %q links = %d, want 0", span.Name, len(span.Links))
	}
	if len(span.Events) != events {
		t.Fatalf("span %q events = %d, want %d", span.Name, len(span.Events), events)
	}
	if got := span.GetStatus().GetCode(); got != status {
		t.Errorf("span %q status = %v, want %v", span.Name, got, status)
	}
	if got := span.GetStatus().GetMessage(); got != message {
		t.Errorf("span %q status message = %q, want %q", span.Name, got, message)
	}
}

func assertObservationWireAttributes(t *testing.T, attributes []*commonpb.KeyValue, want map[string]any) {
	t.Helper()
	got := observationWireAttributeMap(t, attributes)
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal actual attributes: %v", err)
	}
	wantJSON, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatalf("marshal expected attributes: %v", err)
	}
	t.Fatalf("attributes differ\ngot:  %s\nwant: %s", gotJSON, wantJSON)
}

func observationWireAttributeMap(t *testing.T, attributes []*commonpb.KeyValue) map[string]any {
	t.Helper()
	result := make(map[string]any, len(attributes))
	for _, attribute := range attributes {
		if _, duplicate := result[attribute.Key]; duplicate {
			t.Fatalf("duplicate OTLP attribute key %q", attribute.Key)
		}
		result[attribute.Key] = observationWireAnyValue(t, attribute.Value)
	}
	return result
}

func observationWireAnyValue(t *testing.T, value *commonpb.AnyValue) any {
	t.Helper()
	if value == nil {
		return nil
	}
	switch typed := value.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return typed.StringValue
	case *commonpb.AnyValue_BoolValue:
		return typed.BoolValue
	case *commonpb.AnyValue_IntValue:
		return typed.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return typed.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return append([]byte(nil), typed.BytesValue...)
	case *commonpb.AnyValue_ArrayValue:
		result := make([]any, len(typed.ArrayValue.Values))
		for index, item := range typed.ArrayValue.Values {
			result[index] = observationWireAnyValue(t, item)
		}
		return result
	case *commonpb.AnyValue_KvlistValue:
		return observationWireAttributeMap(t, typed.KvlistValue.Values)
	default:
		t.Fatalf("unsupported OTLP AnyValue type %T", typed)
		return nil
	}
}
