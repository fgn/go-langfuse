package langfuse_test

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// These assignments are compile-time checks for the complete v0.1 call shape.
// Reflection below prevents accidental additions to the small root API.
var (
	_ func() langfuse.Config                                           = langfuse.ConfigFromEnv
	_ func(context.Context, langfuse.Config) (*langfuse.Client, error) = langfuse.New

	_ func(*langfuse.Client, context.Context, langfuse.TraceAttributes) context.Context                                                                                   = (*langfuse.Client).WithTraceAttributes
	_ func(*langfuse.Client, context.Context) context.Context                                                                                                             = (*langfuse.Client).WithDetachedTrace
	_ func(*langfuse.Client, context.Context, string, langfuse.ObservationType, langfuse.ObservationAttributes) (context.Context, *langfuse.Observation)                  = (*langfuse.Client).StartObservation
	_ func(*langfuse.Client, context.Context, string, langfuse.ObservationType, langfuse.ObservationAttributes, func(context.Context, *langfuse.Observation) error) error = (*langfuse.Client).Observe
	_ func(*langfuse.Client, context.Context, string, langfuse.ObservationAttributes)                                                                                     = (*langfuse.Client).Event
	_ func(*langfuse.Client, context.Context, langfuse.Score) error                                                                                                       = (*langfuse.Client).RecordScore
	_ func(*langfuse.Client, context.Context) error                                                                                                                       = (*langfuse.Client).Flush
	_ func(*langfuse.Client, context.Context) error                                                                                                                       = (*langfuse.Client).Shutdown

	_ func(*langfuse.Observation, langfuse.ObservationAttributes) = (*langfuse.Observation).Update
	_ func(*langfuse.Observation, error)                          = (*langfuse.Observation).RecordError
	_ func(*langfuse.Observation)                                 = (*langfuse.Observation).End
	_ func(*langfuse.Observation, time.Time)                      = (*langfuse.Observation).EndAt
	_ func(*langfuse.Observation) string                          = (*langfuse.Observation).TraceID
	_ func(*langfuse.Observation) string                          = (*langfuse.Observation).ID
)

func TestPublicMethodSurface(t *testing.T) {
	t.Parallel()

	assertMethodNames(t, (*langfuse.Client)(nil), []string{
		"Event",
		"Flush",
		"Observe",
		"RecordScore",
		"Shutdown",
		"StartObservation",
		"WithDetachedTrace",
		"WithTraceAttributes",
	})
	assertMethodNames(t, (*langfuse.Observation)(nil), []string{
		"End",
		"EndAt",
		"ID",
		"RecordError",
		"TraceID",
		"Update",
	})
}

func TestPublicStructSurface(t *testing.T) {
	t.Parallel()

	assertFieldNames(t, langfuse.Config{}, []string{
		"BaseURL",
		"PublicKey",
		"SecretKey",
		"Environment",
		"Release",
		"ServiceName",
		"TracerProvider",
		"MaxQueueSize",
		"BlockOnQueueFull",
		"Disabled",
		"DisableContentCapture",
		"Mask",
	})
	assertFieldNames(t, langfuse.TraceAttributes{}, []string{
		"Name",
		"UserID",
		"SessionID",
		"Tags",
		"Metadata",
		"Version",
	})
	assertFieldNames(t, langfuse.Usage{}, []string{
		"InputTokens",
		"OutputTokens",
		"CacheReadInputTokens",
		"CacheCreationInputTokens",
		"ReasoningOutputTokens",
		"Details",
	})
	assertFieldNames(t, langfuse.PromptRef{}, []string{"Name", "Version"})
	assertFieldNames(t, langfuse.Score{}, []string{
		"ID",
		"Name",
		"TraceID",
		"SessionID",
		"ObservationID",
		"NumericValue",
		"StringValue",
		"DataType",
		"ConfigID",
		"Comment",
		"Metadata",
		"Timestamp",
	})
	assertFieldNames(t, langfuse.ObservationAttributes{}, []string{
		"Input",
		"Output",
		"Metadata",
		"Level",
		"StatusMessage",
		"Version",
		"Model",
		"ModelParameters",
		"Usage",
		"CostDetails",
		"Prompt",
		"CompletionStartTime",
		"StartTime",
	})

	assertNoExportedFields(t, langfuse.Client{})
	assertNoExportedFields(t, langfuse.Observation{})
}

func TestPublicConstantValues(t *testing.T) {
	t.Parallel()

	levels := map[langfuse.Level]string{
		langfuse.LevelDefault: "DEFAULT",
		langfuse.LevelDebug:   "DEBUG",
		langfuse.LevelWarning: "WARNING",
		langfuse.LevelError:   "ERROR",
	}
	for got, want := range levels {
		if string(got) != want {
			t.Errorf("level %q = %q, want %q", want, got, want)
		}
	}

	types := map[langfuse.ObservationType]string{
		langfuse.TypeSpan:       "span",
		langfuse.TypeGeneration: "generation",
		langfuse.TypeEvent:      "event",
		langfuse.TypeEmbedding:  "embedding",
		langfuse.TypeAgent:      "agent",
		langfuse.TypeTool:       "tool",
		langfuse.TypeChain:      "chain",
		langfuse.TypeRetriever:  "retriever",
		langfuse.TypeEvaluator:  "evaluator",
		langfuse.TypeGuardrail:  "guardrail",
	}
	for got, want := range types {
		if string(got) != want {
			t.Errorf("observation type %q = %q, want %q", want, got, want)
		}
	}

	scoreTypes := map[langfuse.ScoreDataType]string{
		langfuse.ScoreTypeBoolean:     "BOOLEAN",
		langfuse.ScoreTypeCategorical: "CATEGORICAL",
		langfuse.ScoreTypeCorrection:  "CORRECTION",
		langfuse.ScoreTypeNumeric:     "NUMERIC",
		langfuse.ScoreTypeText:        "TEXT",
	}
	for got, want := range scoreTypes {
		if string(got) != want {
			t.Errorf("score data type %q = %q, want %q", want, got, want)
		}
	}
}

func assertMethodNames(t *testing.T, value any, want []string) {
	t.Helper()

	typeOf := reflect.TypeOf(value)
	got := make([]string, 0, typeOf.NumMethod())
	for i := range typeOf.NumMethod() {
		got = append(got, typeOf.Method(i).Name)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("exported methods on %v = %v, want %v", typeOf, got, want)
	}
}

func assertFieldNames(t *testing.T, value any, want []string) {
	t.Helper()

	typeOf := reflect.TypeOf(value)
	got := make([]string, 0, typeOf.NumField())
	for i := range typeOf.NumField() {
		field := typeOf.Field(i)
		if field.IsExported() {
			got = append(got, field.Name)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("exported fields on %v = %v, want %v", typeOf, got, want)
	}
}

func assertNoExportedFields(t *testing.T, value any) {
	t.Helper()

	typeOf := reflect.TypeOf(value)
	for i := range typeOf.NumField() {
		field := typeOf.Field(i)
		if field.IsExported() {
			t.Errorf("%v unexpectedly exports field %s", typeOf, field.Name)
		}
	}
}
