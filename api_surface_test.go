package lunte_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/fgn/lunte"
)

// These assignments are compile-time checks for the complete v0.1 call shape.
// Reflection below prevents accidental additions to the small root API.
var (
	_ func() lunte.Config                                        = lunte.ConfigFromEnv
	_ func(context.Context, lunte.Config) (*lunte.Client, error) = lunte.New

	_ func(*lunte.Client, context.Context, lunte.TraceAttributes) context.Context                                                                             = (*lunte.Client).WithTraceAttributes
	_ func(*lunte.Client, context.Context, string, lunte.ObservationType, lunte.ObservationAttributes) (context.Context, *lunte.Observation)                  = (*lunte.Client).StartObservation
	_ func(*lunte.Client, context.Context, string, lunte.ObservationType, lunte.ObservationAttributes, func(context.Context, *lunte.Observation) error) error = (*lunte.Client).Observe
	_ func(*lunte.Client, context.Context, string, lunte.ObservationAttributes)                                                                               = (*lunte.Client).Event
	_ func(*lunte.Client, context.Context) error                                                                                                              = (*lunte.Client).Flush
	_ func(*lunte.Client, context.Context) error                                                                                                              = (*lunte.Client).Shutdown

	_ func(*lunte.Observation, lunte.ObservationAttributes) = (*lunte.Observation).Update
	_ func(*lunte.Observation, error)                       = (*lunte.Observation).RecordError
	_ func(*lunte.Observation)                              = (*lunte.Observation).End
	_ func(*lunte.Observation) string                       = (*lunte.Observation).TraceID
	_ func(*lunte.Observation) string                       = (*lunte.Observation).ID
)

func TestPublicMethodSurface(t *testing.T) {
	t.Parallel()

	assertMethodNames(t, (*lunte.Client)(nil), []string{
		"Event",
		"Flush",
		"Observe",
		"Shutdown",
		"StartObservation",
		"WithTraceAttributes",
	})
	assertMethodNames(t, (*lunte.Observation)(nil), []string{
		"End",
		"ID",
		"RecordError",
		"TraceID",
		"Update",
	})
}

func TestPublicStructSurface(t *testing.T) {
	t.Parallel()

	assertFieldNames(t, lunte.Config{}, []string{
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
	assertFieldNames(t, lunte.TraceAttributes{}, []string{
		"Name",
		"UserID",
		"SessionID",
		"Tags",
		"Metadata",
		"Version",
	})
	assertFieldNames(t, lunte.Usage{}, []string{
		"InputTokens",
		"OutputTokens",
		"CacheReadInputTokens",
		"CacheCreationInputTokens",
		"ReasoningOutputTokens",
		"Details",
	})
	assertFieldNames(t, lunte.PromptRef{}, []string{"Name", "Version"})
	assertFieldNames(t, lunte.ObservationAttributes{}, []string{
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

	assertNoExportedFields(t, lunte.Client{})
	assertNoExportedFields(t, lunte.Observation{})
}

func TestPublicConstantValues(t *testing.T) {
	t.Parallel()

	levels := map[lunte.Level]string{
		lunte.LevelDefault: "DEFAULT",
		lunte.LevelDebug:   "DEBUG",
		lunte.LevelWarning: "WARNING",
		lunte.LevelError:   "ERROR",
	}
	for got, want := range levels {
		if string(got) != want {
			t.Errorf("level %q = %q, want %q", want, got, want)
		}
	}

	types := map[lunte.ObservationType]string{
		lunte.TypeSpan:       "span",
		lunte.TypeGeneration: "generation",
		lunte.TypeEvent:      "event",
		lunte.TypeEmbedding:  "embedding",
		lunte.TypeAgent:      "agent",
		lunte.TypeTool:       "tool",
		lunte.TypeChain:      "chain",
		lunte.TypeRetriever:  "retriever",
		lunte.TypeEvaluator:  "evaluator",
		lunte.TypeGuardrail:  "guardrail",
	}
	for got, want := range types {
		if string(got) != want {
			t.Errorf("observation type %q = %q, want %q", want, got, want)
		}
	}
}

func assertMethodNames(t *testing.T, value any, want []string) {
	t.Helper()

	typeOf := reflect.TypeOf(value)
	got := make([]string, 0, typeOf.NumMethod())
	for i := 0; i < typeOf.NumMethod(); i++ {
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
	for i := 0; i < typeOf.NumField(); i++ {
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
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		if field.IsExported() {
			t.Errorf("%v unexpectedly exports field %s", typeOf, field.Name)
		}
	}
}
