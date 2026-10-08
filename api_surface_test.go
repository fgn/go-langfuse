package langfuse_test

import (
	"context"
	"iter"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// These assignments are compile-time checks for the complete v0.1 call shape.
// Reflection below prevents accidental additions to the small root API.
var (
	_ func() langfuse.Config                                           = langfuse.ConfigFromEnv
	_ func(context.Context, langfuse.Config) (*langfuse.Client, error) = langfuse.New

	_ func(*langfuse.Client, context.Context, langfuse.TraceAttributes) context.Context                                                                                             = (*langfuse.Client).WithTraceAttributes
	_ func(*langfuse.Client, context.Context, bool) context.Context                                                                                                                 = (*langfuse.Client).WithContentCapture
	_ func(*langfuse.Client, context.Context, *langfuse.Observation) context.Context                                                                                                = (*langfuse.Client).WithParent
	_ func(*langfuse.Client, context.Context, float64) context.Context                                                                                                              = (*langfuse.Client).WithSampleRate
	_ func(*langfuse.Client, context.Context) context.Context                                                                                                                       = (*langfuse.Client).WithBaggagePropagation
	_ func(*langfuse.Client, context.Context) context.Context                                                                                                                       = (*langfuse.Client).WithTraceAttributesFromBaggage
	_ func(*langfuse.Client, context.Context, string, langfuse.ObservationType, langfuse.ObservationAttributes) (context.Context, *langfuse.Observation)                            = (*langfuse.Client).StartObservation
	_ func(*langfuse.Client, context.Context, string, langfuse.ObservationType, langfuse.ObservationAttributes, func(context.Context, *langfuse.Observation) error) error           = (*langfuse.Client).Observe
	_ func(*langfuse.Client, context.Context, string, langfuse.ObservationAttributes)                                                                                               = (*langfuse.Client).Event
	_ func(*langfuse.Client, context.Context, langfuse.Score) error                                                                                                                 = (*langfuse.Client).RecordScore
	_ func(*langfuse.Client, context.Context, string, langfuse.PromptQuery) (langfuse.Prompt, error)                                                                                = (*langfuse.Client).GetPrompt
	_ func(*langfuse.Client, context.Context, langfuse.DatasetSpec) (langfuse.Dataset, error)                                                                                       = (*langfuse.Client).UpsertDataset
	_ func(*langfuse.Client, context.Context, string) (langfuse.Dataset, error)                                                                                                     = (*langfuse.Client).GetDataset
	_ func(*langfuse.Client, context.Context, langfuse.DatasetItemSpec) (langfuse.DatasetItem, error)                                                                               = (*langfuse.Client).UpsertDatasetItem
	_ func(*langfuse.Client, context.Context, string) (langfuse.DatasetItem, error)                                                                                                 = (*langfuse.Client).GetDatasetItem
	_ func(*langfuse.Client, context.Context, string) error                                                                                                                         = (*langfuse.Client).DeleteDatasetItem
	_ func(*langfuse.Client, context.Context, langfuse.DatasetItemQuery) iter.Seq2[langfuse.DatasetItem, error]                                                                     = (*langfuse.Client).DatasetItems
	_ func(*langfuse.Client, context.Context, langfuse.DatasetQuery) iter.Seq2[langfuse.Dataset, error]                                                                             = (*langfuse.Client).Datasets
	_ func(*langfuse.Client, context.Context, langfuse.ExperimentRun) (langfuse.ExperimentResult, error)                                                                            = (*langfuse.Client).RunExperiment
	_ func(*langfuse.Client, context.Context, langfuse.ExperimentQuery) iter.Seq2[langfuse.StoredExperiment, error]                                                                 = (*langfuse.Client).Experiments
	_ func(*langfuse.Client, context.Context, langfuse.ExperimentItemQuery) iter.Seq2[langfuse.StoredExperimentItem, error]                                                         = (*langfuse.Client).ExperimentItems
	_ func(*langfuse.Client, context.Context, langfuse.Experiment, langfuse.ExperimentItem, string, langfuse.ObservationAttributes) (context.Context, *langfuse.Observation, error) = (*langfuse.Client).StartExperimentItem
	_ func(*langfuse.Client, context.Context) error                                                                                                                                 = (*langfuse.Client).Flush
	_ func(*langfuse.Client, context.Context) error                                                                                                                                 = (*langfuse.Client).Shutdown

	_ func(langfuse.DatasetItem) langfuse.ExperimentItem = langfuse.DatasetItem.ExperimentItem
	_ func(langfuse.ExperimentResult, bool) string       = langfuse.ExperimentResult.Summary

	_ langfuse.ExperimentTask = func(context.Context, langfuse.ExperimentItem) (any, error) { return "", nil }
	_ langfuse.Evaluator      = func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) { return nil, nil }
	_ langfuse.RunEvaluator   = func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) { return nil, nil }

	_ func(*langfuse.Observation, langfuse.ObservationAttributes) = (*langfuse.Observation).Update
	_ func(*langfuse.Observation, error)                          = (*langfuse.Observation).RecordError
	_ func(*langfuse.Observation)                                 = (*langfuse.Observation).End
	_ func(*langfuse.Observation, time.Time)                      = (*langfuse.Observation).EndAt
	_ func(*langfuse.Observation) string                          = (*langfuse.Observation).TraceID
	_ func(*langfuse.Observation) string                          = (*langfuse.Observation).ID
	_ func(*langfuse.Observation) bool                            = (*langfuse.Observation).Sampled

	_ func(string, float64) (bool, error) = langfuse.TraceSampledAt
	_ func(sdktrace.ReadOnlySpan) bool    = langfuse.IsDefaultExportSpan
	_ func(sdktrace.ReadOnlySpan) bool    = langfuse.IsLangfuseSpan
	_ func(sdktrace.ReadOnlySpan) bool    = langfuse.IsGenAISpan
	_ func(sdktrace.ReadOnlySpan) bool    = langfuse.IsKnownLLMInstrumentor

	_ func(langfuse.Prompt) *langfuse.PromptRef                      = langfuse.Prompt.Ref
	_ func(langfuse.Prompt, map[string]any) langfuse.Prompt          = langfuse.Prompt.Compile
	_ func(langfuse.Prompt, map[string]any) (langfuse.Prompt, error) = langfuse.Prompt.CompileStrict
	_ func(langfuse.Prompt, any) error                               = langfuse.Prompt.DecodeConfig

	_ error = langfuse.ErrDatasetNotFound
	_ error = langfuse.ErrDatasetItemNotFound
	_ error = langfuse.ErrWriteOutcomeUnknown
	_ error = langfuse.ErrInvalidExperiment
	_ error = langfuse.ErrExperimentItemNotExported
	_ error = langfuse.ErrPromptNotFound
	_ error = langfuse.ErrPromptTypeMismatch
	_ error = langfuse.ErrScoreQueueFull
	_ error = langfuse.ErrShutdownInProgress
	_ error = langfuse.ErrTracerProviderInUse

	_ langfuse.MaskField = langfuse.MaskObservationInput
	_ langfuse.MaskField = langfuse.MaskObservationOutput
	_ langfuse.MaskField = langfuse.MaskTraceMetadata
	_ langfuse.MaskField = langfuse.MaskObservationMetadata
	_ langfuse.MaskField = langfuse.MaskScoreMetadata
	_ langfuse.MaskField = langfuse.MaskErrorMessage
	_ langfuse.MaskField = langfuse.MaskDatasetMetadata
	_ langfuse.MaskField = langfuse.MaskDatasetItemInput
	_ langfuse.MaskField = langfuse.MaskDatasetItemExpectedOutput
	_ langfuse.MaskField = langfuse.MaskDatasetItemMetadata
	_ langfuse.MaskField = langfuse.MaskExperimentMetadata
	_ langfuse.MaskField = langfuse.MaskExperimentItemMetadata
	_ langfuse.MaskField = langfuse.MaskExperimentItemExpectedOutput
)

func TestPublicMethodSurface(t *testing.T) {
	t.Parallel()

	assertMethodNames(t, (*langfuse.Client)(nil), []string{
		"DatasetItems",
		"Datasets",
		"DeleteDatasetItem",
		"Event",
		"ExperimentItems",
		"Experiments",
		"Flush",
		"GetDataset",
		"GetDatasetItem",
		"GetPrompt",
		"Observe",
		"RecordScore",
		"RunExperiment",
		"Shutdown",
		"StartExperimentItem",
		"StartObservation",
		"UpsertDataset",
		"UpsertDatasetItem",
		"WithBaggagePropagation",
		"WithContentCapture",
		"WithParent",
		"WithSampleRate",
		"WithTraceAttributes",
		"WithTraceAttributesFromBaggage",
	})
	assertMethodNames(t, (*langfuse.Observation)(nil), []string{
		"End",
		"EndAt",
		"ID",
		"RecordError",
		"Sampled",
		"TraceID",
		"Update",
	})
	assertMethodNames(t, langfuse.ExperimentResult{}, []string{"Summary"})
	assertMethodNames(t, langfuse.Prompt{}, []string{
		"Compile",
		"CompileStrict",
		"DecodeConfig",
		"Ref",
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
		"SampleRate",
		"ServiceName",
		"TracerProvider",
		"ShouldExportSpan",
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
		"Environment",
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
		"DatasetRunID",
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

	assertFieldNames(t, langfuse.PromptQuery{}, []string{
		"Version",
		"Label",
		"Type",
		"CacheTTL",
		"DisableCache",
		"Fallback",
	})
	assertFieldNames(t, langfuse.PromptFallback{}, []string{
		"Type",
		"Text",
		"Messages",
		"Config",
	})
	assertFieldNames(t, langfuse.PromptMessage{}, []string{
		"Role",
		"Content",
		"PlaceholderName",
		"Extra",
	})
	assertFieldNames(t, langfuse.Prompt{}, []string{
		"Name",
		"Version",
		"Type",
		"Text",
		"Messages",
		"Config",
		"Labels",
		"Tags",
		"CommitMessage",
		"Source",
	})

	assertFieldNames(t, langfuse.DatasetSpec{}, []string{
		"Name",
		"Description",
		"Metadata",
		"InputSchema",
		"ExpectedOutputSchema",
	})
	assertFieldNames(t, langfuse.Dataset{}, []string{
		"ID",
		"Name",
		"Description",
		"Metadata",
		"InputSchema",
		"ExpectedOutputSchema",
		"CreatedAt",
		"UpdatedAt",
	})
	assertFieldNames(t, langfuse.DatasetItemSpec{}, []string{
		"DatasetName",
		"ID",
		"Input",
		"ExpectedOutput",
		"Metadata",
		"SourceTraceID",
		"SourceObservationID",
		"Status",
	})
	assertFieldNames(t, langfuse.DatasetItem{}, []string{
		"ID",
		"DatasetID",
		"DatasetName",
		"Status",
		"Input",
		"ExpectedOutput",
		"Metadata",
		"SourceTraceID",
		"SourceObservationID",
		"CreatedAt",
		"UpdatedAt",
		"Version",
	})
	assertFieldNames(t, langfuse.DatasetItemQuery{}, []string{
		"DatasetName",
		"AsOf",
		"SourceTraceID",
		"SourceObservationID",
		"PageSize",
	})
	assertFieldNames(t, langfuse.Experiment{}, []string{
		"ID",
		"Name",
		"Description",
		"Metadata",
	})
	assertFieldNames(t, langfuse.ExperimentItem{}, []string{
		"ID",
		"DatasetID",
		"Version",
		"Input",
		"ExpectedOutput",
		"Metadata",
	})
	assertFieldNames(t, langfuse.ExperimentRun{}, []string{
		"Name",
		"RunName",
		"Description",
		"Metadata",
		"Items",
		"Task",
		"Evaluators",
		"CompositeEvaluator",
		"RunEvaluators",
		"MaxConcurrency",
	})
	assertFieldNames(t, langfuse.EvaluatorInput{}, []string{
		"Input",
		"Output",
		"ExpectedOutput",
		"Metadata",
		"Evaluations",
	})
	assertFieldNames(t, langfuse.Evaluation{}, []string{
		"Name",
		"NumericValue",
		"StringValue",
		"DataType",
		"ConfigID",
		"Comment",
		"Metadata",
	})
	assertFieldNames(t, langfuse.ExperimentItemResult{}, []string{
		"Item",
		"Output",
		"Evaluations",
		"TraceID",
		"ObservationID",
		"DatasetRunID",
		"Err",
		"EvaluationErr",
	})
	assertFieldNames(t, langfuse.ExperimentResult{}, []string{
		"ExperimentID",
		"Name",
		"RunName",
		"Description",
		"DatasetRunID",
		"ItemResults",
		"RunEvaluations",
		"RunEvaluationErr",
	})

	assertFieldNames(t, langfuse.DatasetQuery{}, []string{"PageSize"})
	assertFieldNames(t, langfuse.ExperimentQuery{}, []string{"From", "To", "IDs", "Names", "DatasetIDs", "PageSize"})
	assertFieldNames(t, langfuse.StoredExperiment{}, []string{
		"ID", "Name", "Description", "DatasetID", "StartTime", "EndTime", "ItemCount", "Metadata", "Scores",
	})
	assertFieldNames(t, langfuse.ExperimentItemQuery{}, []string{
		"From", "To", "ExperimentIDs", "ExperimentNames", "ItemIDs", "DatasetIDs", "PageSize",
	})
	assertFieldNames(t, langfuse.StoredExperimentItem{}, []string{
		"ObservationID", "TraceID", "StartTime", "EndTime", "Level", "Environment",
		"ExperimentID", "ExperimentName", "ExperimentDescription", "ItemID", "DatasetID", "ItemVersion",
		"Input", "Output", "ExpectedOutput", "Metadata", "ItemMetadata", "ExperimentMetadata", "Scores",
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

	promptTypes := map[langfuse.PromptType]string{
		langfuse.PromptTypeText: "text",
		langfuse.PromptTypeChat: "chat",
	}
	for got, want := range promptTypes {
		if string(got) != want {
			t.Errorf("prompt type %q = %q, want %q", want, got, want)
		}
	}

	statuses := map[langfuse.DatasetItemStatus]string{
		langfuse.DatasetItemActive:   "ACTIVE",
		langfuse.DatasetItemArchived: "ARCHIVED",
	}
	for got, want := range statuses {
		if string(got) != want {
			t.Errorf("dataset item status %q = %q, want %q", want, got, want)
		}
	}

	promptSources := map[langfuse.PromptSource]string{
		langfuse.PromptSourceServer:   "server",
		langfuse.PromptSourceCache:    "cache",
		langfuse.PromptSourceStale:    "stale",
		langfuse.PromptSourceFallback: "fallback",
	}
	for got, want := range promptSources {
		if string(got) != want {
			t.Errorf("prompt source %q = %q, want %q", want, got, want)
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
