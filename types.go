package langfuse

import (
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Config configures a Langfuse client.
type Config struct {
	// BaseURL is the Langfuse host or OTLP traces endpoint. It defaults to
	// https://cloud.langfuse.com.
	BaseURL string
	// PublicKey identifies the Langfuse project.
	PublicKey string
	// SecretKey authenticates writes to the Langfuse project.
	SecretKey string

	// Environment and Release are stamped on every span observed by the
	// Langfuse processor.
	Environment string
	Release     string

	// SampleRate selects the fraction of traces exported in isolated mode,
	// decided once per trace by a deterministic threshold on the trace ID and
	// inherited by every SDK observation started on the deciding context
	// path. nil selects the default of 1.0 (export everything); a non-nil
	// value must be finite and within [0, 1], where 0 exports no traces while
	// scores and prompts keep working. Other values are a validation error in
	// [New]. It is ignored with a diagnostic when TracerProvider is set,
	// where the application's sampler remains authoritative.
	// [Client.WithSampleRate] overrides it per context path.
	SampleRate *float64

	// ServiceName overrides service.name on an SDK-owned provider. When empty,
	// the standard OpenTelemetry resource.Default value is retained, including
	// OTEL_SERVICE_NAME. It is ignored when TracerProvider is set.
	ServiceName string

	// TracerProvider attaches the Langfuse processor to a caller-owned provider. The client
	// never replaces the global provider or shuts this provider down.
	TracerProvider *sdktrace.TracerProvider

	// MaxQueueSize bounds how many ended export-eligible spans the client
	// buffers in memory while waiting for batch export. Zero selects the
	// default of 2048; negative values are a validation error in [New].
	MaxQueueSize int

	// BlockOnQueueFull makes ending an exported observation, and recording a
	// score, wait for buffer space instead of dropping when the
	// corresponding queue is full. A sustained export outage can then stall
	// goroutines that end observations or record scores, so it defaults to
	// false: on a full queue new work is dropped with a diagnostic, matching
	// OpenTelemetry defaults.
	BlockOnQueueFull bool

	// Disabled makes the complete client a safe no-op.
	Disabled bool

	// DisableContentCapture removes Input and Output supplied through
	// ObservationAttributes. It does not remove content emitted by third-party
	// OpenTelemetry instrumentation. Identifiers, metadata, model data, and
	// usage are still recorded.
	DisableContentCapture bool

	// Mask applies only to Input, Output, and Metadata supplied through this
	// Client. Each metadata map is passed as one complete value and must remain
	// a map[string]any to be retained. It does not process identifiers, model
	// fields, StatusMessage, [Observation.RecordError] text, or third-party
	// spans and events.
	// Mask may be called concurrently and therefore must be concurrency-safe.
	// A panic is recovered and the affected value is omitted.
	Mask func(value any) any

	envErr error
}

// TraceAttributes are request-scoped fields copied to an active SDK
// observation and to spans subsequently started from the returned context.
type TraceAttributes struct {
	Name      string
	UserID    string
	SessionID string
	// Tags merge in caller order, de-duplicate, and retain at most 64 values
	// and 16 KiB of UTF-8 data over one trace context.
	Tags []string
	// Metadata merges by top-level key and retains at most 32 distinct keys in
	// one trace context so required observation fields remain within default
	// OpenTelemetry span limits. Keys are limited to 200 bytes.
	Metadata map[string]any
	Version  string
	// Environment is a request-scoped override of Config.Environment for
	// spans exported on this context path, validated with the same rule. It
	// is also the only source of the langfuse_environment baggage member
	// under [Client.WithBaggagePropagation]; the client-wide default is
	// never propagated cross-process.
	Environment string
}

// ObservationType identifies how Langfuse presents an observation.
type ObservationType string

// Level is the Langfuse severity of an observation.
type Level string

const (
	LevelDefault Level = "DEFAULT"
	LevelDebug   Level = "DEBUG"
	LevelWarning Level = "WARNING"
	LevelError   Level = "ERROR"
)

const (
	TypeSpan       ObservationType = "span"
	TypeGeneration ObservationType = "generation"
	TypeEvent      ObservationType = "event"
	TypeEmbedding  ObservationType = "embedding"
	TypeAgent      ObservationType = "agent"
	TypeTool       ObservationType = "tool"
	TypeChain      ObservationType = "chain"
	TypeRetriever  ObservationType = "retriever"
	TypeEvaluator  ObservationType = "evaluator"
	TypeGuardrail  ObservationType = "guardrail"
)

// Usage records inclusive provider token counts. InputTokens includes cache
// tokens, and OutputTokens includes reasoning tokens. The SDK normalizes these
// values to Langfuse's exclusive usage_details representation while total
// remains the checked sum of the inclusive input and output totals.
type Usage struct {
	InputTokens              int64
	OutputTokens             int64
	CacheReadInputTokens     int64
	CacheCreationInputTokens int64
	ReasoningOutputTokens    int64

	// Details contains additional provider-specific subsets. Keys prefixed
	// input_ or output_ are subtracted from the corresponding base bucket.
	// Such buckets must be mutually exclusive, must not overlap the typed
	// cache/reasoning fields, and must be subsets of their inclusive total. The
	// canonical input, output, total, cache, and reasoning keys are reserved.
	// At most 64 provider-specific detail buckets are retained, with keys
	// limited to 200 bytes.
	Details map[string]int64
}

// PromptRef links an observation to a versioned Langfuse prompt.
type PromptRef struct {
	Name    string
	Version int
}

// ObservationAttributes contains fields that can be set when an observation
// starts or in a later Update. Zero values are ignored and updates never clear
// an already recorded value. Each serialized structured value is limited to
// 1 MiB and the observation-level payload attributes are limited to 2 MiB in
// aggregate over the observation's current values. Separately bounded
// trace/client propagation and exception events sit outside that aggregate.
// Oversized fields are omitted with a payload-free OpenTelemetry diagnostic.
type ObservationAttributes struct {
	// Input and Output are explicit content. They are never inferred by the
	// SDK, are subject to Config.DisableContentCapture, and pass through Mask.
	Input  any
	Output any
	// Metadata merges by top-level key across Update calls and passes through
	// Mask. Empty maps are ignored and cannot clear earlier metadata. At most 32
	// distinct keys are retained over an observation's lifetime; keys are
	// limited to 200 bytes.
	Metadata map[string]any
	Level    Level
	// StatusMessage is explicit telemetry content, is limited to 16 KiB, and
	// does not pass through Mask. Sanitize it before calling the SDK.
	StatusMessage string
	// Version overrides a propagated TraceAttributes.Version on this
	// observation only.
	Version string

	// The remaining fields are honored only by generation and embedding
	// observations, except Prompt, which Langfuse links only on generations.
	Model               string
	ModelParameters     map[string]any
	Usage               *Usage
	CostDetails         map[string]float64
	Prompt              *PromptRef
	CompletionStartTime time.Time

	// StartTime is honored only by StartObservation; Event and Update ignore
	// it with a diagnostic.
	StartTime time.Time
}
