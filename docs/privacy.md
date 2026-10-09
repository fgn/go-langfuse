# Content and sensitive data

The SDK never inspects function arguments, HTTP bodies, or model clients and
captures no provider content automatically. It does export fields explicitly
supplied by the caller. Input/output are the obvious content fields, but
metadata, model parameters, status messages, and errors can also contain
sensitive data.

Set `LANGFUSE_CONTENT_CAPTURE_ENABLED=false`, or configure
`DisableContentCapture`, to drop SDK-supplied `Input` and `Output` and replace
`RecordError` text with `"error"` while still recording every other field. `Client.WithContentCapture` can override that
default for observations started on one client-scoped local context tree. Each
observation retains the decision made at start for all later `Update` calls.
The privacy boundary is deliberately narrow:

| Data source | Dropped when content capture is disabled | Passed to `Mask` |
| --- | --- | --- |
| `ObservationAttributes.Input` and `Output` | Yes | Yes, unless content capture is disabled |
| `ObservationAttributes.Metadata` | No | Yes, once as the complete `map[string]any` |
| `TraceAttributes.Metadata` | No | Yes, once as the complete `map[string]any` |
| Observation name/type, trace name, user/session IDs, tags, version, level, `StatusMessage`, model/parameters, usage, costs, prompt, and completion time | No | No |
| `RecordError(err)` text (status and exception-event message) | Yes, replaced by `"error"` | Yes, as `MaskErrorMessage`, unless content capture is disabled |
| `RecordError(err)` exception type | No | No |
| `Score` metadata | No | Yes, once as the complete `map[string]any` |
| `ExperimentItem.ExpectedOutput` | Yes, on the starting context | Yes, once per item start, unless content capture is disabled |
| `Experiment.Metadata` (or `ExperimentRun.Metadata`) and `ExperimentItem.Metadata` when it is a JSON object (exported on every span of the item trace) | No | Yes, once per item start as the complete value; other item metadata is not exported |
| `RunExperiment` copies: item input and task output on the item root and in each evaluator observation's input, with the expected output and item metadata; evaluation metadata in the evaluator observation's output | Yes, except the root's metadata | Once per value under its own field (`MaskObservationInput`, `MaskObservationOutput`, `MaskExperimentItemExpectedOutput`, `MaskExperimentItemMetadata`, `MaskScoreMetadata`); every copy reuses that result, frozen when it was masked, so a callback that later changes the value cannot change what is exported. The root's metadata (masked item and run metadata) also passes `MaskObservationMetadata` |
| `RunExperiment` dataset run link: the masked experiment metadata | No | Yes, as `MaskExperimentMetadata` above |
| Task and evaluator errors, recorded with `RecordError` (a recovered panic as a fixed error without the panic value) | Yes, replaced by `"error"` | Yes, as `MaskErrorMessage`, unless content capture is disabled |
| `DatasetItemSpec` input, expected output, and metadata; `DatasetSpec` metadata (REST writes) | No | Yes, once per supplied field; failure rejects the write |
| Evaluation names, values, and comments (scores) | No | Evaluation metadata only, as `Score` metadata |
| Experiment and dataset IDs, names, run names, and descriptions; dataset schemas; item versions, statuses, and source trace and observation IDs | No | No |
| `Score` comment and value | No | No |
| `CreateScore` metadata (REST write) | No | Yes, once as `MaskScoreMetadata`, as for `RecordScore` |
| `CommentSpec.Content`; score config, annotation queue, and comment IDs, names, descriptions, categories, and statuses | No | No |
| OpenTelemetry resource attributes (`resource.Default`/`OTEL_RESOURCE_ATTRIBUTES` in isolated mode; caller resource in borrowed mode) | No | No |
| Third-party OTel span attributes and events | No | No |

**Cross-process propagation sends attributes to every destination.**
`WithBaggagePropagation` places user ID, session ID, trace name, version,
request-scoped environment, string metadata values, and the 32-hex trace ID
of the current application root into W3C baggage on its context branch. Baggage is delivered by whatever propagator the
application has installed, so these values travel with **every** outbound
request that carries the context — third-party APIs and services that have
nothing to do with Langfuse included — until the branch ends. Inbound
metadata accepted by `WithTraceAttributesFromBaggage` passes through the
configured `Mask` exactly once, like local trace metadata; the other
propagated fields are identifiers and are never masked. Enable propagation
only on paths where that disclosure is intended. Baggage diagnostics name
fixed protocol members only; metadata key suffixes and unknown member names
are user- or wire-controlled and appear in diagnostics as counts, never as
text.

Dataset writes are REST calls, not telemetry: content capture does not apply,
and masking fails closed. A nil or panicking `Mask` result fails the write
before anything is sent, because an omitted field keeps its stored value.
Langfuse keeps earlier item versions after an upsert or a delete. Dataset and
experiment reads return stored content unmasked.

Annotation review calls are REST calls too. Comment content and score
comments are explicit caller content, like `Score.Comment`, and are sent as
given: sanitize them before calling the SDK. Score, comment, and queue reads
return stored data unmasked.

`RunExperiment` passes items, outputs, and evaluations to the task and
evaluators unmasked; masking applies only where they leave the process.
Experiment item IDs are identifiers and never masked, which is why the Go
runner requires explicit IDs instead of deriving them from the input as the
official SDKs do.

Disabling content capture does not make metadata, model parameters, or status
messages safe. Error text is content: provider and application errors often
echo request or response text. With content capture disabled, `RecordError`
never calls `err.Error()`; the OTel status description, Langfuse status
message, and exception-event message are the payload-free `"error"`, and only
the Go error type is exported. With capture enabled, the text passes through
`Mask` as `MaskErrorMessage`; a masker that returns anything but a string, or
panics, yields `"error"`. To record a known payload-free failure category in
any mode, set `Level` and `StatusMessage` through `Update`. Never put
credentials, PHI, prompts, or completions in a `StatusMessage`.

`WithContentCapture` is local process state, not an authorization system or a
cross-process propagation mechanism. Call it only after the application has
resolved its own access policy. It does not change score delivery: score
metadata still passes through `Mask`, while score comments and values remain
outside both the content-capture and masking controls.

`Mask` receives a `MaskField` with each SDK value shown in the table. A
trace, observation, or score metadata masker must return a `map[string]any`;
another type omits that metadata. An experiment metadata masker must return a
value that encodes as a JSON object, such as a map, a struct, or an object
`json.RawMessage`; another value fails the item start. Dataset metadata may be
any JSON value. The SDK calls the masker synchronously. It must be fast, non-blocking,
and concurrency-safe. Copy and recursively redact maps and slices rather than
mutating caller-owned data:

```go
cfg := langfuse.ConfigFromEnv()
cfg.Mask = redactSDKValue

func redactSDKValue(field langfuse.MaskField, value any) any {
	switch field {
	case langfuse.MaskObservationInput, langfuse.MaskObservationOutput:
		return "[redacted]"
	case langfuse.MaskErrorMessage:
		return "[redacted error]"
	case langfuse.MaskTraceMetadata, langfuse.MaskObservationMetadata, langfuse.MaskScoreMetadata,
		langfuse.MaskDatasetMetadata, langfuse.MaskDatasetItemMetadata,
		langfuse.MaskExperimentMetadata, langfuse.MaskExperimentItemMetadata:
		return redactMetadata(value)
	default:
		return value
	}
}

func redactMetadata(value any) any {
	switch value := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(value))
		for key, item := range value {
			switch strings.ToLower(key) {
			case "email", "customer_id", "authorization":
				redacted[key] = "[redacted]"
			default:
				redacted[key] = redactMetadata(item)
			}
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for index, item := range value {
			redacted[index] = redactMetadata(item)
		}
		return redacted
	default:
		return value
	}
}
```

The example fully replaces observation input and output and returns other
values unchanged; returning nil would omit them, or fail a dataset write. It
assumes JSON-like
`map[string]any` and `[]any` metadata. A production masker must cover every
concrete value type the application supplies, must be concurrency-safe, and
should have tests proving its redaction policy.

These controls apply **only to data supplied through this client**; they never
rewrite third-party OTel instrumentation, so configure or sanitize those
instrumentors independently in borrowed mode (the client warns when content
capture is disabled there). Resource attributes are also untouched: isolated
mode preserves `resource.Default` (including `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES`) and borrowed mode preserves the caller's resource,
so audit them before export.
