# Configuration and behavior reference

## Configuration

`ConfigFromEnv` reads:

| Variable | Purpose |
| --- | --- |
| `LANGFUSE_PUBLIC_KEY` | Project public key |
| `LANGFUSE_SECRET_KEY` | Project secret key |
| `LANGFUSE_BASE_URL` | Cloud or self-hosted base URL |
| `LANGFUSE_TRACING_ENVIRONMENT` | Environment stamped on observations |
| `LANGFUSE_RELEASE` | Application release stamped on observations |
| `LANGFUSE_TRACING_ENABLED` | Set to `false` for a complete no-op client |
| `LANGFUSE_CONTENT_CAPTURE_ENABLED` | Set to `false` to drop SDK input/output |

Export buffering is tuned only through `Config`; it has no environment
variables:

| Config field | Purpose |
| --- | --- |
| `MaxQueueSize` | Bounds ended spans buffered for export. `0` selects the default 2048; negative values fail validation in `New` |
| `BlockOnQueueFull` | Opts into blocking backpressure when the queue is full instead of the default drop-on-full |

For an isolated provider, an empty `Config.ServiceName` preserves the standard
OpenTelemetry resource (including `OTEL_SERVICE_NAME`). Set `ServiceName` only
when an explicit SDK-local override is desired. A borrowed provider always
keeps the application's resource.

`New` validates configuration without a network request; a disabled client
needs no credentials and every operation is a safe no-op. Runtime exporter
failures go to the standard OTel error handler, so observation calls never
turn telemetry failures into application failures; `Flush` and `Shutdown`
return lifecycle errors to the caller.

Generic `OTEL_EXPORTER_OTLP_*`, `OTEL_BSP_*`, and `OTEL_SPAN_*` variables are
intentionally ignored by the isolated transport and provider; they often
configure an application's separate telemetry backend. HTTPS uses Go's system
trust configuration and the client follows standard
`HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` behavior. Use HTTPS whenever credentials
leave a trusted host; plain HTTP remains available for local development, but
Basic authentication does not encrypt credentials by itself.

## Trace attribute propagation

The client preserves a valid parent `SpanContext`, including a parent created
by another provider, so ordinary W3C trace IDs continue across services and
backends. Langfuse trace attributes are different: v0.1 does **not** place
user, session, tags, metadata, or version in W3C baggage. Those values and the
internal application-root claim propagate only through the local Go context.
Consequently, a downstream service can correctly continue the trace ID while
still being shown as a separate Langfuse application root.

## Observation semantics

For generations and embeddings, keep the model, input, output, usage, cost,
and completion-start time on the same observation. `Usage` accepts inclusive
provider totals: input includes cache tokens and output includes reasoning
tokens. The client normalizes them to Langfuse's exclusive `usage_details`
buckets.

`Update` merges non-zero fields and never clears a value. It is safe to call
`Update`, `RecordError`, and `End` concurrently; `End` is idempotent. Calls
made after an observation has ended are ignored. Each observation retains at
most eight `RecordError` exception events; further errors are omitted with one
payload-free diagnostic.

## Limits

Applied to SDK-authored observations (not third-party spans from a borrowed
provider):

- Scalar trace values and tags: 200 characters. Environment names: at most 40
  characters of lowercase letters, numbers, `_`, or `-`, not starting with
  `langfuse`.
- Trace and observation metadata: 32 top-level keys each, keys up to 200
  bytes. `Usage.Details`: 64 buckets with the same key limit. Tags: caller
  order, at most 64 unique values and 16 KiB per trace context.
- Each JSON-serialized input, output, metadata value, model-parameter map, or
  cost map: 1 MiB. Direct text (names, model names, versions, prompts, status
  messages): 16 KiB. `RecordError` replaces invalid UTF-8 or text over 64 KiB
  with `"error"`.
- Observation payload attributes: 2 MiB in aggregate; lower-priority fields
  over the budget are omitted with a payload-free diagnostic. One OTLP request
  is capped at 4 MiB, with automatic splitting described under
  [Buffering and backpressure](#buffering-and-backpressure).
- Serialization rejects nesting beyond 100 levels using a bounded structural
  preflight. Caller-provided `Mask`, `MarshalJSON`, and `MarshalText` are
  trusted callbacks: their output is still rejected above 1 MiB, but work
  inside the callback cannot be bounded by the SDK.
- A serialized score is limited to 128 KiB.

## Buffering and backpressure

Ended observations wait in a bounded in-memory queue (2048 spans by default)
and are exported in batches of up to 512 spans, at the latest every 5 seconds.
When the queue is full, for example during a sustained Langfuse outage, newly
ended observations are dropped rather than blocking application work, matching
OpenTelemetry defaults. `Config.MaxQueueSize` resizes the queue and
`Config.BlockOnQueueFull` opts into backpressure; choose blocking only when
delivery matters more than latency, because an export outage can then stall
goroutines that end observations. One OTLP/HTTP request is capped at 4 MiB
before compression; larger batches are split across requests, and only a span
that alone exceeds the cap is dropped with a diagnostic.

## Flush and shutdown

Long-running services should end in-flight observations and then call
`Shutdown` during graceful termination. `Shutdown` flushes ended observations.
Short-lived jobs and serverless handlers can call `Flush` before returning if
the client must remain usable.

In borrowed mode, shut down the Langfuse client before the application's
tracer provider; the client never shuts down unrelated processors or
exporters. Create a fresh timeout context for each `Flush` or `Shutdown` call,
since a reused deadline keeps running. Repeated and concurrent lifecycle calls
are safe: the first `Shutdown` owns teardown and later calls return without
starting another.

## Sampling and current limitations

- Isolated mode always samples its own SDK observations, including children
  of an unsampled foreign parent, while retaining the foreign trace and parent
  IDs. In borrowed mode, spans rejected by the application's sampler cannot be
  recovered by the SDK. Smart filtering is an export selection step, not a
  sampler.
- The default smart filter exports SDK observations, spans with `gen_ai.*`
  attributes, known LLM instrumentation scopes, and required application roots.
  Unrelated HTTP, database, and logging spans are excluded by default.
- Filtering cannot reconstruct an unexported parent. Application-root detection
  follows the official SDKs' start-time, direct-parent expectation; late-added
  AI attributes and filtered middle spans can therefore create an additional
  application root.
- Trace attributes and the internal application-root claim are local-context
  only in v0.1. They are not baggage and do not cross process boundaries.
- Input, output, metadata, and model values must be JSON-serializable and are
  subject to the per-field and aggregate limits above plus the caller's OTel
  span limits. Invalid, cyclic, unsupported, or oversized fields are omitted
  and diagnosed without including their payload.
- Batch export improves application latency but cannot survive an abrupt
  process exit. Graceful shutdown is required.
- Custom filters, export-all mode, multiple projects on one provider, prompt
  fetching, datasets, and administrative APIs are outside v0.1.
