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

Work that outlives its request — a goroutine started from an HTTP handler
with `context.WithoutCancel`, for example — should not attach observations to
the handler's already-ended span. Detach it with the standard OpenTelemetry
helper; no SDK-specific API is involved:

```go
import oteltrace "go.opentelemetry.io/otel/trace"

jobCtx := oteltrace.ContextWithSpanContext(ctx, oteltrace.SpanContext{})
jobCtx, job := lf.StartObservation(jobCtx, "background-job", langfuse.TypeChain,
	langfuse.ObservationAttributes{})
```

Clearing the span context makes the next observation the root of a new trace,
marked as an application root, while trace attributes already set through
`WithTraceAttributes` — user, session, tags, metadata, version — continue to
propagate. Session and user grouping in Langfuse therefore survive the
handoff even though the background work is a separate trace. This contract is
locked by the SDK's tests. The span-context reset is shared OpenTelemetry
state: other tracers using the detached context also start new traces.

## Observation semantics

For generations and embeddings, keep the model, input, output, usage, cost,
and completion-start time on the same observation. `Usage` accepts inclusive
provider totals: input includes cache tokens and output includes reasoning
tokens. The client normalizes them to Langfuse's exclusive `usage_details`
buckets.

`Update` merges non-zero fields and never clears a value. It is safe to call
`Update`, `RecordError`, `End`, and `EndAt` concurrently; only the first `End`
or `EndAt` takes effect. Calls made after an observation has ended are
ignored. Each observation retains at most eight `RecordError` exception
events; further errors are omitted with one payload-free diagnostic.

`EndAt` ends an observation at an explicit time for instrumentation that
records work after the fact; pair it with `StartTime` and
`CompletionStartTime` to reproduce the observed timeline. A zero end time
falls back to the current time with a diagnostic, and an end time before an
explicitly supplied `StartTime` is replaced by that start time with a
diagnostic. When no explicit `StartTime` was given the SDK cannot see the
span's actual start and exports the supplied end time unchanged.

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
- A serialized score event is limited to 128 KiB.
- A prompt response body and a prompt fallback: 1 MiB each. Prompt names: 500
  bytes; labels: 200 characters (local wire-safety bounds, not Langfuse
  validation — the server is stricter for labels).

## Prompt management

`GetPrompt` fetches one prompt version from `GET /api/public/v2/prompts` and
caches it in memory. Selection is by exact `Version` or by `Label` (defaulting
to `production`); the two are mutually exclusive. `PromptQuery.Type` may require
`PromptTypeText` or `PromptTypeChat`. The requirement is checked after every
resolution path, including a cache entry populated by an earlier typeless
call. A mismatch resolves to the compatible local fallback when supplied and
otherwise returns a zero `Prompt` with an error wrapping
`ErrPromptTypeMismatch`.

The returned `Prompt` is an independent deep copy, safe to retain or modify.
Its `Source` is `PromptSourceServer` for a completed foreground fetch,
`PromptSourceCache` for a fresh cache hit, `PromptSourceStale` for a stale hit
returned while revalidation runs, or `PromptSourceFallback` for a local
fallback. `Ref()` yields the `PromptRef` that links a generation to the exact
server-backed prompt version, and returns nil when `Source` is
`PromptSourceFallback`, the name is empty, or the version is non-positive.

Caching semantics, per cache key (name plus version or label):

- A fresh entry (age within `CacheTTL`, default 60 seconds) is returned from
  memory with `PromptSourceCache` and no I/O. Freshness is judged against the
  age of the entry at call time, so callers using different TTLs share one
  entry — a deliberate divergence from the official SDKs, which stamp an
  expiry at insert.
- An expired entry is returned immediately while one background refresh per
  key runs (stale-while-revalidate), stamped `PromptSourceStale`. A failed
  refresh keeps serving the stale value, emits one payload-free diagnostic,
  and suppresses further refreshes of that key for 10 seconds; a refresh
  answered with 404 evicts the entry.
- A cache miss fetches synchronously, bounded by the caller's context and a
  10-second fetch budget covering up to two retries of 408/429/5xx and
  network failures (500 ms then 1 s backoff with jitter, honoring
  `Retry-After` within the budget). Concurrent misses for the same key share
  one fetch; every waiter receives `PromptSourceServer`. A caller whose
  context ends leaves immediately with its context error while the shared
  fetch completes for the rest.
- `DisableCache` forces an independent fetch with no cache read or write and
  returns `PromptSourceServer` on success.
- The cache holds at most 256 entries (least-recently-used eviction), runs at
  most 8 background refreshes and 64 foreground fetches concurrently, and
  drains all of them during `Shutdown`.

`Fallback` supplies a local prompt body returned when a fetch fails without a
cached value or a resolved value has the wrong type, so a local prompt can
guarantee availability. Fallback results have `PromptSourceFallback`, are
never cached, and are never linked.
When `PromptQuery.Type` is set, an incompatible declared or inferred fallback
type is rejected before I/O. A caller's context cancellation is always
returned as an error on blocking fetch paths, never masked by a fallback. 404
failures wrap `ErrPromptNotFound` for `errors.Is`.

`GetPrompt` is safe to call on a nil, disabled, or shut-down client. With a
valid `Fallback` it returns that fallback, so optional client setup requires no
nil guard; without a fallback it returns an error. Invalid queries and nil
contexts remain errors in every client state.

`Compile` substitutes `{{variable}}` occurrences (string values verbatim,
other values JSON-encoded) and fills chat placeholder messages from
`[]PromptMessage` variables — an empty slice removes the placeholder, an
invalid message list leaves it unchanged. Unresolved variables and unfilled
placeholders stay verbatim, matching the Python SDK, and `Compile` never
fails or panics. `CompileStrict` produces the same compiled copy but also
returns an error naming every missing content variable, content value that
could not be stringified, and unfilled chat placeholder. Successful
substitutions remain present in the returned copy when strict compilation
reports an error.

`DecodeConfig` unmarshals `Prompt.Config` into a caller-provided target. An
empty config is a no-op, so callers can initialize defaults before applying
the prompt-owned fields; malformed JSON and invalid targets return a wrapped
decode error. Warm the cache during startup with one `GetPrompt` call per
prompt when guaranteed availability matters.

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

Scores accepted by `RecordScore` wait in their own bounded queue of 256
serialized score events serviced by one background sender posting to the JSON
ingestion endpoint. Transient failures — network errors, HTTP 408, 429, and
5xx responses, per-item ingestion errors with those statuses or without a
status, and 207 responses whose body cannot be read or does not account for
the submitted event — are retried with exponential backoff and jitter
(5-second initial interval, 30-second maximum, one minute in total, honoring
`Retry-After`); other error statuses and an exhausted retry budget drop the
score with a payload-free diagnostic. Each score is serialized once as a
complete ingestion event, so a retried delivery resends the identical event
and stays idempotent through the event ID and the score ID. On a full score
queue new scores are dropped with a diagnostic, and `Config.BlockOnQueueFull`
opts into the same blocking backpressure as for observations.

## Flush and shutdown

Long-running services should end in-flight observations and then call
`Shutdown` during graceful termination. `Shutdown` flushes ended observations
and delivers queued scores; when its context ends first, undelivered scores
are dropped with diagnostics. Short-lived jobs and serverless handlers can
call `Flush` before returning if the client must remain usable; `Flush` also
waits for queued scores, including one mid-retry, bounded by its context.

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
