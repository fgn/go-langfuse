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
| `LANGFUSE_SAMPLE_RATE` | Fraction of traces exported in isolated mode, `[0, 1]`; unset keeps everything |
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
backends. Langfuse trace attributes stay local to the Go context unless
cross-process propagation is explicitly enabled with two client verbs:

```go
// Sender: opt in to exporting trace attributes as W3C baggage.
ctx = lf.WithBaggagePropagation(ctx)

// Receiver: apply inbound baggage AFTER authenticating the request.
ctx = lf.WithTraceAttributesFromBaggage(ctx)
```

`WithBaggagePropagation` marks the returned context branch. From then on,
`WithTraceAttributes`, `WithTraceAttributesFromBaggage`, and
`StartObservation` each return a context whose `langfuse_*` baggage members
are rebuilt from the state visible at that point. Propagate the **latest
returned context**: Go contexts are immutable values, so earlier contexts and
aliases keep the members they had when they were created. Injection itself is
the application's job, through whatever W3C propagator it already uses.

The wire protocol is a closed, versioned member set shared with the official
SDKs: `langfuse_user_id`, `langfuse_session_id`, `langfuse_trace_name`,
`langfuse_version`, `langfuse_environment`, `langfuse_metadata_<key>` for
string metadata values, and the `langfuse_trace_id` application-root claim.
Tags and prompt links never propagate (their Python wire forms have no
portable contract), non-string metadata never propagates, and unknown
`langfuse_*` members terminate at the import verb: they are neither applied
nor forwarded. Values must be printable ASCII without spaces or `+` and at
most 200 bytes; metadata key suffixes are limited to letters, digits, and
`. _ ~ -`. Values outside the wire domain stay on the local trace and are
dropped from baggage only, with a diagnostic. These restrictions exist
because the Python and Go baggage codecs disagree outside that domain; inside
it, round trips are validated code point by code point against the pinned
Python SDK (`task interop`).

Receipt is **never** automatic. Inbound baggage is caller-controlled, so
`WithTraceAttributesFromBaggage` must run only after the application has
authenticated the request — this is a deliberate security divergence from the
Python SDK, whose processor reads baggage for every span. Import applies the
allowlisted members into propagated trace state with local values taking
precedence per field, honors the trace claim only when it names the ambient
trace ID, and always strips the entire `langfuse_*` namespace from the
returned branch's baggage: a standard inject after import forwards nothing of
Langfuse's unless `WithBaggagePropagation` re-enables export. Import before
marking; marking first replaces un-imported inbound members from local state
(and says so in a diagnostic).

The `langfuse_trace_id` member is application-root suppression, not trace
continuation — the W3C `traceparent` header continues the trace. A root
started through `StartObservation` on a marked branch replaces the claim; a
root started directly on a borrowed application tracer cannot refresh it, so
start roots through `StartObservation` on propagation-enabled paths when
downstream root classification matters.

W3C limits (64 members, 8192 bytes on the exact serialized header) are
enforced deterministically. Foreign baggage members are preserved ahead of
all Langfuse members; then the claim, environment, session, user, trace
name, version, and metadata keys in lexicographic order. Members are dropped
whole from the tail of that order with a diagnostic, never truncated, so a
crowded header degrades to absent fields rather than corrupt ones. Every
propagation guarantee is conditional on that budget.

`TraceAttributes.Environment` is the request-scoped environment: it overrides
`Config.Environment` for spans on its context path, updates the current
recording span, and is the **only** source of `langfuse_environment` — the
client-wide default is never serialized, so an unset request environment lets
the downstream service's own default apply.

Work that outlives its request, such as a goroutine started from an HTTP
handler with `context.WithoutCancel`, should not attach observations to
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
`WithTraceAttributes` (user, session, tags, metadata, version) continue to
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
  bytes; labels: 200 characters. These are local wire-safety bounds, not
  Langfuse validation; the server is stricter for labels.

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
  entry. This deliberately diverges from the official SDKs, which stamp an
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
`[]PromptMessage` variables. An empty slice removes the placeholder, and an
invalid message list leaves it unchanged. Unresolved variables and unfilled
placeholders stay verbatim, matching the Python SDK, and `Compile` never
fails or panics. `CompileStrict` produces the same compiled copy but also
returns an error naming every missing content variable, content value that
could not be stringified, and unfilled chat placeholder. Successful
substitutions remain present in the returned copy when strict compilation
reports an error.

`DecodeConfig` unmarshals `Prompt.Config` into a caller-provided target. An
empty config is a no-op, so callers can initialize defaults before applying
the prompt-owned fields. Because the config is server-controlled, a decode
failure discloses only the caller-owned target type and a fixed category
(`invalid target` or `incompatible config`); the underlying `encoding/json`
error is intentionally not wrapped, so its text cannot echo config values
into a caller-logged error. Warm the cache during startup with one
`GetPrompt` call per prompt when guaranteed availability matters.

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
ingestion endpoint. Transient failures are retried with exponential backoff
and jitter: network errors, HTTP 408, 429, and 5xx responses, per-item
ingestion errors with those statuses or without a status, and 207 responses
whose body cannot be read or does not account for the submitted event
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

## Sampling

Isolated mode samples whole traces. The default keeps everything;
`Config.SampleRate` (or `LANGFUSE_SAMPLE_RATE`) selects a fraction, and
`Client.WithSampleRate` overrides that fraction for the sampling decision
made by the first SDK observation subsequently started on that context
path. Set it once per request, before the first observation, to give
different kinds of work different rates in one process.

- The decision is a deterministic threshold on the trace ID using the same
  scheme as OpenTelemetry's `TraceIDRatioBased`, so equal fractions always
  agree and smaller fractions select subsets of larger ones. `TraceSampledAt`
  exposes the same predicate for correlated application-level sampling, such
  as running an expensive evaluation on 2% of traces: with an evaluation
  fraction no larger than the export fraction, every evaluated trace is also
  kept for export. Kept is a sampling decision, not delivery: export still
  requires ending observations, a live client, and export success.
- The decision is made once per context path and inherited by every SDK
  observation started from the deciding observation's returned context, even
  across foreign middle spans and later rate changes. Sibling observations
  started independently from a context without a decision re-decide
  deterministically from the trace ID and their own effective rate: equal
  rates always agree; different rates inside one trace make trace membership
  subtree-scoped, with each subtree internally consistent.
- Sampled-out observations keep their trace and span IDs and become cheap
  no-ops: `Update` and `RecordError` return before serialization, `Mask`, or
  `Error()` calls. `Observation.Sampled` reports the sampling decision, not
  a delivery guarantee. Start attributes are necessarily built
  before the decision, so start observations with minimal fields and attach
  expensive payloads in `Update` after checking `Sampled`.
- A score recorded directly on a sampled-out, SDK-originated context path
  targeting that same trace is suppressed (returning nil) so sampled-out
  traces do not accumulate orphaned scores; the first suppression per client
  emits one diagnostic. This is the path's drop decision applied to scores,
  not proof the trace is absent remotely. All other scores are delivered:
  session-only, other traces, out-of-context, traces with remote or foreign
  parents, any path a foreign span has joined, and all borrowed-mode
  scores.
- Detached contexts start a new trace root and re-decide with the surviving
  requested rate. `SampleRate: 0` exports no traces while scores and prompts
  keep working; `Disabled: true` remains the complete no-op.

## Current limitations

- At the default rate, isolated mode samples its own SDK observations,
  including children of an unsampled foreign parent, while retaining the
  foreign trace and parent IDs. In borrowed mode, `SampleRate` and
  `WithSampleRate` are ignored with a diagnostic: the application's sampler
  remains authoritative, and spans it rejects cannot be recovered by the SDK.
  Smart filtering is an export selection step, not a sampler.
- The default smart filter exports SDK observations, spans with `gen_ai.*`
  attributes, known LLM instrumentation scopes, and required application roots.
  Unrelated HTTP, database, and logging spans are excluded by default.
- Filtering cannot reconstruct an unexported parent. Application-root detection
  follows the official SDKs' start-time, direct-parent expectation; late-added
  AI attributes and filtered middle spans can therefore create an additional
  application root.
- Cross-process attribute propagation is opt-in and restricted to the
  documented wire domain; tags, prompt links, and non-string metadata never
  cross process boundaries, and a direct borrowed-tracer root cannot refresh
  the propagated application-root claim.
- Input, output, metadata, and model values must be JSON-serializable and are
  subject to the per-field and aggregate limits above plus the caller's OTel
  span limits. Invalid, cyclic, unsupported, or oversized fields are omitted
  and diagnosed without including their payload.
- Batch export improves application latency but cannot survive an abrupt
  process exit. Graceful shutdown is required.
- Custom filters, export-all mode, multiple projects on one provider,
  datasets, and administrative APIs remain out of scope. Tail sampling
  (keep-all-errors) is not provided: the outcome is unknown when the trace
  root starts; use `WithSampleRate(ctx, 1)` for requests known to matter up
  front.
