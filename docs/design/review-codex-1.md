# Adversarial review: datasets and experiments

Reviewed 2026-10-01. Recommendation: revise the design before implementation.
This is a source review, not a live compatibility test. No code was changed.

Evidence roots used below:

- `GO`: `/home/fgustafsson/projects/go-langfuse`, HEAD `8a1c76e2262aff2f241ee75e6d1d8ee3955bca2c`.
- `LF`: `/home/fgustafsson/projects/langfuse-ref/langfuse`, supplied v4.48.0 source, HEAD `0922c4cbaa40249f51c7fec7ba59a1e6c7736654`.
- `PY`: `/home/fgustafsson/projects/langfuse-ref/langfuse-python`, v4.16.0, HEAD `0bc5897b0e2402e50baf8aa004a1adbffd333a61`.
- `JS`: `/home/fgustafsson/projects/langfuse-ref/langfuse-js`, HEAD `d09651d8b5ec831045d8205a5531d271dd319053`, `packages/client/package.json:2-3` reports `@langfuse/client` 5.11.1.
- `Proposal`: `GO/docs/design/datasets-and-experiments.md`.

Each source reference gives a path relative to its explicitly named root and
inclusive line numbers. Findings distinguish contradictions in the proposed
contract from implementation decisions that the proposal has not specified.

## Findings, ranked by severity

### 1. Blocker: experiment metadata has no defined masking path

**Claim.** The new experiment and item metadata can send sensitive content
through attributes on every exported descendant. Neither metadata field has a
specified `MaskField` or a specified call to `Config.Mask`. The statement that
Mask is the one choke point for content leaving the process is also broader
than this library's actual privacy boundary.

**Evidence.** Proposal:166-171 names four dataset mask fields;
Proposal:236-255 adds only an expected-output mask for experiments and specifies
direct flattening of both experiment metadata maps. `GO/types.go:93-101` and
`GO/docs/privacy.md:16-26,56-59` promise one call on the complete metadata map,
before serialization, and expressly exclude identifiers, status text, scores'
comments, resources, and third-party spans. `GO/context.go:395-400` and
`GO/internal/processor/processor.go:119-126,255-301` propagate already-normalized
attributes without another masking step. `LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3504-3522`
stores both metadata namespaces separately.

**Failure scenario.** A caller masks observation metadata but supplies
`ExperimentItem.Metadata["patient_email"]`. That value is flattened and copied
to the task, generation, and borrowed instrumentor spans without the masker
ever seeing it. Masking the observation's own metadata does not mask these
separate attributes.

**Recommendation.** Specify mask fields for both experiment metadata maps.
Apply Mask once to each complete typed map, validate and flatten the result
once, and store an immutable client-scoped projection. Define nil, changed
type, panic, and oversized-result behavior. Do not mask again per child span.
Add the new sources to the privacy table, including unmasked experiment names,
descriptions, dataset names, schemas, and identifiers. Replace the universal
choke-point claim with the actual field-level boundary.

### 2. Blocker: the independent experiment budgets can make a valid item unexportable

**Claim.** The proposal's per-map budgets do not fit the existing count and
aggregate byte budgets. Always sampling an item does not stop its canonical
span from being dropped because it is too large, or its linkage attributes
from being lost under span limits.

**Evidence.** Proposal:174-175,253-255 allows 32 experiment metadata keys and
32 item metadata keys, each experiment set up to 1 MiB. It does not allocate a
shared span budget. `GO/client.go:286-297` fixes the owned provider at 128
attributes. `GO/internal/attributes/attributes.go:65-79` sets 32 ordinary
metadata entries, 64 usage detail entries, and approximately 2 MiB of SDK
observation attributes. `GO/observation.go:79-82,345-358` accounts for the
attributes built by the observation, while processor propagation is appended
later (`GO/internal/processor/processor.go:126,255-301`).
`GO/internal/transport/exporter.go:27-31,228-242` caps OTLP at 4 MiB and explicitly
drops a single span that exceeds the cap; splitting cannot rescue that span.

**Failure scenario.** A task has about 1.8 MiB of accepted input/output, 1 MiB
of experiment metadata, 1 MiB of item metadata, and a large expected output.
Every field passes the proposal's individual validation, but the root alone
exceeds 4 MiB. Its small child generation exports and points to a missing root.
Separately, four 32-entry metadata namespaces already consume 128 attributes
before identity, environment, observation type, model, or usage fields.

**Recommendation.** Define one total SDK span budget that includes propagated
trace and experiment attributes, expected output, and later updates. Reserve
count and bytes for the required experiment linkage tuple before optional
content. Update accounting as well as owned limits; merely increasing 128 does
not fix the byte problem. State the weaker guarantee on borrowed providers,
whose count and value-length limits the client cannot change. Test combined
maximum fields and late updates through the real protobuf exporter.

### 3. Major: canonical-root export is not guaranteed by the sampling bypass

**Claim.** The design lists an unexported canonical root as a failure it must
prevent, but leaves several current ways to produce it intact. A once-per-client
sampling diagnostic covers only one of those ways.

**Evidence.** Proposal:244-248,263-264. `GO/internal/processor/processor.go:129,189-199`
applies `ShouldExportSpan` at start and again at end. `GO/docs/reference.md:396-406`
defines it as a full override and says filtering cannot reconstruct an
unexported parent. `GO/client.go:230-239` uses a bounded, normally nonblocking
batch queue; `GO/docs/reference.md:23-24` states that full queues drop spans.
`GO/observation.go:611-621` explicitly says `Sampled()` is not a delivery
guarantee. `LF/web/src/features/experiments/server/public/repository.ts:309-310,416-424`
counts and lists only events where the event's span ID equals the experiment
root pointer. The UI's qualification query also requires root rows
(`LF/packages/shared/src/server/repositories/experiments.ts:1191-1199`).

**Failure scenario.** A borrowed client uses a filter that keeps only GenAI
spans. The helper creates a sampled ordinary task root, which the filter drops;
its generation exports. The experiment summary can exist, but its public item
count is zero and `/experiment-items` returns no item. Another case is a task
that ends after `Shutdown`, or a root dropped by queue pressure.

**Recommendation.** Specify the export-selection policy for experiment roots.
Either reserve their admission through the Langfuse filter, or explicitly
require a compatible filter and report rejection, without promising prevention.
Do not silently bypass an application privacy filter. Document that sampling
cannot ensure complete delivery and require callers to end all item work
before flushing/shutting down and reconcile expected item counts after
ingestion. Test root-only rejection at start and at end, queue pressure, and
borrowed `Drop` versus `RecordOnly`. Never choose an exported child as a new
canonical root: that would change the measured task.

### 4. Major: preserving conflicting caller attributes violates the experiment identity contract

**Claim.** “Caller attributes are never overwritten” conflicts with the
promise that every item span carries the same experiment/item/root identity.
Missing-only propagation cannot enforce that invariant.

**Evidence.** Proposal:231-235. `GO/internal/processor/processor.go:287-301`
removes existing keys from the set to be propagated, regardless of their
values. `LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3486-3502,3524-3542`
extracts each span's supplied fields; it does not resolve identity from a
parent. Experiment aggregates group by experiment ID and choose names/dataset
IDs with `any` (`LF/packages/shared/src/server/queries/clickhouse-sql/event-query-builder.ts:1961-1977`).
The official [v4 experiment FAQ](https://langfuse.com/faq/all/experiment-runs-not-visible-v4)
also requires uniform run identity across items and uniform item/root identity
within an item trace.

**Failure scenario.** Instrumentation starts a child with a stale experiment
ID or stale `root_observation_id`. The current hook preserves it. Costs and
errors can attach to a different run; a span whose stale pointer equals its
own ID can become an extra canonical item row.

**Recommendation.** Define precedence for the reserved experiment identity
keys separately from ordinary trace fields. Choose authoritative client
identity or explicit conflict rejection with a static diagnostic; do not claim
uniform identity while preserving contradictory values. Include late
`SetAttributes` mutations in the stated limitations and adversarial tests.
Scope experiment propagation to the new trace ID, so a caller detaching the
returned context for unrelated work does not stamp the old item's root pointer
on a new trace. Clear/replace all old experiment fields for nested starts,
including failed nested starts.

### 5. Major: expected-output encoding is not the stated official SDK contract

**Claim.** Encoding every expected output as JSON changes plain strings and
does not match the supplied SDKs. The server preserves this field as a string;
the public experiment-items service does not automatically undo the extra
JSON quoting.

**Evidence.** Proposal:50,238-239. `PY/langfuse/_client/attributes.py:161-165`
returns strings unchanged and omits `None`; `PY/langfuse/_client/client.py:2995-3014`
uses that serializer for expected output. `JS/packages/core/src/utils.ts:127-131`
also preserves strings, and `JS/packages/client/src/experiment/ExperimentManager.ts:429-435`
uses it. `LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3540-3542`
uses `String(value)`. `LF/web/src/features/experiments/server/public/service.ts:114-119`
returns `row.experiment_item_expected_output` directly. The UI IO query returns
the raw expected-output column (`LF/packages/shared/src/server/repositories/experiments.ts:1449-1458`).
The existing Go content encoder already preserves strings
(`GO/internal/attributes/attributes.go:82-84,103-117`).

**Failure scenario.** `ExpectedOutput: "Paris"` becomes the wire string
`"\"Paris\""` rather than `"Paris"`. Public readback contains the quote
characters. A live test which decodes expected output a second time can hide
the difference from the real API/UI contract.

**Recommendation.** Specify and test the representation per type: absent/nil,
empty string, plain string, JSON-looking string, structured value, and
`json.RawMessage`. Match the existing string-preserving encoder unless there is
an explicit, documented reason to depart. Verify raw public API readback as
well as UI rendering. Do not label JSON-of-everything as SDK parity.

### 6. Major: an item ID prevents duplicate identity, not duplicate version writes

**Claim.** Retrying an item POST with an ID is not fully idempotent on the
versioned server. A retry creates another version even when the payload is
identical, and can overwrite another writer's intervening change.

**Evidence.** Proposal:108,148-155 calls these writes idempotent upserts.
`LF/packages/shared/src/server/repositories/dataset-items.ts:430-480` rereads the
current version, advances `validFrom` by at least 1 ms, invalidates the old row,
and creates a new row on every versioned upsert. There is no same-payload
short-circuit in this path. `LF/web/src/features/datasets/server/publicDatasetService.ts:626-638`
passes the upsert through and `640-650` also emits an audit record. The server's
write strategy is selected independently by configuration
(`LF/packages/shared/src/server/datasets/executeWithDatasetServiceStrategy.ts:22-31`).
Dataset upserts really are keyed by project/name
(`LF/web/src/features/datasets/server/actions/createDataset.ts:168-180`), but
that does not prove all their observable side effects are exactly-once either.

**Failure scenario.** The server commits version A but the connection closes
before a response. A second writer commits B. The SDK retries A, creating
version C with A's values and making B noncurrent. Even without B, the retry
creates two version timestamps and two audit operations for one logical write.

**Recommendation.** State the actual guarantee: stable ID prevents another
item ID, but writes are at-least-once and last-writer-wins. Decide whether to
disable retries after ambiguous item-write outcomes or require callers to
accept version duplication and overwrite risk. Do not claim exact idempotency
without a server-supported operation token/conditional write. Serialize and
mask one immutable body before the first attempt and replay those exact bytes;
never call a mutable masker or marshaler again during retries. Include
commit-then-disconnect and concurrent-writer cases in the tests.

### 7. Major: Mask omission on an upsert can retain old sensitive values

**Claim.** Reusing observation-style “omit on nil/panic” masking semantics for
dataset upserts is unsafe without explaining server merge semantics. Omitting
a write field does not remove its existing server value, and JSON null is
converted to omission by the public item service too.

**Evidence.** `GO/types.go:93-101` and
`GO/internal/attributes/attributes.go:85-104,173-180,751-762` omit masked nil,
panic, or unsupported metadata types. Proposal:168-171 does not specify a
different write policy. `LF/web/src/features/datasets/server/publicDatasetService.ts:630-637`
uses `input.input ?? undefined`, likewise expected output and metadata.
`LF/packages/shared/src/server/repositories/dataset-items.ts:331-349` merges
undefined fields with the existing item. Dataset metadata is also converted
with `?? undefined` (`LF/web/src/features/datasets/server/publicDatasetService.ts:272-275`).

**Failure scenario.** An existing item has sensitive expected output. A
redaction job calls `UpsertDatasetItem` and its masker returns nil for that
field. The SDK omits it, the server retains the sensitive value, and the write
can report success. Sending JSON null to this route does not fix this case.

**Recommendation.** For deliberate dataset writes, distinguish “not supplied”
from “supplied and masking failed/removed it.” Reject the latter before I/O or
require an explicit non-null safe replacement supported by the server. Define
the behavior for each new mask field, including schemas and descriptions
outside Mask. Document that upsert is not deletion/redaction of old content or
history. Test against an already populated item, not only a newly created one.

### 8. Major: dataset pagination has no snapshot or completeness contract

**Claim.** Offset pagination of the current dataset can omit or repeat items
while it changes. The previous-page repetition check and hard cap limit loops,
but do not establish that the experiment received the complete cohort.

**Evidence.** Proposal:127-132,160-163 makes version pinning optional.
`LF/web/src/features/datasets/server/publicDatasetService.ts:525-538` fetches
page and count in separate operations. `LF/packages/shared/src/server/repositories/dataset-items.ts:1303-1337`
uses LIMIT/OFFSET, sorts by mutable version timestamp, and selects current
rows unless a version is supplied. It applies temporal bounds when supplied.
The public response strips `validFrom`
(`LF/web/src/features/public-api/types/datasets.ts:110-115`); the proposed item
does not retain the query timestamp either.

**Failure scenario.** After page 1, an update moves an item from page 2 to
the front. Page 2 repeats an item already yielded and skips the updated one.
No whole page repeats, so iteration succeeds with a biased cohort. An A/B/A
page cycle is also not detected by comparing only the previous page.

**Recommendation.** Define current iteration as explicitly best-effort, and
require one pinned as-of timestamp for experiment cohort iteration. Preserve
that timestamp into experiment conversion; do not invent an item version from
the end of pagination. Document temporal selection as `validFrom <= asOf <
validTo`, not an exact-version equality lookup. Confirm read/write versioned
mode in live tests, because stateful reads ignore the version parameter
(`LF/packages/shared/src/server/repositories/dataset-items.ts:1674-1675`).
Validate page metadata and identities; yield one terminal error on repetition,
cap, inconsistent page numbers, or premature truncation, not normal EOF. Test
cross-page duplicate IDs, A/B/A cycles, changing totals, and concurrent edits.

### 9. Major: “like GetPrompt” does not specify dataset lifecycle admission or an overall time bound

**Claim.** Checking `c.stopped` before a blocking call is insufficient. The
proposal does not define cancellation/draining of admitted dataset I/O, lazy
iterator invocation after shutdown, or who owns any iterator workers. It also
does not define a finite operation budget for a context without a deadline.

**Evidence.** Proposal:144-146,164-165,295-296. `GO/client.go:435-455` stops
prompt admission and cancels its I/O before OTel teardown, then drains it.
`GO/prompt_cache.go:74-104,119-125,309-332,451-481` has an admission gate,
per-fetch budget, lifecycle cancellation, and bounded draining; these are not
properties of the raw HTTP transport. `GO/internal/transport/prompts.go:124-138`
declines a long Retry-After only if there is a context deadline; HTTP request
timeouts do not bound time spent sleeping between requests. Its parser allows
up to a day (`GO/internal/transport/scores.go:448-472`).

**Failure scenario.** A dataset request passes a stopped check, races shutdown,
and starts its POST after teardown began. Or a saved iterator is invoked after
shutdown and fetches through a captured transport pointer. With
`context.Background()`, a 429 with a huge Retry-After leaves a synchronous call
waiting for hours if only the raw prompt retry loop is copied.

**Recommendation.** Specify a dataset admission gate and operation context
bounded by caller cancellation, client lifecycle, and a fixed total retry
budget. Stop admission/cancel I/O before OTel teardown, without holding a
lifecycle lock across Mask, serialization callbacks, HTTP, diagnostics, or
iterator yield. Make iteration synchronous and lazy with no prefetch goroutine;
register only active page I/O, release its admission before yielding, and
recheck client/caller state before another page. Do not wait for user loop code
during `Shutdown`. Cover reentrant shutdown from Mask and loop bodies,
already-canceled contexts, cancellation during response reads/backoff, delayed
iterator invocation, and early break with request/worker counts.

### 10. Major: the response limits reject writes and pages that the proposed input limits allow

**Claim.** A 1 MiB item response limit and fixed 8 MiB pages of 50 items are
incompatible with three independent 1 MiB item fields. A successful write can
be reported as a decoding failure, and a dataset of valid items can be
impossible to iterate.

**Evidence.** Proposal:157,160,173-175. Item responses include input, expected
output, metadata, identifiers, dates, and media references
(`LF/web/src/features/public-api/types/datasets.ts:84-98`), while the POST route
accepts a 4.5 MB request body
(`LF/web/src/pages/api/public/dataset-items/index.ts:14-18`). The service returns
the full stored item after the write
(`LF/web/src/features/datasets/server/publicDatasetService.ts:658-665`).

**Failure scenario.** A new item with 600 KiB input and 600 KiB expected
output passes field validation and is committed. Its response exceeds 1 MiB.
The SDK returns an error without its generated ID, and a caller may create it
again. A page of 50 items with 200 KiB input each exceeds 8 MiB despite every
individual field being far below its limit.

**Recommendation.** Align total request/item-response limits, including JSON
escaping and envelope overhead. Specify a dataset-response limit too. Select a
safe page size or a bounded adaptive policy compatible with the supported item
size. Never automatically retry a POST because its successful response body
was malformed, truncated, or oversized. Preserve a programmatic “outcome
unknown” distinction for such errors, including writes with caller IDs, and
test server-committed responses that fail decoding.

### 11. Major: invalid experiment input silently produces an ordinary successful-looking trace

**Claim.** The helper's two-return-value API cannot report that the explicit
experiment operation was rejected. Falling back to ordinary telemetry keeps
application execution running but makes missing experiment items hard to
detect and can inherit an old experiment projection on a nested call.

**Evidence.** Proposal:209-215,249-252 starts an observation without
experiment attributes on invalid input. `GO/observation.go:61-75` uses no-op
handles for unavailable clients and a fallback name for ordinary observation
input, but `GO/score.go:98-117` reports invalid explicit scores synchronously.
`GO/prompt.go:173-202` validates deliberate REST operations even on unavailable
clients. The server lists only explicitly linked canonical rows
(`LF/web/src/features/experiments/server/public/repository.ts:416-418`).

**Failure scenario.** One cohort row has an oversized experiment metadata
field. The returned observation has IDs and can even report sampled=true. The
caller evaluates it and records scores, then sees a run missing one item with
no machine-readable reason. A malformed managed item with no ID is instead
silently assigned a local hash ID, despite a dataset ID being supplied.

**Recommendation.** Reject invalid experiment linkage atomically. Prefer an
error return for this explicit start operation, with a safe no-op observation;
the application can choose to continue the task. If the two-value signature
is retained, use a no-op rather than a misleading ordinary observation and
document how callers detect rejection. Validate managed dataset/item pairs,
status, name, IDs, version range/format, and required linkage separately from
optional content that can safely be omitted. Ensure no old experiment state
survives an invalid nested start. Decide nil-context and zero-client behavior
before freezing the API.

### 12. Major: automatic input-derived identity is neither SDK-canonical nor a privacy-neutral default

**Claim.** “Canonical JSON ... as the official SDK does” is false for the
supplied sources. Input-derived IDs also collapse duplicate-input cohort rows
and expose a stable fingerprint even when content capture is disabled or Mask
redacts input.

**Evidence.** Proposal:192-194,236-239. Python hashes `_serialize(input_data)`
(`PY/langfuse/_client/client.py:2992-2994`), which preserves strings and otherwise
uses ordinary `json.dumps` (`PY/langfuse/_client/attributes.py:161-165`). JS
hashes its string-preserving `serializeValue`, otherwise `JSON.stringify`
(`JS/packages/core/src/utils.ts:104-131`). Neither specifies a shared canonical
JSON standard. UI item selection groups by experiment item ID and currently
chooses one repetition per item/run pair
(`LF/packages/shared/src/server/repositories/experiments.ts:1191-1199,1338-1340,1359-1368`).
Identifiers are outside Go Mask (`GO/docs/privacy.md:21,36-37`).

**Failure scenario.** The string input `Paris` hashes differently under Go
JSON string quoting than under either SDK's raw-string hashing. Two local
rows share an input but differ in expected output or subgroup metadata: their
auto IDs collide, so the comparison UI chooses one result. A low-entropy
private classification value can be guessed from its deterministic truncated
SHA-256 ID despite disabled capture.

**Recommendation.** Prefer explicit local item IDs for stable cohorts and
require actual item IDs for managed datasets. If a hash fallback remains,
define the precise Go algorithm, bytes, nil/RawMessage/numeric behavior, and
duplicate-input semantics; do not claim cross-SDK parity. Explain the
fingerprint disclosure and whether hashing happens before or after masking.
Require explicit opaque IDs where that disclosure is unacceptable. Do not
serialize or hash content at all for unavailable clients. Golden fixtures must
include strings, object order, escaping, and duplicate inputs.

### 13. Major: the dataset-to-experiment conversion cannot preserve the advertised data without a policy or error

**Claim.** `DatasetItem.ExperimentItem(version)` hides decoding and version
assignment behind an infallible method even though dataset metadata is arbitrary
JSON and experiment metadata is a `map[string]any`. The method also accepts
any timestamp, independent of the fetch that supplied the data.

**Evidence.** Proposal:117-124,191-204. Server item metadata is `z.any()`
(`LF/web/src/features/public-api/types/datasets.ts:89-91,210-218`), not necessarily
an object. The public API removes `validFrom` from returned items
(`LF/web/src/features/public-api/types/datasets.ts:110-115`). Server parsing
requires a date-string version and converts it through a JS Date
(`LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3590-3603`).
Go's existing bounded serializer contains panics and rejects cycles/depth
before serialization (`GO/internal/attributes/attributes.go:451-468,488-489`).

**Failure scenario.** Dataset metadata is an array or scalar. The conversion
must silently discard it, invent a wrapper key, or fail without a way to return
an error. A normal decode of a nested object into `any` can also turn a large
integer into `float64` and lose its value. A caller passes “now” after reading
old data, recording an experiment version that does not describe that input.

**Recommendation.** Remove the convenience method until its conversion policy
is specified, or make decoding explicit/fallible. Preserve raw JSON input and
expected output without double encoding; preserve numeric tokens with
`UseNumber` where decoding is needed. Define non-object/null metadata handling
and copy ownership. Carry the actual as-of query timestamp through the fetch
or require callers to supply it with explicit responsibility. Specify UTC
RFC3339 formatting, millisecond server precision, valid year range, and zero
version omission. A version read test must update an item after a pinned read
and verify the old cohort and emitted timestamp, not just check that a query
parameter exists.

### 14. Major: environment precedence departs from the official runners and can disagree with scores

**Claim.** An inherited `TraceAttributes.Environment` wins in the proposal,
whereas both supplied SDKs force `sdk-experiment` in their experiment
propagation. Meanwhile Go score payloads always use client configuration,
regardless of the returned experiment context.

**Evidence.** Proposal:240-243. `PY/langfuse/_client/propagation.py:534-540`
forces `sdk-experiment` when the root experiment pointer is present;
`JS/packages/core/src/propagation.ts:731-738` does the same. Root spans are also
stamped explicitly (`PY/langfuse/_client/client.py:2995-3006`;
`JS/packages/client/src/experiment/ExperimentManager.ts:418-438`).
`GO/score.go:98-118,240-244` builds score environment from `c.environment`, not
the context; Proposal:228-229 tells callers to use that scoring method.

**Failure scenario.** A task runs in an ambient production context and is
classified as production instead of `sdk-experiment`, contaminating environment
views. Without the override, the task is `sdk-experiment` but its score is
production because the client is configured for production. ID-based score
linking can still work; the inconsistency affects environment classification,
not necessarily attachment.

**Recommendation.** Default experiment identity to authoritative
`sdk-experiment`, as the official implementations do, or explicitly document
and justify this departure. Do not infer an intentional experiment override
from an inherited request environment. State which environment scores use and
either provide a consistent supported scoring path or document the difference.
Test root, SDK child, foreign child, and the actual score HTTP body together.

### 15. Major: flattening lacks collision, depth, key, and snapshot rules

**Claim.** A leaf count and byte cap are not sufficient to make arbitrary Go
metadata safe. The proposal names cycles as a failure but leaves traversal,
collision resolution, validation after Mask, and ownership unspecified.
“Exactly like the official SDK” also conceals differences between Python's
dotted leaves and JS's JSON object metadata.

**Evidence.** Proposal:51-52,253-255,271-272. Python recursively flattens only
dicts and omits `None` leaves
(`PY/langfuse/_client/attributes.py:168-191`). JS sends serialized experiment
metadata under the base namespace
(`JS/packages/client/src/experiment/ExperimentManager.ts:443-449`). The server
accepts the base object plus dotted fields, with dotted fields taking precedence
(`LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3427-3461`), then
flattens the result (`3504-3522`). Go normal serialization already caps depth at
100 and checks active references (`GO/internal/attributes/attributes.go:22-30,488-489,625-637`).
The proposal's 200-byte key limit is for REST top-level metadata, not an
explicit experiment flattened-path budget.

**Failure scenario.** `{"a.b":1,"a":{"b":2}}` produces two identical
attribute paths. Go map iteration can decide the winner nondeterministically.
A long chain of empty maps has no leaf budget to stop recursion. A caller
mutates metadata after start while child processors flatten it again, causing
a race or changing metadata within one item trace.

**Recommendation.** Normalize synchronously once, after whole-map Mask, and
retain only immutable strings in context. Bound traversal depth, visited nodes,
aggregate key/value bytes, flattened path length, and cycles before large
allocations. Define supported concrete map types, arrays, empty maps, null
leaves, UTF-8/control characters, and deterministic ordering. Reject ambiguous
dotted-path collisions rather than choosing a map-order winner. Check the
post-mask projection too. Add recursive/cyclic adversary tests explicitly:
ordinary JSON fuzz inputs cannot construct a self-referential Go map.

### 16. Major: the test plan can validate a happy path while missing the core contract failures

**Claim.** The listed tests do not prove UI latency/cost behavior, completeness,
privacy on all paths, or correct versioned-server mode. Updating an API golden
does not validate the semantics that are being frozen.

**Evidence.** Proposal:276-304 covers two successful items, one child, and
positive readback. `GO/api_golden_test.go:15-24,46-59` records declarations and
signatures only. `LF/web/src/pages/api/public/experiments/index.ts:19-24` and
`LF/web/src/pages/api/public/experiment-items/index.ts:19-24` gate access on v4
write-mode configuration; versioned dataset mode has separate switches
(`LF/packages/shared/src/server/datasets/executeWithDatasetServiceStrategy.ts:22-40`).
Public optional IO and metadata require explicit fields
(`LF/web/src/features/experiments/server/public/service.ts:212-217`). Public
item listing requires canonical roots, whereas UI cost and latency use separate
queries (`LF/packages/shared/src/server/repositories/experiments.ts:1332-1368`).
Existing live tests fail when credentials/tracing are unavailable rather than
silently passing (`GO/live_test.go:28-40`).

**Failure scenario.** A public readback test passes for two tiny spans, but
normal metadata sizes drop a root, a foreign span changes identity, scores use
the wrong environment, string expected output is overquoted, and concurrent
pagination skips a cohort item. None is exercised by the proposed happy path.

**Recommendation.** Add independent server-derived wire fixtures, not only
expected values copied from the design. Make the integration gate assert v4
API availability and actual historical dataset behavior. Request all required
readback fields and assert exact item count, no extra roots, run/dataset/item
identity, raw expected output, version, score targets/environment, and child
cost. Exercise both isolated and borrowed providers, two clients sharing a
context, same-client nested items, a foreign middle span, detached contexts,
root rejection, `RecordOnly`, low attribute limits, combined size limits,
Mask panic/nil/changed types, and callback reentrancy. Compare a short task plus
a slow evaluator against the UI latency query and cost aggregation, including
evaluators started after the task ends. Make non-idempotent commit/response-loss
and pagination mutation tests part of the required gate. Prove early-break
behavior with no future requests or leaked workers. Do not let polling helpers
double-decode strings or fall back to legacy run APIs and conceal a v4 failure.

### 17. Major: automatic third-party propagation works only on the provider that owns the processor

**Claim.** The context hook cannot stamp spans created on another provider.
The unqualified claim that OpenAI/GenAI instrumentor spans inside an item are
linked is false in the default isolated mode when those instrumentors use the
application's global provider. Passing the returned context carries parentage,
but does not cause a different provider to run this client's processor.

**Evidence.** Proposal:231-234. `GO/client.go:267-275,278-282` installs the
processor on its private owned provider or registers it on the supplied
borrowed provider. `GO/types.go:51-53` promises never to replace the global
provider. `GO/internal/processor/processor.go:111-126` only stamps spans for
which that provider invokes OnStart. Default export filtering also deliberately
excludes unrelated scopes (`GO/internal/processor/processor.go:369-385`). The
server's cost sum sees only events carrying the correct experiment/item IDs
(`LF/packages/shared/src/server/queries/clickhouse-sql/query-fragments.ts:415-431`;
`LF/packages/shared/src/server/repositories/experiments.ts:1341-1355`).

**Failure scenario.** An application uses the default isolated Langfuse
client and globally configured OpenAI instrumentation. The task root reaches
Langfuse, while the generation is processed/exported only by the global
provider. Its tokens/costs do not appear in the experiment. If another pipeline
also sends that generation to Langfuse, its experiment attributes are still
missing. The proposed third-party wire test uses a borrowed provider and would
not catch this default-mode failure.

**Recommendation.** State that automatic propagation/export requires spans to
be created on the same borrowed provider that has this client's processor.
For isolated mode, require SDK-authored child generations or a supported
instrumentor/provider configuration; do not imply context alone exports or
stamps foreign-provider spans. Include same-provider and separate-provider
instrumentation examples and tests. Treat spans filtered from this provider as
excluded observations, rather than promising that every started span reaches
Langfuse. Do not silently attach the experiment processor to the global
provider to repair this: that changes the library's isolation boundary.

### 18. Minor: REST error and validation contracts need endpoint-specific decisions

**Claim.** The transport rules are a useful intent, but “matching sentinel”
and generic identifier validation leave observable behavior unspecified. A
404 from an item upsert/list operation can mean its dataset is missing, rather
than that a particular item is absent. Static errors need to preserve useful
machine-readable cancellation and ambiguous-outcome information.

**Evidence.** Proposal:147,155,158-159,172-176. Item listing resolves the dataset
first (`LF/web/src/features/datasets/server/publicDatasetService.ts:513-515`),
and upsert resolves it first too
(`LF/packages/shared/src/server/repositories/dataset-items.ts:299-309`). The
current server reports cross-dataset item-ID conflicts as 409
(`321-324`), whereas older Prisma errors in the public service can map to 404
(`LF/web/src/features/datasets/server/publicDatasetService.ts:668-679`). Public
item IDs allow 255 characters
(`LF/web/src/features/public-api/types/datasets.ts:204-207`), while folder names
are trimmed and slash-validated
(`LF/packages/shared/src/features/folders/validation.ts:7-13`). Go prompts use
explicit escaped string concatenation
(`GO/internal/transport/prompts.go:108-114`), reject redirects (`90-97`), limit
error-body draining (`169-170`), reject invalid UTF-8/trailing JSON (`224-235`),
and semantically match responses (`252-253`). Prompt cancellation is wrapped
for `errors.Is` (`GO/prompt_cache.go:269-290`).

**Failure scenario.** `DatasetItems` for a missing dataset yields
`ErrDatasetItemNotFound`; a caller tries to recreate an item instead of fixing
the dataset. A timeout becomes an opaque static string and callers cannot use
`errors.Is(err, context.DeadlineExceeded)`. A future URL helper cleans or
double-escapes `team/dataset`, percent signs, or dot segments, so httptest's
decoded `URL.Path` appears plausible while the real server receives a different
dataset name.

**Recommendation.** Define sentinel mapping per operation and do not parse
server error bodies to classify failures. Preserve standard cancellation
sentinels and specify an ambiguous-outcome discriminator before publishing the
API. Keep static summaries free of names, IDs, URL/query strings, response
bodies, raw transport/marshal errors, and panic values. Specify zero/nil-field
omission versus replacement for each spec, valid status values, and trace/span
ID formats rather than a single generic 200-byte rule. Label local limits as
local. Specify bounded draining for all non-success responses, JSON content
type on POST, matching response identities, and normalized base endpoints.
Assert `RequestURI`/`EscapedPath` for slash, percent, `?`, `#`, Unicode, `.`,
and `..`; never use path cleaning for an opaque name. Test an actual redirect
target receives no request/credentials. Document that listing returns ACTIVE
items only (`LF/web/src/features/datasets/server/publicDatasetService.ts:518-523`).

### 19. Minor: delete is not a purge, and the proposed “thin” exported surface is understated

**Claim.** The v4 delete statement and cleanup plan imply a stronger removal
than the inspected server implements. The proposal also claims a new context
verb that it never declares, and does not justify each permanent helper.

**Evidence.** Proposal:28-30,67-68,202-204,303-304. Versioned deletion creates a
tombstone and preserves the old item and its media links
(`LF/packages/shared/src/server/repositories/dataset-items.ts:541-574`); public
experiments read event rows independently of current dataset-item existence
(`LF/web/src/features/experiments/server/public/repository.ts:401-424`).
`GO/CONTRIBUTING.md:27-31` requires justification of exported concepts.
The declarations at Proposal:79-140,181-215 add seven data types, two status
constants, two error sentinels, six client methods, an ID generator, a
conversion method, and additional Mask constants. There is no declared new
context helper, and no experiment read API despite the REST readback discussion.

**Failure scenario.** An example or live test deletes items and claims it
removed experiment content, but historical dataset versions and event content
remain. A consumer later needs fallible conversion, explicit version ownership,
or a programmatic rejection result, requiring a breaking signature change to
the convenience methods.

**Recommendation.** Document Delete as the server's item deletion behavior,
not telemetry/history erasure; define live-fixture retention separately. Correct
the context-verb claim. Audit the exported surface before implementation:
dataset read/write types and status constants serve concrete REST contracts;
the explicit start helper can justify typed linkage, but `NewExperimentID`
and the infallible conversion need separate rationale. State whether callers
must implement result retrieval themselves. Resolve signatures, ownership,
nullable/omitted field semantics, and validation before updating the golden.

## Answers to open questions 1 to 4

### 1. One span per item is sound for the inspected server; two spans are not a UI requirement

The server defines an experiment item's canonical observation by
`span_id = experiment_item_root_span_id`, not by having an enclosing item-run
span. Public listing enforces this equality
(`LF/web/src/features/experiments/server/public/repository.ts:416-424`). UI mean
latency measures that observation's own start/end interval
(`LF/packages/shared/src/server/queries/clickhouse-sql/event-query-builder.ts:1981-1982`),
and item latency uses its selected root interval
(`LF/packages/shared/src/server/repositories/experiments.ts:1355-1368`), which the
grid displays (`LF/web/src/features/experiments/components/table/ExperimentGridCell.tsx:344-348`).

The supplied JS runner already uses one `experiment-item-run` span, sets its
own ID as canonical, closes it before evaluators, and scores that observation
(`JS/packages/client/src/experiment/ExperimentManager.ts:374-382,440-479,481-520`).
The supplied Python runner uses the two-span layout and names the task child
as canonical specifically to exclude evaluator latency
(`PY/langfuse/_client/client.py:3008-3017,3053,3076-3102`). Thus the proposal's
claim that the official SDK uniformly uses two spans is incorrect.

Use one span, end it at actual task completion, and attach item scores to it.
Specify whether evaluators use the returned context after End: their spans
then share the identity and contribute to the full-item cost aggregation
(`LF/packages/shared/src/server/repositories/experiments.ts:1332-1355`) while
task latency remains fixed. Ending a parent does not end outstanding child
work. Do not promise meaningful task latency if task work continues after End.
The one-span choice is conditional on exporting that exact canonical span;
descendants cannot replace it.

### 2. Reject invalid experiment linkage; prefer an error plus a no-op observation

Between the two proposed options, a no-op is safer than ordinary telemetry
that looks like a successfully started item. The explicit experiment operation
has failed and should be inspectable by the caller. The strongest API is a
synchronous error return alongside a safe no-op handle; callers can continue
their task if desired. This follows explicit score validation
(`GO/score.go:98-117`) and deliberate prompt validation
(`GO/prompt.go:173-202`), while retaining safe no-op observation behavior
(`GO/observation.go:61-67`). Public experiment readback cannot discover the
ordinary fallback trace (`LF/web/src/features/experiments/server/public/repository.ts:416-418`).

Reject the required identity tuple atomically; optional content failures need
not reject valid linkage if omission is documented. No-op and disabled/stopped
semantics must not call Mask/hash/serialization or perform I/O. Failed nested
starts must not accidentally retain the enclosing item's experiment context.

### 3. Keep the isolated sampling bypass, but implement it before sampling and narrow its guarantee

Full-cohort experiments justify always sampling their new roots. The current
owned sampler can implement this without changing the borrowed sampler:
clear the ambient span to create a fresh trace, install a client-scoped rate
of 1 before `Tracer.Start`, and let the new trace decision be inherited. The
current sampler ignores stale decisions for a different trace ID and then
reads the scoped rate (`GO/sampler.go:44-60`);
`GO/observation.go:138-139,157-175` records the new decision for descendants.
The proposal's preserved context values therefore need not retain an old drop
decision for the new trace. Avoid a provider-wide mutable toggle, which would
race concurrent normal requests. A bypass marker surviving into arbitrary new
traces would be too broad; confine authority to the experiment trace.

A processor cannot revive dropped spans: filtering happens after sampling,
and OnEnd requires sampled status
(`GO/internal/processor/processor.go:189-193`). Borrowed samplers remain
authoritative (`GO/client.go:207-218,267-275`; `GO/docs/reference.md:387-392`).
Check the sampled bit rather than only `IsRecording`, because `RecordOnly`
is recording but does not export through this processor. A diagnostic once
per client is operational notice, not experiment-completeness proof. Expose
per-item rejection through the start result where possible and require result
reconciliation. Root filtering, limits, queue drops, unended spans, and export
failure remain separate risks. A bypass should be documented as an explicit
exception to `SampleRate: 0`, which currently means no trace export
(`GO/docs/reference.md:381-383`), without bypassing `Disabled`.

### 4. Apply Config.Mask to deliberate dataset content, with fail-closed write semantics

Explicitly supplied observations are deliberate content too, yet Mask applies
to them (`GO/docs/privacy.md:3-7,16-24`). Applying the same configured redaction
policy to deliberate dataset input/output/metadata is justified, and new
field names let callers choose a dataset-specific policy. The capture flag's
current boundary is only observation Input/Output
(`GO/types.go:85-101`; `GO/context.go:157-164`), so exempting dataset REST writes
from that flag can be consistent if clearly documented. This does not decide
whether experiment expected output should follow capture: that new content
channel needs an explicit policy rather than an accidental exemption.

Dataset writes must fail closed when supplied content cannot be safely masked,
serialized, or validated. Observation-style omission is not safe redaction of
an existing item because the server retains omitted/nullish fields
(`LF/web/src/features/datasets/server/publicDatasetService.ts:630-637`;
`LF/packages/shared/src/server/repositories/dataset-items.ts:331-349`). Apply Mask
once before serializing the immutable request, validate its result, and replay
the same bytes on any allowed retry. State which values are outside Mask and
that read results are returned as server data without automatic redaction.
Mask must not silently change item IDs or dataset identity.
