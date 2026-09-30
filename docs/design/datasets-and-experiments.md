# Design: datasets and experiments (v2)

Status: **v2, implemented** on branch `datasets-and-experiments`. This
revision answers adversarial review 1 ([`review-codex-1.md`](review-codex-1.md),
19 findings) and review 2 of the intermediate revision
([`review-codex-2.md`](review-codex-2.md), 14 findings). The tables at the end
map every finding to its resolution. Decisions that are really the product
owner's are listed under [Decisions for the user](#decisions-for-the-user).

## Why this is now in scope

`CONTRIBUTING.md` listed datasets as out of scope and asks why a new exported
concept is needed.

1. **The v4 experiment contract is hard to reproduce by hand.** Langfuse v4
   links experiment results to datasets only through `langfuse.experiment.*`
   attributes on every observation of an item trace; the server infers
   nothing from parents. The REST `dataset-run-items` endpoint is deprecated
   (Langfuse Cloud removes it on 2026-11-16; self-hosted deployments lose it
   when they upgrade). By hand, a caller would detach each item into a new
   trace, finalize the canonical root pointer from a span ID that only exists
   after `Tracer.Start`, make identity win over stale values on descendants,
   keep the environment fixed, and stay inside the export budgets. The
   Langfuse FAQ documents runs silently disappearing when a helper got this
   wrong. None of that is expressible through `ObservationAttributes`.
2. **Dataset management is plain REST** that needs the discipline the SDK
   already applies to prompts: no redirects, bounded bodies, bounded retries,
   static errors, lifecycle admission.
3. **There is a real consumer:** a Go alert-triage service that curates a
   dataset and compares workflow versions as experiments. No official Go SDK
   exists.

Out of scope: an experiment runner, evaluators, concurrency policy, an
experiment read API (callers query `/api/public/experiments` and
`/api/public/experiment-items`, as the live test shows), cross-process
experiment baggage, and dataset listing, deletion, or run deletion.

## Verified server contract

Sources: Langfuse `v4.48.0` (`0922c4c`), the Python SDK `v4.16.0`, and the JS
SDK `@langfuse/client` 5.11.1. The reviews give file and line evidence.

- **Identity is read per span** by `OtelIngestionProcessor.ts#extractExperimentFields`:
  `id`, `name`, `description`, `dataset.id`, `item.id`, `item.version`,
  `item.root_observation_id`, `item.expected_output`, and the two metadata
  namespaces. A falsy value (including `""`) is treated as absent.
- **Canonical root.** Public listing and the UI select the span whose ID
  equals `root_observation_id`. One span per item is sound: the JS runner uses
  one, Python uses two only to keep evaluator latency out.
- **Metadata** arrives as a base JSON object attribute and/or dotted keys;
  dotted keys win, and the merged object is flattened with dots. Object
  leaves that are not strings are `JSON.stringify`'d after a JavaScript
  `JSON.parse`, which rounds integers beyond 2^53 and keeps `null` leaves.
- **Expected output** is stored as `String(value)` and returned raw.
- **Environment.** Both official SDKs force `langfuse.environment =
  "sdk-experiment"` on experiment spans.
- **Dataset writes.** `POST /api/public/v2/datasets` upserts by name (1 MB
  route body limit); omitted or null description and metadata keep the stored
  values, while a null schema removes the schema. `POST
  /api/public/dataset-items` upserts by `id` or creates a new item with a
  server ID (4.5 MB body limit). Omitted and null item fields keep the stored
  value. Every versioned item write creates a new version and audit record.
  An ID owned by another dataset is 409. Control characters are sanitized
  server-side.
- **Item listing** (`GET /api/public/dataset-items`) returns ACTIVE items
  only, with offset pages (`limit` 1-100), and separate row and count queries.
  `version` selects `validFrom <= version < validTo` in the versioned read
  implementation (the default) and is ignored by the stateful one.
- **Delete** writes a tombstone in versioned mode; history, media and
  experiment events remain.

## API

```go
var (
    ErrDatasetNotFound           = errors.New("langfuse: dataset not found")
    ErrDatasetItemNotFound       = errors.New("langfuse: dataset item not found")
    ErrWriteOutcomeUnknown       = errors.New("langfuse: write outcome unknown")
    ErrInvalidExperiment         = errors.New("langfuse: invalid experiment")
    ErrExperimentItemNotExported = errors.New("langfuse: experiment item root is not exported")
)

const (
    MaskDatasetMetadata, MaskDatasetItemInput, MaskDatasetItemExpectedOutput,
    MaskDatasetItemMetadata, MaskExperimentMetadata, MaskExperimentItemMetadata,
    MaskExperimentItemExpectedOutput MaskField = ...
)

type DatasetSpec struct {
    Name                 string
    Description          *string         // nil keeps; &"" clears
    Metadata             map[string]any  // nil keeps; non-nil replaces
    InputSchema          json.RawMessage // nil keeps; object sets; null removes
    ExpectedOutputSchema json.RawMessage
}
type Dataset struct { ID, Name, Description string; Metadata, InputSchema, ExpectedOutputSchema json.RawMessage; CreatedAt, UpdatedAt time.Time }

type DatasetItemStatus string // DatasetItemActive, DatasetItemArchived
type DatasetItemSpec struct {
    DatasetName, ID                    string
    Input, ExpectedOutput              any
    Metadata                           map[string]any
    SourceTraceID, SourceObservationID string
    Status                             DatasetItemStatus
}
type DatasetItem struct { ID, DatasetID, DatasetName string; Status DatasetItemStatus; Input, ExpectedOutput, Metadata json.RawMessage; SourceTraceID, SourceObservationID string; CreatedAt, UpdatedAt time.Time }
type DatasetItemQuery struct { DatasetName string; AsOf time.Time; SourceTraceID, SourceObservationID string; PageSize int }

type Experiment struct { ID, Name, Description, DatasetID string; Metadata map[string]any }
type ExperimentItem struct { ID string; Version time.Time; ExpectedOutput any; Metadata map[string]any }

func (c *Client) UpsertDataset(ctx, DatasetSpec) (Dataset, error)
func (c *Client) GetDataset(ctx, name string) (Dataset, error)
func (c *Client) UpsertDatasetItem(ctx, DatasetItemSpec) (DatasetItem, error)
func (c *Client) GetDatasetItem(ctx, id string) (DatasetItem, error)
func (c *Client) DeleteDatasetItem(ctx, id string) error
func (c *Client) DatasetItems(ctx, DatasetItemQuery) iter.Seq2[DatasetItem, error]
func (c *Client) StartExperimentItem(ctx, Experiment, ExperimentItem, name string,
    ObservationAttributes) (context.Context, *Observation, error)
```

The surface is 7 `Client` methods, 8 types, 2 status constants, 5 sentinels
and 7 `MaskField` constants. There is no new context verb. Each piece maps to
a server contract; dropped from v1: the input-hash item ID, `NewExperimentID`
(any stable string created once per run works; a helper cannot prevent
per-item generation), the infallible `DatasetItem.ExperimentItem`
conversion, and `ExperimentItem.Input` (the item input is the root's
`values.Input`). `Score` is unchanged; see Scores.

## Experiments

### Result contract

- `(ctx', obs, nil)`: the item root is started, sampled, admitted by the
  export filter, and carries its complete identity.
- `ErrInvalidExperiment`: identifiers empty, over 255 bytes, invalid UTF-8 or
  containing control characters; a description over 16 KiB; a version outside
  the RFC 3339 years; metadata or expected output that a masker panicked on,
  changed to an unsupported type, or that cannot be encoded within its limits.
  Nothing is started and nothing is masked after the first failure.
- `ErrExperimentItemNotExported`: the root was not sampled by a borrowed
  sampler (including `RecordOnly`), was rejected by `ShouldExportSpan` at
  start, or lost part of its identity to borrowed attribute limits. The root
  is marked aborted in the processor and ended, so a filter that would accept
  it at end cannot export it through this client.
- A stopped client returns a plain error.
- Every error returns a no-op observation and a context with **neither this
  client's experiment identity nor an ambient span**, so a caller that still
  runs the task produces a new, unlinked trace instead of contaminating an
  enclosing item or request trace.
- A nil or disabled client validates identifiers (no Mask, no serialization)
  and then returns `(ctx, no-op, nil)`, like other explicit no-ops; a nil
  `ctx` is an error.

### Root bootstrap (review 2, R2-01)

1. **Before any span:** validate, then Mask each of experiment metadata, item
   metadata and expected output exactly once, and encode them into immutable
   strings. Build three attribute lists: identity (`id`, `name`, `item.id`,
   optional `dataset.id` and `item.version`), content (`langfuse.environment
   = sdk-experiment` and the two metadata objects), and root-only
   (`description`, `expected_output`).
2. **Start:** creation attributes are the type key, identity, a placeholder
   `root_observation_id = ""` (reserving its slot next to the identity),
   content, root-only content, then the ordinary observation fields. The span
   starts with `WithNewRoot`. A per-start **root token** (identity + content +
   root-only) is placed on the context passed to `Tracer.Start` only.
3. **Processor `OnStart` for the root:** the processor's span-aware hook
   `AuthoritativeAttributes(ctx, span)` sees the token; the first SDK-scope,
   parentless span claims it (atomic), so only the root does. It returns the
   token attributes plus `root_observation_id = span's real ID`. The
   processor sets them, records them as the span's authoritative set, and
   only then runs the start-time export filter, which sees the complete tuple.
4. **After start:** the SDK verifies the token was claimed and that the span's
   attributes (read through `sdktrace.ReadOnlySpan`) contain every identity
   value, the real root pointer, and the environment. On failure the root is
   aborted and ended (`ErrExperimentItemNotExported`).
5. **Publish:** the returned context carries an immutable projection
   `{traceID, identity + root pointer + content}` under a client-scoped key.
   The token is shadowed in the returned context.

### Namespace authority (R2-02, R2-06, finding 4)

- For every span started on this client's provider whose trace ID equals the
  projection's, `OnStart` sets the projection (overriding creation values and
  propagated defaults) and records it as the span's authoritative set.
- **At `OnEnd`** the processor exports a view of the span in which the
  authoritative set owns the reserved namespace: every attribute named
  `langfuse.environment` or starting with `langfuse.experiment.` is removed,
  then the authoritative values are appended. This removes foreign dotted
  metadata keys (`langfuse.experiment.item.metadata.email`), optional keys the
  projection deliberately lacks (`dataset.id` for local data), root-only keys
  on descendants, and late `SetAttributes` changes. The view embeds the
  original `ReadOnlySpan`, so every other field is unchanged, and the end
  filter classifies the view.
- The tracking map shares the processor's 4096 active-span bound; beyond it
  the processor reports one diagnostic and enforces identity at start only.
- Other processors on a borrowed provider see the original span; the
  guarantee covers what this client exports.
- SDK observations in an item trace also carry the projection as creation
  attributes placed first, so borrowed count limits drop content before
  linkage. For foreign spans under low borrowed limits, `OnStart` values can
  still be dropped; the export view restores them, since it is built from the
  recorded set, not from span storage.
- A detached context (new trace) gets nothing: the projection is trace-scoped.
  A nested item replaces it; a failed nested start clears it.

### Environment (finding 14, R2-08)

`sdk-experiment` is authoritative on every item span, as in both official
SDKs. `WithTraceAttributes(Environment: …)` on an item context cannot change
it: SDK observations in an item trace treat the environment key as explicit,
the current-span environment stamp is skipped when the active span belongs to
an item trace, and the export view enforces it anyway.

### Scores

`RecordScore` computes the environment per call: a score whose `TraceID`
equals the item trace of the experiment context it is recorded on uses
`sdk-experiment`; every other score uses `Config.Environment`. The client
field is never mutated. No new `Score` field is added (see Decisions).

### Sampling (R2-05, open question 3)

In isolated mode the root start installs a client-scoped rate of 1 on the
context passed to `Tracer.Start` only; the returned context restores the
caller's previous value, so detached traces and later work keep ordinary
sampling (tested at `SampleRate: 0` with a detached root). Descendants
inherit the item trace's recorded decision. Borrowed samplers stay
authoritative; a sampled-out root returns `ErrExperimentItemNotExported`.
`Disabled` still disables everything.

### Delivery

A started root can still be lost by span-queue drops, end-time filtering
through other paths, export failure, spans never ended, or ending after
`Shutdown`. The docs tell callers to end all item work before
`Flush`/`Shutdown` and to reconcile the expected item count through
`/api/public/experiment-items`.

### Encoding

**Expected output** (finding 5) uses the string-preserving rule, capped at
256 KiB, and follows the starting context's content-capture decision:

| Value | Wire value |
| --- | --- |
| `nil`, typed nil, mask returned `nil`, raw `null`, empty raw | omitted |
| `""` | `""` (sent; the server drops falsy values) |
| `"Paris"`, `"{\"a\":1}"` | verbatim |
| structures, numbers, bools | deterministic JSON |
| `json.RawMessage` holding a JSON string | the decoded string (the SDKs decode stored JSON first) |
| other `json.RawMessage` | compact JSON with exact number digits |
| invalid raw JSON, cycles, unsupported types, marshaler panics | `ErrInvalidExperiment` |

**Metadata** (finding 15, R2-09) is one JSON object attribute per set, after
one Mask call per set; `nil` from the masker omits it. Validation and
normalization run on the **serialized** JSON, so custom marshalers and
`json.RawMessage` are covered:

- decoded with `UseNumber`; objects recurse at most 32 levels;
- every leaf becomes a string, matching Python's dotted attributes: strings
  verbatim, numbers with their exact JSON digits (no JavaScript rounding),
  booleans `"true"`/`"false"`, arrays compact JSON; `null` leaves and
  leafless objects are dropped;
- each dotted path must be valid UTF-8, at most 200 bytes, without empty,
  `__proto__`, `constructor` or `prototype` segments;
- two keys flattening to one path (`"a.b"` beside `{"a":{"b":…}}`) are an
  error, never a server-side duplicate;
- output is deterministic (sorted keys, no HTML escaping), at most 16 KiB.

Documented difference: numbers use Go's JSON form (a float64 `1` is `"1"`
where Python writes `"1.0"`; `json.Number` and raw JSON keep their digits),
and arrays are compact where Python adds spaces.

### Budgets (finding 2)

| Source on one item root | Attributes | Bytes |
| --- | --- | --- |
| Observation fields + 32 observation metadata keys | ≤ 45 | 2 MiB aggregate (existing) |
| Trace propagation + 32 trace metadata keys + app-root marker | ≤ 40 | ≈ 45 KiB |
| Identity (5) + root pointer + environment | 7 | ≤ 1.6 KiB |
| Metadata objects + description + expected output | 4 | 16 + 16 + 16 + 256 KiB |
| **Total** | **≤ 96 of 128** | **≈ 2.35 MiB** |

With eight maximal error events (8 × 64 KiB) the worst case is about 2.9 MiB,
under the 4 MiB OTLP request cap. A test exports a root with every field at
its maximum in one piece with no dropped attributes. These are separate,
explicit budgets rather than part of the observation's 2 MiB aggregate.
Borrowed providers keep the application's limits; the root verification
turns a truncated or dropped identity into `ErrExperimentItemNotExported`,
and the export view restores child identity.

## Datasets

### Preparation before admission (finding 9, R2-11)

Every call first validates (for nil and disabled clients too), then checks
availability (nil, disabled, stopped → static error, no Mask, no I/O), then
masks and serializes **outside** the admission count. Only then does it enter
the dataset gate: a 16-slot semaphore (bounding response buffers to 16 × 16
MiB) acquired within the caller's context and the client lifecycle, followed
by a closing check and a wait-group registration under a mutex. The admitted
operation runs on a context bounded by the caller's context, a fixed 30 s
operation budget (per page for the iterator), and the client lifecycle, and
releases its admission before returning or yielding. `Shutdown` stops
admission and cancels dataset I/O before the OTel teardown, then drains with
the prompt and score drains. A Mask callback or `MarshalJSON` that calls
`Shutdown` therefore finds nothing of its own to wait for; its call fails
without I/O (tested).

### Transport

`internal/transport/datasets.go` follows `prompts.go`: no redirects, Basic
auth and SDK identity headers, JSON content type on POST, bounded draining of
error bodies, UTF-8 and single-value checks on responses, semantic matching of
the response to the request (dataset name, item ID, dataset membership), and
static error text naming only the operation and status.

### Write classification (findings 6 and 10, R2-03)

A write is at most one application attempt once dispatched. An
`httptrace.WroteHeaders` hook marks dispatch: a server cannot act on a
request whose headers it never received, so a transport failure before that
point is retried like a read (Go's transport may itself replay a request on
a reused connection only when nothing was written).

| Observable outcome | Result |
| --- | --- |
| validation, Mask, serialization, admission failure, or context done before dispatch | error, known not sent |
| transport failure before any header was written | retried (at most twice), then a known failure |
| 400, 401, 403, 404, 409, 413 | known rejection (Langfuse returns these before applying a write); 404 maps per operation |
| any other status after dispatch, including 3xx, 408, 422, 429 and 5xx | `ErrWriteOutcomeUnknown`, no retry |
| network error or cancellation after dispatch | `ErrWriteOutcomeUnknown` (plus the context error) |
| 2xx that is unreadable, oversized, malformed, or does not match the request | `ErrWriteOutcomeUnknown` |
| fully read and validated 2xx | success, even if the context ends afterwards |

The request body is serialized once and replayed byte-for-byte. Repeating a
write after `ErrWriteOutcomeUnknown` is the caller's decision: with an ID it
is at-least-once and last-writer-wins (an extra version, possibly overwriting
a concurrent writer); without an ID it can create a duplicate item. Reads
retry network errors, 408, 429 and 5xx twice with jittered backoff and
`Retry-After`, declining any delay past the operation deadline.

### Errors (finding 18)

| Operation | 404 |
| --- | --- |
| `GetDataset`, `UpsertDatasetItem`, `DatasetItems` | `ErrDatasetNotFound` (the dataset is resolved first) |
| `GetDatasetItem`, `DeleteDatasetItem` | `ErrDatasetItemNotFound` |
| `UpsertDataset` | plain status error |

Errors wrap `context.Canceled`/`DeadlineExceeded` when the caller's context
or the operation budget ended, `ErrWriteOutcomeUnknown` when applicable, and
a shutdown marker when the client lifecycle canceled the call. Text never
contains names, IDs, URLs, bodies, raw transport or marshal errors, or panic
values.

### Masking fails closed (finding 7, R2-04)

For each supplied content field (non-nil, non-empty raw): Mask once; `nil`,
a panic, a metadata result that is not `map[string]any`, invalid raw JSON,
anything that **serializes to JSON `null`** (raw `null`, custom marshalers),
over 1 MiB, or unserializable → error before I/O, because omission would keep
the stored value. `DisableContentCapture` does not apply (these are
deliberate writes, not telemetry). Names, descriptions, schemas, IDs and
statuses are not masked; read results are returned as stored.

### Presence (R2-12)

| Field | Omitted when | Supplied value |
| --- | --- | --- |
| `DatasetSpec.Description` | nil | replaces; `&""` clears |
| `DatasetSpec.Metadata` | nil | replaces (an empty map stores `{}`; the server cannot clear it) |
| `DatasetSpec` schemas | nil/empty | a JSON object replaces; JSON `null` removes |
| `DatasetItemSpec` content | nil, typed nil, empty raw | replaces; cannot be cleared (server ignores null) |
| `DatasetItemSpec.Status`, source IDs | empty | replaces; new items default to ACTIVE |

### Limits (finding 10)

| Limit | Value |
| --- | --- |
| Identifiers (dataset names, item/trace/observation IDs) | 1-255 bytes, UTF-8, no control characters or surrounding whitespace |
| Dataset description | 16 KiB |
| Item content field after Mask | 1 MiB each |
| Item request body | 4 MiB (server: 4.5 MB) |
| Dataset request body | 1 MiB (server: 1 MB) |
| Item or dataset response | 8 MiB |
| Page response | 16 MiB; `PageSize` 1-100, default 20; a page of one item always fits |
| Pages per iteration | 10,000, checked on the first page |
| Concurrent admitted operations | 16 |

These are local limits and are labelled as such. Items created elsewhere with
larger content fail with a static "exceeds the local size limit" error.

### Pagination (finding 8, R2-10)

`DatasetItems` is lazy, synchronous and goroutine-free: no request until the
loop starts, one admitted request per page, admission released before
yielding, and availability and caller cancellation rechecked before each page.
Breaking out of the loop sends nothing more.

`AsOf` requests item versions valid at that instant, formatted as UTC with
milliseconds and sent on every page. It is rejected when more than a minute in
the future. It holds only on servers using the versioned read implementation
(the default); the field's documentation says so, and that a write still
committing at `AsOf` may appear. The SDK validates rather than proves a
snapshot. Every page must satisfy, before any of its items is yielded:

- `meta.page` and `meta.limit` equal the request, `totalItems ≥ 0`, and
  `totalPages == ceil(totalItems / limit)`;
- `totalItems` equals the first page's (the baseline);
- pages before the last are full, the last holds exactly the remainder, and a
  page past the last is empty (an empty dataset is one empty page);
- items belong to the requested dataset, are ACTIVE, match the source
  filters, and never repeat an ID (catching A/B/A cycles);
- `totalPages ≤ 10,000`.

A violation yields one terminal error; the documentation states that items
already yielded are then not a complete cohort. Archived items are not listed
and not counted.

### Delete (finding 19)

`DeleteDatasetItem` is documented as a server-side tombstone, not a purge.

## Tests

- **Transport (`httptest`)**: exact escaped request URIs for `/`, `%`, `?`,
  `#`, Unicode, `.`, `..`; auth, identity headers, content type, body bytes;
  decoding of null and absent fields; status classification per read/write
  and per status; pre-dispatch retry with identical bytes; commit then
  disconnect; commit then 422/5xx; redirect target receives nothing;
  oversized, malformed, trailing and invalid UTF-8 bodies; identity mismatch;
  `Retry-After` beyond the deadline; cancellation during body read and
  backoff; validated success surviving a late cancellation.
- **Root datasets**: masked body sent once; fail-closed Mask nil/panic/type,
  raw null, null-producing marshaler, cycles, oversize, invalid identifiers;
  per-operation sentinels; `errors.Is` for cancellation and outcome unknown;
  nil/disabled/stopped clients never mask or send; presence semantics;
  pagination exactness, empty dataset, duplicates, A/B/A, changing totals,
  short or long pages, wrong page/limit, page cap, foreign dataset, archived
  items, missing meta; laziness and early break; query validation including
  future `AsOf`; Shutdown cancelling in-flight I/O, re-entered from Mask, a
  marshaler and a loop body; stored iterators after Shutdown; calls racing
  Shutdown under `-race`.
- **OTLP wire (decoded protobuf)**: identity on root, SDK children, events
  and post-End evaluators; new trace despite an ambient span; root pointer
  equals root span ID; root-only fields; environment against request
  overrides; detached, nested and failed-nested contexts; rate-0 bypass not
  leaking; borrowed Drop, RecordOnly, filter rejection, flip-flopping filter
  and low attribute limits; foreign dotted keys, foreign optional keys and
  late mutation removed at export; two clients; expected-output encoding
  table; content capture; Mask once per item trace; invalid input atomic and
  payload-free; the maximal root exporting whole; score environments.
- **Unit and fuzz**: `EncodeContent`, `EncodeMetadataObject` (fuzzed for
  string leaves and stability), processor authority and abort.
- **API golden** and surface tests.
- **Live** (`live` tag, local Langfuse 4.48): unique dataset; items with
  string and structured expected output; a pinned read that survives a later
  edit; a two-item experiment with a child generation, a slow evaluator after
  End and one score per item; flush; then `/api/public/experiments` and
  `/api/public/experiment-items` for the exact item count, identity, dataset
  ID, version, raw expected output, environment, item latency excluding the
  evaluator, and scores. It fails without credentials. UI aggregate latency
  is not exercised beyond the public API's item timing (R2-13).

## Decisions for the user

Conservative choices where the reviews left a product decision open:

1. **Expected output follows content capture.** With capture disabled on the
   starting context it is not exported; opt in with
   `WithContentCapture(ctx, true)`. The official SDKs have no such switch.
2. **No `Score.Environment` field.** Item scores recorded on the item context
   use `sdk-experiment` automatically; other scores keep
   `Config.Environment`. (The Python runner uses the client environment for
   evaluator scores; review 2 preferred alignment with the item trace.)
3. **Writes are never retried after dispatch**, not even on 429; the caller
   decides after `ErrWriteOutcomeUnknown`.
4. **A failed start returns a detached context** (no ambient span), not the
   caller's context.
5. **Experiment metadata masked to `nil` is omitted**; panics and type
   changes are errors.
6. **The item root is always a `span` observation.**
7. **`AsOf` more than a minute in the future is rejected.**
8. **No experiment reader, dataset listing or deletion, or run deletion.**
9. **Metadata leaves become strings** (Python parity, exact numbers) rather
   than JS-style typed JSON leaves.

## Review 1 findings and resolutions

| # | Finding | Resolution |
| --- | --- | --- |
| 1 | Experiment metadata not masked | Three mask fields, one call per value per item, immutable encoded projection; privacy doc updated |
| 2 | Budgets can make items unexportable | Two metadata attributes, explicit budget table (≤ 96/128, ≈ 2.9 MiB worst case), maximal-root test, verification on borrowed providers |
| 3 | Root export not guaranteed | Start-time rejection with abort; bypass limited to isolated sampling; delivery caveats and reconciliation documented |
| 4 | Missing-only propagation | Authoritative set at start and an export view at end that owns the namespace |
| 5 | Expected output over-quoted | String-preserving encoder, `json.RawMessage` rules, table tested |
| 6 | ID writes not idempotent | At most one dispatched attempt; at-least-once only on caller repeats; `ErrWriteOutcomeUnknown` |
| 7 | Mask omission keeps old values | Fail closed before I/O, including JSON null |
| 8 | No pagination snapshot | `AsOf`, baseline totals, exact page arithmetic, duplicate detection, terminal errors |
| 9 | No lifecycle gate or time bound | Preparation before admission, gate with semaphore, 30 s budget, cancel before teardown |
| 10 | Response limits reject valid writes | Aligned 1/4/8/16 MiB limits, `PageSize`, outcome unknown on bad 2xx |
| 11 | Invalid experiment looks successful | Error return, no-op observation, detached identity-free context |
| 12 | Input-hash IDs | Removed; explicit IDs required |
| 13 | Infallible conversion | Removed; explicit `AsOf`/`Version`; raw JSON rules; `UseNumber` in the example |
| 14 | Environment and scores disagree | Authoritative `sdk-experiment`; per-call score environment |
| 15 | Flattening collisions and depth | Serialized-JSON validation: depth, key paths, collisions, string leaves |
| 16 | Tests miss contract failures | Matrix above |
| 17 | Third-party propagation claim | Scoped to spans on the provider running this processor; documented |
| 18 | Endpoint-specific errors | Per-operation sentinels, wrapped context errors, escaping tests, ACTIVE-only |
| 19 | Delete not a purge; surface | Tombstone documented; surface audited and counted |

## Review 2 findings and resolutions

| # | Finding | Resolution |
| --- | --- | --- |
| R2-01 | Root IDs exist only after Start | Root token claimed in `OnStart`, placeholder slot, real pointer before the start filter, post-start verification, abort on failure |
| R2-02 | Overwrite is not namespace authority | Export view strips every reserved key and re-applies the authoritative set at `OnEnd` |
| R2-03 | Blanket 4xx classification | Dispatch marked by `WroteHeaders`; only 400/401/403/404/409/413 are known rejections; 408/422/429 unknown |
| R2-04 | JSON null bypasses fail-closed | Any supplied value serializing to `null` is rejected |
| R2-05 | Rate-1 override leaks | Installed on the start context only and restored on the returned context; tested with a detached root at rate 0 |
| R2-06 | Ending a rejected root can export it | Processor `Abort` suppresses it at `OnEnd`; flip-flop filter test |
| R2-07 | Failed nested start keeps outer identity | Error context has no identity and no ambient span |
| R2-08 | SDK environment helpers can override | Explicit env on item observations, stamp skipped for item traces, enforced at export; per-call score environment |
| R2-09 | Base JSON changes metadata values | String-leaf normalization, null dropping, exact numbers, depth and collision checks on serialized JSON |
| R2-10 | AsOf and count checks overstated | Versioned-mode precondition on the field, future `AsOf` rejected, first-page baseline, partial-cohort warning |
| R2-11 | Gate can deadlock on callback Shutdown | Mask and serialization before admission; tests for Mask, marshaler and loop-body re-entry |
| R2-12 | Presence semantics | `*string` description, schema `null` removes, presence table |
| R2-13 | Live test does not prove latency | Live test asserts item latency from the public API excludes the evaluator; UI aggregates stated as not exercised |
| R2-14 | Surface count | 7 methods, 8 types, 2 constants, 5 sentinels, 7 mask fields |
