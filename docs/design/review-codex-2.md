# Adversarial review: datasets and experiments, revision 2

Reviewed 2026-10-01. **Not ready to implement.** There are **1 blocker, 11 major issues, and 2 minor issues** below. Of the 19 original findings, **8 are closed, 10 are partially closed, and 1 is not closed**.

This is a design and source review, not an implementation or live compatibility test. Only this review file was written. Implementation edits appeared in the shared checkout during the review; they were left untouched and are outside this assessment. Repository code citations refer to the HEAD baseline, not those concurrent edits. A resolution can be closed as a design decision without already having implementation tests.

Evidence roots and versions:

- `GO`: `/home/fgustafsson/projects/go-langfuse`, HEAD `8a1c76e2262aff2f241ee75e6d1d8ee3955bca2c`.
- `LF`: `/home/fgustafsson/projects/langfuse-ref/langfuse`, supplied v4.48.0, HEAD `0922c4cbaa40249f51c7fec7ba59a1e6c7736654`.
- `PY`: `/home/fgustafsson/projects/langfuse-ref/langfuse-python`, v4.16.0, HEAD `0bc5897b0e2402e50baf8aa004a1adbffd333a61`.
- `JS`: `/home/fgustafsson/projects/langfuse-ref/langfuse-js`, HEAD `d09651d8b5ec831045d8205a5531d271dd319053`; `packages/client/package.json:2-3` identifies client 5.11.1.
- `OTEL`: `/home/fgustafsson/go/pkg/mod/go.opentelemetry.io/otel/sdk@v1.45.0`, the dependency pinned by `GO/go.mod:8-12`.
- `STD`: `/home/fgustafsson/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.13.linux-amd64/src`, the toolchain selected by `GO/go.mod:5`.
- `Proposal`: `GO/docs/design/datasets-and-experiments.md`, revision 2, 435 lines at review time.

References below give paths relative to these roots and inclusive line numbers. Failure scenarios are deductions from the cited source, not claims that a live reproduction was performed.

## Assessment of all 19 resolutions

| Review 1 finding | Verdict | Evidence and reason |
| --- | --- | --- |
| 1. Experiment metadata not masked | **closed** | Proposal:241,249,274-283,345-347 defines separate whole-value Mask calls and immutable serialized snapshots. This closes the missing masking path for SDK-supplied metadata, consistent with `GO/docs/privacy.md:18-23`. The new authoritative-namespace claim has a separate problem, R2-02; third-party attributes remain outside the existing privacy boundary (`GO/docs/privacy.md:107-109`). |
| 2. Budgets can make items unexportable | **partially** | Proposal:298-305,327-335 reduces metadata to two bounded attributes and reserves linkage, but it has not specified a realizable root bootstrap or what happens when borrowed limits discard required keys. IDs become known inside `OTEL/trace/tracer.go:100-107`, and sampler attributes precede start attributes at :172-173. See R2-01. |
| 3. Canonical root export not guaranteed | **partially** | Proposal:270-272,336-340 correctly distinguishes sampling from delivery and requires reconciliation. However, rejecting at start and immediately ending does not prove non-export: `GO/internal/processor/processor.go:189-199` independently re-evaluates the filter at end. See R2-06. |
| 4. Missing-only propagation breaks identity | **partially** | Overwrite and a trace-ID guard are specified at Proposal:298-310. This fixes conflicting keys that actually occur in the projection, but not unknown dotted keys or omitted optional keys; server dotted metadata wins (`LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3452-3461`). SDK environment updates and failed nesting also bypass the intended authority. See R2-02, R2-07, R2-08. |
| 5. Expected output over-quoted | **closed** | Proposal:49-51,277-278,388-390 uses the existing string-preserving encoder, matching `GO/internal/attributes/attributes.go:82-115` and `PY/langfuse/_client/attributes.py:161-165`. Tests must expect the server's own empty-string omission, not claim lossless empty-string persistence (`LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3540-3542`). |
| 6. ID writes are not idempotent | **partially** | Proposal:56-62,78-82,180-183 removes application retries and explains repeated version writes. The uncertainty sentinel is appropriate, but blanket 4xx classification is false, and "exactly once" overstates one application attempt. See R2-03. Version creation remains explicit in `LF/packages/shared/src/server/repositories/dataset-items.ts:451-480`. |
| 7. Mask omission keeps old values | **partially** | Proposal:184-190 rejects nil/panic/type/size failures rather than silently omitting supplied fields. It does not reject a supplied value whose JSON representation is `null`. The server converts that to omission at `LF/web/src/features/datasets/server/publicDatasetService.ts:630-635`. See R2-04. |
| 8. Pagination has no snapshot | **partially** | Proposal:144-153,203-210 adds a fixed version parameter and bounded iteration. Count and row queries are separate (`LF/web/src/features/datasets/server/publicDatasetService.ts:525-539`); stateful reads ignore the version (`LF/packages/shared/src/server/repositories/dataset-items.ts:1673-1675`). The unconditional pinning wording and unspecified total baseline remain unsafe. See R2-10. |
| 9. No lifecycle gate or time bound | **partially** | Proposal:166-176,203-205 now specifies cancellation, admission, and release before yield. It does not specify whether callback execution contributes to the count that Shutdown drains. The prompt model registers its wait-group before fetching (`GO/prompt_cache.go:333-345`), which is unsafe if copied around Mask. See R2-11. |
| 10. Response limits reject valid writes | **closed** | Proposal:191-201 provides local total limits, larger item/page limits, a page-size control, and outcome-unknown classification for unreadable or invalid write success responses. That closes the original self-incompatible small response caps. The server can add media references (`LF/web/src/features/datasets/server/publicDatasetService.ts:541-551,589-601`), so the documentation correctly must retain the "local limits" qualification. |
| 11. Invalid experiment looks successful | **partially** | Proposal:252-272 adds an error and rejects the new root rather than exporting an ordinary fallback. But Proposal:309-310 explicitly retains an enclosing item's context on failed nesting, contrary to the original recommendation (`GO/docs/design/review-codex-1.md:689-692`). See R2-07; R2-06 also allows an error result with an exported root. |
| 12. Input-hash IDs are neither parity nor private | **closed** | Proposal:245 removes the hash fallback and requires caller-chosen identity. This avoids the differing Python serialization/hash policy and JS fallback (`PY/langfuse/_client/attributes.py:161-165`; `JS/packages/client/src/experiment/ExperimentManager.ts:413-416`). No hash disclosure or canonicalization promise remains. |
| 13. Infallible conversion | **closed** | Proposal:134-142,244-250 removes the conversion and returns raw JSON. A caller explicitly chooses a version and supplies only object metadata for an experiment; no helper silently discards non-object server data. This is a deliberate restriction, matching the SDK runner's object/dictionary handling (`PY/langfuse/_client/client.py:3049-3052`). The serialized-depth problem in R2-09 still needs handling when raw JSON is accepted. |
| 14. Environment departs; scores disagree | **partially** | Proposal:311-321 chooses the official environment and a context/target-matched score default. That default is implementable without changing client-wide state. The current SDK itself can change a recording span's environment after start (`GO/context.go:104-116`), so startup stamping alone does not enforce the chosen rule. See R2-08. |
| 15. Flattening collisions and depth | **not closed** | Proposal:426 delegates flattening to the server; it does not eliminate flattening. `LF/packages/shared/src/server/otel/utils.ts:246-268` still creates ambiguous dotted paths. The Go serializer's depth guard skips custom JSON (`GO/internal/attributes/attributes.go:488-495`), including RawMessage. Base JSON additionally changes numeric/null semantics relative to Python dotted values. See R2-09. |
| 16. Tests miss contract failures | **partially** | Proposal:349-406 now covers many previously missing cases and follows `GO/CONTRIBUTING.md:16-19`. Some assertions still encode an unimplementable guarantee or the wrong error classification. The slow evaluator is present but UI latency is not explicitly asserted. See the regression requirements in R2-01 through R2-12 and R2-13. |
| 17. Third-party propagation claim too broad | **closed** | Proposal:322-326 correctly limits stamping to the provider running this processor and distinguishes the global provider in isolated mode. This matches installation in `GO/client.go:267-281` and the existing third-party privacy boundary (`GO/docs/privacy.md:107-113`). |
| 18. Endpoint-specific errors | **closed** | Proposal:92-95,211-224 defines operation-specific sentinels, static errors, source-ID dependency, escaping and validation. Dataset listing resolves the dataset first (`LF/web/src/features/datasets/server/publicDatasetService.ts:509-515`); get-item not-found paths are at :570-586; cross-dataset ID conflict is at `LF/packages/shared/src/server/repositories/dataset-items.ts:318-324`. Write uncertainty is assessed separately under original finding 6. |
| 19. Delete is not a purge; surface understated | **closed** | Proposal:67-68,406,430-435 explicitly documents history retention and removes the ID generator/conversion. Versioned delete preserves history and media (`LF/packages/shared/src/server/repositories/dataset-items.ts:540-573`). The revised surface inventory has a small arithmetic error, R2-14, rather than the original scope ambiguity. |

## Remaining and new findings, ranked by severity

### R2-01. Blocker: the root initialization sequence requires IDs before OpenTelemetry creates them

**Claim.** The immutable projection, trace-ID guard, canonical root pointer, and first-position linkage reservation cannot all work in the stated order. This needs an explicit bootstrap design before implementation, not just a new attribute list.

**Evidence.** Proposal:274-283 requires normalization before any span and a projection containing the item's trace ID. Proposal:293-305 requires its span ID as canonical and linkage first in start options. `GO/observation.go:79-105` builds attributes before `Tracer.Start`; the trace decision is installed afterwards at :137-139. `OTEL/trace/tracer.go:100-107` creates the trace/span IDs inside Start; :66-71 invokes OnStart only after creation. The current `GO/internal/processor/processor.go:19-22,119-129` context hook receives only the parent context, not the started span, and projection is applied before the export filter. It cannot test the new span's trace ID through that callback alone.

For descendants, putting reserved attributes first is sound under the owned provider's limits. It is not a complete borrowed-provider guarantee: `OTEL/trace/tracer.go:172-173` installs sampler attributes first; `OTEL/trace/span.go:248-258,330-345` drops new keys when full and truncates values. A count reservation does not defeat an application value-length limit either. Proposal:387's unconditional "linkage survives" test conflicts with Proposal:334-335's weaker borrowed guarantee.

**Failure scenario.** Start an item under a borrowed ID generator. A projection with no new trace ID fails its own guard for the root. Adding the canonical pointer after Start can lose it under a full attribute limit and lets the start filter see incomplete linkage. The method returns a sampled observation, but the public query requires `root_span_id = span_id` and cannot list it (`LF/web/src/features/experiments/server/public/repository.ts:416-424`).

**Recommendation.** Specify two phases: immutable normalized content before Start, then a root-only pending token consumed by a span-aware processor hook. Reserve a valid placeholder key in the start attributes, finalize IDs from the actual span before evaluating the start filter, and publish an immutable trace-bound projection only after successful admission. Define this token's behavior under Shutdown re-entry and make it unique per start, never shared mutable client state. Include all finalized keys in the Observation's lifetime byte/count accounting. Bound and validate experiment IDs/name/version explicitly; "two 16 KiB strings plus IDs" does not itself establish the claimed 33 KiB maximum (Proposal:328-329). For borrowed providers, verify the actual required values survived and return a defined error if they did not, or explicitly disclaim discoverability. Test sampler-filled limits, value truncation, a custom ID generator, and a filter that inspects the complete canonical tuple at OnStart.

### R2-02. Major: overwriting projected keys does not make the reserved namespace authoritative

**Claim.** A base JSON metadata attribute cannot supersede arbitrary dotted attributes already on a span. Optional keys omitted from the new projection can also retain a stale value. Trace-ID equality identifies eligible spans; it does not remove these conflicts.

**Evidence.** Proposal:303-305 promises overwrite of reserved `langfuse.experiment.*` keys. Its precomputed list contains only the selected base attributes. `LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3452-3461` merges dotted attributes after the parsed base object, so dotted entries win; :3504-3521 then flattens the result. `OTEL/trace/span.go:227-236,330-345` supports adding/replacing attributes, not removing arbitrary keys. Optional dataset identity is independently extracted at `LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3491-3492,3530-3532`.

**Failure scenario.** A borrowed instrumentor starts a child in the correct item trace with `langfuse.experiment.item.metadata.email = "patient@example.test"`. Masked projection metadata says `{"email":"[redacted]"}`. Replacing the base key leaves the dotted value, which wins during ingestion. Or a local-data item with no DatasetID inherits a foreign child attribute naming a managed dataset and is displayed as linked to it. This is a conflict present at Start, not the documented late-SetAttributes exception. Third-party PHI was already outside Mask's privacy promise; the revision nevertheless cannot claim that the exported experiment metadata is the authoritative masked snapshot.

**Recommendation.** Define authority over the entire namespace, including the absence of optional keys and both metadata prefixes. A ReadWriteSpan SetAttributes hook alone cannot implement deletion. Choose a sanitized export snapshot that strips conflicting reserved keys, or reject conflicting spans with a documented completeness consequence. Explicitly distinguish SDK-owned data from untouched third-party content in the privacy table. Test dotted overrides, absent DatasetID/version, and root-only expected output/description supplied incorrectly on descendants; comparing only the base-key value is insufficient.

### R2-03. Major: blanket 4xx classification can report a committed write as definitely unprocessed

**Claim.** Removing application retries is a defensible default for versioned writes. `ErrWriteOutcomeUnknown` is useful. The assertion that every 4xx proves no commit is false even on the pinned server. "Sent exactly once" also needs to mean one application attempt, not exactly-once execution.

**Evidence.** Proposal:180-183 treats all 4xx, including 408/429, as known non-commits. `LF/web/src/features/public-api/server/createAuthedProjectAPIRoute.ts:338-349` awaits the handler before serializing its response at :372-379. A JSON string-size failure there throws `PayloadTooLargeError`, whose status is **422**, not 5xx (`LF/packages/shared/src/errors/PayloadTooLargeError.ts:3-5`); `LF/web/src/features/public-api/server/withMiddlewares.ts:164-172` emits that status. Writes already happened in `LF/web/src/features/datasets/server/publicDatasetService.ts:268-293,626-648`. A small update can retain old large fields at :630-637. The local request limit does not bound previously stored response content.

One HTTP Client.Do is also not necessarily one transport attempt. The selected Go transport can retry a replayable body on a reused connection after a zero-byte write failure (`STD/net/http/transport.go:832-835`). This is safe with respect to commit, but contradicts literal "never retry automatically." POST/DELETE are not otherwise treated as replayable by default (`STD/net/http/request.go:1534-1547`). Do not add Idempotency-Key merely to influence Go's transport without a server deduplication contract.

**Failure scenario.** A dataset/item update commits, then its very large retained content makes response serialization fail with 422. The caller is told the write was not processed and repeats it, creating another item version/audit entry. A gateway timeout/rate-limit status is likewise not, by itself, evidence of where processing stopped.

**Recommendation.** Say "at most one application-level attempt; no application retry," retaining safe transport reconnection before any request bytes. Define the following conservative classification and test it:

| Observable outcome | Classification |
| --- | --- |
| Validation/Mask/serialization/request-construction failure before dispatch; failed admission; context already canceled before dispatch | Known not sent; no uncertainty sentinel. |
| Positively established DNS/connect/TLS failure before this request could send bytes | Known not sent. Do not infer this merely from a generic Do error or lack of a completed-write callback. |
| Dispatch begun, outcome not positively established; partial write, network error, cancellation, 5xx, redirect response, or status with ambiguous provenance | Outcome unknown. A received status does not establish non-commit. |
| 4xx after dispatch | Conservatively unknown unless the specific route/failure stage is proven pre-commit. In particular, 422 and gateway 408/429 cannot use the blanket exemption. Because server bodies are intentionally ignored, status alone cannot distinguish the two 422 stages. |
| 2xx with unreadable, oversized, malformed, truncated or identity-mismatched response | Outcome unknown. |
| Fully read and validated success response | Known success; a later cancellation must not turn this into a claimed non-commit. |

The WroteRequest callback executes on return from the request write and carries an error (`STD/net/http/request.go:582-589`); absence of a successful callback does not prove that no bytes were sent. Preserve `errors.Is` for context causes and the uncertainty sentinel without exposing raw transport text. Add commit-then-422 and cancellation racing a validated success to the existing write matrix.

### R2-04. Major: JSON null still bypasses fail-closed dataset redaction

**Claim.** Checking whether the Mask result is nil is insufficient. Supplied, non-nil values can serialize to JSON `null`, which the item API interprets as "keep the stored field."

**Evidence.** Proposal:126-128,184-190 permits arbitrary Input/ExpectedOutput values and promises rejection of unsafe supplied-field omission. `GO/internal/attributes/attributes.go:117-134,451-468` accepts custom JSON through json.Marshal. `LF/web/src/features/datasets/server/publicDatasetService.ts:630-635` converts JSON null to undefined; `LF/packages/shared/src/server/repositories/dataset-items.ts:331-349` merges existing values. An explicit `json.RawMessage("null")` is not a nil Go value.

**Failure scenario.** A privacy masker replaces a supplied sensitive expected output with RawMessage `null`, or a non-nil custom Marshaler produces `null`. Validation accepts valid, small JSON and the request is sent. The server retains the previous sensitive expected output and creates another version containing it.

**Recommendation.** Distinguish suppliedness before Mask from the semantic JSON value after serialization. Reject top-level JSON null for supplied item fields whose server semantics treat it as omission; include RawMessage, typed nil, and custom Marshaler cases. If null is intentionally supported as keep-existing, remove the fail-closed redaction claim for it and require an explicit caller opt-in. Document that this API cannot clear item content with null.

### R2-05. Major: the rate-1 sampling override survives into unrelated traces

**Claim.** The proposed sampling bypass works for the item root with the current owned sampler, but its authority is not trace-scoped. Guarding only attribute projection does not confine the sampling override.

**Evidence.** Proposal:287-291 installs a client-scoped rate of 1 and retains context values. `GO/context.go:136-154` stores the fraction in a persistent context key. `GO/sampler.go:47-55` first checks a matching trace decision, then reads that fraction when the new trace ID differs. `GO/observation.go:137-139,157-175` publishes the item decision but does not remove or restore the requested rate. Proposal:303-306 guards attributes, not this sampler key. Review 1 explicitly required confinement to the experiment trace (`GO/docs/design/review-codex-1.md:704-706`).

**Failure scenario.** With configured SampleRate 0, an application detaches the returned item context to start an unrelated trace, exactly the detached-context case in the test plan. It receives no experiment identity, but is sampled at 100% because the inherited requested fraction is still 1. Nested items can also preserve a previously injected fraction rather than the caller's original normal rate.

**Recommendation.** Make the force-sampling marker root-start-only, or restore the caller's requested/default rate on the returned context after the sampled trace decision has been installed. Descendants of the same trace inherit the decision; unrelated traces recover ordinary sampling. Test a detached SDK root for both absence of identity and the actual sampled bit, with configured rate 0 and with a non-default caller override. Keep borrowed samplers authoritative and avoid mutating provider-wide state.

### R2-06. Major: immediately ending a start-rejected root can export it

**Claim.** The proposed not-exported result is not enforceable by End alone. Current start and end filtering are intentionally independent. The new sentinel's claim that the item could never appear is too strong.

**Evidence.** Proposal:85-88,270-272 returns ErrExperimentItemNotExported after immediately ending a root rejected by ShouldExportSpan at Start. `GO/internal/processor/processor.go:128-137` evaluates the start filter and records admission expectations. OnEnd removes expectation state at :180-186, then evaluates the filter again and exports at :189-199; it does not require start-time acceptance. That accommodates attributes added late, as its comments at :178-179 explain.

**Failure scenario.** A valid borrowed filter returns false on its first invocation and true on the second, or another processor adds the property it needs before End. StartExperimentItem calls End to dispose of the rejected root. OnEnd exports a canonical, almost-zero-duration item, while the caller receives an error and may start a replacement. Public readback now includes an item that the new sentinel says cannot exist.

**Recommendation.** Give an explicitly aborted experiment root a terminal suppression state checked at OnEnd, scoped to this feature so ordinary filters retain their existing late-attribute behavior. Define what other borrowed-provider processors may still export. Alternatively rename/redefine the error as a start-time admission decision and remove the never-appears guarantee. Test a false-at-start/true-at-end filter and a later processor changing attributes, not only an always-false filter.

### R2-07. Major: a failed nested start runs under the enclosing item's identity

**Claim.** Returning an unchanged item context after rejecting a nested item preserves the exact experiment state that can misattribute its task. The error return fixes standalone invalid starts, not this failure mode.

**Evidence.** Proposal:267-273 permits the caller to continue its task; :309-310 specifies unchanged context on nested failure. Existing context projection is read for every subsequent span (`GO/context.go:395-400`; `GO/internal/processor/processor.go:119-126`). Under the revision's trace guard, a normal child remains in the outer trace and therefore still matches that outer projection. Root/item cost aggregation includes matching descendants (`LF/packages/shared/src/server/repositories/experiments.ts:1332-1355`).

**Failure scenario.** While running item A, start B with invalid metadata. The helper returns A's context, a no-op, and an error. The caller logs the error and still runs B using the returned context as permitted. Its generation and evaluator spans are stamped as A, inflating A's cost and contaminating its trace.

**Recommendation.** Define an error context that is safe for continuing the rejected task: remove this client's experiment projection and detach its span chain, or require a separate explicit task context and prohibit using the returned context for failed-item work. A no-op handle alone does not isolate child instrumentation. Cover invalid, stopped, sampling-rejected and filter-rejected nested starts, including a caller that continues the task. Preserve other clients' context values deliberately.

### R2-08. Major: the SDK's own environment helper can violate the forced environment and score alignment

**Claim.** The proposed score default is feasible and correctly compares the target TraceID, but start-time stamping is insufficient to keep all item spans in sdk-experiment. This is an SDK API path, not merely the documented third-party late mutation limitation.

**Evidence.** Proposal:307-313 exempts late third-party SetAttributes yet promises to override TraceAttributes.Environment. `GO/context.go:104-116` immediately updates the current recording span when WithTraceAttributes sets Environment, including a borrowed foreign span. `GO/observation.go:650-667` also applies trace state to an active observation. A reserved-key explicit marker in Observation alone would not prevent the separate stamp at context.go:116. The official force rule is present in `PY/langfuse/_client/propagation.py:534-540` and `JS/packages/client/src/experiment/ExperimentManager.ts:418-421`.

**Failure scenario.** Start an item correctly, then a task helper calls WithTraceAttributes(itemCtx, Environment production). The item root or child is overwritten to production. RecordScore on that same item context defaults to sdk-experiment under Proposal:317-319. Trace and score environments now disagree despite both calls using supported SDK APIs.

**Recommendation.** Make SDK environment mutation paths consult a matching experiment projection and keep sdk-experiment authoritative there. Apply the trace-ID guard to the active span as well as to future starts; unrelated traces must keep ordinary behavior. Compute the effective score environment per call and pass it into payload construction, never mutate Client.environment: `GO/score.go:98-118` has the context, whereas :240-243 currently reads only the client field. Test active-root/child mutation, explicit score overrides, unrelated and session-only scores, two clients, and concurrent scores with different effective environments.

### R2-09. Major: base JSON moves flattening defects to the server and changes metadata values

**Claim.** Sending one JSON object per metadata set is supported and matches the JS runner's wire choice. It is not generally ingested or displayed identically to Python-style dotted attributes. It also does not close the collision/depth finding.

**Evidence.** `JS/packages/client/src/experiment/ExperimentManager.ts:443-449` serializes each metadata object. `LF/packages/shared/src/server/otel/OtelIngestionProcessor.ts:3394-3398` JSON-parses it into JavaScript values; :3504-3521 invokes server flattening. `LF/packages/shared/src/server/otel/utils.ts:246-268` recursively joins object keys with dots, treats arrays as leaves, retains null leaves, and JSON-stringifies numeric/boolean/array leaves. Python serializes leaves first into strings, drops None, and assigns one value per flattened path (`PY/langfuse/_client/attributes.py:161-191`; propagation at `PY/langfuse/_client/client.py:3044-3052`).

The differences reach reads and the UI: `LF/packages/shared/src/server/queries/clickhouse-sql/event-query-builder.ts:190-200` constructs maps from flattened name/value arrays; :1976-1977 uses any such map for experiment summaries; `LF/packages/shared/src/server/repositories/experiments.ts:336-344` returns the resulting metadata. It does not reconstruct the original object or recover lost numeric precision. Moreover, `GO/internal/attributes/attributes.go:488-495,685-692` skips recursive size/depth inspection for custom JSON, including RawMessage; encoded-length validation is not a JSON-depth check.

**Failure scenario.** Metadata contains integer 9007199254740993. Go emits that exact JSON number, but the server parses it as a JavaScript number and flattens the rounded value; a dotted string leaf preserves the original digits. With `{"a.b":1,"a":{"b":2}}`, the base object produces duplicate flattened paths, while Python's dictionary collapses them before sending. With a null leaf, Python emits no attribute but base JSON persists a null entry. A RawMessage containing more than 100 nested objects can fit the 16 KiB cap and bypass the advertised existing serializer depth limit before server recursive flattening.

**Recommendation.** Define the supported metadata domain and observable differences explicitly. For deterministic cross-SDK values, normalize scalar leaves to strings with a declared numeric/null policy before creating the base JSON object, and reject ambiguous flattened paths rather than depending on server duplicate-map behavior. Do not claim arbitrary JSON-object equivalence. Validate depth and duplicate/ambiguous paths in the serialized JSON, including raw/custom representations, rather than only walking Go reflection values. Test nested-vs-dotted collisions, nulls, arrays, empty objects, large signed/unsigned integers, json.Number, RawMessage and custom MarshalJSON. Compare server-extracted arrays and the maps returned by both the public APIs and the experiments repository.

### R2-10. Major: AsOf and final-count checks do not establish the promised pinned cohort

**Claim.** A fixed version query is useful, but the API comment overstates its guarantee and the final-count rule lacks a defined reference total. Cardinality validation cannot certify version enforcement or stable membership.

**Evidence.** Proposal:148-151 says AsOf pins every page, although :63-66 already acknowledges that stateful reads ignore it. `LF/packages/shared/src/server/datasets/executeWithDatasetServiceStrategy.ts:34-40` selects the read mode by deployment configuration; the default is versioned but it is configurable (`LF/packages/shared/src/env.ts:640-646`). Stateful row reads ignore version (`LF/packages/shared/src/server/repositories/dataset-items.ts:1673-1675`) and the corresponding count is current-only at :1793-1802. The service runs row and count queries independently with identical ACTIVE/source/version filters (`LF/web/src/features/datasets/server/publicDatasetService.ts:518-539`) and calculates totalPages as ceil(totalItems/limit) at :552-556. Versioned queries use valid_from/valid_to with ACTIVE filtering and order by valid_from then ID (`LF/packages/shared/src/server/repositories/dataset-items.ts:1308-1337,1369-1385`).

An AsOf in the future passes Proposal:224's four-digit-year rule. Writes assign validFrom before transaction completion (`LF/packages/shared/src/server/repositories/dataset-items.ts:432-480`), so even a past instant is a temporal filter, not a database snapshot token across page requests.

**Failure scenario.** A deployment uses stateful reads. Item B is updated between pages without changing the ACTIVE item count; every page echoes the requested pagination values, there are no duplicate IDs, and the final count matches. The iterator succeeds and the caller records AsOf as B's experiment version although B's content was read after that instant. A future AsOf similarly allows new versions to enter the cohort during iteration. A separately observed total can change during a page; "compare to meta.totalItems" leaves it unclear whether the first or last page wins.

**Recommendation.** Promise a fixed version parameter, conditional historical reads when the deployment uses the versioned read implementation, and validation rather than transactional snapshot proof. State the mode precondition next to AsOf, not only in a server background paragraph. For pinned validation, establish a first-page total baseline, reject later total/totalPages changes, validate page/limit and ceil arithmetic, and reconcile the yielded count with that baseline. Count only rows matching the server's ACTIVE, source-ID and version filters; archived rows are not missing items. Handle empty datasets with totalPages 0. State that a terminal error invalidates the already-yielded cohort and callers must not treat those partial items as a completed run. Specify a future-time policy and the residual in-flight-commit limitation. Test stateful reads with unchanged counts, delayed transaction visibility, filtered/archived historical rows, and distinct totals from the count and data queries.

### R2-11. Major: a prompt-style admission gate can deadlock on callback-triggered Shutdown

**Claim.** Avoiding a mutex across callbacks is necessary but does not prevent a request from waiting for its own admitted work to finish. The revision names the right gate and cancellation model but leaves the decisive ordering unspecified.

**Evidence.** Proposal:169-176 models admission on the prompt cache and forbids locks across Mask/serialization/diagnostics. `GO/prompt_cache.go:333-345` registers active work and defers its completion before invoking Fetch. Shutdown waits for that work at :467-480. `GO/client.go:428-455` publishes shutdown and drains components; its re-entrant protection only helps when teardown has already begun, not when a currently running request is the first caller to initiate it. The cache itself documents moving worker diagnostics off the drain path at `GO/prompt_cache.go:485-498`. Custom serialization is also an application callback (`GO/internal/attributes/attributes.go:451-480`).

**Failure scenario.** A dataset write acquires admission, then Mask calls Client.Shutdown(context.Background()). The first Shutdown waits for dataset work to drain. That work cannot release admission until Mask returns, and Mask cannot return until Shutdown does. There is no held mutex, yet it deadlocks. A finite Shutdown deadline merely limits the stall; a wait goroutine can remain until the callback unwinds. The same pattern can occur through a Marshaler or synchronous diagnostic handler.

**Recommendation.** Specify preparation-before-admission: check client/context state, normalize and serialize outside the drain count, then atomically admit only the actual I/O and recheck stopped/canceled state. Start the deadline before preparation if its elapsed time should consume the budget, but acknowledge that a synchronous trusted callback cannot be forcibly interrupted. Release admission before any diagnostic callback or iterator yield that can initiate teardown. Set an explicit concurrency bound appropriate to 16/32 MiB responses. Add a test where Mask, custom MarshalJSON, and diagnostics are the first callers of Shutdown with an unbounded context, and ensure the test detects a stall without blocking its own cleanup.

### R2-12. Major: DatasetSpec cannot express important update distinctions, and the null rule is wrong for schemas

**Claim.** The new upsert API needs a field-presence policy before its types become public. A string description cannot express both "leave existing" and "clear to empty". Rejecting non-object schemas removes the server's explicit schema-clear operation, and the server-contract summary incorrectly generalizes null preservation to schemas.

**Evidence.** Proposal:56-62 says description/metadata/schemas are overwritten and null keeps stored values; :100-106 uses Description string and raw schemas constrained to objects at :222. The route passes omitted/nullish description and metadata as undefined but forwards schemas without that conversion (`LF/web/src/features/datasets/server/publicDatasetService.ts:268-275`). The actual upsert maps undefined schemas to no update and null schemas to Prisma.DbNull (`LF/web/src/features/datasets/server/actions/createDataset.ts:130-145`). Empty description is not nullish and is a legitimate clear-to-empty value. The public API golden freezes field types as well as signatures (`GO/api_golden_test.go:15-24,54-58`).

**Failure scenario.** A caller updates only a dataset's metadata. If the empty Go description is serialized, it clears the description; if omitted, the caller has no way to deliberately clear it. The caller tries RawMessage `null` to remove a previously installed schema, but local object validation rejects the operation even though the server supports it.

**Recommendation.** Decide presence semantics now. Use an optional description representation that distinguishes absence from an explicit empty string, or separate create/update shapes with explicit presence. For RawMessage schemas, nil can mean omitted and bytes `null` can mean clear, with objects meaning set; document that rule instead of treating every null as keep-existing. Specify item Status zero as create-default/update-preserve, not simply "server default" (Proposal:131; server omission at `LF/web/src/features/datasets/server/publicDatasetService.ts:635`). Test patching one field while all others remain unchanged, explicit description clearing, schema replacement and schema removal. Retrofitting presence by changing exported field types would break callers.

### R2-13. Minor: the live plan still does not prove the UI latency claim

**Claim.** Including a slow evaluator in a test does not establish that the experiment UI reports task latency rather than evaluator latency. The new live assertions name child cost but omit latency and the UI/repository metric path.

**Evidence.** Proposal:401-405 includes the slow evaluator and checks public fields/cost, without an explicit latency assertion. The UI repository selects canonical roots and measures their own duration (`LF/packages/shared/src/server/repositories/experiments.ts:1354-1368`); experiment mean latency has a distinct root-conditioned aggregate (`LF/packages/shared/src/server/queries/clickhouse-sql/event-query-builder.ts:1981-1982`). One-span layout is supported: the inspected JS runner ends its item observation before evaluator work (`JS/packages/client/src/experiment/ExperimentManager.ts:374-382,479-481`), whereas Python uses its task child as canonical (`PY/langfuse/_client/client.py:3008-3017,3053,3076-3102`).

**Failure scenario.** Exact identity/readback and cost assertions pass, but an implementation ends the root after a slow evaluator or a query fixture selects a later child. The UI latency includes evaluation time, undermining workflow comparisons.

**Recommendation.** Assert both per-item latency and experiment mean latency against controlled task duration, with evaluator duration clearly separated. Exercise the actual experiment repository/metric path, or explicitly state that public readback alone did not validate the UI. Add a child starting in a later minute bucket to protect root-first selection, and keep the post-End evaluator cost assertion. This is a remaining test omission, not evidence that the one-span choice itself is wrong.

### R2-14. Minor: the exported-method inventory omits StartExperimentItem

**Claim.** The surface count still understates the change by one method.

**Evidence.** Proposal:156-161 lists six dataset Client methods, :252-258 adds StartExperimentItem, but :435 reports six client methods. `GO/CONTRIBUTING.md:27-31` requires justification for new concepts, and `GO/api_golden_test.go:15-24` treats every exported signature as contract.

**Failure scenario.** A scope/API review or golden update uses the stated inventory and misses the seventh method and its unusual three-result/error-context contract.

**Recommendation.** Count seven Client methods and include StartExperimentItem's failure-context behavior in the public contract checklist. Settle the presence types and sentinel semantics above before updating the golden.

## Implementation decision

The owned sampling mechanism, per-call score-environment selection, lazy synchronous iterator, and one-span item layout are feasible. Their feasibility does not establish the revision's stronger guarantees. Resolve the root bootstrap first, then namespace authority, write classification, supplied-null handling, sampling scope, start-abort suppression, nested failure contexts, SDK environment mutation, metadata normalization, pagination guarantees, lifecycle ordering, and dataset update presence. The revised test plan must assert those corrected contracts rather than the current claims.

No code was changed and no live test was run. The design remains **not ready to implement**.
