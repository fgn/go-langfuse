# SDK gap-closure status

This is an in-progress draft for the uploaded 17 September 2026 delegation brief. It is **not yet a completed P0/P1 release**.

## Delivery boundary

The local editing environment stopped responding while extracting an additional public dependency archive. Substantial local implementation and local tests from that environment had not yet been pushed. Those unavailable files and earlier local test results are **not counted as delivered or as validation of reconstructed code**. Only source committed to this PR branch and checks against an identified branch revision establish the delivered state. Reconstruction and GitHub-run validation are proceeding on this branch.

## Baseline and selected contract

- Baseline `main`: `8a1c76e2262aff2f241ee75e6d1d8ee3955bca2c`.
- Selected official Langfuse OpenAPI snapshot: 17 September 2026.
- SHA-256: `b235737d48b117621f9d607b2beb48b135851fac7bff2aa72b50c6cc8796e9df`.
- The schema was successfully fetched again by GitHub Actions and matched that hash exactly. It is committed at `api/testdata/langfuse-openapi-2026-09-17.yaml`.
- `scripts/index-api-contract.py` verifies the hash and reproducibly indexes operation IDs, paths, and schema source locations. Its index is source navigation, not generated API implementation or a coverage claim.
- No credentials, live Langfuse project writes, releases, tags, merges, or deployments are part of this work.

## Required workstreams

| Priority | Requirement | Delivered source | Validation / example | Status |
| --- | --- | --- | --- | --- |
| P0 | Standalone synchronous API transport | `api/client.go` | `api/client_test.go`: finite total deadlines, bounded bodies, read-only retries, redirect refusal, safe error formatting, caller-client ownership | Implemented; reconstructed candidate checks in progress |
| P0 | Prompt create/read/list/labels/explicit deletion | `api/prompts.go`, `api/json_types.go`, `api/pagination.go` | `api/prompts_test.go`: message unions, empty/null/omitted fields, selectors, exact routes, lossless numbers, bounded pagination; runnable workflow example pending | Implemented; reconstructed candidate checks in progress |
| P0 | Runtime cache invalidation and prompt deployment workflow | Not yet committed | Pending | Required, unfinished |
| P0 | Current observations, scores, metrics, experiment/item reads | Not yet committed | Pending | Required, unfinished |
| P0 | Dataset definitions, versioned items and bounded import | Not yet committed | Pending | Required, unfinished |
| P0 | Bounded OTel experiment execution and evaluations | Not yet committed | Pending | Required, unfinished |
| P0 | Native Anthropic Messages/SSE instrumentation | Not yet committed | Pending | Required, unfinished |
| P1 | Project-resource APIs and explicit safe media uploads | Not yet committed | Pending | Required, unfinished |
| P1 | xAI compatibility, framework cookbook and migrations | Not yet committed | Pending | Required, unfinished |
| P1 | Complete guides, API coverage, consumer checks and release preparation | Not yet committed | Pending | Required, unfinished |
| Later | Organization provisioning, keys and memberships | No implementation promised | Separate backlog | Outside this assignment's required application scope |

The empty service types reserved on `api.Client` do not imply implemented operations. They will receive methods and tests as their workstreams are committed, or be removed before any scoped handoff. Pending P0/P1 work is not reclassified as organization-administration backlog.

## Compatibility and verification

No actual Langfuse server tag/image has been tested. Mock HTTP and local JSON tests do not establish live compatibility. The final report must record exact source revisions and check results, including all nested modules, Go 1.25.0 minimum compatibility, the Go 1.25.13 toolchain gate, and any skipped credentialed/live checks.

The temporary development workflow formats and validates only this same-repository draft branch and preserves its own check revision in the job summary. Remove it before the final handoff; existing CI remains the release gate.
