# SDK gap-closure status

This draft implements the uploaded 17 September 2026 delegation brief. It is not yet a completed P0/P1 release.

## Baseline and contract

- Repository baseline: `8a1c76e2262aff2f241ee75e6d1d8ee3955bca2c` (`main`, checked before changes).
- Contract: supplied official Langfuse OpenAPI snapshot dated 17 September 2026.
- SHA-256: `b235737d48b117621f9d607b2beb48b135851fac7bff2aa72b50c6cc8796e9df`.
- An attempted schema refresh was not retrievable from the editing sandbox; the supplied snapshot remains the selected contract. No unreviewed refresh is implied.
- No credentials, live project writes, releases, tags, merges, or deployments are part of this work.

## Required workstreams

| Priority | Workstream | Status |
| --- | --- | --- |
| P0 | Standalone API transport and prompt authoring | Implementation and focused local tests in progress; not release-verified |
| P0 | Runtime prompt-cache invalidation and deployment workflow | Pending |
| P0 | Current observations, scores, metrics and experiment reads | Implementation in progress; contract tests pending |
| P0 | Dataset definitions, versioned items and bounded import | Implementation in progress; workflow tests pending |
| P0 | Bounded OTel experiment execution and evaluations | Pending |
| P0 | Native Anthropic Messages/SSE instrumentation | Pending |
| P1 | Project resource APIs and explicit safe media uploads | Pending |
| P1 | xAI compatibility, framework cookbook and migrations | Pending |
| P1 | Documentation, consumer checks and release preparation | Pending |

Implementation paths, exact test results, examples, compatibility limits, and operation-ID coverage will be filled in as the corresponding code is committed. Pending P0/P1 requirements are not reclassified as the later organization-administration backlog.

## Validation environment

The editing sandbox initially has Go 1.23.2 and cannot resolve public Git hosts. Standalone standard-library API tests can run there, but this does not establish compliance with the repository's Go 1.25.0 minimum or its Go 1.25.13 toolchain. The temporary validation-workspace workflow packages only committed public source, the pinned Go toolchain, public Go dependencies, and formatting tools for local offline checks; it excludes `.git`, runner state and environment, and VCS cache metadata. Remove that workflow after retrieving the artifact. Existing CI remains unchanged.

No actual Langfuse server or server image has been tested. Mock HTTP contract checks are not a live compatibility claim.
