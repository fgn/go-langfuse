# Releasing

This module uses stable `vMAJOR.MINOR.PATCH` tags. While the major version is
zero, minor releases may contain explicitly documented API changes; patch
releases remain backward compatible.

Tags are an output of the release gate, not its trigger. Do not create or push
a release tag by hand. A tag push does not start the release workflow.

## Prepare the release on `main`

Merge a release-preparation pull request that completes every item below:

1. Exercise the affected SDK path in a representative consuming application
   against a local Langfuse stack. Check for duplicate observations and
   unintended content disclosure. Use synthetic data only; do not send
   release-test data to a shared or production project.
2. Pre-seed the local synthetic Langfuse project with prompt
   `go-langfuse-live-prompt` version 1 and the checklist evaluator. Run
   `go test -count=1 -tags=live -run TestLiveCompatibility -v .` with that
   local project's credentials. Use the logged unique run marker (present in
   the trace name, session, metadata, and test log so cached data cannot be
   mistaken for the current run) to verify in the local Langfuse UI: trace
   visibility, environment/user/session/tag/metadata filters, application
   roots, generation usage and cost, prompt links, and observation-level
   evaluators.
   The test fails closed when credentials are absent, tracing/content capture
   is disabled, or recording IDs are empty; a zero exit therefore cannot mean
   that it merely skipped export.
3. With the toolchain recorded in `go.mod`, run formatting, the standalone
   README compile check, all examples, the live-suite compile-only check,
   normal tests, race tests, fuzz smoke tests, vet, and `govulncheck`. CI and
   the release workflow repeat these gates.
4. Review the exported API surface, decoded OTLP goldens, dependency changes,
   security policy, and compatibility matrix.
5. Move `CHANGELOG.md` entries from `Unreleased` into a heading exactly like
   `## [0.1.0] - YYYY-MM-DD`. Set `sdkVersion` in `version.go` to the same
   version without the `v` prefix.
6. Remove both the `PRE_RELEASE_WARNING` marker and its “do not use in
   production until tagged” text from `README.md`. Replace them with accurate
   post-release stability/support wording. The release workflow intentionally
   fails while either pre-release warning remains.
7. Confirm the tree contains no live credentials, captured production
   telemetry, generated binaries, or the local `ref/` research corpus.

The release commit must be merged to and current with `main`; the workflow
rejects dispatches from another branch or an older commit.

## Dispatch the gated release

From GitHub Actions, manually run the **Release** workflow on `main` and supply:

- the exact stable version, such as `v0.1.0`;
- the module to release.

The workflow repeats the credential-free release gates listed above for the
selected module before it inspects, creates, or verifies the requested tag.
Only after those checks pass does it create an annotated tag on the tested
commit and create generated GitHub release notes. The maintainer who dispatches
the workflow is responsible for completing the local synthetic checks above.

If a run pushes the tag but fails while creating release notes, rerun the same
workflow before `main` advances. It accepts an existing tag only when that tag
resolves to the tested commit. A tag on any other commit fails closed. If
`main` has advanced, verify the existing annotated tag against the original
tested commit before creating the missing GitHub release from that tag; never
move or replace the release tag.

The workflow-created annotated tag is not a developer GPG signature. Release
authority instead comes from the maintainer's repository write and Actions
permissions, the manual dispatch from current `main`, and the workflow's scoped
`contents: write` token.

## Contrib module releases

The adapter modules (`contrib/openai`, `contrib/googlegenai`) version
independently with slash tags (`contrib/openai/v0.1.0`). Use the same
release workflow with the `module` input set to the module path; the
tag is derived as `<module>/<version>`. Order matters: when a contrib
release requires new core API, tag and publish the core release first,
update the contrib `go.mod` to that released core version, and only
then release the contrib module (the workflow verifies the declared
core version is downloadable without the workspace). Each contrib
module keeps its own `CHANGELOG.md`, which must contain the released
version heading. After tagging, verify proxy installability from a
clean directory: `GOMODCACHE=$(mktemp -d) go mod download
github.com/fgn/go-langfuse/<module>@<version>`.
