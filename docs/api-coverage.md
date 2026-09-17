# Public API coverage

This ledger describes the unreleased `api` package, not the total capability of
the runtime tracing SDK. The pinned 17 September 2026 contract contains 117
operations on 71 paths. **Five prompt operations are implemented here.** Mock
contract tests do not establish compatibility with a live server image or tag.

Contract: [`langfuse-openapi-2026-09-17.yaml`](../api/testdata/langfuse-openapi-2026-09-17.yaml).
SHA-256: `b235737d48b117621f9d607b2beb48b135851fac7bff2aa72b50c6cc8796e9df`.
`python3 scripts/index-api-contract.py --check` verifies its hash and source index.

The implemented operations have route, field-presence, union, selection,
bounded-response, and error tests in `api/*_test.go`. The deployment workflow
is mock-tested in `examples/promptmanagement/main_test.go`; root invalidation
races are covered in `prompt_invalidation_test.go`. All other rows below are
**not implemented by this package**, including required application work still
listed in [the delivery ledger](gap-closure-status.md). Generic JSON/pagination
types and the source index are not operation implementations. The existing root
score queue and OTLP tracing continue independently and are not replaced by this
HTTP client. Organization administration remains a separate later backlog.

| Operation ID | Method | Path | `api` support |
| --- | --- | --- | --- |
| `annotationQueues_listQueues` | GET | `/api/public/annotation-queues` | Not implemented |
| `annotationQueues_createQueue` | POST | `/api/public/annotation-queues` | Not implemented |
| `annotationQueues_getQueue` | GET | `/api/public/annotation-queues/{queueId}` | Not implemented |
| `annotationQueues_listQueueItems` | GET | `/api/public/annotation-queues/{queueId}/items` | Not implemented |
| `annotationQueues_createQueueItem` | POST | `/api/public/annotation-queues/{queueId}/items` | Not implemented |
| `annotationQueues_getQueueItem` | GET | `/api/public/annotation-queues/{queueId}/items/{itemId}` | Not implemented |
| `annotationQueues_updateQueueItem` | PATCH | `/api/public/annotation-queues/{queueId}/items/{itemId}` | Not implemented |
| `annotationQueues_deleteQueueItem` | DELETE | `/api/public/annotation-queues/{queueId}/items/{itemId}` | Not implemented |
| `annotationQueues_createQueueAssignment` | POST | `/api/public/annotation-queues/{queueId}/assignments` | Not implemented |
| `annotationQueues_deleteQueueAssignment` | DELETE | `/api/public/annotation-queues/{queueId}/assignments` | Not implemented |
| `blobStorageIntegrations_getBlobStorageIntegrations` | GET | `/api/public/integrations/blob-storage` | Not implemented |
| `blobStorageIntegrations_upsertBlobStorageIntegration` | PUT | `/api/public/integrations/blob-storage` | Not implemented |
| `blobStorageIntegrations_getBlobStorageIntegrationStatus` | GET | `/api/public/integrations/blob-storage/{id}` | Not implemented |
| `blobStorageIntegrations_deleteBlobStorageIntegration` | DELETE | `/api/public/integrations/blob-storage/{id}` | Not implemented |
| `comments_create` | POST | `/api/public/comments` | Not implemented |
| `comments_get` | GET | `/api/public/comments` | Not implemented |
| `comments_get-by-id` | GET | `/api/public/comments/{commentId}` | Not implemented |
| `datasetItems_create` | POST | `/api/public/dataset-items` | Not implemented |
| `datasetItems_list` | GET | `/api/public/dataset-items` | Not implemented |
| `datasetItems_get` | GET | `/api/public/dataset-items/{id}` | Not implemented |
| `datasetItems_delete` | DELETE | `/api/public/dataset-items/{id}` | Not implemented |
| `datasetRunItems_create` | POST | `/api/public/dataset-run-items` | Not implemented |
| `datasetRunItems_list` | GET | `/api/public/dataset-run-items` | Not implemented |
| `datasets_list` | GET | `/api/public/v2/datasets` | Not implemented |
| `datasets_create` | POST | `/api/public/v2/datasets` | Not implemented |
| `datasets_get` | GET | `/api/public/v2/datasets/{datasetName}` | Not implemented |
| `datasets_getRun` | GET | `/api/public/datasets/{datasetName}/runs/{runName}` | Not implemented |
| `datasets_deleteRun` | DELETE | `/api/public/datasets/{datasetName}/runs/{runName}` | Not implemented |
| `datasets_getRuns` | GET | `/api/public/datasets/{datasetName}/runs` | Not implemented |
| `evaluationRules_create` | POST | `/api/public/v2/evaluation-rules` | Not implemented |
| `evaluationRules_list` | GET | `/api/public/v2/evaluation-rules` | Not implemented |
| `evaluationRules_get` | GET | `/api/public/v2/evaluation-rules/{evaluationRuleId}` | Not implemented |
| `evaluationRules_update` | PATCH | `/api/public/v2/evaluation-rules/{evaluationRuleId}` | Not implemented |
| `evaluationRules_delete` | DELETE | `/api/public/v2/evaluation-rules/{evaluationRuleId}` | Not implemented |
| `evaluators_create` | POST | `/api/public/v2/evaluators` | Not implemented |
| `evaluators_list` | GET | `/api/public/v2/evaluators` | Not implemented |
| `evaluators_get` | GET | `/api/public/v2/evaluators/{evaluatorId}` | Not implemented |
| `evaluators_update` | PATCH | `/api/public/v2/evaluators/{evaluatorId}` | Not implemented |
| `evaluators_delete` | DELETE | `/api/public/v2/evaluators/{evaluatorId}` | Not implemented |
| `evaluators_listVersions` | GET | `/api/public/v2/evaluators/{evaluatorId}/versions` | Not implemented |
| `experiments_list` | GET | `/api/public/experiments` | Not implemented |
| `experiments_listItems` | GET | `/api/public/experiment-items` | Not implemented |
| `feedback_submit` | POST | `/api/public/feedback` | Not implemented |
| `health_health` | GET | `/api/public/health` | Not implemented |
| `ingestion_batch` | POST | `/api/public/ingestion` | Not implemented |
| `legacy_metricsV1_metrics` | GET | `/api/public/metrics` | Not implemented |
| `legacy_observationsV1_get` | GET | `/api/public/observations/{observationId}` | Not implemented |
| `legacy_observationsV1_getMany` | GET | `/api/public/observations` | Not implemented |
| `legacy_scoreV1_delete` | DELETE | `/api/public/scores/{scoreId}` | Not implemented |
| `llmConnections_list` | GET | `/api/public/llm-connections` | Not implemented |
| `llmConnections_upsert` | PUT | `/api/public/llm-connections` | Not implemented |
| `llmConnections_delete` | DELETE | `/api/public/llm-connections/{id}` | Not implemented |
| `media_get` | GET | `/api/public/media/{mediaId}` | Not implemented |
| `media_patch` | PATCH | `/api/public/media/{mediaId}` | Not implemented |
| `media_getUploadUrl` | POST | `/api/public/media` | Not implemented |
| `metrics_metrics` | GET | `/api/public/v2/metrics` | Not implemented |
| `models_create` | POST | `/api/public/models` | Not implemented |
| `models_list` | GET | `/api/public/models` | Not implemented |
| `models_upsert` | PUT | `/api/public/models/{id}` | Not implemented |
| `models_get` | GET | `/api/public/models/{id}` | Not implemented |
| `models_delete` | DELETE | `/api/public/models/{id}` | Not implemented |
| `observations_getMany` | GET | `/api/public/v2/observations` | Not implemented |
| `opentelemetry_exportTraces` | POST | `/api/public/otel/v1/traces` | Not implemented |
| `organizations_getOrganizationMemberships` | GET | `/api/public/organizations/memberships` | Not implemented |
| `organizations_updateOrganizationMembership` | PUT | `/api/public/organizations/memberships` | Not implemented |
| `organizations_deleteOrganizationMembership` | DELETE | `/api/public/organizations/memberships` | Not implemented |
| `organizations_getProjectMemberships` | GET | `/api/public/projects/{projectId}/memberships` | Not implemented |
| `organizations_updateProjectMembership` | PUT | `/api/public/projects/{projectId}/memberships` | Not implemented |
| `organizations_deleteProjectMembership` | DELETE | `/api/public/projects/{projectId}/memberships` | Not implemented |
| `organizations_getOrganizationProjects` | GET | `/api/public/organizations/projects` | Not implemented |
| `organizations_getOrganizationApiKeys` | GET | `/api/public/organizations/apiKeys` | Not implemented |
| `projects_get` | GET | `/api/public/projects` | Not implemented |
| `projects_create` | POST | `/api/public/projects` | Not implemented |
| `projects_update` | PUT | `/api/public/projects/{projectId}` | Not implemented |
| `projects_delete` | DELETE | `/api/public/projects/{projectId}` | Not implemented |
| `projects_getApiKeys` | GET | `/api/public/projects/{projectId}/apiKeys` | Not implemented |
| `projects_createApiKey` | POST | `/api/public/projects/{projectId}/apiKeys` | Not implemented |
| `projects_deleteApiKey` | DELETE | `/api/public/projects/{projectId}/apiKeys/{apiKeyId}` | Not implemented |
| `promptVersion_update` | PATCH | `/api/public/v2/prompts/{name}/versions/{version}` | `Prompts.UpdateLabels` |
| `prompts_get` | GET | `/api/public/v2/prompts/{promptName}` | `Prompts.Get` |
| `prompts_delete` | DELETE | `/api/public/v2/prompts/{promptName}` | `Prompts.Delete` |
| `prompts_list` | GET | `/api/public/v2/prompts` | `Prompts.List` |
| `prompts_create` | POST | `/api/public/v2/prompts` | `Prompts.Create` |
| `scim_getServiceProviderConfig` | GET | `/api/public/scim/ServiceProviderConfig` | Not implemented |
| `scim_getResourceTypes` | GET | `/api/public/scim/ResourceTypes` | Not implemented |
| `scim_getSchemas` | GET | `/api/public/scim/Schemas` | Not implemented |
| `scim_listUsers` | GET | `/api/public/scim/Users` | Not implemented |
| `scim_createUser` | POST | `/api/public/scim/Users` | Not implemented |
| `scim_getUser` | GET | `/api/public/scim/Users/{userId}` | Not implemented |
| `scim_deleteUser` | DELETE | `/api/public/scim/Users/{userId}` | Not implemented |
| `scoreConfigs_create` | POST | `/api/public/score-configs` | Not implemented |
| `scoreConfigs_get` | GET | `/api/public/score-configs` | Not implemented |
| `scoreConfigs_get-by-id` | GET | `/api/public/score-configs/{configId}` | Not implemented |
| `scoreConfigs_update` | PATCH | `/api/public/score-configs/{configId}` | Not implemented |
| `scoresV3_getManyV3` | GET | `/api/public/v3/scores` | Not implemented |
| `scores_create` | POST | `/api/public/scores` | Not implemented |
| `scores_get-many` | GET | `/api/public/v2/scores` | Not implemented |
| `scores_get-by-id` | GET | `/api/public/v2/scores/{scoreId}` | Not implemented |
| `sessions_list` | GET | `/api/public/sessions` | Not implemented |
| `sessions_get` | GET | `/api/public/sessions/{sessionId}` | Not implemented |
| `trace_get` | GET | `/api/public/traces/{traceId}` | Not implemented |
| `trace_delete` | DELETE | `/api/public/traces/{traceId}` | Not implemented |
| `trace_list` | GET | `/api/public/traces` | Not implemented |
| `trace_deleteMultiple` | DELETE | `/api/public/traces` | Not implemented |
| `unstable_dashboardWidgets_list` | GET | `/api/public/unstable/dashboard-widgets` | Not implemented |
| `unstable_dashboardWidgets_create` | POST | `/api/public/unstable/dashboard-widgets` | Not implemented |
| `unstable_dashboardWidgets_get` | GET | `/api/public/unstable/dashboard-widgets/{widgetId}` | Not implemented |
| `unstable_dashboardWidgets_update` | PATCH | `/api/public/unstable/dashboard-widgets/{widgetId}` | Not implemented |
| `unstable_dashboardWidgets_delete` | DELETE | `/api/public/unstable/dashboard-widgets/{widgetId}` | Not implemented |
| `unstable_dashboards_list` | GET | `/api/public/unstable/dashboards` | Not implemented |
| `unstable_dashboards_create` | POST | `/api/public/unstable/dashboards` | Not implemented |
| `unstable_dashboards_get` | GET | `/api/public/unstable/dashboards/{dashboardId}` | Not implemented |
| `unstable_dashboards_update` | PATCH | `/api/public/unstable/dashboards/{dashboardId}` | Not implemented |
| `unstable_dashboards_delete` | DELETE | `/api/public/unstable/dashboards/{dashboardId}` | Not implemented |
| `unstable_dashboards_addPlacement` | POST | `/api/public/unstable/dashboards/{dashboardId}/placements` | Not implemented |
| `unstable_dashboards_updatePlacement` | PATCH | `/api/public/unstable/dashboards/{dashboardId}/placements/{placementId}` | Not implemented |
| `unstable_dashboards_deletePlacement` | DELETE | `/api/public/unstable/dashboards/{dashboardId}/placements/{placementId}` | Not implemented |
