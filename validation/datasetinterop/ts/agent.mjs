// Official TypeScript SDK agent for the go-langfuse dataset interop harness.
//
// Reads one JSON request from stdin, performs it with the pinned official
// SDK, and writes one JSON response to stdout. Credentials come from the
// standard LANGFUSE_* environment variables only; nothing here prints them.

import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

import { LangfuseClient } from "@langfuse/client";
import { LangfuseAPIError } from "@langfuse/core";
import { LangfuseSpanProcessor } from "@langfuse/otel";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

const ITEM_FIELDS = [
  "datasetName",
  "id",
  "input",
  "expectedOutput",
  "metadata",
  "sourceTraceId",
  "sourceObservationId",
  "status",
];

// The shared synthetic task: a fixed lookup that answers some items wrongly.
const ANSWERS = { Austria: "Wien", France: "Paris", Japan: "Kyoto", Peru: { capital: "Lima" } };

const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);

function apiError(error) {
  if (error instanceof LangfuseAPIError) {
    return { error: { type: error.constructor.name, status: error.statusCode ?? null } };
  }
  throw error;
}

function withoutFunctions(value) {
  return JSON.parse(JSON.stringify(value));
}

async function runExperiment(langfuse, request) {
  const taskDelay = (request.task_delay ?? 0) * 1000;
  const evaluatorDelay = (request.evaluator_delay ?? 0) * 1000;
  const params = {
    name: request.name,
    runName: request.run_name,
    description: request.description,
    metadata: request.metadata,
    task: async (item) => {
      await sleep(taskDelay);
      const country = typeof item.input === "object" && item.input !== null ? item.input.country : item.input;
      if (country === "Fail") throw new Error("synthetic task failure");
      return ANSWERS[country] ?? "unknown";
    },
    evaluators: [
      async function exactMatch({ output, expectedOutput }) {
        await sleep(evaluatorDelay);
        return { name: "exact_match", value: equal(output, expectedOutput) ? 1 : 0, comment: "typescript" };
      },
      async function verdict({ output, expectedOutput }) {
        return { name: "verdict", value: equal(output, expectedOutput) ? "match" : "miss", dataType: "CATEGORICAL" };
      },
    ],
    runEvaluators: [
      async ({ itemResults }) => {
        const values = itemResults.flatMap((r) => r.evaluations.filter((e) => e.name === "exact_match").map((e) => e.value));
        return {
          name: "accuracy",
          value: values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0,
          comment: "mean",
        };
      },
    ],
  };
  let result;
  if (request.dataset) {
    const dataset = await langfuse.dataset.get(request.dataset, request.version ? { version: request.version } : undefined);
    result = await dataset.runExperiment(params);
  } else {
    result = await langfuse.experiment.run({ ...params, data: request.local_items });
  }
  return {
    experiment_id: result.experimentId,
    dataset_run_id: result.datasetRunId ?? null,
    run_name: result.runName,
    item_results: result.itemResults.map((r) => ({
      item_id: r.item.id ?? null,
      input: r.item.input,
      trace_id: r.traceId,
      output: r.output,
      dataset_run_id: r.datasetRunId ?? null,
      evaluations: r.evaluations.map((e) => ({
        name: e.name,
        value: e.value,
        comment: e.comment ?? null,
        data_type: e.dataType ?? null,
      })),
    })),
    run_evaluations: result.runEvaluations.map((e) => ({ name: e.name, value: e.value, comment: e.comment ?? null })),
  };
}

async function readExperimentOnce(langfuse, request) {
  const experiments = await langfuse.api.experiments.list({
    fromStartTime: request.from,
    id: request.experiment_id,
    fields: "core,metadata,scores",
  });
  const items = [];
  const seen = new Set();
  let cursor;
  for (;;) {
    const page = await langfuse.api.experiments.listItems({
      fromStartTime: request.from,
      experimentId: request.experiment_id,
      fields: "core,dataset,io,metadata,itemMetadata,experimentMetadata,scores",
      limit: request.page_size ?? 2,
      cursor,
    });
    items.push(...page.data);
    cursor = page.meta.cursor;
    if (!cursor) break;
    if (seen.has(cursor)) throw new Error("experiment item listing repeated a cursor");
    seen.add(cursor);
  }
  const counts = [
    items.length,
    items.reduce((n, item) => n + (item.scores?.length ?? 0), 0),
    experiments.data.reduce((n, e) => n + (e.scores?.length ?? 0), 0),
    items.every((item) => item.endTime),
  ];
  return { experiments: experiments.data, items, counts };
}

// Polls until exactly the expected records exist, then reads again after a
// settling delay to confirm nothing more arrives.
async function readExperiment(langfuse, request) {
  const expected = [request.items, request.item_scores, request.run_scores ?? 0, true];
  const deadline = Date.now() + (request.timeout ?? 120) * 1000;
  let result;
  for (;;) {
    result = await readExperimentOnce(langfuse, request);
    if (equal(result.counts, expected) || Date.now() > deadline) break;
    await sleep(2000);
  }
  let settled = equal(result.counts, expected);
  if (settled) {
    await sleep((request.settle ?? 5) * 1000);
    result = await readExperimentOnce(langfuse, request);
    settled = equal(result.counts, expected);
  }
  return withoutFunctions({ settled, counts: result.counts, experiments: result.experiments, items: result.items });
}

async function perform(langfuse, request) {
  switch (request.op) {
    case "create_dataset": {
      const body = {};
      for (const key of ["name", "description", "metadata", "inputSchema", "expectedOutputSchema"]) {
        if (key in request) body[key] = request[key];
      }
      return await langfuse.api.datasets.create(body);
    }
    case "upsert_items": {
      const results = [];
      for (const spec of request.items) {
        const body = {};
        for (const key of ITEM_FIELDS) if (key in spec) body[key] = spec[key];
        results.push(await langfuse.dataset.createItem(body));
      }
      return results;
    }
    case "get_dataset": {
      const options = { fetchItemsPageSize: request.page_size ?? 50 };
      if (request.version) options.version = request.version;
      const dataset = await langfuse.dataset.get(request.name, options);
      const { items, runExperiment, ...rest } = dataset;
      return withoutFunctions({ dataset: rest, items: items.map(({ link, ...item }) => item) });
    }
    case "list_datasets": {
      const datasets = [];
      for (let page = 1; ; page++) {
        const result = await langfuse.api.datasets.list({ page, limit: request.page_size ?? 50 });
        datasets.push(...result.data.map((d) => ({ id: d.id, name: d.name })));
        if (page >= result.meta.totalPages) return { datasets, pages: page };
      }
    }
    case "list_items": {
      const items = [];
      for (let page = 1; ; page++) {
        const result = await langfuse.api.datasetItems.list({
          datasetName: request.dataset_name,
          sourceTraceId: request.source_trace_id,
          sourceObservationId: request.source_observation_id,
          page,
          limit: request.page_size ?? 50,
        });
        items.push(...result.data);
        if (page >= result.meta.totalPages) return { items };
      }
    }
    case "get_item":
      try {
        return await langfuse.api.datasetItems.get(request.id);
      } catch (error) {
        return apiError(error);
      }
    case "delete_item":
      try {
        await langfuse.api.datasetItems.delete(request.id);
        return { deleted: true };
      } catch (error) {
        return apiError(error);
      }
    case "run_experiment":
      return await runExperiment(langfuse, request);
    case "read_experiment":
      return await readExperiment(langfuse, request);
    default:
      throw new Error(`unknown op ${request.op}`);
  }
}

async function main() {
  const request = JSON.parse(readFileSync(0, "utf8"));
  if (request.op === "meta") {
    const version = (name) => JSON.parse(readFileSync(new URL(`./node_modules/${name}/package.json`, import.meta.url))).version;
    return { langfuse_client: version("@langfuse/client"), langfuse_otel: version("@langfuse/otel"), node: process.version };
  }
  const spanProcessor = new LangfuseSpanProcessor();
  const provider = new NodeTracerProvider({ spanProcessors: [spanProcessor] });
  provider.register();
  const langfuse = new LangfuseClient();
  try {
    return await perform(langfuse, request);
  } finally {
    await spanProcessor.forceFlush();
    await langfuse.flush();
    await provider.shutdown();
    await langfuse.shutdown();
  }
}

process.stdout.write(JSON.stringify(await main()));
