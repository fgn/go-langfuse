"""Official Python SDK agent for the go-langfuse dataset interop harness.

Reads one JSON request from stdin, performs it with the pinned official SDK,
and writes one JSON response to stdout. Credentials come from the standard
LANGFUSE_* environment variables only; nothing here prints them.
"""

import asyncio
import datetime as dt
import importlib.metadata
import json
import sys
import time

from langfuse import Evaluation, Langfuse
from langfuse.api.core.api_error import ApiError

ITEM_FIELDS = (
    "dataset_name",
    "id",
    "input",
    "expected_output",
    "metadata",
    "source_trace_id",
    "source_observation_id",
    "status",
)


def dump(model):
    return model.model_dump(mode="json", by_alias=True)


def instant(text):
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def api_error(error):
    body = getattr(error, "body", None)
    message = body.get("message") if isinstance(body, dict) else body
    return {
        "error": {
            "type": type(error).__name__,
            "status": getattr(error, "status_code", None),
            "message": str(message)[:300] if message is not None else None,
        }
    }


# The shared synthetic task: a fixed lookup that answers some items wrongly.
ANSWERS = {"Austria": "Wien", "France": "Paris", "Japan": "Kyoto", "Peru": {"capital": "Lima"}}


# Callbacks are async so their delays do not block the SDK's event loop,
# which runs the items concurrently. The country "Fail" makes the task fail.
def make_task(delay):
    async def task(*, item, **kwargs):
        value = item["input"] if isinstance(item, dict) else item.input
        await asyncio.sleep(delay)
        country = value.get("country") if isinstance(value, dict) else value
        if country == "Fail":
            raise ValueError("synthetic task failure")
        return ANSWERS.get(country, "unknown")

    return task


def make_evaluators(delay):
    async def exact_match(*, input, output, expected_output=None, **kwargs):
        await asyncio.sleep(delay)
        return Evaluation(name="exact_match", value=1.0 if output == expected_output else 0.0, comment="python")

    def verdict(*, input, output, expected_output=None, **kwargs):
        return Evaluation(
            name="verdict", value="match" if output == expected_output else "miss", data_type="CATEGORICAL"
        )

    def overall(*, input, output, expected_output=None, metadata=None, evaluations, **kwargs):
        exact = next(e.value for e in evaluations if e.name == "exact_match")
        return Evaluation(name="overall", value=exact, comment="composite")

    def accuracy(*, item_results, **kwargs):
        values = [e.value for r in item_results for e in r.evaluations if e.name == "exact_match"]
        return Evaluation(name="accuracy", value=sum(values) / len(values) if values else 0.0, comment="mean")

    return [exact_match, verdict], overall, [accuracy]


def run_experiment(langfuse, request):
    evaluators, composite, run_evaluators = make_evaluators(request.get("evaluator_delay", 0))
    arguments = dict(
        name=request["name"],
        run_name=request["run_name"],
        description=request.get("description"),
        task=make_task(request.get("task_delay", 0)),
        evaluators=evaluators,
        composite_evaluator=composite,
        run_evaluators=run_evaluators,
        metadata=request.get("metadata"),
    )
    if "dataset" in request:
        dataset = langfuse.get_dataset(request["dataset"], version=instant(request.get("version")))
        result = dataset.run_experiment(**arguments)
    else:
        result = langfuse.run_experiment(data=request["local_items"], **arguments)
    langfuse.flush()
    return {
        "experiment_id": result.experiment_id,
        "dataset_run_id": result.dataset_run_id,
        "run_name": result.run_name,
        "item_results": [
            {
                "item_id": None if isinstance(r.item, dict) else r.item.id,
                "input": r.item.get("input") if isinstance(r.item, dict) else r.item.input,
                "trace_id": r.trace_id,
                "output": r.output,
                "dataset_run_id": r.dataset_run_id,
                "evaluations": [
                    {"name": e.name, "value": e.value, "comment": e.comment, "data_type": e.data_type}
                    for e in r.evaluations
                ],
            }
            for r in result.item_results
        ],
        "run_evaluations": [{"name": e.name, "value": e.value, "comment": e.comment} for e in result.run_evaluations],
    }


def read_experiment_once(langfuse, request, since):
    experiments = langfuse.api.experiments.list(
        from_start_time=since, id=request["experiment_id"], fields="core,metadata,scores"
    )
    items, cursor, seen = [], None, set()
    while True:
        page = langfuse.api.experiments.list_items(
            from_start_time=since,
            experiment_id=request["experiment_id"],
            fields="core,dataset,io,metadata,itemMetadata,experimentMetadata,scores",
            limit=request.get("page_size", 2),
            cursor=cursor,
        )
        items.extend(dump(item) for item in page.data)
        cursor = page.meta.cursor
        if not cursor:
            break
        if cursor in seen:
            raise RuntimeError("experiment item listing repeated a cursor")
        seen.add(cursor)
    found = [dump(e) for e in experiments.data]
    counts = (
        len(items),
        sum(len(item.get("scores") or []) for item in items),
        sum(len(e.get("scores") or []) for e in found),
        all(item.get("endTime") for item in items),
    )
    return found, items, counts


def read_experiment(langfuse, request):
    """Polls the v4 experiment read API until exactly the expected records
    exist, then reads again after a settling delay to confirm nothing more
    arrives."""
    since = instant(request["from"])
    expected = (request["items"], request["item_scores"], request.get("run_scores", 0), True)
    deadline = time.monotonic() + request.get("timeout", 120)
    while True:
        found, items, counts = read_experiment_once(langfuse, request, since)
        if counts == expected or time.monotonic() > deadline:
            break
        time.sleep(2)
    settled = counts == expected
    if settled:
        time.sleep(request.get("settle", 5))
        found, items, again = read_experiment_once(langfuse, request, since)
        settled = again == expected
    return {"settled": settled, "counts": list(counts), "experiments": found, "items": items}


def trace_observations(langfuse, request):
    page = langfuse.api.observations.get_many(
        trace_id=request["trace_id"], from_start_time=instant(request["from"]), limit=100
    )
    return {"observations": [dump(o) for o in page.data]}


def main():
    request = json.load(sys.stdin)
    op = request["op"]
    if op == "meta":
        return {"langfuse": importlib.metadata.version("langfuse"), "python": sys.version.split()[0]}
    langfuse = Langfuse()
    try:
        if op == "create_dataset":
            arguments = {k: request[k] for k in ("name", "description", "metadata", "input_schema",
                                                 "expected_output_schema") if k in request}
            return dump(langfuse.create_dataset(**arguments))
        if op == "upsert_items":
            return [dump(langfuse.create_dataset_item(**{k: spec[k] for k in ITEM_FIELDS if k in spec}))
                    for spec in request["items"]]
        if op == "get_dataset":
            dataset = langfuse.get_dataset(
                request["name"],
                version=instant(request.get("version")),
                fetch_items_page_size=request.get("page_size", 50),
            )
            return {
                "dataset": {
                    "id": dataset.id,
                    "name": dataset.name,
                    "description": dataset.description,
                    "metadata": dataset.metadata,
                    "inputSchema": dataset.input_schema,
                    "expectedOutputSchema": dataset.expected_output_schema,
                },
                "items": [dump(item) for item in dataset.items],
            }
        if op == "get_item":
            try:
                return dump(langfuse.api.dataset_items.get(request["id"]))
            except ApiError as error:
                return api_error(error)
        if op == "delete_item":
            try:
                langfuse.api.dataset_items.delete(request["id"])
                return {"deleted": True}
            except ApiError as error:
                return api_error(error)
        if op == "list_items":
            items, page = [], 1
            while True:
                result = langfuse.api.dataset_items.list(
                    dataset_name=request["dataset_name"],
                    source_trace_id=request.get("source_trace_id"),
                    source_observation_id=request.get("source_observation_id"),
                    page=page,
                    limit=request.get("page_size", 50),
                )
                items.extend(dump(item) for item in result.data)
                if page >= result.meta.total_pages:
                    return {"items": items}
                page += 1
        if op == "trace_observations":
            return trace_observations(langfuse, request)
        if op == "list_datasets":
            datasets, page = [], 1
            while True:
                result = langfuse.api.datasets.list(page=page, limit=request.get("page_size", 50))
                datasets.extend({"id": d.id, "name": d.name} for d in result.data)
                if page >= result.meta.total_pages:
                    return {"datasets": datasets, "pages": page}
                page += 1
        if op == "run_experiment":
            return run_experiment(langfuse, request)
        if op == "read_experiment":
            return read_experiment(langfuse, request)
        if op == "legacy_runs":
            try:
                runs = langfuse.get_dataset_runs(dataset_name=request["dataset_name"])
                return {"runs": [dump(run) for run in runs.data]}
            except ApiError as error:
                return api_error(error)
        raise ValueError("unknown op " + op)
    finally:
        langfuse.shutdown()


if __name__ == "__main__":
    json.dump(main(), sys.stdout)
