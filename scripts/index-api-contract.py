#!/usr/bin/env python3
"""Index the exact selected OpenAPI snapshot without third-party dependencies.

This is a source-navigation index, not a general YAML parser or code generator.
The selected snapshot's hash is checked before its known layout is indexed.
Run with --check in CI; omit --check to regenerate after an explicit contract review.
"""
from pathlib import Path
import argparse
import hashlib
import json
import re

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "api/testdata/langfuse-openapi-2026-09-17.yaml"
TARGET = ROOT / "api/testdata/contract-index.json"
EXPECTED = "b235737d48b117621f9d607b2beb48b135851fac7bff2aa72b50c6cc8796e9df"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    data = SOURCE.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != EXPECTED:
        raise SystemExit("Pinned API contract hash changed; review the contract explicitly.")
    lines = data.decode("utf-8").splitlines()
    paths, schemas, operations = {}, {}, []
    current_path, current_method, current_schema = None, None, None
    in_paths, in_schemas = False, False
    for number, line in enumerate(lines, 1):
        if line == "paths:":
            in_paths = True
            continue
        if line == "components:":
            in_paths = False
            if current_path is not None:
                paths[current_path][1] = number - 1
            continue
        if line == "  schemas:":
            in_schemas = True
            continue
        if in_paths:
            match = re.fullmatch(r"  (/[^:]+):", line)
            if match:
                if current_path is not None:
                    paths[current_path][1] = number - 1
                current_path, current_method = match.group(1), None
                paths[current_path] = [number, len(lines)]
            match = re.fullmatch(r"    (get|post|put|patch|delete|head|options):", line)
            if match:
                current_method = match.group(1).upper()
            match = re.fullmatch(r"      operationId: (\S+)", line)
            if match and current_path and current_method:
                operations.append({"id": match.group(1), "method": current_method, "path": current_path})
        if in_schemas:
            if re.fullmatch(r"  \S.*", line):
                if current_schema is not None:
                    schemas[current_schema][1] = number - 1
                in_schemas = False
                continue
            match = re.fullmatch(r"    ([A-Za-z0-9_-]+):", line)
            if match:
                if current_schema is not None:
                    schemas[current_schema][1] = number - 1
                current_schema = match.group(1)
                schemas[current_schema] = [number, len(lines)]
    if not paths or not schemas or not operations:
        raise SystemExit("Unexpected pinned contract layout; refusing an incomplete index.")
    index = {"sha256": digest, "source": SOURCE.name, "paths": paths, "schemas": schemas, "operations": operations}
    rendered = json.dumps(index, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.check:
        if not TARGET.exists() or TARGET.read_text() != rendered:
            raise SystemExit("Contract index differs; regenerate and review it.")
    else:
        TARGET.write_text(rendered)
    print(f"Verified {len(paths)} paths, {len(schemas)} schemas, and {len(operations)} operations.")


if __name__ == "__main__":
    main()
