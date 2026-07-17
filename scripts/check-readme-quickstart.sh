#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
temporary_dir=${TMPDIR:-/tmp}
readme_go=$(mktemp "$temporary_dir/readme-quickstart.XXXXXX.go")
readme_bin=$(mktemp "$temporary_dir/readme-quickstart.XXXXXX")
trap 'rm -f "$readme_go" "$readme_bin"' EXIT

awk '
  /<!-- README_QUICKSTART_BEGIN -->/ { in_block=1; next }
  in_block && /^```go[[:space:]]*$/ { in_code=1; next }
  in_block && in_code && /^```[[:space:]]*$/ { exit }
  in_code { print }
' "$repo_root/README.md" > "$readme_go"

test -s "$readme_go"
test "$(grep -c '^package main$' "$readme_go")" = 1

cd "$repo_root"
go build -mod=readonly -o "$readme_bin" "$readme_go"
