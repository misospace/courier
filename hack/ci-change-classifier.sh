#!/bin/bash

set -euo pipefail

usage() {
  echo "usage: $0 --files-from FILE" >&2
  echo "       $0 --base-ref REF" >&2
  exit 2
}

mode=""
arg=""

if [ "$#" -eq 0 ]; then
  usage
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    --files-from)
      [ -z "$mode" ] || usage
      mode="files"
      shift
      [ "$#" -ge 1 ] || usage
      arg="$1"
      ;;
    --base-ref)
      [ -z "$mode" ] || usage
      mode="ref"
      shift
      [ "$#" -ge 1 ] || usage
      arg="$1"
      ;;
    *)
      usage
      ;;
  esac
  shift
done

if [ "$mode" = "files" ]; then
  if [ ! -r "$arg" ]; then
    echo "error: cannot read files list: $arg" >&2
    exit 1
  fi
  paths_file="$arg"
else
  if ! git rev-parse --verify "${arg}^{commit}" >/dev/null 2>&1; then
    echo "error: cannot resolve base ref: $arg" >&2
    exit 1
  fi
  paths_file="$(mktemp)"
  trap 'rm -f "$paths_file"' EXIT
  git -c core.quotePath=false diff --name-only --no-renames "${arg}...HEAD" > "$paths_file"
fi

value="false"
count=0

while IFS= read -r line || [ -n "$line" ]; do
  if [ -z "$line" ]; then
    value="true"
    continue
  fi
  count=$((count + 1))
  case "$line" in
    docs/* | */README.md)
      ;;
    */*)
      value="true"
      ;;
    *.md)
      ;;
    *)
      value="true"
      ;;
  esac
done < "$paths_file"

if [ "$count" -eq 0 ]; then
  value="true"
fi

echo "images_required=${value}"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "images_required=${value}" >> "$GITHUB_OUTPUT"
fi
