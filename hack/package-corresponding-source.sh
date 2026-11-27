#!/usr/bin/env bash
set -euo pipefail

# Package the exact committed tree and its build-relevant vendored Go dependencies.
out=$(realpath -m "${1:?output archive required}")
revision=$(git rev-parse HEAD)
epoch=$(git show -s --format=%ct "$revision")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/courier" "$(dirname "$out")"
git archive "$revision" | tar -xf - -C "$work/courier"
(
  cd "$work/courier"
  go mod vendor
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOPROXY=off GOSUMDB=off go build -mod=vendor ./cmd/...
)
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner -C "$work" -cf - courier | gzip -n > "$out"
