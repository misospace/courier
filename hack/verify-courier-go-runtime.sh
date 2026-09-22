#!/bin/sh

set -eu

test "$(id -u)" = "65532"
test "$(id -g)" = "65532"
go version
make --version >/dev/null
helm version --short >/dev/null
controller-gen --version >/dev/null
test ! -x "$(command -v gh 2>/dev/null || true)"

# Keep build and module caches in writable ephemeral storage. The runtime image
# is intentionally tested as the coordinator UID, without relying on a host
# Go installation or a pre-populated repository-specific image cache.
export GOCACHE=${GOCACHE:-/tmp/courier-go-build-cache}
export GOMODCACHE=${GOMODCACHE:-/tmp/courier-go-mod-cache}

make manifests
make generate
go build ./...
go test ./...
