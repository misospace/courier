#!/bin/sh

set -eu

test "$(id -u)" = "65532"
test "$(id -g)" = "65532"
go version
make --version >/dev/null
helm version --short >/dev/null
controller-gen --version >/dev/null
test ! -x "$(command -v gh 2>/dev/null || true)"

# The runtime image pins GOCACHE and GOMODCACHE onto the writable toolchain
# cache; the fallbacks cover a bare `docker run` without pod volumes.
export GOCACHE=${GOCACHE:-/courier-toolchain-cache/go-build}
export GOMODCACHE=${GOMODCACHE:-/courier-toolchain-cache/go-mod}

# The pinned GOMODCACHE must resolve to the writable toolchain cache.
modcache="$(go env GOMODCACHE)"
case "$modcache" in
	/courier-toolchain-cache/*) ;;
	*) echo "GOMODCACHE=$modcache is not under /courier-toolchain-cache" >&2; exit 1 ;;
esac
mkdir -p "$modcache" && test -w "$modcache"

# The pinned GOCACHE must resolve to the writable toolchain cache.
gocache="$(go env GOCACHE)"
case "$gocache" in
	/courier-toolchain-cache/*) ;;
	*) echo "GOCACHE=$gocache is not under /courier-toolchain-cache" >&2; exit 1 ;;
esac
mkdir -p "$gocache" && test -w "$gocache"

make manifests
make generate
go build ./...
go test ./...
