#!/bin/sh

set -eu

test "$(id -u)" = "65532"
test "$(id -g)" = "65532"
go version
make --version >/dev/null
helm version --short >/dev/null
controller-gen --version >/dev/null
test ! -x "$(command -v gh 2>/dev/null || true)"

# internal/harness artifact validation runs `git bundle unbundle` against a
# GIT_OBJECT_DIRECTORY quarantine and requires git >= 2.41; older git reports
# self-contained bundles as `bundle is malformed or not self-contained`. The
# installed binary is the contract surface: encode it as a numeric gate so a
# future image change that lowers the bundled git fails this verifier instead
# of producing misleading internal/harness failures.
git_version="$(git version | awk '{print $3}')"
if ! printf '%s\n%s\n' '2.41' "$git_version" | sort -V -C; then
	echo "git >= 2.41 required, got $git_version" >&2
	exit 1
fi

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
