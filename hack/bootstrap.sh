#!/bin/sh
# Reproducible development bootstrap entrypoint (#416).
#
# usage:
#   hack/bootstrap.sh          ensure mise, then run the "bootstrap" mise task
#   hack/bootstrap.sh tools    internal: run the toolchain steps inside a
#                              mise environment (invoked by [tasks.bootstrap])
#
# Noninteractive, non-root, idempotent: no sudo, no global config mutation,
# no git-hook installation, no test suite, no source generation or formatting.
# The mise launcher is pinned by version and SHA-256 below; everything else
# is pinned by .mise/config.toml, .mise/mise.lock, and the Makefile.
# Prerequisites: git, and one of curl, wget, or node when mise is absent.

set -eu

# shellcheck disable=SC1007  # CDPATH= prefix is the deliberate CDPATH guard
REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

# Pinned mise launcher. To bump: raise MISE_VERSION here and in .mise/config.toml
# (min_version), and replace the digests with those from the release's
# SHASUMS256.txt (https://github.com/jdx/mise/releases).
MISE_VERSION=v2026.10.13

log() { printf 'bootstrap: %s\n' "$*"; }
fail() { printf 'bootstrap: %s\n' "$*" >&2; exit 1; }

run_tools_steps() {
	command -v make >/dev/null 2>&1 ||
		fail "GNU make is required but not on PATH; the lane toolchain image and most contributor hosts provide it (mise does not manage make)"
	command -v git >/dev/null 2>&1 ||
		fail "git is required but not on PATH"

	# internal/harness bundle validation requires git >= 2.41 (README
	# Prerequisites); warn rather than fail because everything else builds
	# and tests on older git.
	git_version=$(git version | awk '{print $3}')
	if ! printf '%s\n%s\n' '2.41' "$git_version" | sort -V -C; then
		log "warning: git ${git_version} < 2.41; internal/harness bundle tests fail on this version gap alone (see README Prerequisites)"
	fi

	log "installing pinned tools from .mise/config.toml + .mise/mise.lock"
	# The task process may lack the launcher's mise on PATH (e.g. a
	# user-local install outside PATH), so the launcher passes it through.
	"${BOOTSTRAP_MISE:-mise}" install --locked ||
		fail "mise install --locked failed; after changing tool versions run 'mise lock' and commit .mise/mise.lock"

	# Repo-local build dependencies (idempotent: the Makefile targets skip
	# existing binaries).
	make controller-gen setup-envtest

	# envtest assets only — the suite itself stays behind an explicit task.
	k8s=$(sed -n 's/^ENVTEST_K8S_VERSION ?= //p' Makefile)
	[ -n "$k8s" ] || fail "could not read ENVTEST_K8S_VERSION from Makefile"
	assets=$(bin/setup-envtest use -p path "$k8s")

	log "environment ready"
	go version
	printf 'helm: %s (%s)\n' "$(helm version --short)" "$(command -v helm)"
	printf 'make: %s (%s)\n' "$(make --version | awk 'NR==1{print $3}')" "$(command -v make)"
	printf 'controller-gen: %s (%s)\n' "$(bin/controller-gen --version)" "$REPO_ROOT/bin/controller-gen"
	printf 'KUBEBUILDER_ASSETS=%s\n' "$assets"
	printf 'GOMODCACHE=%s\n' "$(go env GOMODCACHE)"
	printf 'GOCACHE=%s\n' "$(go env GOCACHE)"
	printf 'To reuse this environment in later commands: mise exec -- <command>\n'
}

if [ "${1:-}" = "tools" ]; then
	run_tools_steps
	exit 0
fi

if command -v mise >/dev/null 2>&1; then
	BOOTSTRAP_MISE=$(command -v mise)
	export BOOTSTRAP_MISE
	exec mise run bootstrap
fi

if [ -z "${MISE_INSTALL_DIR:-}" ] && [ -z "${HOME:-}" ]; then
	fail "HOME is unset; set MISE_INSTALL_DIR to a writable directory for the user-scoped mise install"
fi
MISE_BIN=${MISE_INSTALL_DIR:-$HOME/.local/bin}/mise
if [ ! -x "$MISE_BIN" ]; then
	case "$(uname -s)/$(uname -m)" in
		Linux/x86_64)
			mise_asset="mise-${MISE_VERSION}-linux-x64"
			mise_sha256=94c45523c7b6760b7da704f16051171ca32e8447a849a02266a439a4455e9a12
			;;
		Linux/aarch64)
			mise_asset="mise-${MISE_VERSION}-linux-arm64"
			mise_sha256=983fabf36625566f216400b907b33b4daca99adb5bc3d3cdab123b5e791575b2
			;;
		Darwin/arm64)
			mise_asset="mise-${MISE_VERSION}-macos-arm64"
			mise_sha256=92fd6898dc213808d1c81f0bead9668dbea036e5946c94c0e82554603205a3aa
			;;
		Darwin/x86_64)
			mise_asset="mise-${MISE_VERSION}-macos-x64"
			mise_sha256=4e4a996f4747eb59d55497fdd4b0d60d3add9a06c0dc5d65a70402015ee445d0
			;;
		*)
			fail "unsupported platform $(uname -s)/$(uname -m); install mise ${MISE_VERSION} yourself and re-run"
			;;
	esac

	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	url="https://github.com/jdx/mise/releases/download/${MISE_VERSION}/${mise_asset}"
	log "installing pinned mise ${MISE_VERSION} (${mise_asset})"
	if command -v curl >/dev/null 2>&1; then
		curl -fsSL -o "$tmp/mise" "$url"
	elif command -v wget >/dev/null 2>&1; then
		wget -nv -O "$tmp/mise" "$url"
	elif command -v node >/dev/null 2>&1; then
		# shellcheck disable=SC2016  # JS template: $1/$2 are process.argv, not shell refs
		node -e '
const [url, dest] = process.argv.slice(1);
fetch(url).then((r) => {
	if (!r.ok) throw new Error(`HTTP ${r.status}`);
	return r.arrayBuffer();
}).then((b) => require("fs").writeFileSync(dest, Buffer.from(b))
).catch((e) => { console.error(`download failed: ${e.message}`); process.exit(1); });
' "$url" "$tmp/mise"
	else
		fail "downloading mise requires curl, wget, or node"
	fi

	if command -v sha256sum >/dev/null 2>&1; then
		got=$(sha256sum "$tmp/mise" | awk '{print $1}')
	elif command -v shasum >/dev/null 2>&1; then
		got=$(shasum -a 256 "$tmp/mise" | awk '{print $1}')
	else
		fail "verifying the mise download requires sha256sum or shasum"
	fi
	[ "$got" = "$mise_sha256" ] ||
		fail "checksum mismatch for ${mise_asset}: got ${got}, want ${mise_sha256}"

	mkdir -p "$(dirname "$MISE_BIN")"
	cp "$tmp/mise" "$MISE_BIN.tmp.$$"
	chmod 755 "$MISE_BIN.tmp.$$"
	mv "$MISE_BIN.tmp.$$" "$MISE_BIN"
	rm -rf "$tmp"
	log "installed mise at ${MISE_BIN}"
	case ":$PATH:" in
	*":$(dirname "$MISE_BIN"):"*) ;;
	*) log "note: $(dirname "$MISE_BIN") is not on PATH; prefix later commands with it or use mise exec --" ;;
	esac
fi

BOOTSTRAP_MISE=$MISE_BIN
export BOOTSTRAP_MISE
exec "$MISE_BIN" run bootstrap
