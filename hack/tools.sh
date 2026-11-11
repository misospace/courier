#!/bin/sh
set -eu

# shellcheck disable=SC1007  # CDPATH= prefix is the deliberate CDPATH guard
REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

usage() {
	printf 'usage: %s {install|generate}\n' "$0" >&2
	exit 2
}

read_pin() {
	value=$(sed -n "s/^$1 ?= //p" Makefile)
	[ -n "$value" ] || {
		printf 'could not read %s from Makefile\n' "$1" >&2
		exit 1
	}
	printf '%s' "$value"
}

# The worker image's baked repo-local tools (#797): the build installs
# controller-gen and setup-envtest from the same Makefile pins into this dir
# beside the baked mise data, with a .tool-pins file in the same format this
# script writes to bin/.tool-pins. COURIER_TOOLS_DIR is a deployment-supplied
# override only; an image without the bake has no such dir and keeps the
# networked go install fallback.
baked_tools_dir=${COURIER_TOOLS_DIR:-/usr/local/share/courier/tools}

install_tools() {
	cg_pin=$(read_pin CONTROLLER_TOOLS_VERSION)
	se_pin=$(read_pin ENVTEST_VERSION)
	pins="controller-gen $cg_pin
setup-envtest $se_pin"
	if { [ -x bin/controller-gen ] || [ -x bin/setup-envtest ]; } &&
		{ [ ! -f bin/.tool-pins ] || [ "$(cat bin/.tool-pins)" != "$pins" ]; }; then
		printf 'tools: repo-local tools do not match the Makefile pins; reinstalling\n'
		rm -f bin/controller-gen bin/setup-envtest
	fi
	# Hermetic resolution (#797): when the image carries the baked tools and
	# they match the Makefile pins, copy them into the repo-local bin instead
	# of running go install — the secure worker's network cannot reach the
	# module proxy. A bake whose pins do not match the checked-out base's
	# Makefile cannot satisfy the contract offline, so under the offline
	# toolchain contract (MISE_OFFLINE=true, set by the devenv wiring) it
	# fails fast with an actionable error instead of stalling into the
	# worker's network-policy black hole.
	if [ -f "$baked_tools_dir/.tool-pins" ]; then
		if [ "$(cat "$baked_tools_dir/.tool-pins")" = "$pins" ] &&
			[ -x "$baked_tools_dir/bin/controller-gen" ] &&
			[ -x "$baked_tools_dir/bin/setup-envtest" ]; then
			mkdir -p bin
			# rm first: cp onto an existing file truncates in place and
			# preserves the destination's mode, which need not be executable.
			rm -f bin/controller-gen bin/setup-envtest
			cp "$baked_tools_dir/bin/controller-gen" bin/controller-gen
			cp "$baked_tools_dir/bin/setup-envtest" bin/setup-envtest
			printf '%s\n' "$pins" > bin/.tool-pins
			return 0
		fi
		if [ "${MISE_OFFLINE:-}" = "true" ]; then
			if [ "$(cat "$baked_tools_dir/.tool-pins")" = "$pins" ]; then
				[ -x "$baked_tools_dir/bin/controller-gen" ] ||
					printf 'tools: baked toolchain at %s is missing bin/controller-gen\n' "$baked_tools_dir" >&2
				[ -x "$baked_tools_dir/bin/setup-envtest" ] ||
					printf 'tools: baked toolchain at %s is missing bin/setup-envtest\n' "$baked_tools_dir" >&2
			else
				printf 'tools: baked toolchain at %s does not match the Makefile pins (controller-gen %s, setup-envtest %s); the offline contract cannot install repo-local tools over the network\n' "$baked_tools_dir" "$cg_pin" "$se_pin" >&2
			fi
			exit 1
		fi
	fi
	[ -x bin/controller-gen ] || GOBIN="$REPO_ROOT/bin" go install "sigs.k8s.io/controller-tools/cmd/controller-gen@$cg_pin"
	[ -x bin/setup-envtest ] || GOBIN="$REPO_ROOT/bin" go install "sigs.k8s.io/controller-runtime/tools/setup-envtest@$se_pin"
	printf '%s\n' "$pins" > bin/.tool-pins
}

generate() {
	install_tools
	# Mirrors the Makefile targets; the Makefile must stay the pin authority.
	bin/controller-gen rbac:roleName=manager-role crd paths="./..." output:crd:artifacts:config=charts/courier/crd-manifests output:rbac:artifacts:config=config/rbac
	bin/controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
}

case "${1:-}" in
	install) [ "$#" -eq 1 ] || usage; install_tools ;;
	generate) [ "$#" -eq 1 ] || usage; generate ;;
	*) usage ;;
esac
