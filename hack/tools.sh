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
