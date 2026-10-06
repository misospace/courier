# Base images are pinned tag@sha256 for reproducible builds. Renovate updates
# these automatically; to bump by hand, resolve the tag's current digest
# (`crane digest <image:tag>`, or the gcr.io web UI for distroless) and
# replace the digest. Never retag to a different version.

# BINARIES selects where the manager and executor binaries come from: builder
# compiles them here (release builds), prebuilt copies them from dist/ (CI builds
# them on the runner, where the Go build cache persists between runs).
ARG BINARIES=builder

# Build the manager and executor binaries.
FROM golang:1.27.1@sha256:e0174e51e81218523251d85d248a90d24c3d5e81543b4f07a5d66229397db190 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN --mount=type=cache,target=/go/pkg/mod \
	go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
	go build -o manager cmd/main.go && \
	go build -o courier-executor ./cmd/courier-executor && \
	go build -o courier-control ./cmd/courier-control && \
	go build -o courier-worker ./cmd/courier-worker && \
	go build -o courier-broker ./cmd/courier-broker

FROM builder AS binaries-builder

FROM scratch AS binaries-prebuilt
# Each image job populates dist/ with only the binaries it needs; the
# directory copy is tolerant of that.
COPY dist/ /workspace/

FROM binaries-${BINARIES} AS binaries

FROM golang:1.27.1@sha256:e0174e51e81218523251d85d248a90d24c3d5e81543b4f07a5d66229397db190 AS toolchain
ARG CONTROLLER_TOOLS_VERSION=v0.16.5
ARG HELM_VERSION=v3.18.6

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	mkdir -p /out && \
	GOBIN=/out go install sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_TOOLS_VERSION} && \
	GOBIN=/out go install helm.sh/helm/v3/cmd/helm@${HELM_VERSION}

# Coordinator image: the bootstrap OpenCode runtime. The executor contract
# (BOOTSTRAP.md) requires courier-executor, git, and opencode in one image.
# Debian (not alpine) because the opencode npm package ships glibc binaries.
FROM node:24-bookworm-slim@sha256:d6aa754f16b3197301076f047b5def2f02ea1dbbc2ca920407d46d7ec7f87b20 AS coordinator
ARG OPENCODE_VERSION=1.18.31

RUN apt-get update && \
	apt-get install -y --no-install-recommends ca-certificates git && \
	apt-get clean && rm -rf /var/lib/apt/lists/* && \
	npm install -g opencode-ai@${OPENCODE_VERSION} && \
	groupadd --gid 65532 courier && \
	useradd --create-home --uid 65532 --gid 65532 courier && \
	mkdir -p /workspace && chown courier:courier /workspace

COPY --from=binaries /workspace/courier-executor /usr/local/bin/courier-executor

USER 65532:65532
ENV HOME=/home/courier
WORKDIR /workspace

# Repository-specific toolchain image for lanes that need Go development
# tools. It is a separate target so the universal coordinator image remains
# the small bootstrap contract above.
FROM coordinator AS coordinator-go

USER root
RUN apt-get update && \
	apt-get install -y --no-install-recommends make && \
	apt-get clean && rm -rf /var/lib/apt/lists/* && \
	mkdir -p /courier-toolchain-cache && chown 65532:65532 /courier-toolchain-cache

COPY --from=toolchain /usr/local/go /usr/local/go
COPY --from=toolchain /out/controller-gen /usr/local/bin/controller-gen
COPY --from=toolchain /out/helm /usr/local/bin/helm

ENV PATH=/usr/local/go/bin:${PATH} \
	CONTROLLER_GEN=/usr/local/bin/controller-gen \
	GOMODCACHE=/courier-toolchain-cache/go-mod \
	GOCACHE=/courier-toolchain-cache/go-build \
	GOTOOLCHAIN=local

USER 65532:65532

# Harness image: the secure topology's control, worker, and broker binaries
# on the coordinator base (git + ca-certificates; no opencode usage). One
# image for all three components keeps the release unit single.
FROM coordinator AS harness

COPY --from=binaries /workspace/courier-control /usr/local/bin/courier-control
COPY --from=binaries /workspace/courier-worker /usr/local/bin/courier-worker
COPY --from=binaries /workspace/courier-broker /usr/local/bin/courier-broker

USER 65532:65532

# Manager image: distroless, manager binary only. Kept last so a bare
# `docker build .` produces the operator image; build paths still pass
# --target explicitly.
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS manager
WORKDIR /
COPY --from=binaries /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
