# Base images are pinned tag@sha256 for reproducible builds. Renovate updates
# these automatically; to bump by hand, resolve the tag's current digest
# (`crane digest <image:tag>`, or the gcr.io web UI for distroless) and
# replace the digest. Never retag to a different version.

# Build the manager and executor binaries.
FROM golang:1.27.1@sha256:3680233e3204827fbdc66088528ae6d4b3d034f51d03a99d454f6de034888244 AS builder
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
	go build -o courier-executor ./cmd/courier-executor

# Coordinator image: the bootstrap OpenCode runtime. The executor contract
# (BOOTSTRAP.md) requires courier-executor, git, and opencode in one image.
# Debian (not alpine) because the opencode npm package ships glibc binaries.
FROM node:24-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6 AS coordinator
ARG OPENCODE_VERSION=1.18.31

RUN apt-get update && \
	apt-get install -y --no-install-recommends ca-certificates git && \
	apt-get clean && rm -rf /var/lib/apt/lists/* && \
	npm install -g opencode-ai@${OPENCODE_VERSION} && \
	groupadd --gid 65532 courier && \
	useradd --create-home --uid 65532 --gid 65532 courier && \
	mkdir -p /workspace && chown courier:courier /workspace

COPY --from=builder /workspace/courier-executor /usr/local/bin/courier-executor

USER 65532:65532
ENV HOME=/home/courier
WORKDIR /workspace

# Manager image: distroless, manager binary only. Kept last so a bare
# `docker build .` produces the operator image; build paths still pass
# --target explicitly.
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS manager
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
