# Build the manager and executor binaries.
FROM golang:1.26.6 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
	go build -a -o manager cmd/main.go && \
	go build -a -o courier-executor ./cmd/courier-executor

# Coordinator image: the bootstrap OpenCode runtime. The executor contract
# (BOOTSTRAP.md) requires courier-executor, git, and opencode in one image.
# Debian (not alpine) because the opencode npm package ships glibc binaries.
FROM node:22-bookworm-slim AS coordinator
ARG OPENCODE_VERSION=1.18.31

RUN apt-get update && \
	apt-get install -y --no-install-recommends ca-certificates git && \
	apt-get clean && rm -rf /var/lib/apt/lists/* && \
	npm install -g opencode-ai@${OPENCODE_VERSION} && \
	useradd --create-home --uid 65532 courier && \
	mkdir -p /workspace && chown courier:courier /workspace

COPY --from=builder /workspace/courier-executor /usr/local/bin/courier-executor

USER courier
ENV HOME=/home/courier
WORKDIR /workspace

# Manager image: distroless, manager binary only. Kept last so a bare
# `docker build .` produces the operator image; build paths still pass
# --target explicitly.
FROM gcr.io/distroless/static:nonroot AS manager
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
