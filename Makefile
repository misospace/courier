# Courier operator Makefile.

IMG ?= ghcr.io/misospace/courier:latest
EXECUTOR_IMG ?= ghcr.io/misospace/courier-opencode:latest
OPENCODE_VERSION ?= 1.18.31

# Tool versions.
CONTROLLER_TOOLS_VERSION ?= v0.16.5
ENVTEST_VERSION ?= release-0.19
ENVTEST_K8S_VERSION ?= 1.31.x!

LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
SETUP_ENVTEST ?= $(LOCALBIN)/setup-envtest

.PHONY: all
all: build

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CRDs into the Helm chart and the RBAC reference into config/.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd \
		paths="./..." \
		output:crd:artifacts:config=charts/courier/crd-manifests \
		output:rbac:artifacts:config=config/rbac

.PHONY: generate
generate: controller-gen ## Generate deepcopy code.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet ## Run tests.
	go test -race ./... -coverprofile cover.out

.PHONY: envtest
envtest: setup-envtest ## Download Kubernetes assets for envtest.
	@KUBEBUILDER_ASSETS=$$($(SETUP_ENVTEST) use -p path $(ENVTEST_K8S_VERSION)) \
		go test -race ./internal/controller -coverprofile cover.out

.PHONY: govulncheck
govulncheck: ## Run the Go vulnerability scanner.
	go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build the manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run the manager against the current kubeconfig.
	go run ./cmd/main.go

.PHONY: docker-build
docker-build: ## Build the operator image.
	docker build --target manager -t $(IMG) .

.PHONY: docker-build-coordinator
docker-build-coordinator: ## Build the bootstrap coordinator image (courier-executor + git + opencode).
	docker build --target coordinator --build-arg OPENCODE_VERSION=$(OPENCODE_VERSION) -t $(EXECUTOR_IMG) .

##@ Dependencies

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: controller-gen
controller-gen: $(LOCALBIN) ## Install controller-gen into ./bin.
	@test -x $(CONTROLLER_GEN) || \
		GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: setup-envtest
setup-envtest: $(LOCALBIN) ## Install setup-envtest into ./bin.
	@test -x $(SETUP_ENVTEST) || \
		GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)
