# AGENTS.md

Conventions for anyone — human or model — working in this repository. Read this
and [DESIGN.md](./DESIGN.md) before starting. DESIGN.md describes the intended
current architecture. When code and DESIGN.md disagree, determine which is
stale and update the appropriate source rather than treating either as
immutable.

## Keeping DESIGN.md current

`DESIGN.md` describes the current architecture, contracts, invariants, and
rationale maintainers need to understand the system. It is not a changelog or
an issue-by-issue history.

- Update `DESIGN.md` in the same PR only when a change materially alters a
  documented architectural boundary, public contract, security property,
  durable invariant, or capability.
- Edit the relevant current-state section in place. Keep the rationale needed
  to understand the design; do not narrate the implementation history.
- **Do not append dated entries for individual PRs, issues, bug fixes, tests, or
  routine implementation decisions.** Git history and linked issues/PRs are
  the historical record.
- A separate architecture decision record is optional and should be rare:
  use one only for a durable cross-cutting trade-off whose rationale cannot be
  understood from the current design and its history. Never create one by
  default.
- Reviewers: check that the resulting document remains accurate. Do not require
  a documentation edit for a local implementation detail or a change already
  covered by the current-state prose.

## What this is

A Kubernetes operator (Go, controller-runtime) plus a coordinator harness. The
operator reconciles `CoderRun` objects; the harness runs the coordinator model
that plans, delegates, and produces PRs. See DESIGN.md for the full architecture.

## Layout

```
api/v1alpha1/          CRD types (CoderRun, LaneProfile) + generated deepcopy
charts/courier/        Helm chart (bjw-s/common) — the install path
charts/courier/crd-manifests/
                       generated CRDs, rendered as templates so helm upgrade
                       evolves them (do not hand-edit)
cmd/main.go            manager entrypoint
internal/controller/   reconcilers
config/rbac/           generated RBAC reference
hack/                  codegen boilerplate
.mise/                 pinned dev toolchain: config.toml + mise.lock (#416)
```

`charts/courier/values.yaml` carries the chart's RBAC rules; when you change a
`+kubebuilder:rbac` marker, regenerate `config/rbac/role.yaml` and mirror the
change in the chart's values.

## Build, test, generate

- `hack/bootstrap.sh` — idempotent one-shot dev setup after a fresh checkout
  (#416): pinned mise/Go/Helm, controller-gen, setup-envtest, and envtest
  assets; does not require GNU make. Go and Helm versions live in
  `.mise/config.toml` + `.mise/mise.lock`; the Makefile remains the authority
  for pinned controller-gen/setup-envtest versions.
- `mise run check` — CI-equivalent verification (generated-code diff,
  gofmt, vet, build, image-gating scripts); requires no GNU make, reads pins
  from the Makefile, and installs repo-local tools itself. `mise run test` is
  the authoritative race-detector suite with envtest assets; it also requires
  no GNU make and fails fast with the missing prerequisite if no C compiler is
  available (the race suite needs cgo). Reuse bootstrap environment in later
  commands with `mise exec -- <command>`.
- `make build` — generate + fmt + vet + build the manager (requires GNU make).
- `make test` — generate + fmt + vet + `go test -race ./... -coverprofile
  cover.out` (requires GNU make).
- `make manifests generate` — regenerate CRDs, RBAC, and deepcopy after any
  change to `api/` (requires GNU make). `sh hack/tools.sh generate` is the
  make-free equivalent. **Commit the regenerated output**; CI fails if it is
  stale.
- `make run` — run the manager against the current kubeconfig (requires GNU make).

Go is the only language. The harness drives models over litellm's
OpenAI-compatible HTTP API, so no Python SDK is needed anywhere.

## Design principles that constrain code review

The detailed review checklist lives in
[.agents/skills/courier-architecture-review/SKILL.md](.agents/skills/courier-architecture-review/SKILL.md).
Load it for controller, API/CRD, RBAC, Helm chart, coordinator lifecycle, or
architecture changes. DESIGN.md remains the north star; update it in the same
PR when a documented architectural decision or capability changes.

## Git and PR conventions

- **Conventional commits**: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`,
  `test:`.
- **No attribution lines of any kind** in commits or PR descriptions — no
  `Co-authored-by`, no `Assisted-by`, no session links. Ever.
- **Config/manifest edits are bare**; put rationale in the PR body, never in
  inline file comments.
- **PR body length scales to the diff**: a one-line change gets a one-line body;
  reserve structure for medium or risky changes.
- **Pin GitHub Actions to full semver tags** (`@v6.1.0`), never bare majors (`@v6`) or commit SHAs.
- Keep the branch synced to base before opening or updating a PR.

## Issue conventions (for work you file)

- Every issue names the **real file paths** it touches — never guessed — so the
  reviewer can vouch scope.
- Give each issue a clear **done-condition** an implementer can check.
- Label for the queue: priority (`pN`), a `status`, and a `type`. Unlabelled
  issues are invisible to the scheduler.
- Keep issues **bounded and file-scoped**. A task that sprawls across the whole
  codebase should be split; scoped-edit models converge on small briefs and
  thrash on open-ended ones.
