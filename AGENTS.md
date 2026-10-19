# AGENTS.md

Conventions for anyone — human or model — working in this repository. Read this
and [DESIGN.md](./DESIGN.md) before starting. DESIGN.md is the north star; when
code and DESIGN.md disagree, DESIGN.md wins unless an issue says otherwise.

## Keeping DESIGN.md current

DESIGN.md is a living document, not a one-time spec. Keep it in step with the
code in the same change that moves it:

- If a PR alters an architectural decision, or adds or removes a capability
  DESIGN.md describes, **update DESIGN.md in that same PR.** A change that drifts
  from the doc without updating it is incomplete.
- When a decision is made, reversed, or superseded, add a dated entry (with the
  *why*, and a linked issue) to the **Decisions** log at the end of DESIGN.md.
  The body of the doc describes what is true now; the log preserves the
  reasoning, and git history holds what was superseded — so edit the body freely
  rather than accreting caveats.
- Reviewers: check whether a change contradicts or extends DESIGN.md and whether
  the doc was updated to match.

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
  assets. Go and Helm versions live in `.mise/config.toml` + `.mise/mise.lock`
  and controller-gen/setup-envtest versions in the `Makefile`; do not restate
  them elsewhere.
- `mise run check` — CI-equivalent verification (generated-code diff,
gofmt, vet, build, image-gating scripts). `mise run test` —
authoritative suite (`make test` with envtest assets). Reuse bootstrap
environment in later commands with `mise exec -- <command>`.
- `make build` — generate + fmt + vet + build the manager.
- `make test` — generate + fmt + vet + `go test ./...`.
- `make manifests generate` — regenerate CRDs, RBAC, and deepcopy after any
  change to `api/`. **Commit the regenerated output**; CI fails if it is stale.
- `make run` — run the manager against the current kubeconfig.

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
