# AGENTS.md

Conventions for anyone — human or model — working in this repository. Read this
and [DESIGN.md](./DESIGN.md) before starting. DESIGN.md is the north star; when
code and DESIGN.md disagree, DESIGN.md wins unless an issue says otherwise.

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
```

`charts/courier/values.yaml` carries the chart's RBAC rules; when you change a
`+kubebuilder:rbac` marker, regenerate `config/rbac/role.yaml` and mirror the
change in the chart's values.

## Build, test, generate

- `make build` — generate + fmt + vet + build the manager.
- `make test` — generate + fmt + vet + `go test ./...`.
- `make manifests generate` — regenerate CRDs, RBAC, and deepcopy after any
  change to `api/`. **Commit the regenerated output**; CI fails if it is stale.
- `make run` — run the manager against the current kubeconfig.

Go is the only language. The harness drives models over litellm's
OpenAI-compatible HTTP API, so no Python SDK is needed anywhere.

## Design principles that constrain code review

These are not style preferences; a change that violates them is wrong.

- **Inform, don't constrain.** Do not add hard caps, gates, or governors on
  model behavior. Bound *stuck* (liveness), never *duration* (no wall-clock
  timeouts on runs). Prompts are goals plus tools, not paragraphs of scaffolding.
- **The world is the source of truth.** Never trust internal/checkpoint state
  over git, the PR, or CI. On resume, re-read the world and let it win on
  conflict.
- **Model- and source-agnostic.** Nothing in the core may hardcode a provider, a
  model, a forge, or "local." Provider/hardware specifics belong in a
  `LaneProfile`; source specifics belong in a source adapter.
- **Thin harness.** Prefer git, the log stack, and Kubernetes primitives over new
  machinery. Do not add a PVC where git + a CR-status checkpoint suffice.
- **The coordinator can push and open PRs, never merge.** Merge is a human gate.

## Git and PR conventions

- **Conventional commits**: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`,
  `test:`.
- **No attribution lines of any kind** in commits or PR descriptions — no
  `Co-authored-by`, no `Assisted-by`, no session links. Ever.
- **Config/manifest edits are bare**; put rationale in the PR body, never in
  inline file comments.
- **PR body length scales to the diff**: a one-line change gets a one-line body;
  reserve structure for medium or risky changes.
- **Pin GitHub Actions to `@vN` tags**, not commit SHAs (a bot pins SHAs later).
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
