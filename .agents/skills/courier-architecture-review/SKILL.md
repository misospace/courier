---
name: courier-architecture-review
description: Review Courier Kubernetes operator, CoderRun and LaneProfile APIs, controller reconciliation, generated CRDs/RBAC, Helm chart, and coordinator lifecycle changes.
---

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

## Focused review procedure

- Treat DESIGN.md as the current-state architecture source of truth. Read the sections relevant to the changed subsystem, not the entire document blindly. If a PR materially changes a documented architectural boundary, contract, security property, durable invariant, or capability, require the same PR to update the current-state prose. Do not require a dated decision-log entry: Git history and linked issues/PRs preserve implementation history. A separate architecture decision record is optional and reserved for rare, durable cross-cutting trade-offs that cannot be understood from the current design and its history.
- For API changes under api/, verify generated deepcopy/manifests output is refreshed with `sh hack/tools.sh generate` (or `make manifests generate` where make is installed) and committed. Do not hand-edit generated CRDs in charts/courier/crd-manifests/.
- For RBAC changes, verify the kubebuilder marker, generated config/rbac/role.yaml, and charts/courier/values.yaml remain in sync.
- For controller changes, follow the reconciliation path and state transitions through success, retry, failure, cancellation, and terminal states. Check idempotency and whether observed Git/PR/CI state remains authoritative where DESIGN.md says it is.
- For coordinator or lane changes, inspect the matching DESIGN.md section for capacity, provider availability, checkpoint/recovery, dispatch-boundary, and security assumptions before deciding whether behavior is a regression.
- Run `mise run test` (or `make test`) for behavior changes; use `sh hack/tools.sh generate` for API changes and confirm generated files are clean afterward.
