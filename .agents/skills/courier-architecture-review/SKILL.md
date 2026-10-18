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

- Treat DESIGN.md as the architecture source of truth. Read the sections relevant to the changed subsystem, not the entire document blindly. If a PR changes an architectural decision or a capability documented there, require the same PR to update the current-state prose and add a dated decision-log entry with the rationale and linked issue.
- For API changes under api/, verify generated deepcopy/manifests output is refreshed with make manifests generate and committed. Do not hand-edit generated CRDs in charts/courier/crd-manifests/.
- For RBAC changes, verify the kubebuilder marker, generated config/rbac/role.yaml, and charts/courier/values.yaml remain in sync.
- For controller changes, follow the reconciliation path and state transitions through success, retry, failure, cancellation, and terminal states. Check idempotency and whether observed Git/PR/CI state remains authoritative where DESIGN.md says it is.
- For coordinator or lane changes, inspect the matching DESIGN.md section for capacity, provider availability, checkpoint/recovery, dispatch-boundary, and security assumptions before deciding whether behavior is a regression.
- Run make test for behavior changes; use make manifests generate for API changes and confirm generated files are clean afterward.
