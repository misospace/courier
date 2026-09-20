# Courier

Courier is a Kubernetes operator that runs autonomous coding *coordinators* in
pods. You feed it an issue (or a PR with feedback); it runs a coordinator that
plans, delegates to model sub-agents, opens a PR, drives CI green, and leaves it
mergeable — then hands control back to a human to merge or send back with
feedback.

It is **model-agnostic** (run any models — cloud APIs, local servers, or a mix)
and **source-agnostic** (dispatch, GitHub labels, cron, CLI, and a web UI are all
adapters, none a dependency). It is **local-capable, not local-constrained**:
built so consumer-hardware local models can iterate their way to a good PR, but
equally happy driving cloud models.

Courier replaces a narrow one-shot executor with a long-lived, resumable
coordinator that can watch CI, take feedback, and iterate — the loop that lets a
model actually converge on a mergeable change.

Courier is designed as general infrastructure that anyone can run, not as a
system scoped to one operator's problems. Its core does not require a particular
organization, cluster, model provider, source queue, forge, or hardware: those
choices enter through `LaneProfile`, source adapters, executor implementations,
and forge clients. The GitHub + OpenCode path currently checked in is the first
reference deployment and a replaceable bootstrap configuration, not a product
boundary.

## Status

Bootstrap MVP. Courier can admit a `CoderRun`, claim its source work, derive or
adopt a branch, launch the temporary OpenCode coordinator, and map the completed
pod back to review/needs-human state. The resumable custom harness and liveness
recovery remain later work. See [BOOTSTRAP.md](./BOOTSTRAP.md) for the temporary
executor contract and a manual first-run example. Contributor conventions are
in [AGENTS.md](./AGENTS.md).

## Install

Courier ships a Helm chart built on the
[bjw-s common library](https://bjw-s-labs.github.io/helm-charts/) — consumable
by any Helm workflow, including Argo CD:

```sh
helm dependency build charts/courier
helm install courier charts/courier --namespace courier-system --create-namespace
```

The chart installs the `CoderRun`/`LaneProfile` CRDs, the manager's RBAC, and
the Deployment. Deployment-specific choices — forge remote, credential secrets,
coordinator image — are plain values in `charts/courier/values.yaml`; nothing
assumes a particular cluster or GitOps tooling.

## Design

- **Inform, don't constrain.** Give the coordinator context and tools; trust its
  judgment. Prompts are goals plus tools, not scaffolding.
- **The world is the source of truth.** Git, the PR, and CI are ground truth;
  internal state is a hint reconciled toward the world.
- **No wall-clock deadlines.** Bound *stuck* (via activity liveness), not
  *duration*. Long runs on slow hardware are fine.
- **Thin harness.** Git for durable state, the log stack for transcripts,
  Kubernetes for lifecycle. Build only what nothing else provides.

See [DESIGN.md](./DESIGN.md) for the full architecture — CRDs, reconcile loop,
checkpointing, liveness, contention, sources, and MCP surface.

## Custom resources

- **`CoderRun`** — one immutable attempt at one goal (resolve an issue or fix a
  PR). Carries its own resumable checkpoint in status; self-reaping.
- **`LaneProfile`** — a reusable model ensemble (which models fill the
  coordinator/coder/reviewer roles) plus concurrency and runtime framing. The
  seam that keeps Courier model-agnostic.

## License

Apache-2.0. See [LICENSE](./LICENSE).
