# Bootstrap dogfood path

The MVP uses a temporary headless OpenCode coordinator so Courier can begin
working on Courier before the resumable harness is complete.

This is one bootstrap assembly, not a required Courier stack. Courier is general
infrastructure intended for any operator to run; it is not scoped to the
original operator's organization or problems. Its core is model-, provider-,
source-, forge-, and hardware-agnostic, so operators can supply different
adapters, credentials, model gateways, and executor images without changing the
lifecycle machinery. The GitHub remote template, Secret names, OpenCode image,
and example LiteLLM model below are deployment defaults for this first dogfood
path and are all replaceable.

## Runtime requirements

The image selected with `--executor-image` must contain:

- `/usr/local/bin/courier-executor` built from `./cmd/courier-executor`;
- `git`;
- the `opencode` binary; and
- any OpenCode configuration needed to reach the models named by a
  `LaneProfile`.

The published bootstrap image is
`ghcr.io/misospace/courier-opencode:0.1.0`; replace `0.1.0` with the Courier
release selected for the deployment. Build it from this repository's
`coordinator` Dockerfile target (Debian-based: the opencode npm package ships
glibc binaries) and publish it, or point `--executor-image` at an equivalent
image that satisfies the contract above:

```sh
make docker-build-coordinator EXECUTOR_IMG=ghcr.io/misospace/courier-opencode:0.1.0
```

Create `courier-github` in the operator namespace with `username` and `token`
keys. The token needs branch push, pull-request read, Checks read, and commit-status
read permissions, but must retain least privilege and must not have
administration or protected-branch bypass. The repository's default branch must
require independent human approval.

Provider or gateway environment variables can be placed in another Secret and
selected with `--executor-environment-secret`.

## Manual first run

Install the chart, then apply a lane and a manual run:

```sh
helm dependency build charts/courier
helm install courier charts/courier --namespace courier-system --create-namespace
```

```yaml
apiVersion: courier.misospace.dev/v1alpha1
kind: LaneProfile
metadata:
  name: bootstrap
  namespace: courier-system
spec:
  concurrency: 1
  roles:
    coordinator: litellm/your-model
  framing: keep parallelism modest and leave the pull request ready for review
---
apiVersion: courier.misospace.dev/v1alpha1
kind: CoderRun
metadata:
  name: courier-issue-22
  namespace: courier-system
spec:
  mode: resolve-issue
  source: manual
  workItemID: github-issue-22
  repo: misospace/courier
  ref: 22
  lane: bootstrap
```

The operator derives the resolve branch, creates the coordinator pod, and
observes its exit. Exit `0` moves the run to `Verifying`, where the operator
polls the external PR and CI state until it reaches `AwaitingReview` or
`NeedsHuman`; exit `2` moves it to `NeedsHuman`; any other exit moves it to
`Failed`.

The OpenCode shim does not survive pod/model restarts with conversational state.
Git commits and the remote branch are its durable floor until the custom harness
replaces it.
