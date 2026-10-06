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

For repository-specific toolchains, set `spec.runtimeImage` on the selected
`LaneProfile`. A non-empty value overrides `--executor-image` for runs on that
lane; an empty value keeps the deployment default. This is the generic runtime
selection seam, so another repository can select a different image without a
Courier code change.

Courier publishes `ghcr.io/misospace/courier-go:0.1.0` as the first dogfood
runtime. It layers Go 1.27.1, `make`, `controller-gen`, and Helm onto the
bootstrap image. It is an optional lane image, not part of the universal
coordinator contract:

```yaml
spec:
  runtimeImage: ghcr.io/misospace/courier-go:0.1.0
```

The image deliberately does not require the `gh` CLI. Forge operations use the
configured forge capability; `git` remains the local repository transport.

Create `courier-github` in the operator namespace with `username` and `token`
keys. The token needs branch push, pull-request read, Checks read, and commit-status
read permissions, but must retain least privilege and must not have
administration or protected-branch bypass. The repository's default branch must
be protected so Courier cannot push or merge directly. Merge authority must
remain with a human maintainer.

Provider or gateway environment variables can be placed in another Secret and
selected with `--executor-environment-secret`.

## Failure evidence (#115)

When evidence capture is enabled, the operator persists a bounded snapshot of a
failed run's unrecoverable workspace state — uncommitted edits, local commits
not on the run branch — as Kubernetes Secrets owned by the `CoderRun`. The
mechanism is invisible when disabled.

The design contract is in DESIGN.md § "Failure evidence for dirty runs
(#115)". The coordinator-pod wiring (#199) is implemented: with the intake
key and service configured, coordinator pods receive the evidence URL, a
per-incarnation token and nonce, their pod UID, and a 45-second termination
grace. The intake listener (#200) and executor capture (#197/#198) are not
implemented, so nothing is persisted until they ship; with either setting
empty the mechanism is invisible.

Enable it with three settings on the operator:

- `--evidence-intake-bind` (for example `:8082`): binds the write-only intake
  listener. Empty disables evidence capture entirely. The chart renders the
  `courier-evidence` ClusterIP Service that coordinator pods reach it on;
  optionally restrict its ingress to coordinator pods with a NetworkPolicy.
  Lands with #200.
- `--evidence-intake-key-secret`: name of a Secret in the operator namespace
  whose `key` entry holds a random HMAC key (for example
  `openssl rand -hex 32`). The operator derives each run's evidence token
  from this key, the run's identity, and a per-incarnation nonce, so a token
  is valid only for the incarnation it was minted for; rotating the key
  invalidates tokens of runs in flight until their next relaunch. The operator
  loads the key once at startup, so a rotation takes effect when the operator
  restarts. The operator fails closed at startup when this is configured and
  the Secret or its `key` entry is missing.
- `--evidence-intake-service`: the URL coordinators are told to POST to.
  Wiring coordinator pods requires it together with
  `--evidence-intake-key-secret`; deriving a default from the chart Service
  lands with #200.

The operator requires `create/get/list/patch/delete` on `secrets` in its
namespaces to persist bundles and derive the `EvidenceCaptured` condition (see
DESIGN.md, Security and boundaries, for what that grant means). Evidence
Secrets carry the label
`courier.misospace.dev/evidence: <run>`, one per coordinator pod incarnation,
and are garbage-collected with the run; the operator also deletes them when a
run reaches `AwaitingReview` or `Done` — the states where its own world
observation has proven the work landed — while a `Verifying` run that falls to
`NeedsHuman` keeps its evidence. A
terminal run with any evidence Secret shows the `EvidenceCaptured` condition;
the condition is informational — retrieve by label, never by condition.

Retrieval and application are manual, and the content is model-authored
untrusted data — treat it like a patch from an untrusted contributor:

```sh
# one Secret per pod incarnation; the manifest names what was captured and
# what was withheld (secrets, binary, over-limit content is never stored)
kubectl -n courier-system get secrets -l courier.misospace.dev/evidence=<run>
kubectl -n courier-system get secret courier-evidence-<run>-<inc> \
  -o jsonpath='{.data.manifest\.json}' | base64 -d

# list entry paths first — the bundle is untrusted content and contains no
# symlink members, so extraction cannot create links
kubectl -n courier-system get secret courier-evidence-<run>-<inc> \
  -o jsonpath='{.data.bundle\.tar\.gz}' | base64 -d | tar -tzf -

# extract OUTSIDE any checkout, never as root. This command is safe only
# because the intake already rejected absolute and `..` entry paths and
# symlink members — never generalize it to arbitrary untrusted tars
kubectl -n courier-system get secret courier-evidence-<run>-<inc> \
  -o jsonpath='{.data.bundle\.tar\.gz}' | base64 -d | tar -xzf - -C /tmp/evidence-<run>
```

Review the extracted files, apply the chosen ones to a fresh checkout with
`git apply`, and publish through normal human review. Evidence is never
adopted automatically: a #97 retry is a fresh attempt, never a restoration of
the old checkout, and deleting a `NeedsHuman` run deletes its evidence —
retrieve before deleting.

To enable native Dispatch discovery, configure the chart's `dispatch` values and
create the referenced Secret with the `DISPATCH_AGENT_TOKEN` value under the
configured key. The queue lane selects Dispatch work; `laneProfile` selects the
Courier `LaneProfile` for created runs. A single deployment can serve several
bindings through `dispatch.lanes`, each pairing a Dispatch `queueLane` with a
Courier `laneProfile` and running its own discovery runner. Each binding admits
into its own LaneProfile, so an escalation lane can use a different profile
(e.g. larger hosted models) without taking capacity from the default lane.
Several bindings may share one `laneProfile`, in which case they share that
profile's concurrency and suspend gate. `queueLane`/`laneProfile` remain the
single-binding shorthand and cannot be combined with `lanes`. Dispatch uses the
agent token for `next-task`, claim/status, unclaim, and task-report requests.

## Manual first run

Install the chart, then apply a lane and a manual run:

```sh
helm dependency build charts/courier
helm install courier charts/courier --namespace courier-system --create-namespace
```

Applying the LaneProfile directly works once the CRD is established; the
GitOps-safe path is the manager's bootstrap config file
(`--bootstrap-lane-profiles-file`, with profiles labeled
`courier.misospace.dev/bootstrap-managed`), which the manager creates and
updates itself after install. Chart plumbing for the file lands with
follow-up work (#74). A bad config file at startup prevents the manager from
starting; a bad edit afterwards is logged and retried on the next pass.

```yaml
apiVersion: courier.misospace.dev/v1alpha1
kind: LaneProfile
metadata:
  name: bootstrap
  namespace: courier-system
spec:
  concurrency: 1
  runtimeImage: ghcr.io/misospace/courier-go:0.1.0
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
observes its exit. The goal gives the coordinator the exact outcome-file path in
executor-owned per-run scratch outside the checkout. Exit `0` (`changes`) moves
the run to `Verifying` only after committed work is verified on the run branch;
the operator then polls the external PR and CI state until it reaches
`AwaitingReview` or `NeedsHuman`. Exit `2` (`needs_decision` or
`blocked_external`) moves the run to `NeedsHuman`. Exit `3` (`no_change_needed`)
moves a resolve-issue run to `AwaitingReview` with the source `in-review`, and a
fix-pr run to `NeedsHuman`, never settling the source; its blocked lifecycle
report parks the PR-fix item as `BLOCKED`/needs-human. The evidence, question,
or missing explanation is posted in full after redaction; only the separate
termination reason is bounded and redacted. Comment posting is best-effort:
failures are logged as `outcome.comment` and do not change the ending.

When no outcome is declared, recoverable endings — uncommitted changes, commits
off the run branch, or no commit and no workspace changes — resume the same
session with a short state message, up to `COURIER_MAX_CONTINUATIONS` times
(default 3). Invalid, zero, or negative values use the default; any positive
value, including 1, is accepted. Without a captured session, these undeclared
endings fail as incomplete. A no-progress guard ends the run as `NeedsHuman` with
reason `looping`; reaching the continuation cap also ends it as `NeedsHuman` with
state history. A crash resumes the session with exponential backoff (default 5s,
`COURIER_RESUME_BACKOFF_SECONDS`), sharing the same continuation budget, before
the run moves to `Failed`; a crash before any session ID is observed moves it to
`Failed` immediately. A child exit code alone is not a declared outcome.

The OpenCode shim resumes a run's session only inside the running pod; it does
not survive pod/model restarts with conversational state. Git commits and the
remote branch are its durable floor until the custom harness replaces it.
