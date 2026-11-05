# The rebase lane (#715)

A dedicated execution path for mechanical rebase work. When Dispatch classifies
a queued PR followup as `REBASE` (misospace/dispatch#1327), Courier must never
run it as ordinary PR repair: the fix-pr path merges the base into the branch
during workspace preparation and instructs the coordinator to never rebase or
force-push — the exact opposite of the task's contract. This document records
the version-safe contract Courier implements, the supported version
combinations, and the operator configuration that enables the lane. It pairs
with misospace/dispatch#1327 (queue support) and misospace/dispatch#1532
(strict rebase-only intake and negotiated handout eligibility).

## What a rebase run is

A `REBASE` PR-fix item materializes as a CoderRun with `spec.mode: rebase` and
the PR number as `spec.ref`. The mode is preserved end to end — discovery, run
creation, recovery (claim receipt, capacity settlement, retry requeue all keep
the `followup-pr` transport type and the exact `prFixItem` receipt), execution,
and outcome reporting — so a rebase attempt can never silently become fix-pr
work.

Execution is trusted deterministic code, not a model session. The executor
binary takes an explicit branch before workspace preparation:

1. **Workspace preparation adopts the PR branch verbatim** (`SkipBaseSync`):
   no base merge is ever started. Fix-pr preparation is unreachable from this
   path.
2. **Identity checks** run first: the fork/head-ownership guard, the live
   pull-request base guard (the actual PR base is read through the forge
   capability, exactly as fix-pr reads it), and the admission head.
3. **The rebase engine** (`internal/rebase`) re-reads the remote head, leases
   the attempt to it, and replays the branch onto the pull request's live
   base. Conflicted hunks are classified by closed rules: a one-sided edit
   (the diff3 base section proves which side moved) and a comment-only overlap
   where one side is a subset of the other. Anything else — two-sided code
   edits, deletions against modifications — stops the attempt for a human with
   the file list. The rules never pick a side for
   code and never merge code text. When the operator explicitly enables
   model-assisted conflict resolution (below), the engine may hand one round's
   rejected set to the configured coordinator model for a single validated
   resolution turn; the closed rules, the validation, the lease, and the
   bounded checks all still govern what may land, and a disabled (nil)
   resolver is exactly the mechanical-only behavior.
4. **Bounded repository checks** run before publication: rebase fully
   completed, tree clean, branch identity unchanged, base contained, no commit
   lost, plus a bounded `go build ./...` when the repository is a Go module. A
   build failure in a PR-touched path is a semantic verdict; a failure only
   outside them predates the replay and does not block publication.
5. **Publication** is `git push --force-with-lease=refs/heads/<branch>:<head>`
   to the existing PR branch and nothing else. The branch must still carry the
   leased head at push time; a concurrent writer's push is refused, never
   overwritten, and never silently recreated if the branch vanished.

### Outcomes

| Engine outcome | Executor ending | Run phase | Source report |
| --- | --- | --- | --- |
| published | exit 0 | Verifying | (world verification settles it) |
| up-to-date | exit 3, declared no-change | Verifying | (world verification settles it) |
| semantic conflict | exit 2, needs-decision | NeedsHuman | `blocked` + BLOCKED mark (a human decides) |
| remote head moved | exit 1 | Failed | `failed` (the source's attempt cap governs) |
| failed | exit 1 | Failed | `failed` |

One attempt per conflicting episode: the engine never retries itself, and a
declined or failed attempt returns the episode to the source's own bounded
attempt policy.

## The rebase-only intake contract

Capability means the connector can execute the task safely — not that it can
parse the label. The contract is negotiated per Dispatch binding, never at
startup, so an optional rebase lane can never become a general dependency.
Courier implements the wire contract Dispatch shipped in misospace/dispatch#1533
(the versioned intake handshake of misospace/dispatch#1532).

A binding configured with `--dispatch-rebase-lane <queueLane>` declares the
intake contract on every `next-task` call:

```
GET /api/agents/<agent>/next-task?...&work_scope=rebase-only&work_scope_version=1&capabilities=rebase-executor:1
```

- `work_scope=rebase-only` — the poll serves only mechanical rebase work.
- `work_scope_version=1` — the version of the `work_scope` shape.
- `capabilities=rebase-executor:1` — Courier implements the mechanical rebase
  execution contract (the capability is proof, not a request).

A deployment that has adopted the contract answers with a versioned intake
acknowledgment on the task:

```json
{
  "intake": {
    "version": 1,
    "scope": "rebase-only",
    "scopeEnforced": true,
    "capabilities": [
      { "name": "rebase-executor", "requested": 1, "granted": true }
    ]
  }
}
```

Fail-closed rules — none of these is ever read as successful negotiation:

- **Absent intake acknowledgment**: the declaration was ignored (an older
  Dispatch). Offered work is declined unclaimed; idle stays idle (there is
  nothing to decline), with the missing acknowledgment surfaced in connector
  diagnostics.
- **Unknown contract version**: any `intake.version` other than `1` fails
  negotiation.
- **Wrong scope**: any `intake.scope` other than `rebase-only` fails
  negotiation.
- **Unenforced scope**: `intake.scopeEnforced` other than `true` fails
  negotiation — "declared but not enforceable" is never read as honored.
- **Capability not granted**: a `rebase-executor` verdict with
  `granted: false` unlocks nothing.
- **Capability requested at another version**: a `rebase-executor` verdict
  with `requested` other than `1` is a different claim and fails negotiation.
- **Ignored scope**: ordinary work returned to a negotiated rebase-only
  connector is a protocol error, never accepted and never released on the
  item's behalf.
- **Missing receipt**: a rebase offer without a valid `prFixItem` receipt is a
  protocol error; it can never be settled truthfully, so it is never accepted.
- **Missing task type**: an unsupported task type fails discovery as before.

### Declining incompatible work

A rebase offer Courier cannot safely execute (capability not negotiated,
negotiation failed) is declined **before** any claim, run, branch change, or
publication. The decline uses the defined release path: the generation-fenced
`waiting-external` mark on the exact receipt (`repo`, PR, generation), which
keeps the item discoverable for a compatible rebase executor without consuming
attempt budget, fabricating completion, or parking a human.

If the deployment refuses that release — an older Dispatch without the
recoverable-wait mark, or any refusal Courier cannot prove moot — the decline
surfaces with the full ownership evidence (receipt id, generation, handout
token) and latches for that generation: one release attempt per generation per
process, never an indefinite retry. The source's stale-handout reclaimer
remains the documented bounded recovery path for the unacknowledged handout.
Courier never fabricates success, resets generations, steals ownership, or
leaves the release retrying forever.

## Supported version combinations

| Courier | Dispatch | Behavior |
| --- | --- | --- |
| old (≤ 0.1.16) | any | Unchanged legacy behavior — including the unsafe REBASE→fix-pr mapping this change fixes. Upgrade Courier first. |
| new | old (no contract) | Ordinary lanes unaffected. A rebase-lane binding declares the rebase-only intake, gets no intake acknowledgment, and declines every offer unclaimed; ordinary work on that binding is also declined while the declaration goes unanswered. |
| new | new (contract adopted) | REBASE work flows as rebase runs under the negotiated v1 intake acknowledgment. |
| new | new, contract version > 1 | Fails closed: unknown contract version is declined, never guessed at. |

Both release orders fail safe, and neither release requires a simultaneous
upgrade: rebase intake stays disabled until both peers support the contract,
while every non-rebase lane keeps operating with older Dispatch deployments
(they send no intake declaration at all — their discovery bytes are
unchanged).

## Operator enablement

No production configuration changes with this feature. To enable the lane when
the paired Dispatch release ships (see #1327/#1532 for the server side — the
agent allowlist, the serialized concurrency-one gate, and rebase-only intake):

1. Add a Dispatch binding for a dedicated agent (e.g. `courier-rebase`) with
   its own queue lane:
   `--dispatch-lane-binding rebase-lane:courier-rebase:courier-rebase`
2. Declare it a rebase executor:
   `--dispatch-rebase-lane rebase-lane`
3. Create the LaneProfile the binding names — the planned shape is a local
   lane with concurrency 1 and a single coordinator role, no delegation:

   ```yaml
   apiVersion: courier.misospace.dev/v1alpha1
   kind: LaneProfile
   metadata:
     name: courier-rebase
   spec:
     concurrency: 1
     roles:
       coordinator: litellm/qwen3.8-27b
   ```

   The coordinator role satisfies the lane schema and the pod contract;
   rebase execution itself is the trusted deterministic path.
4. Keep the DISPATCH_REBASE_AGENTS allowlist on the Dispatch side in step with
   #1327/#1532 so the server grants rebase tasks only to this agent.

### Model-assisted conflict resolution (#732)

Off by default. When `COURIER_REBASE_MODEL_ASSIST` is enabled (one of `1`,
`true`, `enabled`) and `COURIER_MODEL_GATEWAY_URL` names the run's
OpenAI-compatible gateway (`COURIER_MODEL_GATEWAY_API_KEY` authenticates it),
the engine consults the configured coordinator model exactly once per
conflict round for the files the mechanical rules rejected. The request
carries repository content only: the bounded PR diff, the base-side diff
restricted to the conflicted paths, the marked conflict files, and the run
goal. No credentials ever cross the engine boundary. The answer must be one
JSON object resolving exactly the rejected files with marker-free full file
content. Anything else — a refusal, a foreign path, leftover markers,
oversized content — is treated as a refusal, and the attempt stops for a
human with the attempted-and-did-not-resolve summary. When the
conflict set includes `go.mod`/`go.sum` and the `go` tool exists, the
allowlisted `go mod tidy` runs once inside the workspace after the
resolution is written, and its outputs are staged again. A tidy failure
leaves the conflict unresolved. A resolver transport failure is a `failed`
attempt, never a semantic conflict, so the source's failure policy, not a
human queue, governs it. Every model-assisted turn is reported on the
`rebase.model.assist` run event (model, turns, files, token usage),
for published and unresolved endings alike.

The cross-repository capability negotiation that would let Dispatch route
model-backed rebase work is not settled yet (misospace/dispatch#1540). The
wire contract stays `rebase-executor:1`, and a deterministic-only run must
never be reported or described as model execution. This mode stays an
explicit operator choice until that contract lands.

## Limitations of this release

- The secure-mode broker path does not accept rebase runs; the rebase lane
  requires the legacy executor path, which is where the deterministic engine
  lives.
- The bounded check set recognizes Go modules only; repositories without a
  recognized toolchain are published after the structural checks alone.
- Mechanical conflict resolution is deliberately narrower than #1327's full
  wish list (lockfiles and version strings need their tools and are classified
  semantic here, stopping for a human). The rules can widen only with new
  proofs, never by picking a likely side for code. Model-assisted resolution,
  when explicitly enabled, is a bounded escape hatch beside the rules: one
  validated turn per round, never a widening of the rules themselves.
