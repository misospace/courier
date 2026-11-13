# Native graduation, coexistence, and rollback runbook (#823)

This document is the operator decision runbook for moving work from legacy v1
execution to native execution. Parent: #818. Evidence issues: #102 and #731.
Ownership isolation contract: #632.

This is a decision and procedure document. It is not a deployment script. Every
step below names existing chart keys and controller flags. It proposes no new
migration machinery.

## Terms

- Legacy v1 execution. One operator process runs a single coordinator pod that
  contains OpenCode, the model credentials, and git. HARNESS.md labels this
  mode explicitly insecure.
- Native execution. The operator process runs with `--secure-mode` (chart key
  `secure.enabled`). Each run gets a trusted control pod, an untrusted worker
  pod, and a per-run broker pod in one dedicated run namespace.
- Run namespace. The single namespace that holds all run workloads of a native
  release. Configure it with `secure.runNamespace` (chart) or `--run-namespace`
  (flag).
- Ownership boundary. The set of run namespaces one release reconciles.
  Configure it with `ownership.namespaces` (chart) or `--owned-namespace`
  (repeatable flag). Introduced by #632.
- Canary. A second chart release that runs native execution for a bounded
  slice of sources or lanes while the v1 release keeps serving the rest.
- Cutover. The decision to make native execution the primary path for a slice.
- Rollback. Returning a slice to v1 execution without losing source ownership,
  pull request branches, or durable run history.

## State of the system at release v0.1.25

The table maps what ships today. Chart values live in
`charts/courier/values.yaml`. Controller flags live in `cmd/main.go`.

| Concern | Legacy v1 | Native | Where configured |
| --- | --- | --- | --- |
| Process mode | Default. `--secure-mode` off. | `--secure-mode` on. One process runs one mode, never both. | `cmd/main.go` (`--secure-mode`), `secure.enabled` |
| Coordinator topology | Single pod with OpenCode and credentials. | Control pod, worker pod, broker pod per run. | `executor.openCode.agent`, `secure.harnessImage`, `secure.laneRuntimeImage` |
| Run workloads | In every namespace the controller watches. | In `secure.runNamespace` only. The chart fails to render if it is empty. | `secure.runNamespace`, `--run-namespace` |
| Ownership boundary | Default `owns()` claims every namespace when `ownership.namespaces` is empty. | Controller reconciles only owned namespaces. A foreign run is a strict no-op before any read or write. | `ownership.namespaces`, `--owned-namespace`; `owns()` in `internal/controller/coderun_controller.go` |
| Model access | Model credentials in the coordinator pod. | Model gateway in trusted control only. Empty gateway makes the harness fail closed. | `secure.modelGateway`, `--model-gateway-*` |
| Admission sources | Shared registry: `manual`, `adhoc`, `dispatch`, `label`, `cron`. | Same registry. Sources are not mode-specific. | `internal/source/`, `internal/controller/admission.go` |
| Status writes | Coordinator writes status. | Broker writes status through a trusted gRPC boundary on port 8444. TokenReview uses the chart ClusterRole. | HARNESS.md section 4, `secure.brokerTokenReviewClusterRole` |
| Terminal result | Operator verifies world state. | Trusted `NativeResult` classification supplements operator verification. Publication only moves a run to `Verifying`. | `NativeResult` in `api/v1alpha1/coderun_types.go` |
| Retention | `retention.done: 5m`, `retention.failed: 168h`, `retention.needsHuman: 168h`, `retention.auditKeep: 5`. | Same flags. | `--done-retention`, `--failed-retention`, `--needs-human-retention`, `--terminal-audit-keep` |
| CRD management | Release renders CRDs with `helm.sh/resource-policy: keep`. | Disable `crds.enabled` on the canary so one release owns CRDs out of band. | `crds.enabled` |
| Cluster-scoped names | Leader election ID `courier.misospace.dev`. | Set a distinct ID per release that shares an operator namespace. Set a distinct `courier-broker-tokenreview` ClusterRole name per release. | `ownership.leaderElectionID`, `secure.brokerTokenReviewClusterRole` |

Two releases with separate operator namespaces get separate leader leases. Two
releases in one operator namespace need distinct `ownership.leaderElectionID`
values. Agent names alone do not isolate controllers. Do not deploy a second
operator on agent-name isolation alone (#632).

The chart cross-validates ownership at render time. It fails if the
`--owned-namespace` arguments disagree with `ownership.namespaces`, if
`secure.runNamespace` sits outside the ownership boundary, or if discovery is
enabled while the release namespace is unowned
(`charts/courier/templates/common.yaml.tpl`).

## Go/no-go evidence

A criterion is go only when live evidence exists. Passing tests are
implementation evidence. They never count as live proof.

| # | Criterion | Evidence source | Test proof | Live proof | Status |
| --- | --- | --- | --- | --- | --- |
| 1 | `status.heartbeat` advances at the coalesced cadence during real stream and tool activity. | #102 criterion 1, #731 | Status reporter coalescing and cadence under test (`internal/harness/status.go`, `internal/status/status.go`). | One heartbeat persisted in the #797 canary. Sustained cadence over a long tool or subagent episode is unobserved. | No-go. #731 is open and blocked on live evidence. |
| 2 | Long-tool liveness and failure behavior. The reaper treats a stalled run conservatively and stays bounded. | #731, #102 criterion 2 | `internal/controller/liveness.go` and `liveness_test.go` cover reaping decisions and UID fencing. | No recorded live stall-and-reap on a native run. | No-go for live proof. |
| 3 | Meaningful checkpoint and resume. | HARNESS.md section 7 | Checkpoint types and ordering tests (`api/v1alpha1/coderun_types.go`, `native_termination_test.go`). | Restart and resume of a real interrupted run is unobserved in production. | No-go for live proof. |
| 4 | Publication `lastCommit`. Only a broker-confirmed remote OID is recorded. | #102, HARNESS.md section 7 | Publication verification tests. | A native run that publishes to a real pull request with confirmed `status.lastCommit` is unobserved. | No-go for live proof. |
| 5 | Current-head CI and PR handoff after publication. | #102 | Tests cover the `Verifying` handoff contract. | No native run carried a published branch through CI to merge. | No-go for live proof. |
| 6 | Trusted status and credential boundary. The broker can patch only one named run status subresource. | HARNESS.md section 4 | `TestNativeCanaryDoesNotMutateForeignNamespaceRun` and the cross-direction isolation tests in `internal/controller/native_canary_isolation_test.go`. | Native canaries have run in production beside v1, but no deployment carries a complete acceptance record for the boundary. | No-go for live proof. Isolation is the strongest tested area. |

Rule for reading this table: a criterion moves to go when #731 or #102 records
live observations from a deployed run, with the deployed run version named.
Keep the two evidence classes separate in every entry.

Decision today: no-go for cutover. Do not start a broad canary to reproduce
the #731 environment failure. If fresh defects appear, file them narrowly, as
#731 instructs.

## Coexistence invariants

These invariants must hold while v1 and native run side by side. Each one has
a code or test anchor.

1. One controller per run namespace. A run outside the owned set causes no
   writes, no source calls, no pod deletion, and no secret reads. Anchor:
   `owns()` in `internal/controller/coderun_controller.go`, first check in
   `Reconcile`. Tests in `internal/controller/native_canary_isolation_test.go`
   cover both directions.
2. One active source attempt per run. The controller claims before launch so
   the claim reserves capacity. `status.sourceReceipt` fences the source
   against a second claim. Anchor: `Reconcile` admission in
   `internal/controller/coderun_controller.go`, `internal/source/`.
3. One publisher per run. A native publication only moves the run to
   `Verifying`. The operator verifies world state before `Done`. Never let the
   v1 controller settle a run that a native controller claimed. The ownership
   no-op preserves this.
4. Evidence intake is fenced by namespace. A shared HMAC key cannot make one
   release's intake a secret writer inside another release's namespaces. A
   foreign run gets 403. Anchor: `internal/controller/evidence_intake.go`.
5. Fenced identities survive mode switches. Source receipts, PR-fix
   generations, rebase lane identity (`spec.mode: rebase` plus the Dispatch
   rebase executor binding), and run namespaces never merge across releases.
6. Capacity stays per release. `LaneProfileSpec.concurrency` counts runs per
   lane profile. Two releases with disjoint lanes keep independent capacity.
   Retry state lives on the run annotations
   (`courier.misospace.dev/retry`, `courier.misospace.dev/soft-stop`) and
   moves with the run, not with the release.

## Bounded canary procedure

The steps below enable a bounded native canary next to a running v1 release.
Pick one source or lane slice before you start. Do not widen the slice during
the canary.

1. Record the current v1 release version and its `ownership.namespaces`.
   The v1 release must declare its owned namespaces explicitly. A v1 release
   with an empty `ownership.namespaces` claims every namespace and will fight
   the canary.
2. Deploy the canary release with `secure.enabled: true`,
   `secure.runNamespace` set to a dedicated namespace, `crds.enabled: false`,
   and a distinct `ownership.leaderElectionID` and
   `secure.brokerTokenReviewClusterRole` if both releases share an operator
   namespace. Set `ownership.namespaces` to the run namespace, plus the
   canary release's own namespace if the canary runs a discovery source
   (step 5): the chart render and the operator's `resolveOwnership` startup
   check both fail when a discovery-enabled release's namespace sits outside
   the boundary (`charts/courier/templates/common.yaml.tpl`, `cmd/main.go`).
3. Make sure that the canary release fails the render when the ownership
   boundary is wrong. The chart cross-validation in
   `charts/courier/templates/common.yaml.tpl` must pass.
4. Run the ownership isolation tests against the deployed version:
   `go test ./internal/controller -run 'Foreign|Owned' -count=1`. This is
   a check on the artifact, not live proof.
5. Admit canary runs on the chosen slice only. Prefer a lane-profile-only
   slice. A dispatch lane requires `dispatch.enabled` on the canary release,
   which enables discovery and forces the canary release's own namespace
   into `ownership.namespaces` (step 2). Two releases that share one
   operator namespace cannot both run discovery sources there: both
   boundaries would own the runs discovery materializes in that namespace.
   Keep `concurrency: 1` for the first slice.
6. Observe per run: `status.heartbeat.at` advancing at the 15 second cadence
   during a long tool episode, `status.activeOperations` populated,
   `status.checkpoint` populated, `status.lastCommit` confirmed at
   publication, and a terminal `status.nativeResult` classification. Record
   the deployed image and chart version with every observation.
7. Promote by slice. Add one lane or source at a time. Stop adding when any
   criterion from the go/no-go table loses live evidence.

### Stop intake and roll back

If the canary fails a go/no-go criterion, stop intake first and decide second.

1. Suspend the canary lanes. Apply the `courier.misospace.dev/suspend`
   annotation to the canary LaneProfiles, or disable the admission source for
   the slice. New runs stay `Pending`. Nothing in-flight is deleted.
2. Let `Running` and `Verifying` runs settle. Do not delete their pods and do
   not remove the `secureTopologyFinalizer` finalizer by hand. Revocation runs
   in order when a run finalizes.
3. For `NeedsHuman` runs and runs with partial pushed work, make a human
   decision per run. The safe default is to leave the run in place with its
   history and let a human re-admit it on v1. Never bulk-unclaim source
   receipts.
4. Restore the slice on v1. The canary held the slice by admitting it only on
   the canary release; bind the slice's lane on the v1 release again
   (`dispatch.lanes` in chart values, or the repeatable
   `--dispatch-lane-binding` flag), or re-enable the slice's admission source
   on v1 if the canary disabled it, and confirm v1 claims the slice again.
5. Keep the native release installed but scaled to no new admissions, or
   uninstall it after its retention windows pass
   (`retention.needsHuman: 168h`). Keep the run namespace until you no longer
   need the audit history.

## What rollback preserves

Rollback must not lose durable state. The following survives a rollback to v1
because it lives on the run objects, the git remotes, or the sources, not in
the native release:

- Pull request branches and their head SHAs (`status.branch`,
  `status.headSHA`, `status.lastCommit`).
- Source receipts and claim history (`status.sourceReceipt`), so a source does
  not retry a run that already produced work.
- Run history and terminal audit entries, subject to the retention flags.
- Retry and capacity state (`courier.misospace.dev/retry`,
  `courier.misospace.dev/soft-stop` annotations, `CapacityRecovery`
  condition).
- Checkpoints (`status.checkpoint`) on the retained run objects, for audit
  and for a later native resume. They are not v1-resumable state: the legacy
  OpenCode executor cannot replay native harness brief and checkpoint
  progress. What v1 can pick up is what lives in the world — branches,
  commits, and fenced source attempts. A partially completed run re-admitted
  on v1 still requires the explicit human decision described below.

Ambiguous work, for example a pushed branch with no confirmed publication or a
run stuck between `Verifying` and `NeedsHuman`, requires an explicit human
decision. The runbook owner records the decision on the run object, not in
chat.

## Risks and stop conditions

- #731 is open. Heartbeat cadence is unproven live. This alone keeps the
  system at no-go for cutover, because the liveness reaper is blind without
  heartbeats and a hung native run sits unseen.
- Both releases hold cluster-scoped RBAC. Explicit `ownership.namespaces` on
  both releases is the mitigation. Treat an empty list on either release as a
  stop condition for the canary.
- Do not reproduce #731 with a broad canary. Its workflow note forbids that.
  File new defects narrowly.
- Do not treat a dry-run or a single-node exercise as production proof. Only
  live observations from deployed runs count for the go/no-go table.
- The model wire format is an explicit lane setting, never inferred from a
  model name: `LaneProfileSpec.Transport` is `openai` (the LiteLLM-compatible
  surface, the default when empty) or `anthropic` (the native Anthropic
  Messages surface) (#716). Choose `anthropic` when the backend requires
  native Messages — for example thinking-block-sensitive models — and keep
  model-role values at the gateway-visible model id on the configured route.
  Neither copying OpenCode provider or model strings into a LaneProfile nor
  forcing every native lane onto the OpenAI surface is valid (#632). The
  control binary fails closed on an unknown transport value.

## Limitations

This runbook describes release v0.1.25. It contains both the ownership
isolation (#634) and the trusted native terminal-result and publication
contract (#577), which first shipped in v0.1.12. Check the chart version in
`charts/courier/Chart.yaml` and re-verify the keys above before you act on a
different release. Evidence status in the go/no-go table reflects the linked
issues on 2026-11-13.
