# External contributions: source-neutral ad-hoc work and fork-backed publication

Design for [#511](https://github.com/misospace/courier/issues/511). It settles the
authority model, admission identity, fork policy, idempotency, and security
posture for the primitive:

> Given a GitHub repository Courier can read, an approved repository/fork it
> may write, and a natural-language goal, produce one reviewable PR without
> prior Dispatch inventory, ownership of the upstream issue, or upstream write
> authority.

This document is the contract every frontend consumes (CLI, operator web UI,
OpenClaw/MCP, optional Dispatch frontend per misospace/dispatch#1349). No
frontend implements fork/PR behavior of its own. Dispatch remains optional and
must not become a Courier dependency.

## Authority model

Repository authority and work-item ownership are separate concepts.

- **Base repository** (`spec.repo` for a contribution run): the upstream
  repository Courier may *read* and open a PR *into*. It is the PR base.
- **Head repository** (`spec.workRepo`, new optional field): the one
  repository Courier may *push* to. V1 requires it to be an existing approved
  fork of the base. Courier never creates forks in V1.
- **Upstream issue** (`SourceContext`, optional): task context only. Supplying
  an issue URL or number is not an ownership claim.

```
base  -> github.com/someone/project.git   # read/base; PR base ref
head  -> github.com/jory/project.git      # approved write target; PR head
PR    jory/project:courier/someone/project/goal-<key> -> someone/project:main
```

There is no implicit "whatever origin currently means" authority. Publication
targets are explicit, validated at admission, and revalidated at publication.

### What this reuses today

Fork-backed publication is partially modeled already; the design closes the
gaps rather than inventing parallel machinery:

- `PublicationPolicy` already carries `BaseRepo`/`BaseRef` and a separate
  `WorkRepo`/`WorkRef` (`api/v1alpha1/coderun_types.go:629-693`), persisted in
  `status.publicationPolicy`.
- `forge.CreatePullRequestInput.HeadRepo` already supports cross-repo PR heads
  (`internal/forge/forge.go:246`); the forge adapter passes
  `HeadRepo: work.Canonical` (`internal/broker/forge_adapter.go:165`).
- The broker's git pusher already pins only the Base and Work repository
  endpoints and refuses any other destination
  (`internal/broker/git_transport.go:52,85`) — the structural "no upstream
  push" guarantee.
- `fix-pr` already resolves a foreign head repository into
  `policy.WorkRepo` (`internal/controller/secure_control.go:375`).

The gaps are admission hardwiring, work identity without an issue number, and
PR discovery/matching for cross-repo heads.

## Work identity and admission

### Source-neutral contract

A contribution is materialized as an ordinary `CoderRun` by a new source
adapter (`internal/source/adhoc/`) implementing the existing `source.Adapter`
contract (`internal/source/source.go:319`). Core gains no Dispatch-specific or
ad-hoc-specific fields.

Admission inputs (all validated server-side; nothing in the goal text or any
repository file can grant authority):

- base repository (`owner/name`) and base ref (explicit, or the default branch
  resolved read-only at admission and pinned into the policy);
- natural-language goal, carried in `SourceContext` (`spec.SourceContext`,
  ≤16 KiB opaque JSON, `api/v1alpha1/coderun_types.go:168-171`) under a
  source-owned key; core never interprets its content;
- optional upstream issue URL/number, also inside `SourceContext`;
- approved writable head repository (`spec.workRepo`, optional field, only
  valid for the contribute mode);
- lane, and optional publication preferences the policy allows.

### CRD surface changes

- New `Mode` value `contribute` alongside `resolve-issue`/`fix-pr`
  (`api/v1alpha1/coderun_types.go:153-155`). The fork-write authority must be
  explicit in the object, not inferred from an overloaded resolve-issue.
- `spec.Ref` becomes optional overall with a validation rule: required and
  ≥1 for `resolve-issue`/`fix-pr`, must be empty for `contribute`
  (`spec.Ref` is currently a required `int`, `:202-204`).
- New optional `spec.workRepo` (`owner/name` pattern); must be empty for
  `resolve-issue`/`fix-pr` and required for `contribute`.
- `spec.repo` for a contribute run is the **base** repository. This preserves
  registry selection semantics, which key on `spec.repo` today.

### Idempotency and duplicate submission

- `WorkItemID` (`spec.WorkItemID`, required, `:173-177`) carries a
  source-neutral idempotency key. Clients may supply one; otherwise admission
  mints `adhoc-<12 hex>` digested from (base repo, base ref, goal, head repo).
  The key, not the issue number, is the durable identity: it survives restart
  because it is in the immutable spec.
- Duplicate submission with the same key is **resume**, not a new run: the
  adapter looks up the existing run by key and returns its identity instead of
  creating a second `CoderRun`. An intentional new contribution is a fresh
  key; the CLI makes that explicit rather than silent.
- Rejection and failure are two distinct points, both forge-mutation-free:
  - **Before a CR exists**, only static spec validation rejects the request:
    CRD-level rules (required `spec.workRepo`, empty `spec.Ref`, well-formed
    `owner/name`). Registry allowlist membership is *not* checked here — the
    CLI creates the `CoderRun` through the Kubernetes API and registry
    selection happens in controller admission. At this point there is no CR,
    no branch, no PR.
  - **After the CR exists**, admission failures — allowlist membership and
    authority checks in `ResolveAdmission`
    (`internal/controller/secure_control.go:218`), e.g. the fork is not served
    by any registration — terminalize the run to `NeedsHuman` before any forge
    mutation. Authority checks must not be moved into frontends to get earlier
    rejection; a "rejected" contribution that already has a CR is a terminal
    run, not a rollback.

### Who may submit

Submitters come through an authenticated frontend; lane and head-repo choice
are validated against configured policy server-side at admission
(`internal/controller/secure_control.go`, `ResolveAdmission`) and again at
publication. The natural-language goal and repository contents are untrusted
data; neither can change the approved fork, base, credential scope, or
publication destination. Redirects and changed remote URLs cannot extend
authority — the pusher refuses any destination it was not pinned to
(`internal/broker/git_transport.go:85`).

## Fork policy (V1)

Settled decisions:

- **V1 requires a pre-existing approved fork.** Fork creation, fork discovery
  for users without one, and renamed-fork migration are follow-up work; none
  of them ship implicitly.
- **Write access is verified where the credential lives, not assumed.**
  Admission's `AdmissionProvider` runs on the *operator's read identity*
  (`internal/controller/secure_control.go:106-121`) and must not perform
  credential-bound checks: at admission, write authority is established from
  *configuration policy* — the fork's allowlist coverage and the
  registration's credential refs. Actual push-permission verification
  (`RepositoryPolicyProvider.CanWriteRepository`,
  `internal/forge/forge.go:313-318`, implemented with run credentials in
  `internal/github/forge.go:151`) happens in the broker preflight under the
  run's pinned credentials, before any push. Read access to base and write
  access to head are checked separately.
- **Registry coverage.** The forge registration serving a contribution must
  cover both base and head in its `Serves` allowlist patterns
  (`internal/forge/registry.go:60-63`); git credentials resolve per repository
  from the same registration's credential refs (`:38`,
  `internal/broker/git_transport.go:15-30`). A fork outside the allowlist
  fails closed.
- **Fail closed on authority change.** Fork rename/deletion, revoked push
  access, changed base identity, or upstream branch deletion/rename mid-run:
  the run fails to `NeedsHuman`. Courier never silently picks another target,
  never force-pushes or resets another active writer to recover, and never
  re-targets after base movement — the observed base OID pinned at admission
  is rechecked at publication and a changed base is a human decision.
- **Preserve the human merge boundary.** The forge capability vocabulary has
  no merge capability by construction (`internal/forge/forge.go:25-45`);
  contributions change nothing about that.

## Branch naming and durable PR idempotency

- Contribute branches derive from the idempotency key:
  `courier/<base-owner>/<base-repo>/goal-<12 hex of key>` — a new pure helper
  beside `ResolveIssueBranch` in `internal/branch/branch.go:14`, keeping the
  repository-scoped, collision-free convention. No issue number is involved.
- **Work-ref admission semantics.** Contribute work refs are mutable branch
  refs owned by the contribution identity; an explicitly pinned commit is a
  follow-up option, not V1. At first admission, if the derived branch already
  exists on the fork, its tip is read: a tip matching Courier's own prior
  publication of this key (recorded in `WorkOID`/`HeadAnchorOID`,
  `api/v1alpha1/coderun_types.go:658-669`) is adopted as resume; any foreign
  tip fails closed to `NeedsHuman` — Courier never adopts or overwrites a
  branch it did not author, regardless of key-entropy collisions.
- Durable publication identity is the existing status subresource:
  `status.Branch`, `status.HeadRepo`, `status.HeadSHA`, `status.PR` (already
  present, `api/v1alpha1/coderun_types.go:716-719` area). Retries rediscover
  and update the *same* branch and the *same* PR; they never spray duplicates.
- PR discovery matches **base/head repositories and refs**, never title text.
  Cross-repo head resolution already exists end to end:
  `Observer.FindPullRequest` receives head repo and ref
  (`internal/broker/policy.go:85`, called at `:468`), the adapter filters
  `pr.HeadRepo` and builds the `owner:branch` selector
  (`internal/broker/forge_adapter.go:148-155,191`), and the GitHub provider
  supports fork heads (`internal/github/forge.go:223-260`). The work here is
  contribute-mode coverage and fail-closed behavior, not new machinery:
  ambiguous matches (multiple PRs from the same head) fail closed to
  `NeedsHuman` rather than guess.
- Recovery behaviors, all reusing the existing world-wins machinery:

  | Scenario | Behavior |
  | --- | --- |
  | Push recorded, PR not created | Re-publish is idempotent (same head SHA); create PR path resumes. |
  | PR created, URL not recorded | Discovery by base/head repos+refs rediscovers the same PR before creating a new one (`FindPullRequest` call at `policy.go:468` with the duplicate guard, plus the post-create recheck at `policy.go:497-515`). |
  | Already-merged contribution | Fail closed to `NeedsHuman`; never reopen or duplicate. |
  | Closed-unmerged PR | Reported, not reopened; a new contribution is an explicit new key. |
  | Draft / no-diff PR | Adopted as-is on resume; no-diff is surfaced as evidence, not an error state. |
  | Stale lease/ownership token or concurrent same-identity retries | Second writer fails closed; cannot create two PRs or overwrite the first writer's branch. |

## Optional upstream issue context

An upstream issue referenced in `SourceContext` is read-only task context:

- Courier may *read* it via the `read-work-item` capability when credentials
  permit; an inaccessible issue is non-fatal and the run proceeds without it.
- Courier must **not** label, assign, comment on, claim, or otherwise mutate
  upstream issue state merely because the issue supplied the goal. The forge
  vocabulary does contain issue-mutation verbs — `CapabilityComment`
  (`internal/forge/forge.go:39`) and `CapabilityRemoveWorkItemLabel`
  (`internal/forge/forge.go:42`) — so the prohibition must be an explicit
  capability rule, not a vocabulary property: admission and publication policy
  must refuse to grant contribute runs any issue-mutation capability
  (`CapabilityComment`, `CapabilityRemoveWorkItemLabel`), permitting only read
  verbs (`CapabilityReadWorkItem`, `CapabilityListWorkItemsByLabel`,
  `CapabilityListComments`) plus the publication-only PR verbs
  (`CapabilityCreatePullRequest`, `CapabilityUpdatePullRequest`).
- Opening the explicitly approved PR is the one authorized write, distinct
  from any issue operation.

## PR body contract

The resolve-issue linkage contract (`ValidateLinkage` in
`internal/broker/linkage.go:119`) requires a validated `Closes owner/repo#N`.
For contributions:

- Default wording is **non-closing**: `Refs #123` or a plain issue URL.
- `Fixes #123` (closing semantics) requires an explicit policy choice made at
  admission and recorded in the publication policy — never inferred from the
  goal text.
- Linkage is optional: a contribution with no issue context carries no
  linkage line.
- **Contribution runs never pin acceptance criteria.** Criteria are pinned
  from a trusted source-issue snapshot today
  (`api/v1alpha1/coderun_types.go:613-615`); an upstream issue referenced by a
  contribution is explicitly untrusted context. V1 contribute runs therefore
  carry no criterion-evidence block; if criteria ever become pinnable for
  contributions, they must come from the submitting frontend's own trusted
  policy configuration at admission — never from upstream issue text. Absent
  criteria, the body is the goal summary plus linkage.

## Completion semantics

Local validation and publication success are distinct from CI approval and
human merge, exactly as for resolve-issue runs:

- A contribute run follows the existing run lifecycle through its verifying
  phase and ends `HandedOff` with the PR open; the operator observes CI and
  human review after handoff (`DESIGN.md` run lifecycle). A successful push
  is never proof the goal was satisfied.
- A no-change completion requires evidence relevant to the submitted goal,
  under the same contract as resolve-issue runs ([#513](https://github.com/misospace/courier/issues/513)).
- The PR is left open, non-draft, for operator verification and human merge;
  Courier never merges.

## Security and trust

- Upstream repository content is untrusted input. It gains no publication
  authority beyond the approved head target; repo bootstrap/tooling trust
  remains governed by the existing Courier runtime/bootstrap policy
  (`DESIGN.md`, `HARNESS.md`).
- No source/forge credentials in CRs, model context, logs, or checkpoints.
  Credentials stay deployment-level in the forge registry
  (`internal/forge/registry.go`) and are copied only into the run's dedicated
  broker topology (`internal/controller/secure_launcher.go:844`); the harness
  holds only a broker token (`internal/harness/brokertoken.go`).
- Publication target and base/head identities are revalidated immediately
  before push and before every PR mutation (broker preflight; pinned-endpoint
  push).
- A request rejected before a CR exists performs no forge mutation; authority
  failures after the CR exists terminalize the run without one.

## Admission surface

- V1 ships a thin CLI (`cmd/courierctl/`, `contribute` subcommand) that
  validates inputs client-side for ergonomics, then creates the `CoderRun`
  through the Kubernetes API. All real validation is server-side in the
  controller admission path, so the CLI is a dumb frontend by construction.
- An HTTP admission API is follow-up work; when it lands it consumes the same
  adapter and the same server-side validation. UI/MCP/Dispatch frontends
  likewise stay thin.

## Failure cases covered by tests

The test matrix uses a fake forge/git fixture with independently observable
read/write operations — new work patterned on the canned stub provider in
`internal/forge/forge_test.go:13` (which today returns fixed responses and
records nothing) and the reusable-double convention of
`internal/connector/connectortest/cycle.go` — asserting both intended results
and the *absence* of forbidden mutations:

- no upstream branch push, issue claim, label, assignment, closing comment, or
  other issue-state mutation from optional issue context; the contribute
  run's granted capability set never includes `CapabilityComment`;
- rejection of goal/repository instructions that change the approved fork,
  base, credential scope, or publication destination;
- concurrent retries with the same identity create exactly one PR; a stale
  ownership token cannot overwrite another writer;
- injected failure after push but before recording it, after PR creation but
  before recording its URL, and during a transient forge lookup — each
  recovers without duplicate PRs;
- missing fork, no push permission, changed upstream base, existing PR
  discovery, restart after push-before-PR, restart after PR creation, stale
  branch ownership, and closed/merged upstream work all fail or resume per the
  table above;
- rejected-before-admission requests leave no forge mutation and no
  credentials in logs, CRs, checkpoints, or frontend responses.

## Decomposition

Implemented as bounded, file-scoped issues (each filed under #511):

1. `contribute` mode + CRD surface (`api/v1alpha1/coderun_types.go`):
   mode value, optional `Ref`, `workRepo` field, validation rules, deepcopy.
2. Ad-hoc source adapter and work identity (`internal/source/adhoc/`,
   `internal/source/source.go`): adapter, idempotency key, duplicate-as-resume.
3. Admission authority resolution (`internal/controller/secure_control.go`,
   `internal/forge/registry.go`): contribute path resolving base/work repos,
   allowlist coverage for base and fork, fail-closed rules (push-permission
   verification stays in the broker preflight under run credentials).
4. Branch naming and durable idempotency (`internal/branch/branch.go`,
   `internal/controller/phase_machine.go`): goal branches, work-ref admission
   semantics, restart recovery, stale-ownership handling.
5. Fork-backed publication (`internal/broker/policy.go`,
   `internal/broker/git_transport.go`, `internal/broker/forge_adapter.go`,
   `internal/github/forge.go`): preflight write verification, contribute-mode
   discovery coverage, mode-aware linkage contract, non-closing default.
6. Fake forge fixture and security/failure matrix (`internal/forge/` test
   support + broker/controller tests): observable ops, injected failures,
   absence-of-mutation assertions.
7. CLI admission surface (`cmd/courierctl/`): thin `contribute` frontend.
8. Follow-on frontends: operator web UI (#510 ecosystem) and the optional
   Dispatch frontend (misospace/dispatch#1349) — blocked on this contract;
   they must consume it, not reimplement it.
