# Repository settings and the merge boundary

This repository's GitHub-side policy is part of Courier's security model, not
incidental configuration. Maintainers hold these settings directly in GitHub;
they are not managed as code, and nothing in this repository changes them.
The live settings, readable through the API, are the source of truth over
this document:

```sh
gh api repos/misospace/courier --jq '{allow_auto_merge, allow_update_branch, delete_branch_on_merge}'
gh api repos/misospace/courier/branches/main/protection
```

## What main requires

`main` is protected. Every change arrives through a pull request and needs
all of the following before it can merge:

- **Required checks green** — `static`, `test`, `images`, `vulnerability`,
  `chart`, `review` (the AI PR review check), and `Analyze (go)` (CodeQL).
  Required checks are separate from reviews: passing them satisfies nothing
  else.
- **One approving review from a code owner.** `CODEOWNERS` names the
  repository's human maintainer(s), and only code-owner approvals count
  toward this requirement. Bot identities — Saffron, Courier runtime — may
  post reviews and approvals, but those can never satisfy it. The human
  approval and the automated `review` check are independent gates; Saffron's
  approval is input to the human's decision, not a substitute for it.
- **A current branch.** New pushes dismiss existing approvals and the branch
  must be up to date before merging, so the human approves the diff that
  actually merges.
- No force pushes, no deletions, no direct writes; admins are inside the
  enforcement, not above it.

The human maintainer holds a review bypass so sole-maintainer changes are
not deadlocked (the author of a PR cannot approve it themselves). Required
checks still apply to their PRs. No automation identity holds this bypass,
and Courier runtime credentials hold no administration rights.

## What Courier runtime identities can do

Courier's coordinator runtime may create branches, push commits, and open
and update pull requests. It holds no administrator rights, appears in no
bypass list, cannot push to `main`, and cannot merge. The blast radius of a
compromised or hallucinating coordinator is "opened a bad PR."

## Auto-merge

Repository auto-merge is enabled as an ergonomics feature. A queued PR
completes only after every required check is green **and** the code-owner
approval is in place — the trigger is always the human approval, never a
Courier action. The distinction matters: the *coordinator* has no merge
capability at all, while *GitHub* performs merges only downstream of the
human gate.

## Forge neutrality

GitHub is the current deployment, not the architecture. The invariant is
forge-neutral: writes to the default branch arrive through reviewable
requests, required machine checks gate them, and an approval that only a
human can give sits between green CI and merge. An operator bringing Courier
to another forge must reproduce that boundary with equivalent forge
mechanisms before pointing Courier at the repository.
