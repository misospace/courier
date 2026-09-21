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

## Runtime security invariant

Courier's coordinator runtime may create branches, push feature-branch
commits, and open and update pull requests. It holds no administrator
rights, appears in no bypass list, cannot push directly to `main`, and
cannot merge. The blast radius of a compromised or hallucinating
coordinator is "opened a bad PR." Branch protection is what makes that
hold: `main` requires pull requests, so a raw push credential cannot land
code on the default branch.

## What main requires

`main` is protected. Every change arrives through a pull request, and the
required checks must be green before merge:

- `static`, `test`, `images`, `vulnerability`, `chart`, `review` (the AI PR
  review check), and `Analyze (go)` (CodeQL).
- Branch freshness is not enforced: a PR whose own required checks are green
  does not need a rebase just because `main` advanced. Maintainers update
  branches when there is meaningful integration risk, not mechanically.
- No force pushes, no deletions.
- Approving reviews are not a hard GitHub gate: no review count and no
  code-owner requirement is enforced. Saffron's automated review is input to
  a human decision, not a substitute for one.

Maintainers — the repository's administrators — retain the normal merge
ergonomics of the maintained misospace repositories, including bypassing
protection to merge deliberately when the situation calls for it.
Automation identities hold no such authority.

## Who merges

Courier itself has no merge authority. Merge remains an explicit
human-maintainer action, or a GitHub auto-merge previously enabled by a
human maintainer. The maintainer chooses whether a PR merges and when;
that decision does not have to be expressed as a formal GitHub approval.

## Forge neutrality

GitHub is the current deployment, not the architecture. The invariant is
forge-neutral: writes to the default branch arrive through reviewable
requests, required machine checks gate them, and merge authority belongs to
a human maintainer — never to Courier automation. An operator bringing
Courier to another forge must reproduce that boundary with equivalent forge
mechanisms before pointing Courier at the repository.
