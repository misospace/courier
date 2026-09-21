# Security Policy

## Reporting a vulnerability

Report suspected security vulnerabilities privately through **GitHub Private
Vulnerability Reporting**, which is enabled for this repository: open the
repository's **Security** tab and choose **Report a vulnerability**.

Do not open a public issue, discussion, or pull request with exploit details,
and do not disclose details publicly until a coordinated disclosure date has
been agreed where practical.

Reports are acknowledged within **3 business days** and receive an initial
triage or status response within **7 business days**. Remediation and
disclosure are coordinated privately with the reporter. Resolution time
depends on severity and complexity, so no fixed patch deadline is promised.

## Supported versions

- The `main` branch is supported.
- The most recent published Courier release is supported; older releases are
  not guaranteed security fixes.
- Courier has not yet published a release, so `main` is currently the
  supported line. The most recent release becomes the supported stable line
  once releases exist.

## Scope

Courier is a Kubernetes operator that manages coordinator pods executing
model-driven work: those pods consume forge and provider credentials from
Kubernetes Secrets, push feature branches, and open and update pull requests.
See [DESIGN.md](./DESIGN.md) for the architecture and its trust boundaries.

Reports where Courier fails a security boundary are in scope, for example:

- privilege escalation or RBAC bypass in the operator;
- the coordinator or its runtime identity being able to merge, push to a
  protected branch, or otherwise exceed the "opened a bad PR" blast radius;
- leakage of forge or provider credentials into logs, pull requests, or other
  surfaces;
- credential scope or authentication bypass in source adapters or forge
  clients;
- command or code execution crossing an intended trust boundary (operator to
  cluster, coordinator to forge, report content to coordinator);
- unsafe handling of untrusted repository, issue, or tool content that creates
  a genuine privilege boundary violation.

The distinction is impact, not whether an LLM was involved. Prompt injection
whose blast radius stays inside reviewable PR content is not a security
vulnerability; injection that crosses an authorization or secret boundary is.

## Not security vulnerabilities

These belong in normal GitHub issues instead:

- poor model output or generated code with no boundary bypass;
- ordinary CI failures;
- unsupported model or provider behavior;
- feature requests;
- ordinary bugs with no security impact.
