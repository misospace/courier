# Terminal runtime diagnostics

When a `CoderRun` ends in a non-success terminal phase (`Failed` or
`NeedsHuman`), the operator records `status.terminalDiagnostic`: a bounded,
redacted snapshot of how the coordinator ended. It is captured **before pod
cleanup**, is set once, and survives pod deletion and controller restart. The
purpose is to distinguish an executor crash, a model transport error,
continuation exhaustion, a verifier rejection, and a Kubernetes/container
failure after the pod needed to diagnose them is gone.

## Categories

| Category | Meaning |
| --- | --- |
| `executor-crash` | The executor bootstrap/harness process failed before or around the model run, or a non-zero coordinator exit produced no valid handoff. |
| `model-transport-error` | The model/agent child process exited non-zero. |
| `continuation-exhaustion` | The bounded continuation budget was exhausted, or a no-progress loop was detected. |
| `verifier-rejection` | The verified world (git, the PR, or authoritative linkage) disagreed with the declared or expected outcome. |
| `kubernetes-failure` | Eviction, failed scheduling, OOMKill, or a lost pod. |
| `needs-decision` | The coordinator declared it needed a human decision. |
| `blocked-external` | The coordinator declared it was blocked on an external prerequisite. |
| `unknown` | Evidence was missing or ambiguous. The operator never invents a cause. |

Classification uses the strongest available evidence: a concrete category the
executor emitted in its handoff wins; otherwise Kubernetes/container failure
evidence; otherwise a declared decision or external block; otherwise a non-zero
exit with no valid handoff is `executor-crash`; otherwise `unknown`.

## Inspecting a run

While the `CoderRun` object exists:

```sh
kubectl get coderun <name> -n <namespace> -o jsonpath='{.status.terminalDiagnostic}' | jq .
kubectl get coderun <name> -n <namespace> -o yaml
kubectl describe coderun <name> -n <namespace>
```

The diagnostic's `container` block retains `podName`, `podUID`, `containerID`,
`image`, `nodeName`, `restartCount`, and `terminationReason`, so
kubectl-describe-equivalent inspection is possible after the pod is deleted.

## While the pod still exists

Before the pod is garbage-collected, inspect it directly with the retained
identity:

```sh
kubectl describe pod <podName> -n <namespace>
kubectl logs <podName> -c coordinator -n <namespace> --previous
```

## After the pod is deleted

Correlate the retained identity with node/runtime inspection and cluster events:

```sh
kubectl get events -n <namespace> --field-selector involvedObject.name=<podName>
```

A `kubernetes-failure` diagnostic with no `container` block means the pod was
already gone at capture; no container details are fabricated.

## Bounds and redaction

The reason is length-bounded (1024 bytes) and redacted: tokens, credentials,
raw prompts, and unbounded provider responses are never stored. Capture is
best-effort, so an absent diagnostic is not proof of success, and `unknown`
means the evidence was missing or ambiguous.
