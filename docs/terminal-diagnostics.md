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
| `capacity-exhaustion` | Execution capacity, not task correctness, was confirmed exhausted by a structured provider signal. |
| `provider-unavailable` | The model provider repeatedly failed to serve this attempt without producing a structured allowance-exhaustion signal; the work was released for another capacity. |
| `forge-unavailable` | A forge read could not complete on a transient availability condition, so the world was never observed; the work was released for another capacity. |
| `unknown` | Evidence was missing or ambiguous. The operator never invents a cause. |

Classification uses the strongest available evidence: a concrete category the
executor emitted in its handoff wins; otherwise causal Kubernetes/container
failure evidence (`OOMKilled`, `ContainerStatusUnknown`, authoritative eviction,
scheduling, or node-loss reasons; a `PodFailed` with no recorded terminated
state and no valid handoff; or a missing pod); otherwise a declared decision
or external block; otherwise a non-zero exit with no valid handoff is
`executor-crash`; otherwise `unknown`. `Error`, `Completed`, and `Shutdown` remain safe to display but do
not by themselves establish an infrastructure cause. The exact causal subsets
are maintained separately from the broader display allowlists in the operator.

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

The reason is **operator-derived**: a short, allowlisted category label,
optionally followed by the kubelet's allowlisted `Terminated.Reason` and the
pod's `Status.Reason`, and (for the operator's own wedge-reap and pod-loss
ceiling paths) a fixed operator-controlled override. The executor's freeform
text — `needs-decision` questions, `blocked-external` missing descriptions,
verifier fail-reasons, bootstrap error strings — is never an input to the
diagnostic's reason. The user-facing explanation lives on the source issue
or pull request, where the executor's terminal handler posts it through the
registered-secret redactor.

The kubelet-derived and pod-derived text is constrained to a small allowlist
of values the runtime itself writes (`Completed`, `Error`, `OOMKilled`,
`ContainerStatusUnknown`, `Evicted`, `DeadlineExceeded`, `Preempt` for the
container; `Evicted`, `NodeAffinity`, `NodeLost`, `Shutdown`,
`Unschedulable`, `UnexpectedAdmissionError`, `Error`,
`ContainerStatusUnknown` for the pod). Any other value is dropped before it
reaches the operator-derived reason, so a non-pattern-matching secret in the
kubelet's `Terminated.Reason` or the pod's `Status.Reason` cannot land on
the durable status.

The reason is length-bounded to 1024 bytes and run through a defensive
pattern-redaction pass on whatever operator-derived text it does carry
(so a kubelet state reason that happens to look like a credential is
still replaced). Capture is best-effort, so an absent diagnostic is not
proof of success, and `unknown` means the evidence was missing or
ambiguous.

`container.terminationReason` applies the same allowlist, plus a hard
128-byte cap that mirrors the CRD's `+kubebuilder:validation:MaxLength=128`
in code. This prevents an overlong kubelet value from causing the API
server to reject the terminal status patch (which would block
terminalization and source reporting).
