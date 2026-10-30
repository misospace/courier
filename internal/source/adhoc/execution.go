package adhoc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
)

var (
	// ErrNilRun indicates that ContributionExecution was called with a nil
	// run.
	ErrNilRun = errors.New("adhoc: coder run is required")
	// ErrInvalidExecutionContext indicates that a CoderRun's durable
	// source context could not be decoded into a contribution execution
	// input. Each failure carries a specific reason and fails closed:
	// no execution input is ever derived from a partial, foreign, or
	// malformed context.
	ErrInvalidExecutionContext = errors.New("adhoc: invalid execution context")
)

var _ executor.ContributionContext = Adapter{}

// ContributionExecution decodes the run's durable, source-owned
// spec.sourceContext into the source-neutral execution input the
// coordinator-goal renderer needs (#624). It is a pure decode of durable
// run state: no Kubernetes client, no forge, no source contact — the
// receiver is not used, so the same run always yields the same result and
// a resume or relaunch renders the identical goal.
//
// The decoded goal is untrusted task data: it grants no repository
// authority, mutates no policy, selects no credential, and moves no
// publication target. Core copies this context verbatim and never
// interprets it; the knowledge of what the bytes mean lives only in this
// source, which is why this decode lives here and not in the generic
// renderer. A nil run fails with ErrNilRun; any context that is empty,
// oversized, malformed, foreign, or missing a required field fails closed
// with ErrInvalidExecutionContext and a specific reason.
func (a Adapter) ContributionExecution(run *courierv1alpha1.CoderRun) (executor.ContributionExecution, error) {
	if run == nil {
		return executor.ContributionExecution{}, ErrNilRun
	}

	if run.Spec.Source != SourceName {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: coder run %s/%s is owned by source %q, not %q",
			ErrInvalidExecutionContext, run.Namespace, run.Name, run.Spec.Source, SourceName)
	}
	if run.Spec.Mode != courierv1alpha1.ModeContribute {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: coder run %s/%s is in mode %q, not %q",
			ErrInvalidExecutionContext, run.Namespace, run.Name, run.Spec.Mode, courierv1alpha1.ModeContribute)
	}

	raw := run.Spec.SourceContext
	if strings.TrimSpace(raw) == "" {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: coder run %s/%s has an empty source context",
			ErrInvalidExecutionContext, run.Namespace, run.Name)
	}
	if len(raw) > source.MaxSourceContextBytes {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context exceeds %d bytes",
			ErrInvalidExecutionContext, source.MaxSourceContextBytes)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context is not valid json: %v",
			ErrInvalidExecutionContext, err)
	}
	if len(envelope) != 1 {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context has %d top-level keys, want 1",
			ErrInvalidExecutionContext, len(envelope))
	}
	entry, ok := envelope[contextKey]
	if !ok {
		other := ""
		for key := range envelope {
			other = key
		}
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context carries key %q, not the %q object",
			ErrInvalidExecutionContext, other, contextKey)
	}

	// UpstreamIssue is untrusted display context, not part of the
	// execution input; it is decoded for shape only and discarded.
	var payload contextPayload
	if err := json.Unmarshal(entry, &payload); err != nil {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context value is not the %q object: %v",
			ErrInvalidExecutionContext, contextKey, err)
	}
	if strings.TrimSpace(payload.Goal) == "" {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context goal is missing or blank", ErrInvalidExecutionContext)
	}
	baseRef := payload.BaseRef
	if strings.TrimSpace(baseRef) == "" {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context base ref is missing or blank", ErrInvalidExecutionContext)
	}
	// Re-enforce the admission bounds on the durable value so a context
	// that outlived a relaxed admission can never yield a partial input.
	if len(baseRef) > maxBaseRefBytes {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context base ref exceeds %d bytes", ErrInvalidExecutionContext, maxBaseRefBytes)
	}
	if hasWhitespace(baseRef) {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context base ref must not contain whitespace", ErrInvalidExecutionContext)
	}
	if strings.HasPrefix(baseRef, "/") || strings.HasSuffix(baseRef, "/") {
		return executor.ContributionExecution{}, fmt.Errorf(
			"%w: source context base ref must not start or end with /", ErrInvalidExecutionContext)
	}

	// The goal is returned byte-for-byte as stored: admission already
	// trimmed it once, and the renderer must see exactly what was
	// admitted so every relaunch renders the same goal.
	return executor.ContributionExecution{Goal: payload.Goal, BaseRef: baseRef}, nil
}
