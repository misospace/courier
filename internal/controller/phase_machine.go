package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/branch"
	"github.com/misospace/courier/internal/source"
)

var (
	ErrSourceRegistryRequired = errors.New("coderun controller: source registry is required")
	ErrUnknownSource          = errors.New("coderun controller: unknown source")
	ErrMissingWorkItemID      = errors.New("coderun controller: work item ID is required")
	ErrPRHeadResolverRequired = errors.New("coderun controller: fix-pr head resolver is required")
	ErrEmptyResolvedBranch    = errors.New("coderun controller: branch resolver returned an empty branch")
)

// SourceRegistry keeps source adapters behind the source name persisted in a
// CoderRun spec. The registry is deliberately explicit: source identity must
// never be inferred from a Kubernetes name or numeric ref.
type SourceRegistry struct {
	Adapters map[string]source.Adapter
}

// NewSourceRegistry constructs a registry keyed by adapter name.
func NewSourceRegistry(adapters map[string]source.Adapter) *SourceRegistry {
	registry := &SourceRegistry{Adapters: make(map[string]source.Adapter, len(adapters))}
	for name, adapter := range adapters {
		registry.Adapters[name] = adapter
	}
	return registry
}

// Register adds or replaces an adapter under name.
func (r *SourceRegistry) Register(name string, adapter source.Adapter) {
	if r == nil || strings.TrimSpace(name) == "" || adapter == nil {
		return
	}
	if r.Adapters == nil {
		r.Adapters = make(map[string]source.Adapter)
	}
	r.Adapters[name] = adapter
}

func (r *SourceRegistry) lookup(name string) (source.Adapter, error) {
	if r == nil {
		return nil, ErrSourceRegistryRequired
	}
	adapter, ok := r.Adapters[name]
	if !ok || adapter == nil {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSource, name)
	}
	return adapter, nil
}

// ExistingPRHeadResolver resolves the branch currently attached to an
// existing pull request. A fix-pr run must use this injected source/forge
// lookup; it must never synthesize a branch from the PR number.
type ExistingPRHeadResolver interface {
	ResolveHead(context.Context, *courierv1alpha1.CoderRun) (string, error)
}

// ExistingPRHeadResolverFunc adapts a function to ExistingPRHeadResolver.
type ExistingPRHeadResolverFunc func(context.Context, *courierv1alpha1.CoderRun) (string, error)

func (f ExistingPRHeadResolverFunc) ResolveHead(ctx context.Context, run *courierv1alpha1.CoderRun) (string, error) {
	return f(ctx, run)
}

func sourceWorkItem(run *courierv1alpha1.CoderRun) (source.WorkItem, error) {
	if run == nil || strings.TrimSpace(run.Spec.WorkItemID) == "" {
		return source.WorkItem{}, ErrMissingWorkItemID
	}
	return source.WorkItem{
		ID:    run.Spec.WorkItemID,
		Mode:  string(run.Spec.Mode),
		Repo:  run.Spec.Repo,
		Ref:   run.Spec.Ref,
		Lane:  run.Spec.Lane,
		Debug: run.Spec.Debug,
	}, nil
}

func resolveRunBranch(ctx context.Context, run *courierv1alpha1.CoderRun, resolver ExistingPRHeadResolver) (string, error) {
	if run == nil {
		return "", ErrEmptyResolvedBranch
	}
	switch run.Spec.Mode {
	case courierv1alpha1.ModeResolveIssue:
		resolved := branch.ResolveIssueBranch(run.Spec.Repo, run.Spec.Ref)
		if resolved == "" {
			return "", fmt.Errorf("%w: resolve-issue", ErrEmptyResolvedBranch)
		}
		return resolved, nil
	case courierv1alpha1.ModeFixPR:
		if resolver == nil {
			return "", ErrPRHeadResolverRequired
		}
		resolved, err := resolver.ResolveHead(ctx, run)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(resolved) == "" {
			return "", ErrEmptyResolvedBranch
		}
		return resolved, nil
	default:
		return "", fmt.Errorf("%w: unsupported mode %q", ErrEmptyResolvedBranch, run.Spec.Mode)
	}
}
