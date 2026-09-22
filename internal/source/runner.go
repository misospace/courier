package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	ErrRunnerClientRequired  = errors.New("source runner: client is required")
	ErrRunnerAdapterRequired = errors.New("source runner: adapter is required")
	ErrRunnerSourceRequired  = errors.New("source runner: source name is required")
	ErrRunnerInvalidMode     = errors.New("source runner: unsupported run mode")
)

const defaultPollInterval = 30 * time.Second

// RunnerConfig controls one source discovery loop. LaneProfile, when set, is
// the Courier LaneProfile assigned to every discovered item.
type RunnerConfig struct {
	Source       string
	LaneProfile  string
	Namespace    string
	PollInterval time.Duration
}

// Runner polls a source and materializes source work as CoderRuns.
type Runner struct {
	client.Client
	Scheme  *runtime.Scheme
	Adapter Adapter
	Config  RunnerConfig
	pollMu  sync.Mutex
}

func NewRunner(c client.Client, adapter Adapter, config RunnerConfig) *Runner {
	return &Runner{Client: c, Adapter: adapter, Config: config}
}

func (r *Runner) Start(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	interval := r.Config.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}

	if err := r.Poll(ctx); err != nil {
		log.FromContext(ctx).Error(err, "source discovery failed", "source", r.Config.Source)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Poll(ctx); err != nil {
				log.FromContext(ctx).Error(err, "source discovery failed", "source", r.Config.Source)
			}
		}
	}
}

func (r *Runner) Poll(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	r.pollMu.Lock()
	defer r.pollMu.Unlock()

	lane := &courierv1alpha1.LaneProfile{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Config.Namespace, Name: r.Config.LaneProfile}, lane); err != nil {
		if apierrors.IsNotFound(err) {
			log.FromContext(ctx).Info("waiting for LaneProfile before source discovery", "laneProfile", r.Config.LaneProfile, "namespace", r.Config.Namespace)
			return nil
		}
		return fmt.Errorf("get LaneProfile %s/%s: %w", r.Config.Namespace, r.Config.LaneProfile, err)
	}

	items, err := r.Adapter.Discover(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	validItems := make([]WorkItem, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.ID) == "" {
			log.FromContext(ctx).Info("skipping malformed source work item", "reason", "missing ID")
			continue
		}
		if item.Mode != string(courierv1alpha1.ModeResolveIssue) && item.Mode != string(courierv1alpha1.ModeFixPR) {
			log.FromContext(ctx).Info("skipping malformed source work item", "workItemID", item.ID, "reason", "unsupported mode", "mode", item.Mode)
			continue
		}
		if strings.TrimSpace(item.Repo) == "" || item.Ref < 1 {
			log.FromContext(ctx).Info("skipping malformed source work item", "workItemID", item.ID, "reason", "invalid repository or reference")
			continue
		}
		validItems = append(validItems, item)
	}
	items = validItems

	var runs courierv1alpha1.CoderRunList
	if err := r.List(ctx, &runs, client.InNamespace(r.Config.Namespace)); err != nil {
		return fmt.Errorf("list CoderRuns: %w", err)
	}
	existing := make(map[string]struct{}, len(runs.Items))
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.Spec.Source == r.Config.Source && strings.TrimSpace(run.Spec.WorkItemID) != "" {
			existing[workKey(run.Spec.Source, run.Spec.WorkItemID)] = struct{}{}
		}
	}

	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(item.ID) == "" {
			continue
		}
		key := workKey(r.Config.Source, item.ID)
		if _, ok := existing[key]; ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		spec := item.Spec(r.Config.Source)
		if strings.TrimSpace(r.Config.LaneProfile) != "" {
			spec.Lane = r.Config.LaneProfile
		}
		if strings.TrimSpace(spec.Lane) == "" {
			return fmt.Errorf("work item %q has no LaneProfile", item.ID)
		}
		run := &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Namespace: r.Config.Namespace},
			Spec: courierv1alpha1.CoderRunSpec{
				Mode:       courierv1alpha1.Mode(spec.Mode),
				Source:     spec.Source,
				WorkItemID: spec.WorkItemID,
				Repo:       spec.Repo,
				Ref:        spec.Ref,
				Lane:       spec.Lane,
				Debug:      spec.Debug,
			},
		}
		run.Name = runName(r.Config.Source, item.ID)
		if err := r.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create CoderRun for %q: %w", item.ID, err)
		}
		existing[key] = struct{}{}
	}
	return nil
}

func (r *Runner) validate() error {
	if r == nil || r.Client == nil {
		return ErrRunnerClientRequired
	}
	if r.Adapter == nil {
		return ErrRunnerAdapterRequired
	}
	if strings.TrimSpace(r.Config.Source) == "" {
		return ErrRunnerSourceRequired
	}
	if strings.TrimSpace(r.Config.Namespace) == "" {
		return errors.New("source runner: namespace is required")
	}
	if strings.TrimSpace(r.Config.LaneProfile) == "" {
		return errors.New("source runner: LaneProfile is required")
	}
	return nil
}

func workKey(sourceName, workItemID string) string {
	return sourceName + "\x00" + workItemID
}

func runName(sourceName, workItemID string) string {
	digest := sha256.Sum256([]byte(workKey(sourceName, workItemID)))
	return "courier-" + hex.EncodeToString(digest[:])[:32]
}
