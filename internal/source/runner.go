package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
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
	SourceAgent  string
	LaneProfile  string
	Namespace    string
	PollInterval time.Duration
}

// Runner polls a source and materializes source work as CoderRuns.
type Runner struct {
	client.Client
	Scheme         *runtime.Scheme
	Adapter        Adapter
	Config         RunnerConfig
	pollMu         sync.Mutex
	laneWaiting    bool
	laneSuspended  bool
	laneAtCapacity bool
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
			if !r.laneWaiting {
				log.FromContext(ctx).Info("waiting for LaneProfile before source discovery", "laneProfile", r.Config.LaneProfile, "namespace", r.Config.Namespace)
				r.laneWaiting = true
			}
			return nil
		}
		return fmt.Errorf("get LaneProfile %s/%s: %w", r.Config.Namespace, r.Config.LaneProfile, err)
	}
	if r.laneWaiting {
		log.FromContext(ctx).Info("LaneProfile available, resuming source discovery", "laneProfile", r.Config.LaneProfile, "namespace", r.Config.Namespace)
		r.laneWaiting = false
	}
	// A suspended lane takes no new work; runs it already admitted carry on.
	if lane.Suspended() {
		if !r.laneSuspended {
			log.FromContext(ctx).Info("lane suspended, pausing source discovery", "laneProfile", r.Config.LaneProfile, "annotation", courierv1alpha1.SuspendAnnotation)
			r.laneSuspended = true
		}
		return nil
	}
	if r.laneSuspended {
		log.FromContext(ctx).Info("lane resumed, resuming source discovery", "laneProfile", r.Config.LaneProfile)
		r.laneSuspended = false
	}

	var runs courierv1alpha1.CoderRunList
	if err := r.List(ctx, &runs, client.InNamespace(r.Config.Namespace)); err != nil {
		return fmt.Errorf("list CoderRuns: %w", err)
	}
	capacity := lane.Spec.Concurrency
	if capacity <= 0 {
		capacity = 1
	}
	reserved := laneCapacityReservations(runs.Items, r.Config.LaneProfile)
	if reserved >= capacity {
		if !r.laneAtCapacity {
			log.FromContext(ctx).Info("lane at capacity, pausing source discovery", "laneProfile", r.Config.LaneProfile, "capacity", capacity, "reserved", reserved)
			r.laneAtCapacity = true
		}
		return nil
	}
	if r.laneAtCapacity {
		log.FromContext(ctx).Info("lane capacity available, resuming source discovery", "laneProfile", r.Config.LaneProfile, "capacity", capacity, "reserved", reserved)
		r.laneAtCapacity = false
	}

	type rankedExclusion struct {
		item      WorkItem
		unsettled bool
		created   metav1.Time
		name      string
	}
	ranked := make([]rankedExclusion, 0, len(runs.Items))
	for i := range runs.Items {
		run := &runs.Items[i]
		if strings.TrimSpace(run.Spec.WorkItemID) == "" {
			continue
		}
		// Dependency recovery requires a fresh source attempt identity. Do not
		// exclude blocked work from discovery, which must see reactivation, but
		// retain same-identity dedupe below to preserve history and prevent churn.
		if run.Status.Phase == courierv1alpha1.PhaseBlockedExternal {
			continue
		}
		retainedSource := run.Spec.Source == r.Config.Source
		if r.Config.Source == "dispatch" {
			retainedSource = retainedSource || run.Spec.Source == "dispatch" || strings.HasPrefix(run.Spec.Source, "dispatch:")
		}
		if retainedSource {
			ranked = append(ranked, rankedExclusion{
				item: WorkItem{
					ID:   run.Spec.WorkItemID,
					Mode: string(run.Spec.Mode),
					Repo: run.Spec.Repo,
					Ref:  run.Spec.Ref,
				},
				unsettled: phaseUnsettled(run.Status.Phase),
				created:   run.CreationTimestamp,
				name:      run.Name,
			})
		}
	}
	// Most-relevant-first: a bounded exclusion window drops the tail, so work
	// whose source generation is still open outranks terminal retained runs,
	// and newer work outranks older work within each group.
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].unsettled != ranked[j].unsettled {
			return ranked[i].unsettled
		}
		if !ranked[i].created.Time.Equal(ranked[j].created.Time) {
			return ranked[i].created.Time.After(ranked[j].created.Time)
		}
		return ranked[i].name < ranked[j].name
	})
	exclusions := make([]WorkItem, 0, len(ranked))
	for i := range ranked {
		exclusions = append(exclusions, ranked[i].item)
	}
	var items []WorkItem
	var err error
	if excluder, ok := r.Adapter.(DiscoveryExcluder); ok {
		items, err = excluder.DiscoverExcluding(ctx, exclusions)
	} else {
		items, err = r.Adapter.Discover(ctx)
	}
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

	existing := make(map[string]struct{}, len(runs.Items))
	legacyIdentities := make(map[string]struct{})
	for i := range runs.Items {
		run := &runs.Items[i]
		if strings.TrimSpace(run.Spec.WorkItemID) == "" {
			continue
		}
		identity := r.identity(run.Spec.WorkItemID)
		legacyBinding := run.Spec.Source == "dispatch" || strings.HasPrefix(run.Spec.Source, "dispatch:")
		if run.Spec.Source == r.Config.Source {
			existing[workKey(run.Spec.Source, identity)] = struct{}{}
		}
		// Previous Dispatch bindings encoded queue lanes in Source. Keep those
		// retained runs from being rematerialized during the identity rollout.
		if legacyBinding {
			legacyIdentities[identity] = struct{}{}
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
		itemIdentity := r.identity(item.ID)
		key := workKey(r.Config.Source, itemIdentity)
		if _, ok := legacyIdentities[itemIdentity]; ok {
			continue
		}
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
				Mode:        courierv1alpha1.Mode(spec.Mode),
				Source:      spec.Source,
				SourceAgent: r.Config.SourceAgent,
				WorkItemID:  spec.WorkItemID,
				Repo:        spec.Repo,
				Ref:         spec.Ref,
				Lane:        spec.Lane,
				Debug:       spec.Debug,
			},
		}
		run.Name = runName(r.Config.Source, itemIdentity)
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

// identity is the dedupe identity of a work item ID: the adapter's, when it
// defines one, otherwise the opaque ID itself.
func (r *Runner) identity(workItemID string) string {
	if identifier, ok := r.Adapter.(WorkIdentifier); ok {
		if id := identifier.WorkIdentity(workItemID); id != "" {
			return id
		}
	}
	return workItemID
}

// phaseUnsettled reports whether a retained run has not yet reached a terminal
// phase, so its source attempt is still open: the terminal lifecycle report
// that settles it has not been published. Unsettled runs sort first among
// discovery exclusions. Verifying is included, unlike capacity reservation,
// because exclusion relevance tracks settlement, not execution slots.
func phaseUnsettled(phase courierv1alpha1.Phase) bool {
	switch phase {
	case "", courierv1alpha1.PhasePending, courierv1alpha1.PhaseClaimed, courierv1alpha1.PhaseRunning, courierv1alpha1.PhaseVerifying:
		return true
	}
	return false
}

// laneCapacityReservations conservatively counts empty-phase and Pending runs as
// discovery reservations, in addition to the Claimed and Running phases admitted
// by the operator. Verifying and later phases reserve no discovery capacity.
func laneCapacityReservations(runs []courierv1alpha1.CoderRun, lane string) int {
	reserved := 0
	for i := range runs {
		if runs[i].Spec.Lane != lane {
			continue
		}
		switch runs[i].Status.Phase {
		case "", courierv1alpha1.PhasePending, courierv1alpha1.PhaseClaimed, courierv1alpha1.PhaseRunning:
			reserved++
		}
	}
	return reserved
}

func workKey(sourceName, workItemID string) string {
	return sourceName + "\x00" + workItemID
}

func runName(sourceName, workItemID string) string {
	digest := sha256.Sum256([]byte(workKey(sourceName, workItemID)))
	return "courier-" + hex.EncodeToString(digest[:])[:32]
}
