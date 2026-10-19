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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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

// A failed hand-out acknowledgement is retried a bounded number of times
// within the poll that materialized the run, because a lost ack otherwise
// waits on the settlement report or the stale-handout reclaimer to release
// the outstanding hand-out (#456). Retries run after the whole discovery pass
// has materialized, under a concurrency cap and an explicit budget, so a
// stalled Dispatch endpoint can never hold up another item's run (#456).
// maxHandoutAckAttempts is an upper bound, not a per-item guarantee: an item
// gets at most that many attempts and fewer when the drain budget expires
// first, and how many it gets depends on where it lands in the queue behind
// the acks ahead of it.
const (
	maxHandoutAckAttempts = 3
	handoutAckRetryDelay  = 100 * time.Millisecond
	// handoutAckConcurrency caps how many hand-out acks are in flight at once
	// during the drain, so a source offering many items cannot turn the drain
	// into an unbounded fan-out against Dispatch.
	handoutAckConcurrency = 4
	// handoutAckAttemptTimeout bounds a single AckHandout call, independent of
	// the shared Dispatch HTTP client's own timeout: one stalled endpoint must
	// not be able to spend that budget on every attempt of every item.
	handoutAckAttemptTimeout = 3 * time.Second
	// handoutAckDrainTimeout bounds the whole post-pass drain. It is sized so
	// one item's worst case — maxHandoutAckAttempts attempts of
	// handoutAckAttemptTimeout plus maxHandoutAckAttempts-1 backoffs — fits
	// inside it, so an item draining on its own keeps its full retry budget
	// while total ack latency per poll stays capped at the budget regardless
	// of how many items the source offers. Items queued behind a stalled
	// endpoint get fewer attempts, never more.
	handoutAckDrainTimeout = 15 * time.Second
)

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
	laneCooling    bool
	// Now is the runner's time source, injectable so tests never sleep.
	// Nil falls back to time.Now.
	Now func() time.Time
	// ackRetryDelay spaces out bounded hand-out ack retries. NewRunner sets
	// the production backoff; zero keeps retries immediate, which tests use.
	ackRetryDelay time.Duration
	// ackAttemptTimeout bounds one AckHandout call. NewRunner sets the
	// production attempt deadline; values <= 0 fall back to it at use time,
	// because a zero-value Runner must not mean "no deadline".
	ackAttemptTimeout time.Duration
	// ackDrainTimeout bounds the whole acknowledgement drain for one poll.
	// NewRunner sets the production budget; values <= 0 fall back to it at use
	// time, for the same reason.
	ackDrainTimeout time.Duration
}

func (r *Runner) clock() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func NewRunner(c client.Client, adapter Adapter, config RunnerConfig) *Runner {
	return &Runner{
		Client:            c,
		Adapter:           adapter,
		Config:            config,
		ackRetryDelay:     handoutAckRetryDelay,
		ackAttemptTimeout: handoutAckAttemptTimeout,
		ackDrainTimeout:   handoutAckDrainTimeout,
	}
}

// ackAttemptBudget and ackDrainBudget never return a non-positive duration: a
// Runner built field-by-field (as tests do) must still bound its acks.
func (r *Runner) ackAttemptBudget() time.Duration {
	if r.ackAttemptTimeout > 0 {
		return r.ackAttemptTimeout
	}
	return handoutAckAttemptTimeout
}

func (r *Runner) ackDrainBudget() time.Duration {
	if r.ackDrainTimeout > 0 {
		return r.ackDrainTimeout
	}
	return handoutAckDrainTimeout
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
	// A confirmed capacity-exhaustion cooldown pauses discovery for this
	// lane's capacity identity only (#327): the exhausted capacity stops
	// immediately re-claiming work, while every other identity — including
	// other profiles drawing on a different pool — keeps discovering. The
	// cooldown lives on the LaneProfile status, so an operator restart
	// re-derives the remaining window from the cluster, not process memory.
	var lanes courierv1alpha1.LaneProfileList
	if err := r.List(ctx, &lanes, client.InNamespace(r.Config.Namespace)); err != nil {
		return fmt.Errorf("list LaneProfiles: %w", err)
	}
	if cooldown, active := courierv1alpha1.CapacityCooldownActive(lane, lanes.Items, r.clock()); active {
		if !r.laneCooling {
			log.FromContext(ctx).Info("capacity cooldown active, pausing source discovery",
				"laneProfile", r.Config.LaneProfile,
				"capacityID", lane.CapacityIdentity(),
				"nextEligibleAt", cooldown.NextEligibleAt.Time)
			r.laneCooling = true
		}
		return nil
	}
	if r.laneCooling {
		log.FromContext(ctx).Info("capacity cooldown expired, resuming source discovery", "laneProfile", r.Config.LaneProfile)
		r.laneCooling = false
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
		// Only an explicitly settled capacity exhaustion releases the
		// identity-level discovery barrier. The exact consumed source token
		// remains in the dedupe map below, so reactivation requires a fresh ID.
		if settledCapacityExhaustion(run) {
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
	// Most-relevant-first: a bounded exclusion window drops the tail, so
	// pre-terminal runs (which have not yet published the report that usually
	// settles their source attempt) outrank terminal retained runs, and newer
	// work outranks older work within each group.
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
	if discoverer, ok := r.Adapter.(CapacityAwareDiscoverer); ok {
		items, err = discoverer.DiscoverForCapacity(ctx, lane.CapacityIdentity(), exclusions)
	} else if excluder, ok := r.Adapter.(DiscoveryExcluder); ok {
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
			if !settledCapacityExhaustion(run) {
				existing[workKey(run.Spec.Source, identity)] = struct{}{}
				existing[workKey(run.Spec.Source, run.Spec.WorkItemID)] = struct{}{}
			}
		}
		// Previous Dispatch bindings encoded queue lanes in Source. Keep those
		// retained runs from being rematerialized during the identity rollout.
		if legacyBinding && !settledCapacityExhaustion(run) {
			legacyIdentities[identity] = struct{}{}
		}
	}

	// Hand-out acknowledgements are queued here and drained once the whole
	// pass has materialized, so an ack that blocks on a stalled Dispatch
	// endpoint can never delay the materialization of another item (#456).
	pendingAcks := make([]WorkItem, 0, len(items))
	defer func() { r.drainHandoutAcks(ctx, pendingAcks) }()

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
		_, hasLegacyIdentity := legacyIdentities[itemIdentity]
		_, hasExistingRun := existing[key]
		if hasLegacyIdentity || hasExistingRun {
			// The run already owns this attempt, but a hand-out whose ack
			// failed at materialization stays outstanding — exclusions are
			// hints, so the source can keep offering the item. Re-attempt
			// the ack: it is idempotent and a moot refusal proves the
			// ledger already moved on (#456).
			pendingAcks = append(pendingAcks, item)
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
				Mode:          courierv1alpha1.Mode(spec.Mode),
				Source:        spec.Source,
				SourceAgent:   r.Config.SourceAgent,
				WorkItemID:    spec.WorkItemID,
				SourceContext: spec.Context,
				Repo:          spec.Repo,
				Ref:           spec.Ref,
				Lane:          spec.Lane,
				Debug:         spec.Debug,
			},
		}
		name := runName(r.Config.Source, itemIdentity)
		if predecessor := settledPredecessor(runs.Items, r.Config.Source, itemIdentity, r); predecessor != nil {
			name = successorRunName(r.Config.Source, itemIdentity, predecessor.UID)
		}
		run.Name = name
		if err := r.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create CoderRun for %q: %w", item.ID, err)
		}
		// The run now durably owns the attempt: acknowledge the stamped
		// hand-out so the source can tell a live attempt from a lost one
		// (#456). A failed ack leaves the hand-out in the reclaimable state
		// the stale-handout reclaimer exists for; it must never block
		// materialization or starve the rest of this discovery pass, so the
		// ack is queued and drained once every item has been materialized.
		pendingAcks = append(pendingAcks, item)
		existing[key] = struct{}{}
	}
	return nil
}

// drainHandoutAcks acknowledges the pass's stamped hand-outs after every item
// has been materialized, so a stalled Dispatch endpoint can never delay the
// materialization of another item (#456). At most handoutAckConcurrency acks
// run at once, every attempt carries its own deadline, and the whole drain runs
// under one budget sized so a single stalled item still fits inside it. Items
// still outstanding when the budget expires are abandoned with an Info log and
// stay in the reclaimable state the stale-handout reclaimer exists for — never
// an error return, and never a reason to abandon another item's ack. Every
// goroutine started here is joined before the poll returns.
func (r *Runner) drainHandoutAcks(ctx context.Context, pending []WorkItem) {
	if len(pending) == 0 {
		return
	}
	acker, ok := r.Adapter.(HandoutAcker)
	if !ok {
		return
	}
	// A poll cancelled before the drain cannot ack anything; issuing the calls
	// anyway would only burn a futile attempt per pending item.
	if ctx.Err() != nil {
		return
	}
	drainCtx, cancel := context.WithTimeout(ctx, r.ackDrainBudget())
	defer cancel()

	var wg sync.WaitGroup
	slots := make(chan struct{}, handoutAckConcurrency)
	for i := range pending {
		wg.Add(1)
		// Acquiring the slot here, rather than inside the goroutine, bounds
		// both the in-flight acks and the goroutines this poll creates.
		slots <- struct{}{}
		go func(item WorkItem) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := r.ackHandout(drainCtx, acker, item); err != nil {
				// The drain budget expired (or the poll was cancelled)
				// before this hand-out settled. Abandoned, never fatal.
				log.FromContext(ctx).Info("pr-fix hand-out acknowledgement abandoned",
					"workItemID", item.ID, "error", err.Error())
			}
		}(pending[i])
	}
	wg.Wait()
}

// ackHandout acknowledges a stamped hand-out for a queue-backed attempt,
// retrying a failure a bounded number of times within the pass before giving
// up: the ack is idempotent and moot refusals collapse to no-ops in the
// adapter, so a repeat attempt is always safe. A final failure is logged,
// never fatal — the hand-out stays in the reclaimable state the
// stale-handout reclaimer exists for, and terminal settlement reports
// release it regardless (#456). Each attempt runs under its own deadline so a
// stalled endpoint cannot consume the shared Dispatch client timeout. A
// non-nil return means the context expired first and the caller must treat the
// hand-out as abandoned.
func (r *Runner) ackHandout(ctx context.Context, acker HandoutAcker, item WorkItem) error {
	var err error
	for attempt := 0; attempt < maxHandoutAckAttempts; attempt++ {
		if attempt > 0 {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if r.ackRetryDelay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(r.ackRetryDelay):
				}
			}
		}
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, r.ackAttemptBudget())
		err = acker.AckHandout(attemptCtx, item)
		cancelAttempt()
		if err == nil {
			return nil
		}
	}
	log.FromContext(ctx).Info("pr-fix hand-out acknowledgement failed",
		"workItemID", item.ID, "attempts", maxHandoutAckAttempts, "error", err.Error())
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

// settledCapacityExhaustion is the sole history state that permits discovery
// past the semantic identity barrier. Incomplete, negative, or human-marked
// settlement records remain barriers. Both recoverable categories count —
// a confirmed quota exhaustion and an availability handoff
// (provider-unavailable, #327) settle through the same fenced channel, and a
// settled outage run must not strand the work item forever.
func settledCapacityExhaustion(run *courierv1alpha1.CoderRun) bool {
	if run.Status.Phase != courierv1alpha1.PhaseBlockedExternal ||
		run.Status.TerminalDiagnostic == nil ||
		(run.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion &&
			run.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable) {
		return false
	}
	recovery := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
	reported := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionLifecycleReported)
	return recovery != nil && recovery.Status == metav1.ConditionTrue && recovery.Reason == "Settled" &&
		reported != nil && reported.Status == metav1.ConditionTrue
}

// phaseUnsettled reports whether a retained run has not yet reached a terminal
// phase. It is a ranking heuristic for discovery-exclusion order, not
// settlement authority: terminal runs have usually published the lifecycle
// report that settles their source attempt, but a terminal run can still owe a
// pending report, and ranking cannot change what settles. Verifying is
// included, unlike capacity reservation, because exclusion relevance tracks
// settlement, not execution slots.
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

func successorRunName(sourceName, workItemID string, predecessorUID types.UID) string {
	digest := sha256.Sum256([]byte(workKey(sourceName, workItemID) + "\x00" + string(predecessorUID)))
	return "courier-" + hex.EncodeToString(digest[:])[:32]
}

func settledPredecessor(runs []courierv1alpha1.CoderRun, sourceName, identity string, runner *Runner) *courierv1alpha1.CoderRun {
	var predecessor *courierv1alpha1.CoderRun
	for i := range runs {
		run := &runs[i]
		if run.Spec.Source != sourceName || !settledCapacityExhaustion(run) || run.UID == "" || runner.identity(run.Spec.WorkItemID) != identity {
			continue
		}
		if predecessor == nil || run.CreationTimestamp.Time.After(predecessor.CreationTimestamp.Time) ||
			(run.CreationTimestamp.Equal(&predecessor.CreationTimestamp) && run.Name > predecessor.Name) {
			predecessor = run
		}
	}
	return predecessor
}
