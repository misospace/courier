package controller

// The mechanical-CI park-and-resume loop (#475, #494).
//
// A run that published its pull request waits on CI without holding a
// coordinator pod or a lane slot, and one eligible failed mechanical check on
// the exact recorded head wakes the same run for one bounded repair. The
// loop is opt-in: a lane that declares no repair policy leaves every run on
// it on the ordinary post-publication grading path, unchanged.
//
// Strict trigger separation is the classifier's job (see mechanical.go): only
// a configured mechanical check failing on the exact parked head, with repair
// budget remaining, may wake a coordinator. This file owns everything around
// that verdict — what a parked run records, when it may wake, what a repair
// attempt costs, and how the run finishes when it may not wake.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

const (
	// ciparkPollDelay is how often a parked run re-reads the world. The park
	// owns the wait, so the cadence is slower than the post-publication
	// grading poll it replaces, and it never becomes a busy loop against the
	// forge.
	ciparkPollDelay = 30 * time.Second

	// ciparkMaxFailedChecks bounds how many failed check names one repair
	// context carries. The context is a prompt input, not an archive.
	ciparkMaxFailedChecks = 20

	// ciparkMaxCheckLogBytes bounds the log evidence one failed check
	// contributes to a repair context. The evidence is a prompt input, and a
	// single check's output is unbounded upstream, so each check's block is
	// cut before it joins the context.
	ciparkMaxCheckLogBytes = 2 * 1024

	// ciparkMaxRepairLogBytes bounds the log evidence one repair context
	// carries in total. The context is persisted in the run's status and
	// rendered into the coordinator's goal, so it stays a bounded prompt
	// input rather than a log archive.
	ciparkMaxRepairLogBytes = 8 * 1024

	// ciparkTruncatedLog marks evidence cut at a bound, so a reader can
	// tell a short log from a cut one.
	ciparkTruncatedLog = "[truncated]"
)

// ciRepairPolicy returns the lane's declared mechanical-CI repair policy.
//
// A nil policy disables the loop entirely: the run behaves exactly as it
// does without one, and the ordinary post-publication grading path decides
// its outcome. A policy that names no mechanical check, or grants no repair
// attempt, disables it the same way — there is nothing the loop could
// repair and nothing it could wait for.
func (r *CoderRunReconciler) ciRepairPolicy(ctx context.Context, run *courierv1alpha1.CoderRun) (*courierv1alpha1.CIRepairPolicy, error) {
	var lane courierv1alpha1.LaneProfile
	if err := r.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, &lane); err != nil {
		if apierrors.IsNotFound(err) {
			// A lane that no longer exists configures no repair policy, so
			// the loop is disabled and the ordinary path decides. A read that
			// failed without an answer is not a missing lane and is
			// returned, so the run retries instead of losing its policy.
			return nil, nil
		}
		return nil, err
	}
	policy := lane.Spec.CIRepair
	if policy == nil || len(policy.MechanicalChecks) == 0 || policy.MaxAttempts <= 0 {
		return nil, nil
	}
	return policy, nil
}

// normalizedMechanicalChecks returns the lane's configured mechanical check
// names, with empty and duplicate names dropped and every other name kept
// exactly as declared. Selection and verdicts share this one normalization so
// the predicate and the coverage rule can never disagree about what the lane
// configured.
//
// Names are never trimmed: a declared name that carries surrounding space is
// its own exact name, so it can never widen to a differently spelled forge
// check. Such a declaration simply never registers, and the run fails closed
// to handoff instead of silently repairing under a name the lane did not
// write.
func normalizedMechanicalChecks(names []string) []string {
	seen := make(map[string]struct{}, len(names))
	normalized := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

// mechanicalSelector returns the strict trigger separator for the lane's
// configured mechanical checks. Names match exactly and case-sensitively, so
// the policy stays auditable and can never widen to a check the lane did not
// name — an AI-review workflow, a human review, or any other non-mechanical
// check is simply not selectable.
func mechanicalSelector(names []string) MechanicalCheckSelector {
	configured := make(map[string]struct{})
	for _, name := range normalizedMechanicalChecks(names) {
		configured[name] = struct{}{}
	}
	return func(name string) bool {
		_, ok := configured[name]
		return ok
	}
}

// publishedHead reports whether the observed head is this run's own published
// head, and returns it when it is.
//
// Attribution uses durable, run-owned publication evidence only: the last
// commit the harness reported as published, which the broker verified against
// the live work ref, and — only while no published head is recorded — the
// commits of the run's completed briefs. Once a published head is recorded it
// supersedes every older commit the run accounts for: an observation of one
// is the world having moved on (or a stale read of it), and a head the run
// cannot currently account for is somebody else's work or a superseded head,
// either of which fails closed instead of parking or repairing.
func publishedHead(run *courierv1alpha1.CoderRun, observed string) (string, bool) {
	if observed == "" {
		return "", false
	}
	if run.Status.LastCommit != "" {
		if run.Status.LastCommit == observed {
			return observed, true
		}
		return "", false
	}
	if run.Status.Checkpoint != nil {
		briefs := run.Status.Checkpoint.CompletedBriefs
		for i := len(briefs) - 1; i >= 0; i-- {
			if briefs[i].Commit == observed {
				return observed, true
			}
		}
	}
	return "", false
}

// failedMechanicalChecks returns the names of the failed checks the lane
// configures as mechanical. Only those may ever wake a coordinator, and only
// those are evidence a repair context carries.
func failedMechanicalChecks(observation PRObservation, selector MechanicalCheckSelector) []string {
	if selector == nil {
		return nil
	}
	names := make([]string, 0, len(observation.Checks))
	for _, check := range observation.Checks {
		if check.State != CheckStateFailed || !selector(check.Name) {
			continue
		}
		names = append(names, check.Name)
	}
	slices.Sort(names)
	if len(names) > ciparkMaxFailedChecks {
		names = names[:ciparkMaxFailedChecks]
	}
	return names
}

// failedCheckLogs renders the bounded log evidence of the failed checks the
// lane configures as mechanical — and only those. It selects exactly the
// checks failedMechanicalChecks selects, so the evidence can never name a
// check the lane did not configure, and it decides nothing: the failure
// itself is still read from Name and State alone.
//
// The rendering is one block per check, prefixed with the check name, cut to
// the per-check bound and to the total bound. A cut is marked, so the
// coordinator can tell a short log from a cut one.
func failedCheckLogs(observation PRObservation, selector MechanicalCheckSelector) string {
	if selector == nil {
		return ""
	}
	var logs strings.Builder
	for _, check := range observation.Checks {
		if check.State != CheckStateFailed || !selector(check.Name) {
			continue
		}
		output := strings.TrimSpace(check.Output)
		if output == "" {
			continue
		}
		block := fmt.Sprintf("--- %s ---\n%s\n", check.Name, output)
		if len(block) > ciparkMaxCheckLogBytes {
			block = ciparkCutLog(block, ciparkMaxCheckLogBytes)
		}
		remaining := ciparkMaxRepairLogBytes - logs.Len()
		if remaining <= 0 {
			break
		}
		if len(block) > remaining {
			if remaining <= len(ciparkTruncatedLog) {
				break
			}
			logs.WriteString(ciparkCutLog(block, remaining))
			break
		}
		logs.WriteString(block)
	}
	return logs.String()
}

// ciparkCutLog cuts log evidence to a byte limit on a UTF-8 rune boundary and
// marks the cut.
func ciparkCutLog(text string, limit int) string {
	if limit <= len(ciparkTruncatedLog) {
		return ciparkTruncatedLog
	}
	cut := limit - len(ciparkTruncatedLog)
	for cut > 0 && cut < len(text) && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + ciparkTruncatedLog
}

// mechanicalVerdict reports whether every configured mechanical check is
// present in the observation and passed, and how many configured checks the
// observation carried.
//
// Green requires coverage, not just the passage of whatever happened to be
// observed: a configured check that never registered means the observation
// is partial, and a partial observation is not a passing check set. A run
// whose configured mechanical checks never registered at all has no eligible
// check to grade and never claims green for the same reason. A failed check
// of any kind — mechanical or not — also keeps the run from claiming green,
// because a failed AI-review check is still a failed check.
func mechanicalVerdict(observation PRObservation, configuredNames []string) (green bool, eligible int) {
	configured := normalizedMechanicalChecks(configuredNames)
	if len(configured) == 0 {
		return false, 0
	}
	states := make(map[string]CheckState, len(observation.Checks))
	for _, check := range observation.Checks {
		if _, seen := states[check.Name]; !seen {
			states[check.Name] = check.State
		}
	}
	passed := 0
	for _, name := range configured {
		state, present := states[name]
		if !present {
			continue
		}
		eligible++
		if state == CheckStatePassed {
			passed++
		}
	}
	if eligible != len(configured) || passed != eligible {
		return false, eligible
	}
	for _, check := range observation.Checks {
		if check.State == CheckStateFailed {
			return false, eligible
		}
	}
	return true, eligible
}

// observationFailed reports whether any observed check failed.
func anyCheckFailed(observation PRObservation) bool {
	for _, check := range observation.Checks {
		if check.State == CheckStateFailed {
			return true
		}
	}
	return false
}

// ciparkFenceToken derives the fence token of one repair attempt of one run
// incarnation. It is derived from the immutable run identity and the attempt
// number alone: never from a model, a worker, or a lane name, so it cannot
// be guessed, and never from anything the model can influence.
func ciparkFenceToken(run *courierv1alpha1.CoderRun, attempt int) string {
	return ciparkDigest(string(run.UID), "fence", strconv.Itoa(attempt))
}

// ciparkParkKey derives the idempotency key of a park notice. It is stable
// for one park of one head, so a retried report of the same park collapses
// into one transition.
func ciparkParkKey(run *courierv1alpha1.CoderRun, head string) string {
	return ciparkDigest(string(run.UID), "park", head)
}

// ciparkRepairKey derives the idempotency key of one repair attempt.
func ciparkRepairKey(run *courierv1alpha1.CoderRun, attempt int) string {
	return ciparkDigest(string(run.UID), "repair-key", strconv.Itoa(attempt))
}

// ciparkDigest hashes the parts of a derived identifier.
func ciparkDigest(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

// ciparkCapacityAvailable reports whether the lane can take one more
// execution slot right now. A repair attempt reacquires capacity; it never
// assumes it, and it never exceeds the lane's concurrency.
//
// A suspended lane and a lane under an active capacity cooldown both admit
// nothing new, so neither admits a repair: the cooldown bounds re-claim of
// work on an exhausted capacity, and a repair is work.
func (r *CoderRunReconciler) ciparkCapacityAvailable(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	var lane courierv1alpha1.LaneProfile
	if err := r.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, &lane); err != nil {
		return false, err
	}
	if lane.Suspended() {
		return false, nil
	}
	var lanes courierv1alpha1.LaneProfileList
	if err := r.List(ctx, &lanes, client.InNamespace(run.Namespace)); err != nil {
		return false, err
	}
	if _, active := courierv1alpha1.CapacityCooldownActive(&lane, lanes.Items, r.clock()); active {
		return false, nil
	}
	var runs courierv1alpha1.CoderRunList
	if err := r.List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
		return false, err
	}
	return laneHasCapacity(runs.Items, run.Spec.Lane, lane.Spec.Concurrency), nil
}

// observeCIPark applies the mechanical-CI park-and-resume policy to one
// observation of a published run.
//
// handled is false when the lane declares no repair policy, and when the
// ordinary path owns the observation outright (a missing or draft pull
// request). In both cases the caller applies the ordinary post-publication
// grading path unchanged.
func (r *CoderRunReconciler) observeCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, observation PRObservation) (bool, ctrl.Result, error) {
	policy, err := r.ciRepairPolicy(ctx, run)
	if err != nil {
		return false, ctrl.Result{}, err
	}
	if policy == nil {
		return false, ctrl.Result{}, nil
	}
	if observation.PR == "" || observation.Draft {
		// An unobservable pull request is not this loop's business: the
		// ordinary path settles it as a human-facing condition.
		return false, ctrl.Result{}, nil
	}
	// The attempt in flight, if any. Its head is what the park decides on
	// whether that attempt made progress, and its fence token is what the
	// source must be handed back when the attempt ends.
	attempt := run.Status.CIRepair
	fenceToken := ""
	if attempt != nil {
		fenceToken = attempt.FenceToken
	}
	head, attributable := publishedHead(run, observation.Head)
	if !attributable {
		if run.Status.CIPark == nil {
			// The run published nothing this operator can account for:
			// there is no head to park on, and the ordinary path decides.
			return false, ctrl.Result{}, nil
		}
		// The recorded head is gone. The world moved on, and a repair of a
		// head this run does not own is a repair of the wrong commit.
		result, err := r.finishCIPark(ctx, run, observation, courierv1alpha1.CIParkOutcomeUnsupported, fenceToken)
		return true, result, err
	}
	// One status write carries the observed pull request, the park record,
	// the settle candidates, and the end of the attempt in flight. Splitting
	// them would let a controller restart between the two writes lose the
	// fact that an attempt was consumed on this head, and spend the budget
	// again on a head that never moved.
	before := run.DeepCopy()
	changed := false
	if observation.PR != run.Status.PR {
		run.Status.PR = observation.PR
		changed = true
	}
	park, established, republished, noProgress := nextCIPark(run.Status.CIPark, head, attempt, r.clock().UTC())
	if noProgress {
		// The repair attempt published nothing on this head. Spending the
		// budget again on the same head would loop without progress.
		result, err := r.finishCIPark(ctx, run, observation, courierv1alpha1.CIParkOutcomeExhausted, fenceToken)
		return true, result, err
	}
	if !reflect.DeepEqual(park, run.Status.CIPark) {
		run.Status.CIPark = park
		changed = true
	}
	if attempt != nil {
		run.Status.CIRepair = nil
		changed = true
	}
	// The settle candidates follow the ordinary path's rule: a failed check
	// set records the failed candidate, a mechanically green one records the
	// green candidate, and anything else clears both. A flapping report
	// therefore never settles twice, and a check set that is still
	// registering never settles green.
	selector := mechanicalSelector(policy.MechanicalChecks)
	failedCandidate, greenCandidate := ciparkFingerprints(observation, policy.MechanicalChecks)
	failureSettled := failedCandidate != "" && run.Status.FailedCheckFingerprint == failedCandidate
	greenSettled := greenCandidate != "" && run.Status.CheckFingerprint == greenCandidate
	if run.Status.FailedCheckFingerprint != failedCandidate {
		run.Status.FailedCheckFingerprint = failedCandidate
		changed = true
	}
	if run.Status.CheckFingerprint != greenCandidate {
		run.Status.CheckFingerprint = greenCandidate
		changed = true
	}
	if changed {
		if err := r.patchStatus(ctx, before, run); err != nil {
			return true, ctrl.Result{}, err
		}
	}
	if established {
		r.reportCIParked(ctx, run, park, observation.PR)
	}
	if republished {
		r.reportCIRepair(ctx, run, observation.PR, head, source.CIRepairRepushed, fenceToken)
	}
	remaining := policy.MaxAttempts - park.Attempts
	trigger := ClassifyCIParkTrigger(observation, park.Head, selector, remaining)
	switch trigger {
	case CIParkHold:
		result, err := r.holdCIPark(ctx, run, park, policy, observation)
		return true, result, err
	case CIParkResumeRepair:
		result, err := r.resumeCIPark(ctx, run, park, policy, observation, selector, failureSettled)
		return true, result, err
	default:
		result, err := r.settleCIPark(ctx, run, park, observation, policy.MechanicalChecks, remaining, failureSettled, greenSettled)
		return true, result, err
	}
}

// ciparkFingerprints applies the ordinary path's settle-candidate rule to one
// observation: a failed check set records the failed candidate, a mechanically
// green one records the green candidate, and anything else clears both.
//
// The failed candidate is what makes a failure settle: the same failed check
// set must be observed failing twice in a row. An observation that is
// neither failed nor green — a pending check, an unregistered check set —
// clears it, so a failure that flaps red → pending → red never settles and
// never wakes a coordinator.
func ciparkFingerprints(observation PRObservation, configuredNames []string) (failed, green string) {
	mechanicallyGreen, _ := mechanicalVerdict(observation, configuredNames)
	switch {
	case anyCheckFailed(observation):
		return checkSetFingerprint(observation), ""
	case mechanicallyGreen:
		return "", checkSetFingerprint(observation)
	default:
		return "", ""
	}
}

// nextCIPark computes the park record for the head this run published.
//
// A nil park means the run must not continue on this head: the attempt in
// flight consumed its budget on this head and the head has not moved since,
// so the attempt published nothing.
func nextCIPark(current *courierv1alpha1.CIParkState, head string, attempt *courierv1alpha1.CIRepairContext, now time.Time) (park *courierv1alpha1.CIParkState, established, republished, noProgress bool) {
	switch {
	case current == nil:
		return &courierv1alpha1.CIParkState{Head: head, ParkedAt: metav1.NewTime(now)}, true, false, false
	case current.Head != head:
		// The run republished after a repair attempt: the park follows the
		// run's own new head, and the attempt is accounted as republished.
		attempts := current.Attempts
		return &courierv1alpha1.CIParkState{
			Head:        head,
			ParkedAt:    metav1.NewTime(now),
			Attempts:    attempts,
			LastOutcome: courierv1alpha1.CIParkOutcomeRepushed,
		}, true, attempts > 0, false
	case attempt != nil && current.Attempts > 0 && current.Head == attempt.Head:
		return nil, false, false, true
	default:
		return current, false, false, false
	}
}

// holdCIPark keeps the run parked while its configured mechanical checks are
// still pending. The wait is bounded: a check that never reports must not
// keep the run waiting forever, and it must never wake the coordinator.
func (r *CoderRunReconciler) holdCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, park *courierv1alpha1.CIParkState, policy *courierv1alpha1.CIRepairPolicy, observation PRObservation) (ctrl.Result, error) {
	if r.ciparkWindowExpired(park, policy) {
		// A check that never reports must not keep the run waiting forever,
		// and it must never wake the coordinator.
		result, err := r.finishCIPark(ctx, run, observation, courierv1alpha1.CIParkOutcomeUnsupported, "")
		return result, err
	}
	return ctrl.Result{RequeueAfter: ciparkPollDelay}, nil
}

// ciparkWindowExpired reports whether the park has waited longer than the
// lane's window allows. The window bounds every wait the park takes — the
// hold, the settle, and the wait for a lane slot — so no wait can outlive it.
func (r *CoderRunReconciler) ciparkWindowExpired(park *courierv1alpha1.CIParkState, policy *courierv1alpha1.CIRepairPolicy) bool {
	if policy.ObservationWindowSeconds <= 0 || park == nil {
		return false
	}
	window := time.Duration(policy.ObservationWindowSeconds) * time.Second
	return r.clock().UTC().Sub(park.ParkedAt.Time) > window
}

// settleCIPark ends a parked attempt that may not wake the coordinator.
//
// A run whose configured mechanical checks are all present and passed is
// green and hands the pull request on for review. Every other outcome — a
// non-mechanical failure, a configured check that never registered, no
// eligible check at all, an exhausted budget — hands the work off without
// waking the model, and says so in the park record.
func (r *CoderRunReconciler) settleCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, park *courierv1alpha1.CIParkState, observation PRObservation, configuredNames []string, remaining int, failureSettled, greenSettled bool) (ctrl.Result, error) {
	green, _ := mechanicalVerdict(observation, configuredNames)
	if green {
		return r.settleCIParkGreen(ctx, run, observation, greenSettled)
	}
	if anyCheckFailed(observation) && run.Spec.Mode != courierv1alpha1.ModeFixPR && !failureSettled {
		// A resolve-issue run hands off only once two consecutive failed
		// observations carry the same fingerprint, so a single transient
		// failure never ends the attempt.
		return ctrl.Result{RequeueAfter: ciparkPollDelay}, nil
	}
	outcome := courierv1alpha1.CIParkOutcomeUnsupported
	if remaining <= 0 {
		outcome = courierv1alpha1.CIParkOutcomeExhausted
	}
	return r.finishCIPark(ctx, run, observation, outcome, "")
}

// settleCIParkGreen ends a parked run whose configured mechanical checks
// passed. Green settles only onto an identical candidate — two consecutive
// observations of the same check set — so checks that are still registering
// cannot pass.
func (r *CoderRunReconciler) settleCIParkGreen(ctx context.Context, run *courierv1alpha1.CoderRun, observation PRObservation, greenSettled bool) (ctrl.Result, error) {
	if !greenSettled {
		// Green settles only onto an identical candidate — two consecutive
		// observations of the same check set — so checks that are still
		// registering cannot pass.
		return ctrl.Result{RequeueAfter: ciparkPollDelay}, nil
	}
	if run.Status.DeclaredNoChange && run.Spec.Mode == courierv1alpha1.ModeFixPR {
		// A no-change attempt on an open pull request waits for
		// authoritative resolution instead of claiming a push.
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseBlockedExternal, observation.PR, terminalLifecycleIntent{
			error:      "no changes pushed; queued feedback resolution is not verified",
			diagnostic: buildTerminalDiagnostic(r.clock(), nil, "", "", "", false, nil),
		}, nil)
	}
	return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseAwaitingReview, observation.PR, terminalLifecycleIntent{}, nil)
}

// resumeCIPark wakes the same run for one bounded repair.
//
// The failure must be settled, the lane must have capacity, and the budget
// must have room: a parked run never wakes the coordinator on a transient
// report, never exceeds the lane's concurrency, and never repairs more times
// than the lane allows.
func (r *CoderRunReconciler) resumeCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, park *courierv1alpha1.CIParkState, policy *courierv1alpha1.CIRepairPolicy, observation PRObservation, selector MechanicalCheckSelector, failureSettled bool) (ctrl.Result, error) {
	if !failureSettled {
		// The failure must be observed failing twice in a row before it may
		// wake a coordinator, so a flapping report never spends an attempt.
		return ctrl.Result{RequeueAfter: ciparkPollDelay}, nil
	}
	if r.ciparkWindowExpired(park, policy) {
		// The park window bounds every wait, not only the hold: a run that
		// cannot wake in time hands off instead of waiting for a slot that
		// may never free.
		result, err := r.finishCIPark(ctx, run, observation, courierv1alpha1.CIParkOutcomeUnsupported, "")
		return result, err
	}
	available, err := r.ciparkCapacityAvailable(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !available {
		// The slot is reacquired, never assumed: the run stays parked until
		// the lane has room.
		return ctrl.Result{RequeueAfter: capacityRequeueDelay}, nil
	}
	attempt := park.Attempts + 1
	fenceToken, err := r.reserveCIRepair(ctx, run, observation.PR, park.Head, attempt)
	if err != nil {
		if errors.Is(err, source.ErrSuperseded) || errors.Is(err, source.ErrStaleWork) {
			// A newer generation owns the work now. Fail closed rather than
			// race it.
			return r.finishCIPark(ctx, run, observation, courierv1alpha1.CIParkOutcomeUnsupported, "")
		}
		return ctrl.Result{}, err
	}
	// The attempt is consumed in the same patch as the phase change, so a
	// controller restart can never wake the coordinator twice for one
	// attempt. Claimed is the admission reservation: it reacquires the lane
	// slot the repair needs, and the next reconcile launches the coordinator.
	before := run.DeepCopy()
	run.Status.CIPark.Attempts = attempt
	run.Status.CIRepair = &courierv1alpha1.CIRepairContext{
		Head:         park.Head,
		Attempt:      attempt,
		FailedChecks: failedMechanicalChecks(observation, selector),
		Logs:         failedCheckLogs(observation, selector),
		FenceToken:   fenceToken,
		MaxAttempts:  policy.MaxAttempts,
	}
	run.Status.Phase = courierv1alpha1.PhaseClaimed
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// finishCIPark ends a parked attempt without waking the coordinator and
// records why. The park record is the run's durable account of the attempt:
// what it observed and how it ended.
func (r *CoderRunReconciler) finishCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, observation PRObservation, outcome courierv1alpha1.CIParkOutcome, fenceToken string) (ctrl.Result, error) {
	before := run.DeepCopy()
	changed := false
	if run.Status.CIPark != nil {
		run.Status.CIPark.LastOutcome = outcome
		changed = true
	}
	if run.Status.CIRepair != nil {
		// The attempt is over. Its context is cleared in the same write as
		// the outcome, so a restart between the two cannot leave a stale
		// context that reads as an attempt still in flight.
		run.Status.CIRepair = nil
		changed = true
	}
	if changed {
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	result := source.CIRepairUnsupported
	if outcome == courierv1alpha1.CIParkOutcomeExhausted {
		result = source.CIRepairExhausted
	}
	// Nothing was published by the attempt, so the outcome carries no head.
	r.reportCIRepair(ctx, run, observation.PR, "", result, fenceToken)
	if run.Spec.Mode == courierv1alpha1.ModeFixPR {
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseFailed, observation.PR, terminalLifecycleIntent{ciFailure: true}, nil)
	}
	return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseHandedOff, observation.PR, terminalLifecycleIntent{}, nil)
}

// reportCIParked tells the source that the run parked on pending CI, when the
// source can represent that at all. The notice is advisory: a source that
// cannot fence a park never gets one, and a failed notice never blocks the
// run.
func (r *CoderRunReconciler) reportCIParked(ctx context.Context, run *courierv1alpha1.CoderRun, park *courierv1alpha1.CIParkState, pr string) {
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return
	}
	lifecycle, ok := adapter.(source.CIParkLifecycle)
	if !ok {
		return
	}
	notice := source.CIParkNotice{PR: pr, Head: park.Head, IdempotencyKey: ciparkParkKey(run, park.Head)}
	if err := lifecycle.ReportCIParked(ctx, item, notice); err != nil {
		log.FromContext(ctx).V(1).Info("source park notice failed", "run", run.Name, "error", err.Error())
	}
}

// reserveCIRepair reserves the repair opportunity at the source, when the
// source can fence one, and returns the fence token the source issued.
//
// The token is the source's, not the core's: the outcome report must present
// it back, and a source that issues its own token refuses an outcome that
// carries a different one. A source that does not implement the capability is
// never asked, and the core never emulates it with ordinary lifecycle calls.
//
// A superseded or stale reservation fails closed: a newer generation owns
// the work, and a repair would race it.
func (r *CoderRunReconciler) reserveCIRepair(ctx context.Context, run *courierv1alpha1.CoderRun, pr, head string, attempt int) (string, error) {
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return "", err
	}
	lifecycle, ok := adapter.(source.CIParkLifecycle)
	if !ok {
		return "", nil
	}
	reservation := source.CIRepairReservation{
		FenceToken:     ciparkFenceToken(run, attempt),
		IdempotencyKey: ciparkRepairKey(run, attempt),
		PR:             pr,
		Head:           head,
	}
	grant, err := lifecycle.ReserveCIRepair(ctx, item, reservation)
	if err != nil {
		return "", err
	}
	if grant.FenceToken != "" {
		return grant.FenceToken, nil
	}
	return reservation.FenceToken, nil
}

// reportCIRepair publishes the fenced outcome of the last repair attempt to
// the source, when the source can fence one. The report is advisory: the
// operator's own park record is the authoritative account of the attempt, and
// a failed report never blocks the run's ending.
//
// The outcome presents the token the source issued for the attempt. An
// attempt that no source fenced carries no token, and nothing is reported:
// there is no grant to answer.
func (r *CoderRunReconciler) reportCIRepair(ctx context.Context, run *courierv1alpha1.CoderRun, pr, head string, result source.CIRepairResult, fenceToken string) {
	if fenceToken == "" {
		return
	}
	attempts := 0
	if run.Status.CIPark != nil {
		attempts = run.Status.CIPark.Attempts
	}
	if attempts == 0 {
		return
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return
	}
	lifecycle, ok := adapter.(source.CIParkLifecycle)
	if !ok {
		return
	}
	report := source.CIRepairOutcome{
		FenceToken:     fenceToken,
		IdempotencyKey: ciparkRepairKey(run, attempts),
		PR:             pr,
		Head:           head,
		Result:         result,
	}
	if err := lifecycle.ReportCIRepair(ctx, item, report); err != nil {
		log.FromContext(ctx).V(1).Info("source repair outcome report failed", "run", run.Name, "error", err.Error())
	}
}
