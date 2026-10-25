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

	// ciparkSettlementRetryDelay is how often a run whose fenced outcome the
	// source has not accepted yet re-presents that settlement. The report is
	// idempotent — same fence, same idempotency key — so a retry replays the
	// committed transition instead of double-applying it.
	ciparkSettlementRetryDelay = 30 * time.Second

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
	if policy == nil || len(normalizedMechanicalChecks(policy.MechanicalChecks)) == 0 || policy.MaxAttempts <= 0 {
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

// ciparkSettleKey derives the idempotency key of a fenced outcome report
// from the fence token the report presents. The fence is the attempt's own
// identity — it travels inside the recorded settlement, unlike the park
// record the attempt count used to be read from, so a settlement recorded by
// an ending that clears the park against a granted reservation can still be
// re-presented byte-identical from the record alone. The reserve call's key
// stays derived from run and attempt; nothing couples the two, and the
// source contract treats them as independent transitions with independent
// keys. A distinct digest salt keeps the settle key out of every other
// derivation's namespace.
func ciparkSettleKey(fenceToken string) string {
	return ciparkDigest("settle", fenceToken)
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
//
// The admitted count goes through the uncached reader, exactly as ordinary
// admission does: a cached List can lag another run's just-persisted Claimed
// admission, and a lagging count would oversubscribe the lane on reacquisition
// (#548).
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
	if err := r.reader().List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
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
		// A run that is not parked owes the loop nothing and stays on the
		// ordinary path untouched. A parked run walks out of the loop it
		// just left carrying its park and its deferral, and no ordinary
		// ending releases the latter — the ordinary path would transition
		// the run while the source stays deferred against an opportunity
		// that is already over. So the abandon settles the park's source
		// side first (#553), as every one of the loop's own endings would
		// have, and only then hands the observation to the ordinary path
		// unchanged.
		result, err := r.abandonCIPark(ctx, run, observation)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		return false, result, nil
	}
	if observation.PR == "" || observation.Draft {
		// An unobservable pull request is not this loop's business: the
		// ordinary path settles it as a human-facing condition. A park
		// established on the request leaves the loop with it, so like every
		// out-of-loop ending its source side is settled first (#553), and
		// the ordinary path decides the transitions unchanged afterwards.
		result, err := r.abandonCIPark(ctx, run, observation)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		return false, result, nil
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
	if republished && fenceToken != "" {
		// The repushed outcome is owed to the source. Recording it in the
		// same write that advances the park and ends the attempt is what
		// makes a lost response durable retry intent rather than a leak:
		// the fence travels with the record, and a later reconcile
		// re-presents it until the source accepts it.
		run.Status.CISettlement = &courierv1alpha1.CISettlement{
			FenceToken: fenceToken,
			Result:     courierv1alpha1.CIParkOutcomeRepushed,
			Head:       head,
		}
		changed = true
	}
	if established {
		if adapter, _, err := r.adapterAndWorkItem(run); err == nil {
			if _, ok := adapter.(source.CIParkLifecycle); ok {
				// The deferral is owed to the source. Recording it in the
				// same write that establishes the park is what makes a lost
				// notice durable retry intent rather than a duplicate-worker
				// window nobody will ever close.
				run.Status.CIParkDeferral = &courierv1alpha1.CIParkDeferral{
					Head:           park.Head,
					IdempotencyKey: ciparkParkKey(run, park.Head),
				}
				changed = true
			}
		}
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
		if delivered, err := r.deliverCIParkNotice(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source park notice not delivered", "run", run.Name, "error", err.Error())
		}
	}
	if republished && fenceToken != "" {
		if delivered, err := r.deliverCISettlement(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source repair settlement not delivered", "run", run.Name, "error", err.Error())
		}
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
//
// A green ending is still a park that ended without a reservation, so it
// converts the park's deferral into the fence-less close exactly as the
// plain finish does (#534): no reservation was granted, so no fenced
// outcome will ever release the deferral, and a deferral nobody releases
// defers the source's handout race past the run's own ending. The close is
// recorded before it is attempted and the run's ending never waits on it —
// an undelivered close stays durable retry intent that the run's own
// reconciles re-present.
func (r *CoderRunReconciler) settleCIParkGreen(ctx context.Context, run *courierv1alpha1.CoderRun, observation PRObservation, greenSettled bool) (ctrl.Result, error) {
	if !greenSettled {
		// Green settles only onto an identical candidate — two consecutive
		// observations of the same check set — so checks that are still
		// registering cannot pass.
		return ctrl.Result{RequeueAfter: ciparkPollDelay}, nil
	}
	before := run.DeepCopy()
	changed := false
	if run.Status.CIParkDeferral != nil {
		// The pending notice is cleared exactly when the close it owes is
		// recorded, in the same write, as on the plain finish: a source
		// lookup that cannot answer the capability question returns the
		// error before any status write and persists no half-conversion.
		run.Status.CIParkDeferral = nil
		changed = true
	}
	if run.Status.CIPark != nil {
		recorded, err := r.recordCIParkRelease(run)
		if err != nil {
			return ctrl.Result{}, err
		}
		changed = changed || recorded
	}
	if changed {
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Status.CIParkRelease != nil {
		if delivered, err := r.deliverCIParkRelease(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source deferral release not delivered", "run", run.Name, "error", err.Error())
		}
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
		if errors.Is(err, source.ErrSuperseded) || errors.Is(err, source.ErrStaleWork) || errors.Is(err, source.ErrUnsupportedCIPark) {
			// A newer generation owns the work now, or the source cannot
			// defer this work at all: fail closed and hand stewardship back
			// rather than race the newer owner or error forever on work the
			// source will never fence.
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
//
// The park's deferral ends with the park. Where the attempt was fenced, the
// fenced outcome releases the deferral at the source. Where no reservation
// was granted — every caller that lands here with an empty fence token —
// there is no fence to settle, so the close is sent fence-less instead
// (#534): the deferral the park notice established is released by the
// notice's own identity, and the record of the pending notice itself is
// folded into the same write (a late notice must never defer the source
// against an opportunity that is already over).
func (r *CoderRunReconciler) finishCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, observation PRObservation, outcome courierv1alpha1.CIParkOutcome, fenceToken string) (ctrl.Result, error) {
	before := run.DeepCopy()
	changed := false
	if run.Status.CIPark != nil {
		run.Status.CIPark.LastOutcome = outcome
		changed = true
	}
	if run.Status.CIParkDeferral != nil {
		// The opportunity the pending notice described just ended. Whether
		// the notice was accepted or is still pending, the deferral it
		// names is released from here: the pending notice itself would
		// defer the source against nothing, so it is cleared in the same
		// write that records the release intent the fence-less close
		// realizes (the close is advisory when the notice was never
		// accepted).
		run.Status.CIParkDeferral = nil
		changed = true
	}
	if run.Status.CIRepair != nil {
		// The attempt is over. Its context is cleared in the same write as
		// the outcome, so a restart between the two cannot leave a stale
		// context that reads as an attempt still in flight.
		run.Status.CIRepair = nil
		changed = true
	}
	if run.Status.CIPark != nil && fenceToken != "" {
		// The outcome is owed to the source. Recording it in the same write
		// as the outcome, before the report is attempted, is what makes a
		// lost response durable retry intent rather than a leak.
		run.Status.CISettlement = &courierv1alpha1.CISettlement{
			FenceToken: fenceToken,
			Result:     outcome,
		}
		changed = true
	}
	if run.Status.CIPark != nil && fenceToken == "" {
		// The park ends with no granted reservation, so no fenced outcome
		// can release it and none is owed; the fence-less close is owed
		// instead, recorded in this same write before it is attempted, so
		// a lost response stays durable retry intent rather than a
		// deferral nobody will ever release. A source lookup that cannot
		// answer the capability question ends nothing: the deferral is
		// converted into the release as one write — cleared exactly when
		// the close it owes is recorded — so a failed lookup returns the
		// error and persists no half-conversion, and the whole finish
		// retries.
		recorded, err := r.recordCIParkRelease(run)
		if err != nil {
			return ctrl.Result{}, err
		}
		changed = changed || recorded
	}
	if changed {
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Status.CISettlement != nil {
		if delivered, err := r.deliverCISettlement(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source repair settlement not delivered", "run", run.Name, "error", err.Error())
		}
	}
	if run.Status.CIParkRelease != nil {
		if delivered, err := r.deliverCIParkRelease(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source deferral release not delivered", "run", run.Name, "error", err.Error())
		}
	}
	if run.Spec.Mode == courierv1alpha1.ModeFixPR {
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseFailed, observation.PR, terminalLifecycleIntent{ciFailure: true}, nil)
	}
	return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseHandedOff, observation.PR, terminalLifecycleIntent{}, nil)
}

// abandonCIPark releases the source-side park a run carries with it out of
// the CI park loop when the loop abandons the run without a park-ending of
// its own (#553): the lane's ciRepair policy is gone — an operator removed or
// emptied it, or the lane itself disappeared — or the pull request the run
// parked behind is no longer parkable, observed without a pull request or as
// a draft. Both bail-outs hand the observation to the ordinary grading path,
// which then transitions the run (AwaitingReview, NeedsHuman, HandedOff)
// while the park record and its accepted deferral stay intact — and nothing
// on that path ever releases the deferral the park notice established, so
// the source stays deferred against an opportunity that is already over
// until the run is reaped.
//
// A live repair fence reaches here: the coordinator's exit handler turns a
// finished exit back to Verifying alone, and the repair context is meant to
// survive exactly that — the loop clears it in the write of the reconcile
// whose observation consumes the attempt, so an attempt whose coordinator
// just exited carries its fence into Verifying untouched. A policy removed
// across the exit, or a pull request that went unparkable under it, makes
// this abandon the observe that attempt waited for: the armed branch settles
// the reservation exactly as the loop's own endings would rather than
// silently killing it. Releasing a deferral a live fence holds would strand
// it — the fence-less close's moot reading of ErrSuperseded presumes the
// reservation's own fenced outcome will follow, and the abandon kills that
// premise unless it records the outcome itself.
//
// Both conversions are one write: the intent the source is owed is recorded,
// the pending notice a late acceptance would defer against nothing is
// cleared with it, and the park record is cleared in the same patch rather
// than annotated with an outcome. That clearance is also the fire-once
// fence — the conversion happens while the run may still be Verifying for
// several more ordinary reconciles (the two-consecutive-green settle is
// still counting, the pre-dispatch hooks still retry pending reports), and
// each of them walks the same bail-out again, so the record's absence keeps
// every later entry a no-op instead of a second conversion on every poll;
// clearing it retires the park's attempt account with the loop. Neither
// ending ever waits on the source: an undelivered report stays durable
// retry intent, re-presented by the pre-dispatch hook on later reconciles —
// including reconciles while the run is still in Verifying.
func (r *CoderRunReconciler) abandonCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, observation PRObservation) (ctrl.Result, error) {
	if run.Status.CIPark == nil {
		return ctrl.Result{}, nil
	}
	if attempt := run.Status.CIRepair; attempt != nil && attempt.FenceToken != "" {
		return r.abandonFencedCIPark(ctx, run, attempt, observation)
	}
	before := run.DeepCopy()
	// The close is recorded first, while the park record still carries the
	// head it is keyed on; a lookup error abandons nothing.
	if _, err := r.recordCIParkRelease(run); err != nil {
		return ctrl.Result{}, err
	}
	if run.Status.CIParkDeferral != nil {
		// The opportunity the pending notice described is over with the
		// park: the notice is cleared in the same write that records the
		// release intent the fence-less close realizes, as on every finish.
		run.Status.CIParkDeferral = nil
	}
	// The record's absence is what fences the next abandon against the
	// ordinary reconciles that follow while the run is still Verifying; it
	// is also the attempt account going away.
	run.Status.CIPark = nil
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}
	if run.Status.CIParkRelease != nil {
		if delivered, err := r.deliverCIParkRelease(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source deferral release not delivered", "run", run.Name, "error", err.Error())
		}
	}
	return ctrl.Result{}, nil
}

// abandonFencedCIPark is the abandon's armed branch: a repair attempt is
// still in flight with the fence the source granted its reservation. The
// attempt's outcome is never ambiguous at the abandon — the observation that
// walks the run out of the loop is exactly the observation the loop's own
// endings would grade the attempt against, and the mapping reads the
// attempt's own fields, not the park record the same write clears — so the
// mapping is the loop's own:
//
//   - a new head the run still attributes as its own published head is the
//     repair's republish, and the settlement is what the park advance would
//     have recorded — the repushed outcome naming the new head;
//   - the recorded head gone from the world is the same failed-closed
//     finish the loop performs for a head it cannot own — unsupported;
//   - otherwise the attempt published nothing on its head — the loop's
//     no-progress finish, exhausted.
//
// The settlement is recorded, and the attempt context, the pending notice,
// and the park record die with it in the same write. No fence-less close is
// recorded here — a granted reservation is never released fence-less, the
// fenced outcome is the release, and the delivery core is exactly the
// finish's: best-effort, with an undelivered report left as durable intent
// the pre-dispatch hook re-presents. The close's best-effort call is no
// counterpart: its record is provably nil after this branch, so calling it
// would only be dead code that reads like a second owed delivery.
func (r *CoderRunReconciler) abandonFencedCIPark(ctx context.Context, run *courierv1alpha1.CoderRun, attempt *courierv1alpha1.CIRepairContext, observation PRObservation) (ctrl.Result, error) {
	before := run.DeepCopy()
	head, attributable := publishedHead(run, observation.Head)
	settlement := &courierv1alpha1.CISettlement{FenceToken: attempt.FenceToken}
	switch {
	case attributable && head != attempt.Head:
		settlement.Result = courierv1alpha1.CIParkOutcomeRepushed
		settlement.Head = head
	case !attributable:
		settlement.Result = courierv1alpha1.CIParkOutcomeUnsupported
	default:
		settlement.Result = courierv1alpha1.CIParkOutcomeExhausted
	}
	run.Status.CISettlement = settlement
	if run.Status.CIParkDeferral != nil {
		// The opportunity ends with the attempt: the settlement releases the
		// deferral the source keeps on the reservation, so the pending
		// notice is cleared in the same write, as on every finish — a late
		// notice must never defer the source against an opportunity that is
		// over.
		run.Status.CIParkDeferral = nil
	}
	run.Status.CIRepair = nil
	run.Status.CIPark = nil
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}
	if run.Status.CISettlement != nil {
		if delivered, err := r.deliverCISettlement(ctx, run); !delivered {
			log.FromContext(ctx).V(1).Info("source repair settlement not delivered", "run", run.Name, "error", err.Error())
		}
	}
	return ctrl.Result{}, nil
}

// recordCIParkRelease records the fence-less close a park that ended
// without a reservation owes the source (#534). The close is keyed by the
// park notice's own identity — that key derives from the run and the parked
// head, not from the notice record — so it releases the deferral the notice
// established even when the notice record itself is already gone. The
// source lookup gates the record: a source without the capability never had
// a deferral settled at it and nothing is recorded for one, and the lookup
// error lets the caller persist no half-conversion. The release is advisory
// by nature: the source refuses it moot when no deferral for this notice
// exists, and deliverCIParkRelease drops the record either way.
func (r *CoderRunReconciler) recordCIParkRelease(run *courierv1alpha1.CoderRun) (bool, error) {
	adapter, _, err := r.adapterAndWorkItem(run)
	if err != nil {
		return false, err
	}
	if _, ok := adapter.(source.CIParkLifecycle); !ok {
		return false, nil
	}
	run.Status.CIParkRelease = &courierv1alpha1.CIParkRelease{
		Head:           run.Status.CIPark.Head,
		IdempotencyKey: ciparkParkKey(run, run.Status.CIPark.Head),
	}
	return true, nil
}

// deliverCIParkNotice announces the park to the source, when the source can
// fence a deferral at all, and clears the pending notice once the source
// accepts it. The notice is idempotent — same key — so a retry after a lost
// response re-establishes the same deferral instead of double-booking it. A
// source that cannot fence never gets a notice, and nothing is recorded for
// one, so there is nothing to retry.
func (r *CoderRunReconciler) deliverCIParkNotice(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	record := run.Status.CIParkDeferral
	if record == nil {
		return true, nil
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return false, err
	}
	lifecycle, ok := adapter.(source.CIParkLifecycle)
	if !ok {
		return false, fmt.Errorf("coderun controller: source %q does not implement the CI park lifecycle", run.Spec.Source)
	}
	notice := source.CIParkNotice{PR: run.Status.PR, Head: record.Head, IdempotencyKey: record.IdempotencyKey}
	if err := lifecycle.ReportCIParked(ctx, item, notice); err != nil {
		if errors.Is(err, source.ErrUnsupportedCIPark) {
			// The source cannot represent this park at all — implement and
			// queue-less work behind a Dispatch adapter — and the refusal is
			// permanent: retrying would wedge the run's own park loop, which
			// requeues ahead of every phase dispatch. Stewardship stays with
			// the core, exactly as for a source without the capability.
			log.FromContext(ctx).V(1).Info("source cannot represent the CI park; dropping the pending notice", "run", run.Name)
			before := run.DeepCopy()
			run.Status.CIParkDeferral = nil
			if err := r.patchStatus(ctx, before, run); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	before := run.DeepCopy()
	run.Status.CIParkDeferral = nil
	if err := r.patchStatus(ctx, before, run); err != nil {
		return false, err
	}
	return true, nil
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
// the work, and a repair would race it. A source that reports the work
// unsupported — it cannot defer this item at all — ends the run the same
// way, so the sentinel never loops as a reconcile error.
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

// reportCIRepair publishes the fenced outcome of one repair attempt to the
// source, when the source can fence one. The report is idempotent: the
// idempotency key is derived from the run and the attempt it settles, so a
// re-presented settlement replays the committed transition instead of
// double-applying it.
//
// The outcome presents the token the source issued for the attempt. An
// attempt that no source fenced carries no token, and nothing is reported:
// there is no grant to answer.
func (r *CoderRunReconciler) reportCIRepair(ctx context.Context, run *courierv1alpha1.CoderRun, pr, head string, result source.CIRepairResult, fenceToken string) error {
	if fenceToken == "" {
		return nil
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return err
	}
	lifecycle, ok := adapter.(source.CIParkLifecycle)
	if !ok {
		return fmt.Errorf("coderun controller: source %q does not implement the CI park lifecycle", run.Spec.Source)
	}
	report := source.CIRepairOutcome{
		FenceToken:     fenceToken,
		IdempotencyKey: ciparkSettleKey(fenceToken),
		PR:             pr,
		Head:           head,
		Result:         result,
	}
	return lifecycle.ReportCIRepair(ctx, item, report)
}

// deliverCISettlement re-presents the fenced outcome a run still owes the
// source — recorded on the run when the attempt settled, before the report
// was attempted — and clears the record once the source accepts it. A lost
// response, a restart, or an unavailable source therefore leaves durable
// retry intent rather than a deferral nobody will ever release: the retry
// presents the same fence and the same idempotency key, so the source
// replays the committed transition instead of double-booking it.
func (r *CoderRunReconciler) deliverCISettlement(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	record := run.Status.CISettlement
	if record == nil {
		return true, nil
	}
	err := r.reportCIRepair(ctx, run, run.Status.PR, record.Head, source.CIRepairResult(record.Result), record.FenceToken)
	if err != nil {
		if errors.Is(err, source.ErrUnsupportedCIPark) {
			// The source cannot represent the park/repair lifecycle for this
			// work at all, and the refusal is permanent: nothing is owed, and
			// retrying would wedge the run's ending behind it forever.
			log.FromContext(ctx).V(1).Info("source cannot represent the CI park; dropping the pending settlement", "run", run.Name)
			before := run.DeepCopy()
			run.Status.CISettlement = nil
			if err := r.patchStatus(ctx, before, run); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	before := run.DeepCopy()
	run.Status.CISettlement = nil
	if err := r.patchStatus(ctx, before, run); err != nil {
		return false, err
	}
	return true, nil
}

// deliverCIParkRelease closes the deferral a finished park still owes the
// source (#534) and clears the record once the close resolves. The close is
// fence-less and keyed by the park notice's own idempotency key, so a retry
// after a lost response re-presents the same close for the same deferral.
//
// The release never waits on the source — the run is already ending — and
// it is advisory by nature: a refusal saying the deferral is not there
// resolves the intent as moot. Three refusals mean exactly that, and the
// record is dropped as the proof of delivery in each:
//
//   - source.ErrStaleWork — the deferral does not exist (the notice was
//     never accepted, an unknown key, or it was already released or
//     settled): there is nothing to close;
//   - source.ErrSuperseded — the deferral is no longer ours to close (a
//     newer generation owns the work, or a granted reservation holds it:
//     that reservation's own fenced outcome releases it);
//   - source.ErrUnsupportedCIPark — the source cannot represent the
//     release at all, and that refusal is permanent: retrying would wedge
//     the run's ending behind a close it will never take;
//   - source.ErrPermanentRefusal — a 400 or 403 an identical retry can
//     never clear (a route that cannot represent the fence-less close
//     under version skew, or an identity that no longer owns the work):
//     the same never-deliverable posture, dropped the same way so the
//     release stays advisory.
//
// Every other failure keeps the record: the close is re-presented on the
// run's own cadence with the same key, and the source's idempotency makes
// the replay collapse into the one close.
func (r *CoderRunReconciler) deliverCIParkRelease(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	record := run.Status.CIParkRelease
	if record == nil {
		return true, nil
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return false, err
	}
	lifecycle, ok := adapter.(source.CIParkLifecycle)
	if !ok {
		// The gate that recorded the release is also the one that could
		// have established a deferral; a source without the capability can
		// never owe one, so a stale record from before resolves as
		// nothing owed rather than wedging the ending behind it.
		log.FromContext(ctx).V(1).Info("source does not implement the CI park lifecycle; dropping the pending release", "run", run.Name)
		before := run.DeepCopy()
		run.Status.CIParkRelease = nil
		if err := r.patchStatus(ctx, before, run); err != nil {
			return false, err
		}
		return true, nil
	}
	release := source.CIParkRelease{PR: run.Status.PR, Head: record.Head, IdempotencyKey: record.IdempotencyKey}
	if err := lifecycle.ReleaseCIPark(ctx, item, release); err != nil {
		switch {
		case errors.Is(err, source.ErrUnsupportedCIPark):
			log.FromContext(ctx).V(1).Info("source cannot represent the CI deferral release; dropping the pending release", "run", run.Name)
			before := run.DeepCopy()
			run.Status.CIParkRelease = nil
			if err := r.patchStatus(ctx, before, run); err != nil {
				return false, err
			}
			return true, nil
		case errors.Is(err, source.ErrPermanentRefusal):
			// A 400 or 403 refusal is equally never-deliverable: a route
			// that cannot represent the fence-less close (version skew) or
			// an identity that no longer owns the work refuses this exact
			// close on every retry. The release is advisory, so the record
			// drops instead of holding the run's terminal lifecycle behind
			// a close the source will never take.
			log.FromContext(ctx).V(1).Info("source refuses the CI deferral release permanently; dropping the pending release", "run", run.Name, "error", err.Error())
			before := run.DeepCopy()
			run.Status.CIParkRelease = nil
			if err := r.patchStatus(ctx, before, run); err != nil {
				return false, err
			}
			return true, nil
		case errors.Is(err, source.ErrStaleWork), errors.Is(err, source.ErrSuperseded):
			// The close resolved moot — there was nothing to release, or
			// the deferral is no longer this run's. Dropping the record is
			// the proof of delivery: nothing is owed for a deferral the
			// source never held.
			log.FromContext(ctx).V(1).Info("source deferral already gone; release resolved moot", "run", run.Name, "error", err.Error())
			before := run.DeepCopy()
			run.Status.CIParkRelease = nil
			if err := r.patchStatus(ctx, before, run); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	before := run.DeepCopy()
	run.Status.CIParkRelease = nil
	if err := r.patchStatus(ctx, before, run); err != nil {
		return false, err
	}
	return true, nil
}
