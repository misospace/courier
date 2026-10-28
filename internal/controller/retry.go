package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/forge"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
)

const (
	// maxRetryRequestIDLength mirrors the CRD maxLength on status.retryRequest.id
	// and the RetryAnnotation bound: 128 Unicode code points, matching the
	// soft-stop request ID and lifecycle retry token.
	maxRetryRequestIDLength = 128

	// retrySettlementRequeueDelay spaces retries of a Requested record whose
	// source call is transiently failing, so durable intent converges without
	// burning the workqueue's rate-limited backoff on an otherwise-settled run.
	retrySettlementRequeueDelay = 30 * time.Second

	// retryForgeRequeueDelay spaces retries of the fix-pr forge guard while the
	// forge read is transiently unavailable, so an unreadable forge neither
	// consumes the request nor fails open into a requeue.
	retryForgeRequeueDelay = time.Minute

	// retryAnnouncedAnnotation records, per run, the set of retry request IDs
	// whose already-requeued event and metric were last announced, as
	// comma-joined 64-bit hash tokens (retryAnnouncedToken). It lives on the
	// object's metadata — never its status — so the settled Requeued record that
	// the "events only" refusal must leave byte-for-byte immutable is not touched,
	// while each announcement stays exactly-once per distinct value across
	// reconciles and restarts.
	retryAnnouncedAnnotation = "courier.misospace.dev/retry-announced"

	// maxRetryAnnouncedTokens bounds the announced set. Each announcement needs a
	// distinct operator-written annotation value, so the bound is far above any
	// honest use; past it, further values are not announced (never announced more
	// than once), and the bounded marker cannot grow the object's metadata
	// without limit or wedge the reconcile against the API server's annotation
	// size limits.
	maxRetryAnnouncedTokens = 32

	// retryAnnouncedTokenLength is the hex length of a marker token: the first
	// 8 bytes of the request ID's SHA-256. Collisions among a bounded set of
	// tokens are negligible and would only suppress an announcement, never
	// duplicate one.
	retryAnnouncedTokenLength = 16

	// retryAnnouncedMarkerPrefix versions the announced-set marker ("v1:<token>,…")
	// so a marker that does not carry it is unambiguously a legacy single raw-ID
	// marker, whatever characters the request ID itself contains.
	retryAnnouncedMarkerPrefix = "v1:"
)

// validRetryRequestID reports whether an annotation value is a usable opaque
// request ID: non-empty, valid UTF-8, and at most 128 Unicode code points. An
// invalid value is the operator's to correct; it consumes nothing and writes no
// record, exactly as the soft-stop and lifecycle-report retry ingress treat it.
func validRetryRequestID(id string) bool {
	return id != "" && utf8.ValidString(id) && utf8.RuneCountInString(id) <= maxRetryRequestIDLength
}

// retryIdempotencyKey derives the source requeue identity from the run's
// namespace, name, and opaque request ID, appending the run UID when set. The
// request ID is operator-written opaque data, so it is folded into a hex
// SHA-256 digest instead of being embedded raw in the slash-delimited key: the
// digest is deterministic across reconciles (a retry after a lost status write
// presents the same key, and a source that has committed the requeue replays
// its original result instead of launching a second replacement) and it keeps
// every key segment unambiguous — no request-ID character can alias another
// request's key or collide with the UID segment.
func retryIdempotencyKey(run *courierv1alpha1.CoderRun, requestID string) string {
	digest := sha256.Sum256([]byte(requestID))
	key := fmt.Sprintf("coderun/%s/%s/%s", run.Namespace, run.Name, hex.EncodeToString(digest[:]))
	if run.UID != "" {
		key += "/" + string(run.UID)
	}
	return key
}

// syncRetry is the controller's translation of the RetryAnnotation into the
// operator-owned status.retryRequest record and its four-state machine —
// Requested, then one of the settled Requeued, Moot, Refused. It is the sole
// writer of the record, runs at the head of the terminal dispatch so an
// operator's explicit request is honored before the run's ordinary retention or
// report duties, and returns handled=true when it has owned this reconcile (a
// record was written, driven, or is still Requested) or handled=false when the
// ordinary terminal dispatch should proceed unchanged. A non-nil error always
// propagates to the workqueue, whether or not it is handled, so a lost status or
// metadata write re-arms and re-drives the durable intent. It never changes the
// phase, never deletes, and never touches evidence: the predecessor stays exactly
// as terminalization left it.
func (r *CoderRunReconciler) syncRetry(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, bool, error) {
	record := run.Status.RetryRequest
	annotation := run.Annotations[courierv1alpha1.RetryAnnotation]

	// Resolve any outstanding Requested record first. Its source call may already
	// have committed, so it is driven to a settled state — replaying its
	// idempotency key — before any newer annotation value is considered. A changed
	// or removed annotation never cancels it.
	if record != nil && record.State == courierv1alpha1.RetryRequestStateRequested {
		return r.settleRetryRecord(ctx, run, record)
	}

	// A valid new request is required to open or refuse anything; an absent or
	// invalid annotation over no pending record leaves the run to its ordinary
	// terminal dispatch.
	if !validRetryRequestID(annotation) {
		return ctrl.Result{}, false, nil
	}

	// A Requeued record is terminal for the predecessor's whole lifetime: exactly
	// one replacement attempt may launch per predecessor, so a new value never
	// reopens. It is refused already-requeued in events only, announced at most
	// once per value, and the settled record is never rewritten.
	if record != nil && record.State == courierv1alpha1.RetryRequestStateRequeued {
		if annotation == record.ID {
			return ctrl.Result{}, false, nil
		}
		return r.announceAlreadyRequeued(ctx, run, annotation)
	}

	// A value that matches a settled Refused/Moot record is the request that
	// already produced it — a no-op. Any other value (no record, or a new value
	// over Refused/Moot where the operator fixed the blocker and asks again) opens
	// a fresh request that replaces the settled record.
	if record != nil && annotation == record.ID {
		return ctrl.Result{}, false, nil
	}
	return r.beginRetry(ctx, run, annotation)
}

// beginRetry validates a fresh request against the core (source-agnostic)
// eligibility rules, persists the Requested record before any source call, then
// settles it against the adapter's fenced requeue capability. A core refusal
// settles Refused immediately without ever calling the source.
func (r *CoderRunReconciler) beginRetry(ctx context.Context, run *courierv1alpha1.CoderRun, requestID string) (ctrl.Result, bool, error) {
	// Core refusals decided before any source call: the phase is not a retryable
	// terminal failure, or the run rode the automatic recovery channel.
	if refusal, refuse := coreRetryRefusal(run); refuse {
		return r.writeRetryRefusal(ctx, run, requestID, refusal)
	}

	// A retryable phase additionally requires the run's source lifecycle report to
	// have settled: requeueing the source's work before the run has reported its
	// ending would mutate source state out from under an unsettled report. A still
	// pending report defers the request (handled=false) so the report-retry path
	// proceeds first; the annotation rides until the report lands.
	if !lifecycleReportSettled(run) {
		return ctrl.Result{}, false, nil
	}

	// Never bypass active ownership: another run for the same source work still
	// active refuses, decided against the world and independent of any source
	// report.
	active, err := r.activeOwnerExists(ctx, run)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if active {
		return r.writeRetryRefusal(ctx, run, requestID, courierv1alpha1.RetryRefusalActiveOwnerExists)
	}

	// The fix-pr forge guard reads the run's recorded pull request through the
	// configured forge capability, exactly as the merge observation does. A merged
	// or closed recorded request is permanent for this predecessor and refuses
	// upstream-closed; an unreadable forge proves nothing, consumes nothing, and
	// requeues rather than failing open into a requeue.
	if run.Spec.Mode == courierv1alpha1.ModeFixPR {
		closed, unreadable, err := r.fixPRUpstreamClosed(ctx, run)
		if err != nil {
			return ctrl.Result{}, true, err
		}
		if unreadable {
			return ctrl.Result{RequeueAfter: retryForgeRequeueDelay}, true, nil
		}
		if closed {
			return r.writeRetryRefusal(ctx, run, requestID, courierv1alpha1.RetryRefusalUpstreamClosed)
		}
	}

	// The source must implement the fenced requeue capability. The core never
	// imitates a requeue with ordinary Claim/Release/Transition calls, so an
	// adapter without it is a permanent unsupported-source refusal.
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	requeuer, ok := adapter.(source.AttemptRequeuer)
	if !ok {
		return r.writeRetryRefusal(ctx, run, requestID, courierv1alpha1.RetryRefusalUnsupportedSource)
	}

	// Persist Requested before the source call. From here the request is durable
	// intent that a later reconcile replays under the same key, so a crash before
	// or during the call loses nothing.
	before := run.DeepCopy()
	now := r.clock().UTC()
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID:          requestID,
		RequestedAt: metav1.NewTime(now),
		State:       courierv1alpha1.RetryRequestStateRequested,
	}
	setRetryCondition(run, courierv1alpha1.RetryRequestStateRequested, "", r.clock(), run.Generation)
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, true, err
	}
	r.observeRetry(run, retryOutcomeRequested)
	r.emitRetry(run, requestID, courierv1alpha1.RetryRequestStateRequested, "", "")
	return r.callRequeue(ctx, run, run.Status.RetryRequest, requeuer, item)
}

// settleRetryRecord drives an already-persisted Requested record to a settled
// state, replaying its idempotency key against the adapter. It is the crash-
// recovery path: whatever happened between the Requested write and the outcome
// write, the same key is re-presented and the source replays its committed
// transition (or reports it moot).
func (r *CoderRunReconciler) settleRetryRecord(ctx context.Context, run *courierv1alpha1.CoderRun, record *courierv1alpha1.RetryRequest) (ctrl.Result, bool, error) {
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	requeuer, ok := adapter.(source.AttemptRequeuer)
	if !ok {
		// A Requested record was written when the adapter supported the capability.
		// If the registry no longer resolves one, the source call's fate is unknown,
		// so the intent is neither consumed nor falsely settled: surface the error
		// and let the workqueue retry until the adapter is restored.
		return ctrl.Result{}, true, fmt.Errorf("retry request %q outstanding but source %q no longer implements the requeue capability", record.ID, run.Spec.Source)
	}
	return r.callRequeue(ctx, run, record, requeuer, item)
}

// callRequeue is the single adapter-call-and-settle point for a Requested record,
// used both immediately after the Requested write and on a later replay. Success
// settles Requeued with any returned attempt identity; a classified refusal settles
// Moot or Refused; any other error is transient and leaves the durable Requested
// record untouched to retry under the same key.
func (r *CoderRunReconciler) callRequeue(ctx context.Context, run *courierv1alpha1.CoderRun, record *courierv1alpha1.RetryRequest, requeuer source.AttemptRequeuer, item source.WorkItem) (ctrl.Result, bool, error) {
	req := source.AttemptRequeueRequest{
		FenceToken:     run.Status.SourceReceipt,
		IdempotencyKey: retryIdempotencyKey(run, record.ID),
		Reason:         record.ID,
	}
	result, err := requeuer.RequeueAttempt(ctx, item, req)
	if err == nil {
		return r.settleRetryOutcome(ctx, run, record, courierv1alpha1.RetryRequestStateRequeued, "", normalizeRetryAttemptID(ctx, run, result.AttemptID))
	}
	var refusal *source.RetryRefusalError
	if errors.As(err, &refusal) {
		if refusal.Moot {
			// The moot family carries exactly one reason in the closed vocabulary.
			// Any adapter-supplied value — empty, unknown, or longer than the CRD's
			// 128-code-point bound — is normalized to it, so a hostile or buggy
			// adapter cannot mint free text, and an oversized reason cannot fail the
			// settle write and wedge the durable record Requested forever.
			return r.settleRetryOutcome(ctx, run, record, courierv1alpha1.RetryRequestStateMoot, courierv1alpha1.RetryRefusalAlreadyRequeued, "")
		}
		reason := refusal.Reason
		if !isRetryAdapterRefusalReason(reason) {
			reason = courierv1alpha1.RetryRefusalSourceRefused
		}
		return r.settleRetryOutcome(ctx, run, record, courierv1alpha1.RetryRequestStateRefused, reason, "")
	}
	// A transient source failure proves nothing about the requeue, so the durable
	// Requested record stands unchanged and retries under the same key. Nothing is
	// written here: the record is already persisted.
	log.FromContext(ctx).V(1).Info("retry requeue transiently failed; keeping the request Requested",
		"run", run.Name, "requestID", record.ID, "error", err.Error())
	return ctrl.Result{RequeueAfter: retrySettlementRequeueDelay}, true, nil
}

// normalizeRetryAttemptID bounds the adapter-supplied attempt identity to the
// CRD's maxLength on status.retryRequest.attemptID. The requeue has already
// committed by the time the identity is examined, so a hostile or buggy adapter
// returning an oversized value must not be allowed to fail the settle write and
// wedge the durable record Requested forever: the value is dropped, and the
// CRD's empty-attemptID meaning — the source requeued without exposing a new
// identity, discovery observes the successor later — already covers that case.
func normalizeRetryAttemptID(ctx context.Context, run *courierv1alpha1.CoderRun, attemptID string) string {
	if utf8.RuneCountInString(attemptID) <= courierv1alpha1.RetryRequestMaxAttemptIDLength {
		return attemptID
	}
	log.FromContext(ctx).V(1).Info("dropping adapter attempt identity over the CRD bound",
		"run", run.Name, "length", utf8.RuneCountInString(attemptID))
	return ""
}

// settleRetryOutcome overwrites the Requested record with its settled state — same
// ID and RequestedAt, new state, bounded reason, optional attempt identity, and a
// settled timestamp — mirrors the condition, and records one event and one metric.
// The record's serialization always carries reason and settledAt, so a later
// replacement Requested clears this record's sub-keys under a merge patch.
func (r *CoderRunReconciler) settleRetryOutcome(ctx context.Context, run *courierv1alpha1.CoderRun, record *courierv1alpha1.RetryRequest, state courierv1alpha1.RetryRequestState, reason, attemptID string) (ctrl.Result, bool, error) {
	before := run.DeepCopy()
	now := r.clock().UTC()
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID:          record.ID,
		RequestedAt: record.RequestedAt,
		State:       state,
		Reason:      reason,
		AttemptID:   attemptID,
		SettledAt:   &metav1.Time{Time: now},
	}
	setRetryCondition(run, state, reason, r.clock(), run.Generation)
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, true, err
	}
	r.observeRetry(run, retryMetricOutcome(state))
	r.emitRetry(run, record.ID, state, reason, attemptID)
	// Once settled, the ordinary terminal dispatch resumes ownership: the run's
	// retention window starts (or resumes) and the report path proceeds.
	return ctrl.Result{}, false, nil
}

// writeRetryRefusal records a core, pre-dispatch refusal as a settled Refused
// request with its bounded reason. No source call is made and nothing requeues.
func (r *CoderRunReconciler) writeRetryRefusal(ctx context.Context, run *courierv1alpha1.CoderRun, requestID, reason string) (ctrl.Result, bool, error) {
	before := run.DeepCopy()
	now := r.clock().UTC()
	run.Status.RetryRequest = &courierv1alpha1.RetryRequest{
		ID:          requestID,
		RequestedAt: metav1.NewTime(now),
		State:       courierv1alpha1.RetryRequestStateRefused,
		Reason:      reason,
		SettledAt:   &metav1.Time{Time: now},
	}
	setRetryCondition(run, courierv1alpha1.RetryRequestStateRefused, reason, r.clock(), run.Generation)
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, true, err
	}
	r.observeRetry(run, retryOutcomeRefused)
	r.emitRetry(run, requestID, courierv1alpha1.RetryRequestStateRefused, reason, "")
	return ctrl.Result{}, true, nil
}

// announceAlreadyRequeued refuses a request arriving over a settled Requeued
// record in events only: the settled record is immutable, so the refusal is
// announced once per distinct annotation value via a metadata marker and the
// ordinary terminal dispatch then proceeds. The event and metric are recorded
// only after the marker write succeeds, so a failed write re-announces rather
// than dropping the audit signal.
func (r *CoderRunReconciler) announceAlreadyRequeued(ctx context.Context, run *courierv1alpha1.CoderRun, annotation string) (ctrl.Result, bool, error) {
	token := retryAnnouncedToken(annotation)
	announced := retryAnnouncedSet(run.Annotations[retryAnnouncedAnnotation])
	if _, ok := announced[token]; ok {
		return ctrl.Result{}, false, nil
	}
	if len(announced) >= maxRetryAnnouncedTokens {
		// The marker is bounded, so past the cap a further distinct value is not
		// announced — at most once per value still holds — and the bounded
		// metadata write cannot wedge the reconcile against annotation-size
		// limits.
		log.FromContext(ctx).V(1).Info("already-requeued announcement cap reached; not announcing further distinct values",
			"run", run.Name, "requestID", annotation, "cap", maxRetryAnnouncedTokens)
		return ctrl.Result{}, false, nil
	}
	announced[token] = struct{}{}
	before := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[retryAnnouncedAnnotation] = retryAnnouncedMarker(announced)
	if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, false, err
	}
	r.observeRetry(run, retryOutcomeRefused)
	r.emitRetry(run, annotation, courierv1alpha1.RetryRequestStateRefused, courierv1alpha1.RetryRefusalAlreadyRequeued, "")
	return ctrl.Result{}, false, nil
}

// retryAnnouncedToken derives the fixed-width marker token for a request ID: the
// hex encoding of the first 8 bytes of its SHA-256. Tokens are compared instead
// of raw IDs so the marker cannot be confused by delimiter characters inside an
// opaque, operator-chosen request ID.
func retryAnnouncedToken(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:8])
}

// retryAnnouncedSet parses the announced marker into the set of tokens it
// records. A marker without the version prefix is a legacy single raw-ID marker
// from before the set encoding: its raw value was itself an announced request
// ID, so its token seeds the set and the value it announced is never announced
// again across the upgrade. Unparsable entries inside a versioned marker cannot
// equal any computed token and are dropped: that can only suppress less, never
// announce more than once.
func retryAnnouncedSet(marker string) map[string]struct{} {
	set := map[string]struct{}{}
	if marker == "" {
		return set
	}
	raw, versioned := strings.CutPrefix(marker, retryAnnouncedMarkerPrefix)
	if !versioned {
		return map[string]struct{}{retryAnnouncedToken(marker): {}}
	}
	for _, token := range strings.Split(raw, ",") {
		if len(token) == retryAnnouncedTokenLength && isHex(token) {
			set[token] = struct{}{}
		}
	}
	return set
}

// retryAnnouncedMarker serializes the announced-token set back into the marker.
func retryAnnouncedMarker(set map[string]struct{}) string {
	tokens := make([]string, 0, len(set))
	for token := range set {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	return retryAnnouncedMarkerPrefix + strings.Join(tokens, ",")
}

// isHex reports whether s consists solely of lowercase hex digits.
func isHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return s != ""
}

// coreRetryRefusal decides, from the run alone and before any source call, whether
// the phase can be retried at all. Failed and NeedsHuman are the retryable
// terminal failures. BlockedExternal refuses the dedicated automatic-recovery-
// channel reason: it is recoverable and its source re-issues the work through the
// capacity-recovery channel, never a retry request. AwaitingReview and HandedOff —
// the only other phases the terminal dispatch reaches — belong to the source's own
// repair and review paths.
func coreRetryRefusal(run *courierv1alpha1.CoderRun) (string, bool) {
	switch run.Status.Phase {
	case courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseNeedsHuman:
		return "", false
	case courierv1alpha1.PhaseBlockedExternal:
		return courierv1alpha1.RetryRefusalAutomaticRecoveryChannel, true
	default:
		return courierv1alpha1.RetryRefusalNotRetryablePhase, true
	}
}

// activeOwnerExists reports whether another CoderRun for the same
// source-granular work — qualified source identity plus repository and ref — is
// still active (Pending, Claimed, Running, or Verifying) in the namespace. The
// identity is Source + Repo + Ref, independent of the source agent: work is
// owned per source work item, not per agent. It reads through the uncached
// reader so a sibling run's just-persisted admission is never missed by a
// lagging cache. This is the "never bypass active ownership" invariant, checked
// against the world independent of any source report.
func (r *CoderRunReconciler) activeOwnerExists(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	var runs courierv1alpha1.CoderRunList
	if err := r.reader().List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
		return false, err
	}
	for i := range runs.Items {
		other := &runs.Items[i]
		if other.Name == run.Name {
			continue
		}
		if other.Spec.Source != run.Spec.Source {
			continue
		}
		if other.Spec.Repo != run.Spec.Repo || other.Spec.Ref != run.Spec.Ref {
			continue
		}
		switch other.Status.Phase {
		case courierv1alpha1.PhasePending, courierv1alpha1.PhaseClaimed,
			courierv1alpha1.PhaseRunning, courierv1alpha1.PhaseVerifying:
			return true, nil
		}
	}
	return false, nil
}

// fixPRUpstreamClosed reads the run's recorded pull request through the forge
// read-pull-request capability, with the same exact head-identity check the
// merge observation applies, and reports whether the upstream request is closed
// for this predecessor (merged or closed without merge). unreadable is true when
// the capability is unavailable or the read fails: the guard proves nothing and
// must not consume the request, and never fails open into a requeue. A run with
// no recorded pull-request identity — no number, or no branch to verify the head
// against — has published nothing verifiable, so there is no upstream to be
// closed and the guard is not engaged. A read pull request whose head identity
// no longer matches the recorded one is not the pull request this run produced,
// so neither a merge nor a close on it is this run's outcome and the guard is
// likewise not engaged.
func (r *CoderRunReconciler) fixPRUpstreamClosed(ctx context.Context, run *courierv1alpha1.CoderRun) (closed, unreadable bool, err error) {
	number, parseErr := strconv.Atoi(strings.TrimSpace(run.Status.PR))
	if parseErr != nil || number <= 0 || strings.TrimSpace(run.Status.Branch) == "" {
		return false, false, nil
	}
	if r.PullRequests == nil || !r.PullRequests.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		return false, true, nil
	}
	pull, readErr := r.PullRequests.ReadPullRequest(ctx, forge.PullRequestRef{Repo: run.Spec.Repo, Number: number})
	if readErr != nil {
		log.FromContext(ctx).V(1).Info("fix-pr retry guard could not read the recorded pull request; not consuming the request",
			"run", run.Name, "repo", run.Spec.Repo, "pr", run.Status.PR, "error", readErr.Error())
		return false, true, nil
	}
	if !exactPullRequestIdentity(run, number, pull) {
		if pull.Merged {
			// A merged PR that is not this run's exact recorded identity is an
			// anomaly worth one durable line: it means the recorded head
			// drifted, and the merged work is not this run's outcome.
			log.FromContext(ctx).Info("recorded pull request is merged but no longer matches the run's head identity; not refusing the retry",
				"run", run.Name, "repo", run.Spec.Repo, "pr", run.Status.PR,
				"recordedHeadRepo", run.Status.HeadRepo, "recordedBranch", run.Status.Branch,
				"observedHeadRepo", pull.HeadRepo, "observedBranch", pull.HeadRef)
		}
		return false, false, nil
	}
	if pull.Merged || strings.EqualFold(strings.TrimSpace(pull.State), "closed") {
		return true, false, nil
	}
	return false, false, nil
}

// isRetryAdapterRefusalReason reports whether an adapter-supplied reason is one of
// the three source-state refusals the record may carry. Any other reason is
// normalized to source-refused so the record's reason stays inside the closed
// vocabulary and a hostile or buggy adapter cannot mint new reason strings.
func isRetryAdapterRefusalReason(reason string) bool {
	switch reason {
	case courierv1alpha1.RetryRefusalStaleGeneration,
		courierv1alpha1.RetryRefusalHeldForHuman,
		courierv1alpha1.RetryRefusalSourceRefused:
		return true
	default:
		return false
	}
}

// setRetryCondition mirrors the retry record into the RetryRequest condition:
// Unknown/Requested, True/Requeued, True/Moot, and False/Refused, with the bounded
// refusal reason carried in the message for a refusal so the condition, the
// record, and the event tell one story.
func setRetryCondition(run *courierv1alpha1.CoderRun, state courierv1alpha1.RetryRequestState, reason string, now time.Time, generation int64) {
	condStatus := metav1.ConditionUnknown
	reasonToken := courierv1alpha1.ConditionReasonRetryRequested
	switch state {
	case courierv1alpha1.RetryRequestStateRequested:
		condStatus, reasonToken = metav1.ConditionUnknown, courierv1alpha1.ConditionReasonRetryRequested
	case courierv1alpha1.RetryRequestStateRequeued:
		condStatus, reasonToken = metav1.ConditionTrue, courierv1alpha1.ConditionReasonRetryRequeued
	case courierv1alpha1.RetryRequestStateMoot:
		condStatus, reasonToken = metav1.ConditionTrue, courierv1alpha1.ConditionReasonRetryMoot
	case courierv1alpha1.RetryRequestStateRefused:
		condStatus, reasonToken = metav1.ConditionFalse, courierv1alpha1.ConditionReasonRetryRefused
	}
	message := reason
	if message == "" {
		message = string(state)
	}
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               courierv1alpha1.ConditionRetryRequest,
		Status:             condStatus,
		Reason:             reasonToken,
		Message:            message,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(now),
	})
}

// retryMetricOutcome maps a retry state to its closed metric result label.
func retryMetricOutcome(state courierv1alpha1.RetryRequestState) string {
	switch state {
	case courierv1alpha1.RetryRequestStateRequested:
		return retryOutcomeRequested
	case courierv1alpha1.RetryRequestStateRequeued:
		return retryOutcomeRequeued
	case courierv1alpha1.RetryRequestStateMoot:
		return retryOutcomeMoot
	default:
		return retryOutcomeRefused
	}
}

// observeRetry records one retry-request outcome on the bounded counter. The
// result label is the closed set requested, requeued, moot, refused — never the
// refusal reason, which rides only the event.
func (r *CoderRunReconciler) observeRetry(run *courierv1alpha1.CoderRun, result string) {
	if r == nil || run == nil {
		return
	}
	r.metrics().ObserveRetryRequest(run.Spec.Lane, run.Spec.Mode, result)
}

// emitRetry writes one structured run.retry event carrying the request ID, state,
// and — when present — the bounded reason and returned attempt identity. It is
// always Verbose so the audit trail reaches the log store on every request, not
// only debug runs.
func (r *CoderRunReconciler) emitRetry(run *courierv1alpha1.CoderRun, requestID string, state courierv1alpha1.RetryRequestState, reason, attemptID string) {
	if r == nil || r.Events == nil || run == nil {
		return
	}
	detail := map[string]any{"request_id": requestID, "state": string(state)}
	if reason != "" {
		detail["reason"] = reason
	}
	if attemptID != "" {
		detail["attempt_id"] = attemptID
	}
	_ = r.Events.Emit(courierlog.Event{
		Type:    courierlog.EventRunRetry,
		RunID:   run.Name,
		Repo:    run.Spec.Repo,
		Ref:     run.Spec.Ref,
		Mode:    string(run.Spec.Mode),
		Status:  string(state),
		Verbose: true,
		Detail:  detail,
	})
}
