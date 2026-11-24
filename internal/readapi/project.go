package readapi

import (
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// DefaultStaleWindow is the recency window a Freshness/Availability staleness
// judgment uses when Options.StaleWindow is unset.
const DefaultStaleWindow = 15 * time.Minute

// DefaultPageSize is the page size a list projection uses when none is given.
const DefaultPageSize = 50

// MaxTimelineEntries bounds a run detail's event timeline; newest entries win.
const MaxTimelineEntries = 32

// Timeline labels are the closed vocabulary emitted by ProjectRunTimeline.
const (
	TimelineCreated           = "created"
	TimelineAdmitted          = "admitted"
	TimelineStarted           = "started"
	TimelinePhase             = "phase"
	TimelineHeartbeat         = "heartbeat"
	TimelineCIParked          = "ci-parked"
	TimelineFinished          = "finished"
	TimelineCoordinatorExited = "coordinator-exited"
	TimelineResultRecorded    = "result-recorded"
	TimelineRetentionMarked   = "retention-marked"
	TimelineCondition         = "condition"
)

// Options carries the pure-projection inputs that are not cluster objects.
// Now is the observation instant (injected, never read from the wall clock);
// StaleWindow bounds Fresh/Stale judgments. The retention windows are the
// phase TTLs the reaper applies: the wiring passes the controller's own
// configured values, and a zero falls back to the defaults mirrored from
// internal/controller/reap.go, which are those flags' own defaults.
type Options struct {
	Now                 time.Time
	StaleWindow         time.Duration
	DoneRetention       time.Duration
	FailedRetention     time.Duration
	NeedsHumanRetention time.Duration
}

// Retention defaults mirrored from internal/controller/reap.go, which remains
// the authority; the wiring supplies the configured values so a deployment that
// overrides a flag never sees a projected expiry it cannot act on.
const (
	DefaultDoneRetention       = 5 * time.Minute
	DefaultFailedRetention     = 168 * time.Hour
	DefaultNeedsHumanRetention = 168 * time.Hour
)

func (o Options) now() time.Time {
	if o.Now.IsZero() {
		return time.Now()
	}
	return o.Now
}

func (o Options) window() time.Duration {
	if o.StaleWindow > 0 {
		return o.StaleWindow
	}
	return DefaultStaleWindow
}

// retentionWindowFor returns the retention window the reaper measures from the
// marker for this phase, and whether the phase has one at all. Review-parked
// terminal phases are exempt from reaping, so they have no window to state.
func (o Options) retentionWindowFor(phase courierv1alpha1.Phase) (time.Duration, bool) {
	switch phase {
	case courierv1alpha1.PhaseDone, courierv1alpha1.PhaseAlreadyDone:
		return positiveOrDefault(o.DoneRetention, DefaultDoneRetention), true
	case courierv1alpha1.PhaseFailed:
		return positiveOrDefault(o.FailedRetention, DefaultFailedRetention), true
	case courierv1alpha1.PhaseNeedsHuman:
		return positiveOrDefault(o.NeedsHumanRetention, DefaultNeedsHumanRetention), true
	}
	return 0, false
}

func positiveOrDefault(configured, fallback time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return fallback
}

// boundTruncate bounds s to max runes (DefaultTextBound when max <= 0),
// stripping control characters so hostile text cannot smuggle control
// sequences into a JSON surface. A truncated string ends with an ellipsis
// inside the bound.
func boundTruncate(s string, max int) string {
	if max <= 0 {
		max = DefaultTextBound
	}
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
	runes := []rune(cleaned)
	if len(runes) <= max {
		return cleaned
	}
	keep := max - 1
	if keep < 0 {
		keep = 0
	}
	return string(runes[:keep]) + "…"
}

// bound is boundTruncate at the shared default bound.
func bound(s string) string { return boundTruncate(s, DefaultTextBound) }

// conditionAllowlist is the closed set of condition types this read model
// projects. Reasons are never projected: they are free text written along
// reconcile paths and can carry source-controlled or token-shaped content.
var conditionAllowlist = map[string]bool{
	courierv1alpha1.ConditionLifecycleReported:         true,
	courierv1alpha1.ConditionSoftStopObserved:          true,
	courierv1alpha1.ConditionCapacityRecovery:          true,
	courierv1alpha1.ConditionPullRequestClosedUnmerged: true,
	courierv1alpha1.ConditionCIRepairInapplicable:      true,
	courierv1alpha1.ConditionRetryRequest:              true,
}

// availabilityDiagnosticCategories are the terminal diagnostic categories
// that mark a source as unavailable (provider or forge read could not
// complete).
var availabilityDiagnosticCategories = map[courierv1alpha1.TerminalDiagnosticCategory]bool{
	courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable: true,
	courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable:    true,
}

// workspaceSafetySafeStates mirrors the operator's own capacity-recovery gate
// (safeCapacityWorkspace in internal/controller): only "empty" and "pushed"
// count as safe, and that gate fails closed on anything else. The read model
// derives its unsafe flag from this safe set rather than enumerating unsafe
// states, so an evidence state the vocabulary does not yet know can never read
// as safe here before the gate has accepted it.
var workspaceSafetySafeStates = map[string]bool{
	"empty":  true,
	"pushed": true,
}

// freshnessFor maps an optional observation time onto the freshness enum.
func freshnessFor(t *metav1.Time, opts Options) Freshness {
	if t == nil || t.Time.IsZero() {
		return FreshnessUnknown
	}
	if t.Time.After(opts.now().Add(-opts.window())) {
		return FreshnessFresh
	}
	return FreshnessStale
}

// lastActivity returns the most recent activity instant carried on a run:
// heartbeat, terminal diagnostic, finish, phase entry, or creation, whichever
// is latest. Nil when the run carries none (no creation timestamp).
func lastActivity(run *courierv1alpha1.CoderRun) *metav1.Time {
	var newest *metav1.Time
	consider := func(t *metav1.Time) {
		if t == nil || t.Time.IsZero() {
			return
		}
		if newest == nil || t.Time.After(newest.Time) {
			newest = t
		}
	}
	if run == nil {
		return nil
	}
	if run.Status.Heartbeat != nil {
		consider(&run.Status.Heartbeat.At)
	}
	if run.Status.TerminalDiagnostic != nil {
		consider(run.Status.TerminalDiagnostic.ObservedAt)
	}
	consider(run.Status.FinishedAt)
	consider(run.Status.PhaseEnteredAt)
	consider(&run.CreationTimestamp)
	return newest
}

// phaseAge returns how long the run has been in its current phase, falling
// back to the creation timestamp when PhaseEnteredAt is unset.
func phaseAge(run *courierv1alpha1.CoderRun, opts Options) time.Duration {
	at := run.Status.PhaseEnteredAt
	if at == nil || at.Time.IsZero() {
		at = &run.CreationTimestamp
	}
	if at == nil || at.Time.IsZero() {
		return 0
	}
	age := opts.now().Sub(at.Time)
	if age < 0 {
		return 0
	}
	return age
}

func secondsOf(d time.Duration) *int64 {
	s := int64(d / time.Second)
	return &s
}

// parsePRNumber extracts a pull-request number from Status.PR, which carries
// "number or URL". Nil when nothing parseable is present, so absence is
// explicit rather than a silent zero.
func parsePRNumber(pr string) *int {
	pr = strings.TrimSpace(pr)
	if pr == "" {
		return nil
	}
	if n, err := strconv.Atoi(pr); err == nil && n > 0 {
		return &n
	}
	if idx := strings.LastIndexByte(pr, '/'); idx >= 0 {
		if n, err := strconv.Atoi(strings.TrimSpace(pr[idx+1:])); err == nil && n > 0 {
			return &n
		}
	}
	return nil
}

// reapMarker reports the run's present reap-marker annotation: its name and
// parsed RFC3339 stamp. The names mirror internal/controller/reap.go exactly;
// this package only reads them, the controller remains their sole writer. The
// first present marker in a fixed order (done, already-done, failed,
// needs-human) wins; the controller holds at most one, and a stable order
// keeps the projection deterministic.
func reapMarker(run *courierv1alpha1.CoderRun) (string, *metav1.Time) {
	markers := []string{
		AnnotationDoneAt,
		AnnotationAlreadyDoneAt,
		AnnotationFailedAt,
		AnnotationNeedsHumanAt,
	}
	for _, key := range markers {
		raw, ok := run.Annotations[key]
		if !ok || strings.TrimSpace(raw) == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, strings.TrimSpace(raw)); err == nil {
			at := metav1.NewTime(t)
			return key, &at
		}
	}
	return "", nil
}

// retentionReasonFor names the retention outcome a marker implies, for ReapHealth.
func retentionReasonFor(marker string) string {
	switch marker {
	case AnnotationDoneAt, AnnotationAlreadyDoneAt:
		return "resolved"
	case AnnotationFailedAt:
		return "failed"
	case AnnotationNeedsHumanAt:
		return "needs-human"
	default:
		return ""
	}
}

// ProjectRunTimeline derives a bounded, deterministic timeline from recorded
// status timestamps only. The observation instant plays no part: every entry
// is a stamped fact the operator or the run's coordinator recorded on this
// object, and each entry names where it was recorded (Source) and who records
// it (Authority). RunDetail.TimelineObservedAt carries the read side
// separately, so the two are never confused.
func ProjectRunTimeline(run *courierv1alpha1.CoderRun) ([]TimelineView, bool) {
	if run == nil {
		return nil, false
	}
	var entries []TimelineView
	add := func(at *metav1.Time, label, detail, source, authority string) {
		if at == nil || at.Time.IsZero() {
			return
		}
		entry := TimelineView{
			At:        at.DeepCopy(),
			Label:     label,
			Source:    source,
			Authority: authority,
		}
		if detail != "" {
			entry.Detail = bound(detail)
		}
		entries = append(entries, entry)
	}
	add(&run.CreationTimestamp, TimelineCreated, "", TimelineSourceCreationTimestamp, TimelineAuthorityAPIServer)
	add(run.Status.AdmittedAt, TimelineAdmitted, "", TimelineSourceAdmittedAt, TimelineAuthorityOperator)
	add(run.Status.StartedAt, TimelineStarted, "", TimelineSourceStartedAt, TimelineAuthorityOperator)
	if run.Status.Phase != "" {
		add(run.Status.PhaseEnteredAt, TimelinePhase, string(run.Status.Phase), TimelineSourcePhaseEnteredAt, TimelineAuthorityOperator)
	}
	if hb := run.Status.Heartbeat; hb != nil {
		add(&hb.At, TimelineHeartbeat, "", TimelineSourceHeartbeatAt, TimelineAuthorityCoordinator)
	}
	if park := run.Status.CIPark; park != nil {
		add(&park.ParkedAt, TimelineCIParked, "", TimelineSourceCIParkParkedAt, TimelineAuthorityOperator)
	}
	add(run.Status.FinishedAt, TimelineFinished, "", TimelineSourceFinishedAt, TimelineAuthorityOperator)
	if diag := run.Status.TerminalDiagnostic; diag != nil {
		add(diag.ObservedAt, TimelineCoordinatorExited, "", TimelineSourceDiagnosticAt, TimelineAuthorityOperator)
	}
	if result := run.Status.NativeResult; result != nil {
		add(&result.At, TimelineResultRecorded, "", TimelineSourceNativeResultAt, TimelineAuthorityCoordinator)
	}
	if marker, at := reapMarker(run); marker != "" {
		add(at, TimelineRetentionMarked, retentionReasonFor(marker), TimelineSourceRetention+"/"+marker, TimelineAuthorityOperator)
	}
	for i := range run.Status.Conditions {
		cond := run.Status.Conditions[i]
		if conditionAllowlist[cond.Type] {
			add(&cond.LastTransitionTime, TimelineCondition, string(cond.Type)+"="+string(cond.Status),
				TimelineSourceCondition+"["+cond.Type+"]", TimelineAuthorityOperator)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].At.Equal(entries[j].At) {
			return entries[i].At.Before(entries[j].At)
		}
		if entries[i].Label != entries[j].Label {
			return entries[i].Label < entries[j].Label
		}
		return entries[i].Detail < entries[j].Detail
	})
	truncated := len(entries) > MaxTimelineEntries
	if truncated {
		entries = append([]TimelineView(nil), entries[len(entries)-MaxTimelineEntries:]...)
	}
	if len(entries) == 0 {
		return nil, truncated
	}
	return entries, truncated
}

// These settlement reasons mirror internal/controller/coderun_controller.go's lifecycle report gate.
const (
	lifecycleReportPublishedReason        = "Published"
	lifecycleReportSupersededReason       = "Superseded"
	lifecycleReportPermanentRefusalReason = "PermanentRefusal"
)

// reapHolds derives why a run cannot be reaped yet from this object alone,
// mirroring the controller's gates (internal/controller/reap.go,
// internal/controller/capacity_recovery.go and coderun_controller.go's terminal
// dispatch). Only single-object facts are used: the per-repository audit floor
// compares other runs and is never derivable here.
func reapHolds(run *courierv1alpha1.CoderRun, marker string, markerAt *metav1.Time, opts Options) []string {
	if !phaseIsReapEligible(run.Status.Phase) {
		holds := []string{"phase is not reap-eligible"}
		// A capacity-exhaustion run is the case where the phase exemption has a
		// reason behind it the operator needs: the object is retained because
		// the operator still owns the attempt's partial work.
		if hold := capacityOwnershipHold(run); hold != "" {
			holds = append(holds, hold)
		}
		return holds
	}
	var holds []string
	if marker == "" {
		holds = append(holds, "no retention marker yet")
	} else if window, ok := opts.retentionWindowFor(run.Status.Phase); ok && markerAt != nil {
		// The window is the phase TTL the reaper measures from the durable
		// marker. Until it passes, the reaper only requeues.
		if opts.now().Before(markerAt.Time.Add(window)) {
			holds = append(holds, "retention window still open")
		}
	}
	// The report-settlement gate mirrors the controller's terminal dispatch
	// (internal/controller/coderun_controller.go): Failed, NeedsHuman, and
	// AlreadyDone runs are held until the LifecycleReported condition settles,
	// but a Done run is reaped through resolveCompleted, which gates on the
	// done-at marker and the source resolve instead, so an unsettled report
	// must not read as a Done-run hold.
	settled := false
	for i := range run.Status.Conditions {
		cond := run.Status.Conditions[i]
		if cond.Type == courierv1alpha1.ConditionLifecycleReported && cond.Status == metav1.ConditionTrue &&
			(cond.Reason == lifecycleReportPublishedReason || cond.Reason == lifecycleReportSupersededReason || cond.Reason == lifecycleReportPermanentRefusalReason) {
			settled = true
			break
		}
	}
	if run.Status.Phase != courierv1alpha1.PhaseDone && !settled {
		holds = append(holds, "source report not settled")
	}
	if run.Status.CISettlement != nil {
		holds = append(holds, "ci settlement outstanding")
	}
	if run.Status.CIParkRelease != nil {
		holds = append(holds, "ci park release outstanding")
	}
	if req := run.Status.RetryRequest; req != nil && req.State == courierv1alpha1.RetryRequestStateRequested {
		holds = append(holds, "retry request outstanding")
	}
	if hold := capacityOwnershipHold(run); hold != "" {
		holds = append(holds, hold)
	}
	return holds
}

// capacityOwnershipHold mirrors the operator's capacity-recovery ownership gate
// (internal/controller/capacity_recovery.go). A capacity-exhaustion run sits in
// BlockedExternal, which the reaper never collects, and what keeps it there is
// the fenced settlement: the operator refuses to release ownership while the
// attempt's partial work is not proven safely held remotely. UnsafeWork is the
// controller's own stopping reason for exactly that, and workspace evidence the
// controller's gate would not accept says the same thing here. A settlement that
// has merely not run yet is reported as unsettled rather than as unsafe, and
// Settled and Stale release the hold — the attempt settled, or a newer attempt
// owns the work — leaving only the phase exemption and the audit floor. The
// reason vocabulary mirrors the controller's; this package only reads it.
func capacityOwnershipHold(run *courierv1alpha1.CoderRun) string {
	if !phaseIsCapacityExhaustion(run) {
		return ""
	}
	const unsafe = "unsafe workspace evidence holds ownership of partial work"
	for i := range run.Status.Conditions {
		cond := run.Status.Conditions[i]
		if cond.Type != courierv1alpha1.ConditionCapacityRecovery {
			continue
		}
		switch cond.Reason {
		case capacityRecoveryUnsafeWork:
			return unsafe
		case capacityRecoveryUnsupported:
			return "capacity recovery is unsupported: an operator must settle this attempt"
		case capacityRecoveryStale:
			return ""
		case capacityRecoverySettled:
			if cond.Status == metav1.ConditionTrue {
				return ""
			}
		}
		break
	}
	if run.Status.WorkspaceSafety != nil && !capacityWorkspaceProvenSafe(run) {
		return unsafe
	}
	return "capacity recovery has not settled this attempt"
}

// capacityWorkspaceProvenSafe mirrors safeCapacityWorkspace in
// internal/controller/capacity_recovery.go exactly: "empty" proves nothing was
// left behind, "pushed" only proves safety when the recorded remote head and
// branch match the run's own, and anything else — including a state this
// vocabulary does not yet know — fails closed.
func capacityWorkspaceProvenSafe(run *courierv1alpha1.CoderRun) bool {
	evidence := run.Status.WorkspaceSafety
	if evidence == nil {
		return false
	}
	switch evidence.State {
	case "empty":
		return true
	case "pushed":
		return evidence.RemoteHead != "" && evidence.Head != "" && evidence.RemoteHead == evidence.Head &&
			evidence.Branch != "" && evidence.Branch == run.Status.Branch && run.Status.Branch != ""
	}
	return false
}

// ProjectRunSummary projects one run's list-row view.
func ProjectRunSummary(run *courierv1alpha1.CoderRun, opts Options) RunSummary {
	if run == nil {
		return RunSummary{}
	}
	summary := RunSummary{
		Name:           run.Name,
		Namespace:      run.Namespace,
		Phase:          string(run.Status.Phase),
		Terminal:       phaseIsTerminal(run.Status.Phase),
		CreatedAt:      run.CreationTimestamp.DeepCopy(),
		PhaseEnteredAt: run.Status.PhaseEnteredAt.DeepCopy(),
		Source:         bound(run.Spec.Source),
		Lane:           bound(run.Spec.Lane),
		PRNumber:       parsePRNumber(run.Status.PR),
		Branch:         bound(run.Status.Branch),
		HeadSHA:        bound(run.Status.HeadSHA),
		Restarts:       run.Status.Restarts,
	}
	if summary.CreatedAt != nil && !summary.CreatedAt.Time.IsZero() {
		summary.AgeSeconds = secondsOf(opts.now().Sub(summary.CreatedAt.Time))
		if summary.AgeSeconds != nil && *summary.AgeSeconds < 0 {
			summary.AgeSeconds = secondsOf(0)
		}
	}
	return summary
}

// ProjectRunDetail projects the single-run view. It is an allowlist: only the
// fields copied below are ever served, and every free-form string passes
// through boundTruncate.
func ProjectRunDetail(run *courierv1alpha1.CoderRun, opts Options) RunDetail {
	detail := RunDetail{
		RunSummary: ProjectRunSummary(run, opts),
	}
	detail.Timeline, detail.TimelineTruncated = ProjectRunTimeline(run)
	// The projection's own observation instant, kept apart from every entry's
	// recorded stamp: it says when the read model read this object, not when
	// anything happened.
	if observed := metav1.NewTime(opts.now()); !observed.Time.IsZero() {
		detail.TimelineObservedAt = &observed
	}
	if run == nil {
		return detail
	}

	if diag := run.Status.TerminalDiagnostic; diag != nil {
		redacted := &CoordinatorDiagnostic{
			Category:   string(diag.Category),
			ExitCode:   diag.ExitCode,
			Outcome:    bound(diag.Outcome),
			Reason:     bound(diag.Reason),
			ObservedAt: diag.ObservedAt.DeepCopy(),
			Freshness:  freshnessFor(diag.ObservedAt, opts),
		}
		if diag.Container != nil {
			redacted.Container = &ContainerIdentity{
				Name:              bound(diag.Container.ContainerName),
				RestartCount:      diag.Container.RestartCount,
				TerminationReason: bound(diag.Container.TerminationReason),
			}
		}
		detail.Coordinator = redacted
		detail.TerminalReason = bound(diag.Reason)
	}

	if hb := run.Status.Heartbeat; hb != nil {
		at := hb.At.DeepCopy()
		detail.Heartbeat = HeartbeatHealth{
			HasHeartbeat:    true,
			LastHeartbeatAt: at,
			Freshness:       freshnessFor(at, opts),
		}
	} else {
		detail.Heartbeat = HeartbeatHealth{HasHeartbeat: false, Freshness: FreshnessUnknown}
	}

	if cp := run.Status.Checkpoint; cp != nil {
		detail.Checkpoint = CheckpointHealth{Present: true, BriefCount: len(cp.CompletedBriefs)}
	}

	detail.Publication = PublicationHealth{
		LastCommit:                bound(run.Status.LastCommit),
		HasCheckFingerprint:       run.Status.CheckFingerprint != "",
		HasFailedCheckFingerprint: run.Status.FailedCheckFingerprint != "",
		DeclaredNoChange:          run.Status.DeclaredNoChange,
	}

	if park := run.Status.CIPark; park != nil {
		detail.CIPark = &CIParkHealth{
			Head:        bound(park.Head),
			ParkedAt:    park.ParkedAt.DeepCopy(),
			Attempts:    park.Attempts,
			LastOutcome: bound(string(park.LastOutcome)),
		}
	}

	detail.LifecycleReport = LifecycleReportHealth{
		HasActiveRetryToken: run.Status.LifecycleReportActiveRetryToken != "",
		HasLastRetryToken:   run.Status.LifecycleReportRetryToken != "",
	}
	for i := range run.Status.Conditions {
		cond := run.Status.Conditions[i]
		if cond.Type != courierv1alpha1.ConditionLifecycleReported {
			continue
		}
		detail.LifecycleReport.ConditionPresent = true
		detail.LifecycleReport.ConditionStatus = string(cond.Status)
		detail.LifecycleReport.ObservedAt = cond.LastTransitionTime.DeepCopy()
		break
	}

	detail.Admission = AdmissionHealth{
		AdmittedAt: run.Status.AdmittedAt.DeepCopy(),
	}
	if run.Status.AdmittedAt != nil && run.Status.Phase == courierv1alpha1.PhasePending {
		detail.Admission.Disagreement = true
		detail.Admission.Note = "admittedAt is stamped but phase is Pending: a released or wedged admission"
	}

	marker, at := reapMarker(run)
	detail.Reap = ReapHealth{
		MarkerAnnotation: marker,
		MarkerAt:         at,
		Reason:           retentionReasonFor(marker),
		EligibleByPhase:  phaseIsReapEligible(run.Status.Phase),
		Holds:            reapHolds(run, marker, at, opts),
	}
	// The phase's retention window and the expiry the reaper reads off the
	// durable marker: the page can then say how long a retained run is still
	// held for, not only that it is held.
	if window, ok := opts.retentionWindowFor(run.Status.Phase); ok {
		detail.Reap.RetentionWindowSeconds = secondsOf(window)
		if at != nil && !at.Time.IsZero() {
			expires := metav1.NewTime(at.Time.Add(window))
			detail.Reap.RetentionExpiresAt = &expires
		}
	}

	if safety := run.Status.WorkspaceSafety; safety != nil {
		detail.WorkspaceSafety = &WorkspaceSafetyView{
			State:      bound(safety.State),
			Branch:     bound(safety.Branch),
			Head:       bound(safety.Head),
			RemoteHead: bound(safety.RemoteHead),
		}
	}

	if nr := run.Status.NativeResult; nr != nil {
		detail.NativeResultKind = string(nr.Kind)
		detail.NativeResultAt = nr.At.DeepCopy()
	}

	for i := range run.Status.Conditions {
		cond := run.Status.Conditions[i]
		if !conditionAllowlist[cond.Type] {
			continue
		}
		detail.Conditions = append(detail.Conditions, ConditionView{
			Type:       cond.Type,
			Status:     string(cond.Status),
			ObservedAt: cond.LastTransitionTime.DeepCopy(),
		})
	}

	return detail
}

// ProjectLaneHealth projects one lane's health from its LaneProfile (nil for a
// lane referenced by runs but not configured) and the namespace's runs. The
// cooldown check reads only the lane's own status; use ProjectLaneHealths for
// the cross-lane shared-capacity-group view.
func ProjectLaneHealth(name string, lane *courierv1alpha1.LaneProfile, runs []courierv1alpha1.CoderRun, opts Options) LaneHealth {
	return projectLane(name, lane, nil, runs, opts)
}

// ProjectLaneHealths projects every configured lane, honoring shared capacity
// groups: a cooldown on any lane profile with the same capacity identity
// blocks the lane (api.CapacityCooldownActive semantics). Lanes referenced by
// runs but with no LaneProfile are projected too, with an Unsupported
// availability, so the lane view never hides traffic it has runs for.
func ProjectLaneHealths(lanes []courierv1alpha1.LaneProfile, runs []courierv1alpha1.CoderRun, opts Options) []LaneHealth {
	configured := make(map[string]bool, len(lanes))
	healths := make([]LaneHealth, 0, len(lanes))
	for i := range lanes {
		configured[lanes[i].Name] = true
		healths = append(healths, projectLane(lanes[i].Name, &lanes[i], lanes, runs, opts))
	}
	referenced := map[string]bool{}
	for i := range runs {
		name := runs[i].Spec.Lane
		if name == "" || configured[name] || referenced[name] {
			continue
		}
		referenced[name] = true
		healths = append(healths, projectLane(name, nil, lanes, runs, opts))
	}
	sort.Slice(healths, func(i, j int) bool { return healths[i].Name < healths[j].Name })
	return healths
}

// derivedFromLane states how a lane's occupancy counts were derived, so a
// consumer never mistakes them for a live scheduler view. It mirrors the two
// operator gates exactly: admitted is the controller's execution-capacity
// gate (Claimed+Running), reserved is the source runner's discovery gate
// (empty phase, Pending, Claimed, Running). Reserved is a superset of
// admitted, so the two counts are never additive.
const derivedFromLane = "admitted = Claimed+Running (operator admission capacity); " +
	"reserved = empty-phase+Pending+Claimed+Running (source discovery reservation; a superset of admitted, so the two counts are not additive); " +
	"Verifying and terminal phases hold neither; occupied slots are never inferred from retained run count"

func projectLane(name string, lane *courierv1alpha1.LaneProfile, allLanes []courierv1alpha1.LaneProfile, runs []courierv1alpha1.CoderRun, opts Options) LaneHealth {
	health := LaneHealth{
		Availability: AvailabilityHealthy,
		Freshness:    FreshnessUnknown,
		DerivedFrom:  derivedFromLane,
	}
	var laneRuns []courierv1alpha1.CoderRun
	for i := range runs {
		if runs[i].Spec.Lane == name {
			laneRuns = append(laneRuns, runs[i])
		}
	}

	concurrency := 1
	if lane != nil {
		concurrency = lane.Spec.Concurrency
		if concurrency <= 0 {
			concurrency = 1
		}
		health.Name = lane.Name
		health.ConfiguredConcurrency = lane.Spec.Concurrency
		health.Suspended = lane.Suspended()
		if lane.Status.CapacityCooldown != nil {
			health.Cooldown = &CooldownHealth{
				CapacityID:      lane.Status.CapacityCooldown.CapacityID,
				Reason:          bound(lane.Status.CapacityCooldown.Reason),
				ExhaustionCount: lane.Status.CapacityCooldown.ExhaustionCount,
				NextEligibleAt:  lane.Status.CapacityCooldown.NextEligibleAt.DeepCopy(),
			}
		}
	}

	var oldestPending *courierv1alpha1.CoderRun
	wedged := false
	for i := range laneRuns {
		run := &laneRuns[i]
		if phaseConsumesCapacity(run.Status.Phase) {
			health.Admitted++
		}
		if phaseReservesDiscovery(run.Status.Phase) {
			health.Reserved++
			if run.Status.Phase == "" || run.Status.Phase == courierv1alpha1.PhasePending {
				if oldestPending == nil || phaseAge(run, opts) > phaseAge(oldestPending, opts) {
					oldestPending = run
				}
			}
		}
		// A wedge is an unadmitted discovery reservation that outlived the
		// staleness window: an empty-phase or Pending run no admission has
		// picked up. Claimed and Running runs reserve discovery too, but
		// they legitimately execute for hours, so age alone is not negative
		// evidence for them.
		if phaseAge(run, opts) > opts.window() && phaseReservesDiscovery(run.Status.Phase) && !phaseConsumesCapacity(run.Status.Phase) {
			wedged = true
		}
	}
	if oldestPending != nil {
		health.PendingOldestSeconds = secondsOf(phaseAge(oldestPending, opts))
	}
	health.Saturated = health.Admitted >= concurrency

	switch {
	case lane == nil:
		health.Name = name
		health.Availability = AvailabilityUnsupported
		health.Notes = append(health.Notes, "lane profile not found")
	case health.Suspended:
		health.Availability = AvailabilityBlocked
		health.Notes = append(health.Notes, "lane suspended by "+courierv1alpha1.SuspendAnnotation)
	default:
		if cooldown, active := courierv1alpha1.CapacityCooldownActive(lane, allLanes, opts.now()); active {
			health.Availability = AvailabilityBlocked
			health.Notes = append(health.Notes, "capacity cooldown active")
			if health.Cooldown == nil && cooldown != nil {
				health.Cooldown = &CooldownHealth{
					CapacityID:      cooldown.CapacityID,
					Reason:          bound(cooldown.Reason),
					ExhaustionCount: cooldown.ExhaustionCount,
					NextEligibleAt:  cooldown.NextEligibleAt.DeepCopy(),
				}
			}
		} else if wedged {
			health.Availability = AvailabilityStale
			health.Notes = append(health.Notes, "an unadmitted run has held a discovery reservation past the staleness window")
		} else if len(laneRuns) == 0 {
			health.Availability = AvailabilityEmpty
		}
	}
	return health
}

// ProjectSourceHealth projects one source adapter's health from its runs. The
// source is named by CoderRun.Spec.Source; a name with zero projected runs is
// Empty. The name is bounded like every other free-form projected string:
// spec.source carries no length bound of its own. The last-success and
// last-error outcomes are the latest recorded finish among the source's
// Done/AlreadyDone and Failed runs; runs parked in a waiting terminal phase
// are neither, and a run whose finish was never recorded stamps nothing.
func ProjectSourceHealth(name string, runs []courierv1alpha1.CoderRun, opts Options) SourceHealth {
	health := SourceHealth{
		Name:         bound(name),
		Availability: AvailabilityHealthy,
		Freshness:    FreshnessUnknown,
	}
	var oldestPending *courierv1alpha1.CoderRun
	staleRun := false
	var lastSuccess, lastError *metav1.Time
	considerFinish := func(dst **metav1.Time, at *metav1.Time) {
		if at == nil || at.Time.IsZero() {
			return
		}
		if *dst == nil || at.Time.After((*dst).Time) {
			*dst = at
		}
	}
	for i := range runs {
		run := &runs[i]
		if run.Spec.Source != name {
			continue
		}
		health.TotalRuns++
		if health.RunsByPhase == nil {
			health.RunsByPhase = map[string]int{}
		}
		health.RunsByPhase[string(run.Status.Phase)]++

		if activity := lastActivity(run); activity != nil {
			if health.ObservedAt == nil || activity.Time.After(health.ObservedAt.Time) {
				health.ObservedAt = activity.DeepCopy()
			}
		}

		switch run.Status.Phase {
		case courierv1alpha1.PhaseDone, courierv1alpha1.PhaseAlreadyDone:
			considerFinish(&lastSuccess, run.Status.FinishedAt)
		case courierv1alpha1.PhaseFailed:
			considerFinish(&lastError, run.Status.FinishedAt)
		}

		nonTerminal := !phaseIsTerminal(run.Status.Phase)
		if nonTerminal && run.Status.WorkspaceSafety != nil && !workspaceSafetySafeStates[run.Status.WorkspaceSafety.State] {
			health.Availability = AvailabilityBlocked
			health.Notes = append(health.Notes, "run "+run.Name+" holds unsafe workspace evidence")
		}
		if diag := run.Status.TerminalDiagnostic; diag != nil &&
			availabilityDiagnosticCategories[diag.Category] &&
			freshnessFor(diag.ObservedAt, opts) == FreshnessFresh {
			health.Availability = AvailabilityUnavailable
			health.Notes = append(health.Notes, "run "+run.Name+" recorded "+string(diag.Category))
		}
		if nonTerminal {
			if age := lastActivity(run); age == nil || opts.now().Sub(age.Time) > opts.window() {
				staleRun = true
			}
			if run.Status.Phase == "" || run.Status.Phase == courierv1alpha1.PhasePending {
				if oldestPending == nil || phaseAge(run, opts) > phaseAge(oldestPending, opts) {
					oldestPending = run
				}
			}
		}
	}

	if health.TotalRuns == 0 {
		health.Availability = AvailabilityEmpty
	} else {
		switch health.Availability {
		case AvailabilityBlocked, AvailabilityUnavailable:
			// Priority holds: safety and availability evidence outrank staleness.
		default:
			if staleRun {
				health.Availability = AvailabilityStale
				health.Notes = append(health.Notes, "a non-terminal run has had no activity past the staleness window")
			}
		}
	}
	if oldestPending != nil {
		health.OldestPendingSeconds = secondsOf(phaseAge(oldestPending, opts))
	}
	if lastSuccess != nil {
		health.LastSuccessAt = lastSuccess.DeepCopy()
	}
	if lastError != nil {
		health.LastErrorAt = lastError.DeepCopy()
	}
	health.Freshness = freshnessFor(health.ObservedAt, opts)
	sort.Strings(health.Notes)
	return health
}

// ProjectSourceHealths projects every source visible in runs plus every
// configured (known) source name, sorted by name. A known source with no runs
// projects Empty.
func ProjectSourceHealths(runs []courierv1alpha1.CoderRun, knownSources []string, opts Options) []SourceHealth {
	names := map[string]bool{}
	for i := range runs {
		names[runs[i].Spec.Source] = true
	}
	for _, name := range knownSources {
		names[name] = true
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	healths := make([]SourceHealth, 0, len(ordered))
	for _, name := range ordered {
		healths = append(healths, ProjectSourceHealth(name, runs, opts))
	}
	return healths
}

// ProjectOverview projects the single-namespace snapshot. knownSources is the
// configured source-name inventory (see ProjectSourceHealths); the overview's
// source counts always describe the same source set GET /v1/sources serves,
// so the two views can never disagree about which sources exist.
func ProjectOverview(namespace string, runs []courierv1alpha1.CoderRun, lanes []courierv1alpha1.LaneProfile, knownSources []string, opts Options) Overview {
	overview := Overview{
		Namespace:     namespace,
		ConditionRuns: map[string]int{},
	}
	now := metav1.NewTime(opts.now())
	overview.GeneratedAt = &now
	overview.Freshness = FreshnessFresh

	for i := range runs {
		run := &runs[i]
		overview.Runs.Total++
		if overview.Runs.ByPhase == nil {
			overview.Runs.ByPhase = map[string]int{}
		}
		overview.Runs.ByPhase[string(run.Status.Phase)]++
		for j := range run.Status.Conditions {
			if conditionAllowlist[run.Status.Conditions[j].Type] {
				overview.ConditionRuns[run.Status.Conditions[j].Type]++
			}
		}
	}
	if len(overview.ConditionRuns) == 0 {
		overview.ConditionRuns = nil
	}

	laneHealths := ProjectLaneHealths(lanes, runs, opts)
	overview.Lanes.Count = len(laneHealths)
	for _, l := range laneHealths {
		if l.Saturated {
			overview.Lanes.Saturated = true
		}
		if l.Suspended {
			overview.Lanes.Suspended = true
		}
		if l.Cooldown != nil {
			overview.Lanes.Cooldown = true
		}
		if l.Availability != AvailabilityHealthy && l.Availability != AvailabilityEmpty {
			overview.Lanes.Unhealthy++
		}
	}

	sourceHealths := ProjectSourceHealths(runs, knownSources, opts)
	overview.Sources.Count = len(sourceHealths)
	for _, s := range sourceHealths {
		switch s.Availability {
		case AvailabilityHealthy:
			overview.Sources.Healthy++
		case AvailabilityEmpty:
			overview.Sources.Empty++
		case AvailabilityStale:
			overview.Sources.Stale++
		case AvailabilityUnavailable:
			overview.Sources.Unavailable++
		case AvailabilityBlocked:
			overview.Sources.Blocked++
		}
	}
	return overview
}

// maxPage bounds the page offset the projector accepts: a page whose
// start offset could overflow int must never reach the offset math, so
// out-of-range pages land on an empty window instead of a wrapped one.
const maxPage = 1_000_000

// ProjectRunSummaries projects a paginated run list, ordered newest-first
// (creation time, then name for determinism). It sorts runs in place;
// callers that need their input order preserved must pass a copy.
func ProjectRunSummaries(runs []courierv1alpha1.CoderRun, page, pageSize int, opts Options) RunSummaryList {
	sort.SliceStable(runs, func(i, j int) bool {
		if !runs[i].CreationTimestamp.Time.Equal(runs[j].CreationTimestamp.Time) {
			return runs[i].CreationTimestamp.Time.After(runs[j].CreationTimestamp.Time)
		}
		return runs[i].Name < runs[j].Name
	})

	list := RunSummaryList{Items: []RunSummary{}, Pagination: pageBounds(len(runs), page, pageSize)}
	for i := list.Pagination.start(); i < len(runs) && i < list.Pagination.start()+list.Pagination.PageSize; i++ {
		list.Items = append(list.Items, ProjectRunSummary(&runs[i], opts))
	}
	return list
}

// pageStart is the single overflow-safe start-offset computation shared by
// pageBounds and Pagination.start: a page whose offset cannot be expressed
// as a non-negative in-range int lands past every total, which callers read
// as an empty window.
func pageStart(page, pageSize int) int {
	if page <= 1 || pageSize <= 0 {
		return 0
	}
	if page > maxPage {
		return int(^uint(0) >> 1)
	}
	start := (page - 1) * pageSize
	if start < 0 {
		return int(^uint(0) >> 1)
	}
	return start
}

// pageBounds clamps pagination inputs and returns the response pagination;
// Start is the absolute offset of the first item of the requested page.
func pageBounds(total, page, pageSize int) Pagination {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	if page <= 0 {
		page = 1
	}
	start := pageStart(page, pageSize)
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return Pagination{
		Page:     page,
		PageSize: pageSize,
		Total:    total,
		More:     end < total,
	}
}

func (p Pagination) start() int { return pageStart(p.Page, p.PageSize) }
