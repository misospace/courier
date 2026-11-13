package adhoc

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// statusReadPrefix is the route prefix of the read-only lifecycle surface
// (#819): GET /v1/contributions/<run>, where <run> is the run name an
// admission response returned. The prefix ends in the separator so the
// collection path itself stays admission-only.
const statusReadPrefix = PathContributions + "/"

// PhaseUnknown is the projection value emitted while the controller has not
// yet stamped a phase onto the run. It is deliberately lowercase: it is not
// a CoderRun phase and never appears in status.phase.
const PhaseUnknown = "unknown"

const (
	// statusReadTimeout bounds one status read's cluster round trip.
	// Reads are polled, so the bound is tighter than the admission
	// deadline: a stalled API server must release pollers quickly.
	statusReadTimeout = 10 * time.Second

	// maxConcurrentStatusReads caps in-flight status reads. The read
	// path's total load is bounded by (concurrency x deadline), not by
	// request counting; a saturated listener answers 503 with a
	// Retry-After hint instead of queueing unbounded readers behind a
	// stalled cluster.
	maxConcurrentStatusReads = 8

	// maxStatusFieldBytes defensively bounds every projected string.
	// Identity fields are short by contract; the cap keeps the response
	// a fixed-shape object even against a CRD field that is not itself
	// length-bounded. An oversized value is dropped, not truncated, so
	// no partial identity can be mistaken for a complete one.
	maxStatusFieldBytes = 1024

	// maxStatusRunNameBytes bounds the run-name path segment; it is the
	// Kubernetes object-name limit.
	maxStatusRunNameBytes = 253

	statusMethodMessage       = "contribution status reads accept GET only"
	statusUnauthorizedMessage = "contribution status reads require a valid bearer token"
	statusForbiddenMessage    = "contribution status reads target a namespace outside the owned namespaces"
	statusNotFoundMessage     = "contribution run not found"
	statusCapacityMessage     = "contribution status reads are at capacity; retry shortly"
	statusTimeoutMessage      = "contribution status read timed out before the cluster answered"
	statusInternalMessage     = "contribution status read failed"
)

// statusRunNamePattern accepts the Kubernetes object-name shape a derived
// run name carries. It is a cheap pre-cluster guard, not a re-derivation:
// the durable question of whether an object is a contribution is answered
// by the run's immutable spec.source, not by its name.
var statusRunNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

// ErrContributionNotFound indicates that no contribution run with the
// requested name exists in the requested namespace. A run that exists but
// belongs to another source also maps here: this endpoint serves
// contributions, and it does not disclose the existence of foreign
// objects.
var ErrContributionNotFound = errors.New("adhoc: contribution run not found")

// StatusReader serves the lifecycle read for one contribution. Adapter
// satisfies it; the indirection keeps the read handler testable without a
// Kubernetes client, mirroring Admitter for the write path.
type StatusReader interface {
	Status(ctx context.Context, namespace, run string) (ContributionStatus, error)
}

// ContributionStatus is the allowlist read projection of a contribution's
// CoderRun: the controller's own recorded state, nothing else. It carries
// no goal text, no upstream issue, no credential, no source receipt, no
// checkpoints, and no unbounded CRD content. Every field is one the
// frontend needs to report progress, and the fixed shape bounds the
// response.
type ContributionStatus struct {
	// Run and Namespace identify the CoderRun the contribution was
	// admitted to — the same identity the admission response returned.
	Run       string `json:"run"`
	Namespace string `json:"namespace"`
	// Lane, BaseRepo, and WorkRepo echo the admitted contribution's
	// approved targets (spec.lane, spec.repo, spec.workRepo). The
	// idempotency key (spec.workItemID) is deliberately not projected:
	// it is caller-supplied, untrusted text (#819), and the frontend
	// that admitted the run already holds it.
	Lane     string `json:"lane"`
	BaseRepo string `json:"baseRepo"`
	WorkRepo string `json:"workRepo"`
	// Phase is the controller's recorded lifecycle phase verbatim
	// (status.phase): Pending is admission accepted and waiting;
	// Claimed and Running are execution; Verifying is publication under
	// CI; AwaitingReview, HandedOff, NeedsHuman, Done, AlreadyDone,
	// Failed, and BlockedExternal are the run's recorded endings. The
	// projection value "unknown" appears only while the controller has
	// not stamped a phase yet; clients must treat any unrecognized
	// value as unknown, because the CRD enum can gain phases.
	Phase string `json:"phase"`
	// CIParked is true while the run waits on CI in a durable park
	// (status.cipark). Verifying without a park is active verification.
	// The controller retains the park record as the attempt's durable
	// account after the park ends, so a run at a recorded ending is not
	// parked: presence alone does not set this flag.
	CIParked bool `json:"ciParked"`
	// Publication records what the controller has observed published:
	// branch, head repository and SHA, and the pull request. Nil until
	// the controller records any part of it.
	Publication *ContributionPublication `json:"publication,omitempty"`
	// Cause is the controller's recorded terminal cause classification
	// (status.terminalDiagnostic), present only when the run recorded
	// one. It is the bounded category and declared outcome, not the
	// diagnostic's prose.
	Cause *ContributionCause `json:"cause,omitempty"`
	// AdmittedAt, StartedAt, PhaseEnteredAt, and FinishedAt are the
	// controller's recorded lifecycle timestamps (status.admittedAt,
	// startedAt, phaseEnteredAt, finishedAt).
	AdmittedAt     *metav1.Time `json:"admittedAt,omitempty"`
	StartedAt      *metav1.Time `json:"startedAt,omitempty"`
	PhaseEnteredAt *metav1.Time `json:"phaseEnteredAt,omitempty"`
	FinishedAt     *metav1.Time `json:"finishedAt,omitempty"`
	// ObservedAt is when this read answered from the API server, not a
	// controller-recorded field. It is always present and bounds the
	// projection's freshness: the state above is the cluster's as of
	// this instant.
	ObservedAt metav1.Time `json:"observedAt"`
}

// ContributionPublication is the recorded publication identity of a
// contribution run: the durable PR idempotency the controller keeps in
// status (branch, headRepo, headSHA, pr).
type ContributionPublication struct {
	Branch   string `json:"branch,omitempty"`
	HeadRepo string `json:"headRepo,omitempty"`
	HeadSHA  string `json:"headSha,omitempty"`
	PR       string `json:"pr,omitempty"`
}

// ContributionCause is the bounded cause projection of a run's terminal
// diagnostic: the stable category and the declared outcome kind.
type ContributionCause struct {
	Category string `json:"category,omitempty"`
	Outcome  string `json:"outcome,omitempty"`
}

// Status implements StatusReader against the operator's own cluster state:
// the CoderRun the controller reconciles is the only source of truth, and
// the read performs no forge call and no state transition. It reads
// through the same direct API-server reader the admission path's
// create-vs-resume lookups use, never the informer cache: a poller must
// never be served a state the controller has already moved past, and the
// response's ObservedAt honestly names the read time. The concurrency cap
// and read deadline bound the direct reads' cluster cost.
func (a Adapter) Status(ctx context.Context, namespace, name string) (ContributionStatus, error) {
	if a.client == nil {
		return ContributionStatus{}, ErrClientRequired
	}
	run := &courierv1alpha1.CoderRun{}
	if err := a.reader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, run); err != nil {
		if apierrors.IsNotFound(err) {
			return ContributionStatus{}, ErrContributionNotFound
		}
		return ContributionStatus{}, fmt.Errorf("get coder run %s/%s: %w", namespace, name, err)
	}
	// The endpoint serves contributions only, never a proxy for
	// arbitrary CR lookups: a run owned by another source is not a
	// contribution, and its existence is not disclosed.
	if run.Spec.Source != SourceName {
		return ContributionStatus{}, ErrContributionNotFound
	}
	out := projectContribution(run)
	out.ObservedAt = metav1.NewTime(time.Now().UTC())
	return out, nil
}

var _ StatusReader = Adapter{}

// projectContribution maps a CoderRun onto the allowlist DTO. It copies
// only the fields documented on ContributionStatus; adding a field here
// is a contract change to the read API, not an implementation detail.
func projectContribution(run *courierv1alpha1.CoderRun) ContributionStatus {
	out := ContributionStatus{
		Run:       boundedField(run.Name),
		Namespace: boundedField(run.Namespace),
		Lane:      boundedField(run.Spec.Lane),
		BaseRepo:  boundedField(run.Spec.Repo),
		WorkRepo:  boundedField(run.Spec.WorkRepo),
		Phase:     string(run.Status.Phase),
	}
	if out.Phase == "" {
		out.Phase = PhaseUnknown
	}
	out.CIParked = ciparkActive(run)
	pub := ContributionPublication{
		Branch:   boundedField(run.Status.Branch),
		HeadRepo: boundedField(run.Status.HeadRepo),
		HeadSHA:  boundedField(run.Status.HeadSHA),
		PR:       boundedField(run.Status.PR),
	}
	if pub != (ContributionPublication{}) {
		out.Publication = &pub
	}
	if diag := run.Status.TerminalDiagnostic; diag != nil {
		out.Cause = &ContributionCause{
			Category: boundedField(string(diag.Category)),
			Outcome:  boundedField(diag.Outcome),
		}
	}
	out.AdmittedAt = run.Status.AdmittedAt
	out.StartedAt = run.Status.StartedAt
	out.PhaseEnteredAt = run.Status.PhaseEnteredAt
	out.FinishedAt = run.Status.FinishedAt
	return out
}

// ciparkActive reports whether the run currently waits inside the
// controller's durable CI park. The controller ends a park by recording
// the outcome on the park record and moving the run to a recorded ending,
// retaining the record as the attempt's durable account; only the abandon
// paths clear it outright. Presence alone therefore over-reports a
// finished run as waiting: the flag tracks the live park, and the
// recorded endings below are the phases no live park can outlive.
func ciparkActive(run *courierv1alpha1.CoderRun) bool {
	if run.Status.CIPark == nil {
		return false
	}
	switch run.Status.Phase {
	case courierv1alpha1.PhaseAwaitingReview,
		courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseNeedsHuman,
		courierv1alpha1.PhaseDone,
		courierv1alpha1.PhaseAlreadyDone,
		courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseBlockedExternal:
		return false
	}
	return true
}

// boundedField returns s when it fits the projection cap and the empty
// string when it does not (see maxStatusFieldBytes).
func boundedField(s string) string {
	if len(s) > maxStatusFieldBytes {
		return ""
	}
	return s
}

// statusRoute splits a request path into the read path's run segment. It
// reports false for the collection path, any deeper path, and an empty
// segment, all of which are route misses.
func statusRoute(path string) (string, bool) {
	run, ok := strings.CutPrefix(path, statusReadPrefix)
	if !ok || run == "" || strings.Contains(run, "/") {
		return "", false
	}
	return run, true
}

// handleStatus runs the read pipeline in the same stage discipline as the
// admission path — cheap structural checks first, cluster contact last:
//
//  1. auth        read-token bearer, constant time        (401)
//  2. run shape   object-name pattern before any lookup   (404)
//  3. ownership   owned-namespace boundary                (403)
//  4. capacity    bounded in-flight reads                 (503 + Retry-After)
//  5. read        adapter Status under a deadline         (200/404/503 + Retry-After/500)
//
// The router admits requests here only when a read token is configured,
// so an unconfigured server never reaches this code. Stage 1 compares
// only against the read token: the admission credential never
// authenticates a read, so handing a frontend a read token grants the
// read and nothing else. Stage 5's errors map to fixed messages; the run
// name is validated input, so 404 never needs to carry cluster error text.
func (s *AdmissionServer) handleStatus(w http.ResponseWriter, r *http.Request, run string) {
	// Stage 1. One constant message for every credential failure, as on
	// the write path; the read token is never logged or echoed.
	token, ok := bearerToken(r)
	if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.readToken)) != 1 {
		writeError(w, http.StatusUnauthorized, statusUnauthorizedMessage)
		return
	}

	// Stage 2. Malformed names cannot address any object; they are
	// indistinguishable from misses, so they map to the same 404.
	if len(run) > maxStatusRunNameBytes || !statusRunNamePattern.MatchString(run) {
		writeError(w, http.StatusNotFound, statusNotFoundMessage)
		return
	}

	// Stage 3. The same ownership boundary that scopes admission: this
	// release never reads runs outside the namespaces it owns.
	namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
	if namespace == "" {
		namespace = s.defaultNamespace
	}
	if len(s.ownedNamespaces) > 0 && !slices.Contains(s.ownedNamespaces, namespace) {
		writeError(w, http.StatusForbidden, statusForbiddenMessage)
		return
	}

	// Stage 4. Non-blocking slot take: a poller learns to back off
	// immediately instead of queueing behind a stalled cluster.
	select {
	case s.statusSlots <- struct{}{}:
		defer func() { <-s.statusSlots }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, statusCapacityMessage)
		return
	}

	// Stage 5.
	readCtx, cancel := context.WithTimeout(r.Context(), statusReadTimeout)
	defer cancel()
	status, err := s.statusReader.Status(readCtx, namespace, run)
	if err != nil {
		switch {
		case errors.Is(err, ErrContributionNotFound):
			writeError(w, http.StatusNotFound, statusNotFoundMessage)
		case errors.Is(err, context.DeadlineExceeded):
			// The same hint as the capacity 503: the polling contract is
			// one rule — every 503 from the read surface names when to
			// come back, so a stalled cluster sends pollers away on the
			// documented interval instead of a silent guess.
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, statusTimeoutMessage)
		default:
			// Arbitrary cluster error text stays out of the response,
			// exactly as on the write path.
			writeError(w, http.StatusInternalServerError, statusInternalMessage)
		}
		return
	}
	writeJSON(w, http.StatusOK, status)
}
