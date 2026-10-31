package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/branch"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

// SecureNeedsHumanError marks a permanent, actionable deployment failure: a
// configuration or admission problem that requeueing cannot fix. A run
// terminalizes NeedsHuman from the launch paths on the verdict classes
// terminalPreflightVerdict selects — the pod-admission rejections the
// rendered spec's author must fix, and the probe-identity refusals no
// cluster-side action clears. Every other error from those paths is
// transient and follows the normal claim/release/retry cycle, even when it
// is itself one of these: a signal that clears on a later pass would strand
// the run if terminalized.
type SecureNeedsHumanError struct {
	Reason string
	Detail string
}

func (e *SecureNeedsHumanError) Error() string { return e.Detail }

func secureNeedsHuman(reason, format string, args ...any) *SecureNeedsHumanError {
	return &SecureNeedsHumanError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// terminalPreflightVerdict returns the permanent secure-preflight verdicts
// requeueing cannot fix, and nil for every other preflight failure. Only this
// class terminalizes from the launch paths: a live probe that outran a
// just-written NetworkPolicy or a stale RBAC informer cache clears on a later
// pass, and terminalizing those would strand a recoverable run. PodAdmission
// covers a rendered spec the API server rejects outright — only its author
// can fix that. A probe pod is in the class although no lane configures it:
// its spec is an implementation constant, so a rejection there implicates the
// probe image or the cluster, and neither clears on a later pass.
// ProbeIdentity covers a probe pod whose deterministic name resolves to a pod
// pinned to another node: its persistent causes (a digest collision in the
// probe tag, a foreign pod holding the name) outlast a requeue, and the read
// cannot tell them from a stray pod a later pass would clear.
// BaseUnverified covers a fix-pr run whose launch could not verify the pinned
// pull request's live base: the legacy launch path has no persisted policy to
// fall back on, so an unverified base has no other answer, and launching
// against the deployment default would repair the pull request under a base
// it does not name.
func terminalPreflightVerdict(err error) *SecureNeedsHumanError {
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) {
		return nil
	}
	switch permanent.Reason {
	case "PodAdmission", "ProbeIdentity", baseUnverifiedReason:
		return permanent
	}
	return nil
}

// laneConfigReason marks a permanent lane-profile misconfiguration — pod
// sizing that could never launch — raised by the launch paths before any pod
// is created.
const laneConfigReason = "LaneConfig"

// contributionContextReason marks a contribute run whose admitted execution
// context cannot be decoded — missing, foreign, or malformed — raised by the
// launch paths before any coordinator starts (#624). The spec and its context
// are immutable, so this is permanent: the run terminalizes NeedsHuman instead
// of relaunching without an objective.
const contributionContextReason = "ContributionContext"

// laneConfigRejection returns the permanent lane-profile misconfiguration the
// lane's author must fix, and nil for every other launch failure. Like a
// pod-admission rejection this is permanent: requeueing cannot change a lane
// the operator does not edit, so the run terminalizes NeedsHuman naming the
// offending lane rather than cycling claim and release.
func laneConfigRejection(err error) *SecureNeedsHumanError {
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != laneConfigReason {
		return nil
	}
	return permanent
}

// launchRejection returns the permanent, human-fixable launch failure — a
// terminal preflight verdict, a misconfigured lane, or a contribute run whose
// own source refuses the admitted execution context — and nil otherwise.
func launchRejection(err error) *SecureNeedsHumanError {
	if permanent := terminalPreflightVerdict(err); permanent != nil {
		return permanent
	}
	if permanent := laneConfigRejection(err); permanent != nil {
		return permanent
	}
	return contributionContextRejection(err)
}

// contributionContextRejection returns the permanent contribute-launch
// rejection — the adapter owning the run decoded its own durable context and
// refused it — and nil for every other launch failure. The context the decode
// refused lives in the immutable spec, so requeueing cannot change it: the run
// terminalizes NeedsHuman naming the decode failure rather than cycling claim
// and release, and the secure path fences any topology it already provisioned
// before terminalizing.
func contributionContextRejection(err error) *SecureNeedsHumanError {
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != contributionContextReason {
		return nil
	}
	return permanent
}

// ProviderFactory constructs the forge provider facets for one selected
// registration over the operator's read identity. The operator never resolves
// a provider credential: the factory receives the registration's references
// and builds a client against the deployment's own read-only forge identity,
// used only for admission-time canonical-identity and ref reads.
type ProviderFactory func(ctx context.Context, registration *forge.Registration) (AdmissionProvider, error)

// AdmissionProvider bundles the provider facets the operator's admission
// resolution needs. Publication enforcement (write access, protection under
// the broker credential) is never admitted here — those checks run in the
// broker's own startup preflight with the broker's credential.
type AdmissionProvider struct {
	Provider   forge.Provider
	Policy     forge.RepositoryPolicyProvider
	HeadLister forge.PullRequestHeadLister // optional facet
}

// SecureConfig is the deployment-level secure-mode configuration.
type SecureConfig struct {
	// RunNamespace is the namespace dedicated to secure runs. Runs outside it
	// fail closed.
	RunNamespace string
	// Registry is the loaded provider registry.
	Registry *forge.Registry
	// HarnessImage is the image carrying courier-control, courier-worker, and
	// courier-broker.
	HarnessImage string
	// ProbeImage runs the disposable network-probe pods.
	ProbeImage string
	// CacheService optionally names "namespace/name" of the approved
	// read-only dependency cache Service; its ClusterIP is pinned per launch.
	CacheService string
	CachePort    int32
	// Gateway optionally pins model-gateway egress for control. The CIDR/port
	// slot feeds the control NetworkPolicy; URL and key secret are the
	// deployment-level model configuration the control pod renders (#124).
	GatewayCIDR      string
	GatewayPort      int32
	GatewayURL       string
	GatewayKeySecret string
	GatewayKeyName   string
	// LiveProbes enables the multi-node live deny/allow probe phase. Tests
	// against envtest opt out explicitly; production defaults to true.
	LiveProbes bool
	// ProbeTimeout bounds one live-probe phase. Zero uses the default. It is
	// a preflight resource bound, not a run-duration limit.
	ProbeTimeout time.Duration
	// Providers builds admission providers for selected registrations.
	Providers ProviderFactory
	// OperatorNamespace is the namespace the operator itself runs in, where
	// the deployment's credential Secrets referenced by the registry live.
	OperatorNamespace string
	// Kubernetes is the clientset used for access reviews.
	Kubernetes kubernetes.Interface
	// TokenReviewClusterRole names the deployment-level ClusterRole that
	// grants TokenReview creation to per-run brokers, bound by every run's
	// ClusterRoleBinding. Side-by-side releases render distinct names so the
	// fixed cluster-scoped name never collides (#632); empty falls back to
	// the historical default. The chart renders the ClusterRole under the
	// same configured name.
	TokenReviewClusterRole string
	// Now supports deterministic tests.
	Now func() time.Time
}

// SecureControl wires the secure topology into the reconciler: admission
// policy resolution, preflight, provisioning, replacement, revocation, and
// topology observation. A nil SecureControl means the deployment runs the
// legacy insecure topology only.
type SecureControl struct {
	Client client.Client
	Config SecureConfig
	// Patcher is the reconciler that owns CoderRun status writes; the secure
	// path terminalizes through it so all status flows through one writer.
	Patcher statusPatcher
	// MaxRestarts is the crashloop ceiling for the secure topology's
	// broken-topology relaunch, wired from the reconciler where Secure is
	// assembled so it shares one configured ceiling with the wedge and
	// pod-loss paths (#216). Zero falls back to the package default.
	MaxRestarts int
	// APIReader reads the API server directly, bypassing the informer
	// cache, so every destructive decision on the broken-topology relaunch
	// path is made against the world rather than a lagging cache (§6): the
	// post-delete re-confirmation, the run read the crashloop ceiling is
	// judged from, and the control-pod read whose delete carries the
	// observed UID as a precondition. Nil falls back to Client, which serves
	// only direct-construction tests.
	APIReader client.Reader
	// Sources resolves the adapter owning a run by its persisted source name.
	// A contribute-mode run's objective lives in that source's own context, so
	// the launch asks the owning adapter to translate it into the
	// source-neutral goal input (#624). Nil means no contribute run can launch,
	// which its launch fails closed.
	Sources *SourceRegistry
}

func (s *SecureControl) now() time.Time {
	if s.Config.Now != nil {
		return s.Config.Now()
	}
	return time.Now()
}

// tokenReviewClusterRole returns the deployment-level ClusterRole name the
// per-run broker bindings reference. Empty falls back to the historical
// fixed name, so a single release is configured exactly as before; a second
// side-by-side release configures a distinct name (#632).
func (s *SecureControl) tokenReviewClusterRole() string {
	if name := strings.TrimSpace(s.Config.TokenReviewClusterRole); name != "" {
		return name
	}
	return topology.TokenReviewClusterRole
}

// reader returns the reader the secure path's live reads go through — the
// post-delete re-confirmation and the run and control-pod reads behind the
// broken-topology relaunch's destructive decisions — preferring the injected
// API reader and falling back to the cached client. Wired from the manager in
// production, so the fallback serves only direct-construction tests and no
// destructive decision is made against cached state.
func (s *SecureControl) reader() client.Reader {
	if s.APIReader != nil {
		return s.APIReader
	}
	return s.Client
}

// LaunchSecure provisions or reconciles one run's secure topology. The run
// must be Claimed with a persisted publication policy.
type SecureLaunchFunc func(context.Context, *courier.CoderRun) (ctrl.Result, error)

// ResolveAdmission selects the unique provider registration for the run's
// immutable spec.repo, resolves the publication policy through live provider
// reads, and returns the registration and policy. It never resolves a
// provider credential and never performs a credential-bound check: those run
// in the broker's own startup preflight.
func (s *SecureControl) ResolveAdmission(ctx context.Context, run *courier.CoderRun) (*forge.Registration, *courier.PublicationPolicy, error) {
	if run == nil || run.UID == "" {
		return nil, nil, errors.New("secure admission: run is required")
	}
	if run.Namespace != s.Config.RunNamespace {
		return nil, nil, secureNeedsHuman("RunNamespace",
			"secure mode requires runs in the dedicated namespace %q; this run lives in %q",
			s.Config.RunNamespace, run.Namespace)
	}
	if err := s.checkContributionShape(run); err != nil {
		return nil, nil, err
	}
	registration, err := s.selectRegistration(run)
	if err != nil {
		return nil, nil, secureNeedsHuman("ProviderSelection", "provider selection failed for %q: %v", run.Spec.Repo, err)
	}
	if s.Config.Providers == nil {
		return nil, nil, errors.New("secure admission: no provider factory is configured")
	}
	admission, err := s.Config.Providers(ctx, registration)
	if err != nil {
		return nil, nil, secureNeedsHuman("ProviderSelection", "provider %q is unavailable: %v", registration.Name, err)
	}
	if admission.Policy == nil {
		return nil, nil, secureNeedsHuman("ProviderCapability",
			"provider %q does not implement the repository policy reads admission requires", registration.Name)
	}

	canonical, err := admission.Policy.ResolveRepository(ctx, run.Spec.Repo)
	if err != nil {
		return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "canonical repository read for %q failed: %v", run.Spec.Repo, err)
	}
	if canonical.ID == "" || canonical.Canonical == "" || canonical.DefaultRef == "" {
		return nil, nil, secureNeedsHuman("ProviderRead",
			"provider returned an incomplete canonical identity for %q", run.Spec.Repo)
	}

	policy := &courier.PublicationPolicy{
		RunUID:              string(run.UID),
		ProviderConfigRef:   registration.Name,
		ProviderEndpoint:    registration.Endpoint,
		CredentialRefDigest: registration.Projection().Digest,
		BaseRepo:            canonical.Canonical,
	}

	switch run.Spec.Mode {
	case courier.ModeResolveIssue:
		base, err := admission.Policy.ReadRef(ctx, canonical, canonical.DefaultRef)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "base ref read for %q failed: %v", canonical.Canonical, err)
		}
		if !base.Exists || base.OID == "" {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"base ref %q of %q does not exist", canonical.DefaultRef, canonical.Canonical)
		}
		workRef := branch.ResolveIssueBranch(canonical.Canonical, run.Spec.Ref)
		if workRef == "" {
			return nil, nil, errors.New("secure admission: cannot derive the work ref")
		}
		work, err := admission.Policy.ReadRef(ctx, canonical, workRef)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "work ref read for %q failed: %v", workRef, err)
		}
		// The adoption guard searches for existing PRs on the full head
		// identity: a closed or merged PR on this head requires a human; an
		// open PR is reported for review by publication, never adopted.
		if admission.HeadLister != nil {
			existing, err := admission.HeadLister.ListPullRequestsByHead(ctx,
				forge.PullRequestRef{Repo: canonical.Canonical}, canonical.Canonical, workRef)
			if err != nil && !errors.Is(err, forge.ErrUnsupported) {
				return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "existing pull request search failed: %v", err)
			}
			for _, pr := range existing {
				if pr.Merged || pr.State == "closed" {
					return nil, nil, secureNeedsHuman("HeadAlreadyMerged",
						"a closed pull request already exists for head %s:%s; publication requires a human decision",
						canonical.Canonical, workRef)
				}
			}
		}
		policy.BaseRef = canonical.DefaultRef
		policy.BaseOID = base.OID
		policy.WorkRepo = canonical.Canonical
		policy.WorkRef = workRef
		policy.WorkInitiallyAbsent = !work.Exists
		if work.Exists {
			policy.WorkOID = work.OID
		}
		// Trusted source-issue read and criterion pinning (#280): the issue
		// body is read through the provider's typed work-item read on the
		// operator's read identity — never through model input — and the
		// acceptance-criterion set extracted from it is frozen into the
		// policy beside the source-issue identity. The response's own
		// identity is verified against the request first: a misrouted or
		// inconsistent provider answer must never pin issue B's criteria as
		// issue A's contract. An issue without a recognizable Acceptance
		// section pins an empty set (linkage remains the only PR-body
		// contract); an ambiguous or over-bound body fails closed here so
		// an unpinnable contract never becomes an unchecked one. The pin is
		// revision-pinned by content: later issue edits do not mutate a
		// live run's policy.
		source := forge.WorkItemRef{Repo: canonical.Canonical, ID: strconv.Itoa(run.Spec.Ref)}
		workItem, err := admission.Provider.ReadWorkItem(ctx, source)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "source issue %d read for %q failed: %v", run.Spec.Ref, canonical.Canonical, err)
		}
		returnedNumber, err := strconv.Atoi(strings.TrimSpace(workItem.ID))
		if err != nil || returnedNumber != run.Spec.Ref {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"source issue read for %q returned work item %q; the trusted issue number is %d",
				canonical.Canonical, workItem.ID, run.Spec.Ref)
		}
		if !strings.EqualFold(workItem.Repo, canonical.Canonical) {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"source issue read for %q returned a work item for %q; repository identity mismatch",
				canonical.Canonical, workItem.Repo)
		}
		owner, name, ok := splitOwnerName(canonical.Canonical)
		if !ok {
			return nil, nil, secureNeedsHuman("ProviderRead", "canonical repository %q is not an owner/name", canonical.Canonical)
		}
		set, err := broker.ExtractCriteria(workItem.Body, broker.SourceIssue{Owner: owner, Name: name, Number: run.Spec.Ref})
		if err != nil {
			return nil, nil, secureNeedsHuman("CriteriaUnpinnable", "%v", err)
		}
		policy.SourceIssue = &courier.SourceIssueRef{Owner: owner, Name: name, Number: run.Spec.Ref}
		for _, criterion := range set.Criteria {
			policy.AcceptanceCriteria = append(policy.AcceptanceCriteria, courier.Criterion{
				ID:     criterion.ID,
				Text:   criterion.Text,
				Digest: criterion.Digest,
			})
		}
		policy.AcceptanceCriteriaDigest = set.Digest
	case courier.ModeFixPR:
		pr, err := admission.Provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: canonical.Canonical, Number: run.Spec.Ref})
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "pull request %d read failed: %v", run.Spec.Ref, err)
		}
		if pr.BaseRepo != canonical.Canonical || pr.BaseRef == "" || pr.HeadRepo == "" || pr.HeadRef == "" || pr.HeadSHA == "" {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"pull request %d of %q has an incomplete or retargeted identity", run.Spec.Ref, canonical.Canonical)
		}
		head, err := admission.Policy.ResolveRepository(ctx, pr.HeadRepo)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "head repository read for %q failed: %v", pr.HeadRepo, err)
		}
		if head.ID == "" || head.Canonical == "" {
			return nil, nil, secureNeedsHuman("ProviderRead", "head repository %q resolved to an incomplete identity", pr.HeadRepo)
		}
		base, err := admission.Policy.ReadRef(ctx, canonical, pr.BaseRef)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "pull request base ref read failed: %v", err)
		}
		if !base.Exists || base.OID == "" {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"pull request %d base ref %q does not exist on %q", run.Spec.Ref, pr.BaseRef, canonical.Canonical)
		}
		policy.BaseRef = pr.BaseRef
		policy.BaseOID = base.OID
		policy.WorkRepo = head.Canonical
		policy.WorkRef = pr.HeadRef
		policy.WorkOID = pr.HeadSHA
		policy.HeadAnchorOID = pr.HeadSHA
		policy.PRNumber = pr.Number
	case courier.ModeContribute:
		// The admitted objective and base ref live in the source's own
		// durable context, and the coordinator goal is rendered from the
		// same decode (#624). Publication must pin that admitted ref, not
		// the base repository's default branch: the broker refuses any
		// pull request whose base differs from the pinned value, so a
		// policy pinned to the default while the goal states another ref
		// would surface the mismatch only at publication, after the run
		// has done the work (#653). Admission resolves the execution input
		// through the owning adapter here and pins the ref it carries. A
		// context the owning source refuses is immutable spec data, so its
		// refusal keeps the permanent ContributionContext classification;
		// a failure to reach an owning adapter at all stays retryable,
		// exactly as the launch paths treat it.
		contribution, err := launchContribution(s.Sources, run)
		if err != nil {
			return nil, nil, err
		}
		// The approved fork's canonical identity is resolved through the
		// operator's read identity, exactly like the base. Write authority
		// is never verified here: admission establishes it from
		// configuration policy alone — the registry's dual serve coverage
		// above and the registration's credential refs — and the
		// credential-bound CanWriteRepository verification runs in the
		// broker preflight under the run's own credentials.
		workRepo, err := admission.Policy.ResolveRepository(ctx, run.Spec.WorkRepo)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "work repository read for %q failed: %v", run.Spec.WorkRepo, err)
		}
		if workRepo.ID == "" || workRepo.Canonical == "" {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"work repository %q resolved to an incomplete identity", run.Spec.WorkRepo)
		}
		// A contribute policy publishes only into the work repository, so a
		// fork whose canonical identity is the base itself would make the
		// base writable; the broker refuses the same shape at its boundary.
		if workRepo.Canonical == canonical.Canonical {
			return nil, nil, secureNeedsHuman("ContributionPolicy",
				"the approved fork %q is the base repository; a contribution publishes from a distinct head",
				workRepo.Canonical)
		}
		// The admitted base ref is resolved read-only through the
		// operator's read identity and pinned with its observed tip, which
		// publication later rechecks. A ref the admission named but the
		// base repository does not carry is immutable-spec misdirection no
		// retry can fix, so it fails closed.
		base, err := admission.Policy.ReadRef(ctx, canonical, contribution.BaseRef)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "base ref read for %q failed: %v", canonical.Canonical, err)
		}
		if !base.Exists || base.OID == "" {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"admitted base ref %q of %q does not exist", contribution.BaseRef, canonical.Canonical)
		}
		workRef := branch.ResolveGoalBranch(canonical.Canonical, run.Spec.WorkItemID)
		if workRef == "" {
			return nil, nil, errors.New("secure admission: cannot derive the work ref")
		}
		// The durable contribution identity is one branch name shared by the
		// claim-time derivation over the submitted spec.Repo and this
		// admission derivation over the provider-canonical base. Git refs
		// are case-sensitive: when the two derivations differ, the run would
		// work one branch while every guard and the broker police another,
		// so the split itself fails closed to a human (#598).
		if run.Status.Branch != "" && run.Status.Branch != workRef {
			return nil, nil, secureNeedsHuman("ContributionPolicy",
				"the branch recorded at claim %q does not match the goal branch %q derived from the canonical base repository",
				run.Status.Branch, workRef)
		}
		// The discovery guard searches for existing PRs on the full head
		// identity before the work-ref read (#598). Discovery matches base
		// and head repositories and refs, never title text, so every result
		// is re-filtered to the pinned identity: a merged or closed PR on
		// this head requires a human, because a contribution is never
		// reopened or duplicated under the same key — a new contribution is
		// a new key; more than one open PR on the same head is ambiguous and
		// fails closed rather than a guess. A single open PR, draft
		// included, is not an error: publication adopts it as-is on resume,
		// and admission records no PR anchor for a contribution.
		if admission.HeadLister != nil {
			existing, err := admission.HeadLister.ListPullRequestsByHead(ctx,
				forge.PullRequestRef{Repo: canonical.Canonical}, workRepo.Canonical, workRef)
			if err != nil && !errors.Is(err, forge.ErrUnsupported) {
				return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "existing pull request search failed: %v", err)
			}
			numbers := map[int]bool{}
			for _, pr := range existing {
				if pr.BaseRepo != canonical.Canonical || pr.HeadRepo != workRepo.Canonical || pr.HeadRef != workRef {
					continue
				}
				if pr.Merged || pr.State == "closed" {
					return nil, nil, secureNeedsHuman("HeadAlreadyMerged",
						"a closed pull request already exists for head %s:%s; a contribution is never reopened or duplicated under the same key",
						workRepo.Canonical, workRef)
				}
				numbers[pr.Number] = true
			}
			if len(numbers) > 1 {
				return nil, nil, secureNeedsHuman("HeadAmbiguousPR",
					"more than one open pull request already shares head %s:%s; publication requires a human decision",
					workRepo.Canonical, workRef)
			}
		}
		work, err := admission.Policy.ReadRef(ctx, workRepo, workRef)
		if err != nil {
			return nil, nil, transientOrNeedsHuman(err, "ProviderRead", "work ref read for %q failed: %v", workRef, err)
		}
		policy.BaseRef = contribution.BaseRef
		policy.BaseOID = base.OID
		policy.WorkRepo = workRepo.Canonical
		policy.WorkRef = workRef
		if !work.Exists {
			policy.WorkInitiallyAbsent = true
		} else if work.OID == "" {
			return nil, nil, secureNeedsHuman("ProviderRead",
				"work ref %s of %s exists but carries no readable tip", workRef, workRepo.Canonical)
		} else if recordedWorkTip(run, work.OID) {
			policy.WorkOID = work.OID
		} else {
			return nil, nil, secureNeedsHuman("WorkRefForeign",
				"work ref %s of %s carries tip %s, which this run never authored; a contribution never adopts or overwrites a branch it did not author",
				workRef, workRepo.Canonical, work.OID)
		}
		// A contribution pins no acceptance criteria and no source-issue
		// identity: upstream issue context in SourceContext is untrusted
		// task data, and a trusted pin would have to come from the
		// submitting frontend's own policy configuration, which V1 does
		// not carry.
	default:
		return nil, nil, fmt.Errorf("secure admission: unknown run mode %q", run.Spec.Mode)
	}
	return registration, policy, nil
}

// recordedWorkTip reports whether a live work-ref tip matches a tip Courier
// itself recorded for this run: the admission tip or head anchor of a
// publication policy bound to this incarnation, or the observed PR head in
// status. A production first admission carries no such record — the policy
// is set-once and release clears the branch status — so these arms are the
// resume boundary for any later re-admission that does carry one. An
// unrecorded tip is foreign under any key derivation and never becomes this
// run's anchor.
func recordedWorkTip(run *courier.CoderRun, oid string) bool {
	if oid == "" {
		return false
	}
	if run.Status.HeadSHA == oid {
		return true
	}
	if policy := run.Status.PublicationPolicy; policy != nil && policy.RunUID == string(run.UID) {
		return policy.WorkOID == oid || policy.HeadAnchorOID == oid
	}
	return false
}

// selectRegistration resolves the registration for the run's immutable
// spec.repo. A contribute run requires dual coverage: the registration
// serving the contribution must carry both the base repository and the
// approved fork in its serves patterns, because git credentials resolve
// per repository from that one registration's credential refs. A fork
// outside the allowlist or an ambiguous selection fails closed.
func (s *SecureControl) selectRegistration(run *courier.CoderRun) (*forge.Registration, error) {
	if run.Spec.Mode == courier.ModeContribute {
		return s.Config.Registry.SelectContribution(run.Spec.Repo, run.Spec.WorkRepo)
	}
	return s.Config.Registry.Select(run.Spec.Repo)
}

// checkContributionShape fails closed before any registry or provider
// contact when a contribute run's immutable spec cannot yield a complete
// publication policy. The CRD rules should make this shape unreachable;
// a direct construction that bypassed them is deterministic — the spec is
// immutable — so it terminalizes NeedsHuman rather than retrying forever.
func (s *SecureControl) checkContributionShape(run *courier.CoderRun) error {
	if run.Spec.Mode != courier.ModeContribute {
		return nil
	}
	if run.Spec.Ref != 0 || run.Spec.WorkRepo == "" || strings.TrimSpace(run.Spec.WorkItemID) == "" {
		return secureNeedsHuman("ContributionPolicy",
			"a contribute run carries an approved fork and no source-owned work item; spec.ref is %d, spec.workRepo is %q, and spec.workItemID is %q",
			run.Spec.Ref, run.Spec.WorkRepo, run.Spec.WorkItemID)
	}
	return nil
}

// transientOrNeedsHuman classifies a provider read failure. Provider
// transport failures are transient (requeue, retry with the same immutable
// inputs); an unsupported capability or missing identity is permanent.
func transientOrNeedsHuman(err error, reason, format string, args ...any) error {
	if errors.Is(err, forge.ErrUnsupported) {
		return secureNeedsHuman("ProviderCapability", "%s: %v", fmt.Sprintf(format, args...), err)
	}
	var permanent *SecureNeedsHumanError
	if errors.As(err, &permanent) {
		return err
	}
	return fmt.Errorf("%s: %w", reason, fmt.Errorf(format, args...))
}

// splitOwnerName splits a canonical "owner/name" repository on the final
// separator. It mirrors the broker's split without importing provider code.
func splitOwnerName(canonical string) (string, string, bool) {
	idx := strings.LastIndex(canonical, "/")
	if idx <= 0 || idx == len(canonical)-1 {
		return "", "", false
	}
	return canonical[:idx], canonical[idx+1:], true
}

// VerifyPersistedPolicy returns the run's persisted publication policy after
// binding it to the live run incarnation. A missing policy is an internal
// sequencing error; a mismatched one is a fail-closed corruption signal.
func VerifyPersistedPolicy(run *courier.CoderRun) (*courier.PublicationPolicy, error) {
	if run == nil {
		return nil, errors.New("run is required")
	}
	policy := run.Status.PublicationPolicy
	if policy == nil {
		return nil, fmt.Errorf("run %s/%s has no persisted publication policy", run.Namespace, run.Name)
	}
	if policy.RunUID != string(run.UID) {
		return nil, fmt.Errorf("persisted publication policy is bound to run UID %q, not this incarnation %q",
			policy.RunUID, run.UID)
	}
	return policy, nil
}

// mintIncarnation generates the operator-minted control incarnation
// identifier provisioned into both the control and worker pods of one
// topology round. Replacements always mint a fresh identifier, so signed
// envelopes from a previous incarnation are rejected by construction.
func mintIncarnation() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("secure topology: entropy unavailable: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// mintSigningKey generates a fresh per-control-incarnation Ed25519 keypair.
func mintSigningKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	public, private, err := protocol.GenerateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("secure topology: %w", err)
	}
	return public, private, nil
}

// cachePin resolves the dependency cache Service's ClusterIP at launch time.
// A stale address fails closed: the pin is re-resolved for every launch, and
// an unavailable cache Service yields a transient error (requeue) rather than
// a launch without the pinned address.
func (s *SecureControl) cachePin(ctx context.Context) (*topology.CachePin, error) {
	if strings.TrimSpace(s.Config.CacheService) == "" {
		return nil, nil
	}
	parts := strings.SplitN(s.Config.CacheService, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("secure topology: --dependency-cache-service must be namespace/name, got %q", s.Config.CacheService)
	}
	service := &corev1.Service{}
	if err := s.reader().Get(ctx, types.NamespacedName{Namespace: parts[0], Name: parts[1]}, service); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("secure topology: dependency cache Service %s not found", s.Config.CacheService)
		}
		return nil, fmt.Errorf("secure topology: read dependency cache Service: %w", err)
	}
	if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == "None" {
		return nil, fmt.Errorf("secure topology: dependency cache Service %s has no ClusterIP to pin", s.Config.CacheService)
	}
	port := s.Config.CachePort
	if port == 0 && len(service.Spec.Ports) > 0 {
		port = service.Spec.Ports[0].Port
	}
	if port == 0 {
		return nil, fmt.Errorf("secure topology: dependency cache Service %s exposes no port", s.Config.CacheService)
	}
	return &topology.CachePin{IP: service.Spec.ClusterIP, Port: port}, nil
}
