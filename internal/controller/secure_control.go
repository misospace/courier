package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

// SecureNeedsHumanError marks a permanent, actionable secure-mode failure:
// a configuration or admission problem that requeueing cannot fix. The
// reconciler records it as a SecurePreflightFailed condition and terminates
// the run NeedsHuman. Any other error from the secure path is treated as
// transient and follows the normal claim/release/retry cycle.
type SecureNeedsHumanError struct {
	Reason string
	Detail string
}

func (e *SecureNeedsHumanError) Error() string { return e.Detail }

func secureNeedsHuman(reason, format string, args ...any) *SecureNeedsHumanError {
	return &SecureNeedsHumanError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
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
	// Gateway optionally pins model-gateway egress for control. Empty until
	// the native model client (#124) supplies its configuration.
	GatewayCIDR string
	GatewayPort int32
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
}

func (s *SecureControl) now() time.Time {
	if s.Config.Now != nil {
		return s.Config.Now()
	}
	return time.Now()
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
	registration, err := s.Config.Registry.Select(run.Spec.Repo)
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
	default:
		return nil, nil, fmt.Errorf("secure admission: unknown run mode %q", run.Spec.Mode)
	}
	return registration, policy, nil
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
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: parts[0], Name: parts[1]}, service); err != nil {
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
