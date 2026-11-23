package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

// legacyInstallationTokenAbsentRequeue bounds the requeue the legacy
// steady-state rotation asks for when no usable installation-token document
// exists to schedule against. The rotation ensure recreates a missing
// document before this delay is ever consulted on the settle path, so the
// value only paces a repeated read failure; the refresh lead is the bound.
const legacyInstallationTokenAbsentRequeue = installationTokenRefreshLead

// Git credentials for legacy coordinator pods served by the GitHub App
// installation minter (#871). The legacy coordinator authenticates git
// transport only — it never publishes through the forge API — so its mints
// carry the minimal transport profile: the base repository is fetched (read
// contents), the work repository is pushed (write contents), and metadata
// read is what GitHub requires to resolve the repository itself. No
// administration, issues, pull requests, checks, or statuses: those belong
// to the secure broker's publication surfaces, never to a legacy pod.
var (
	legacyBaseRepoGitPermissions = map[string]string{
		"contents": "read",
		"metadata": "read",
	}
	legacyWorkRepoGitPermissions = map[string]string{
		"contents": "write",
		"metadata": "read",
	}
)

// InstallationGitOptions configures a legacy (non-secure-mode) launcher for
// App-mode git credentials. Nil on a CoordinatorLauncher keeps every launch
// byte-identical to a static deployment: no mint, no Secret, no run-scoped
// identity, and the static COURIER_GIT_* pair exactly as before.
type InstallationGitOptions struct {
	// Minter is the deployment's GitHub App installation-token minter.
	Minter InstallationTokenMinter
	// Now supports deterministic tests. Nil falls back to time.Now.
	Now func() time.Time
}

func (o *InstallationGitOptions) now() time.Time {
	if o != nil && o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// LegacyGitCredentials is the reconciler's steady-state rotation seam for
// App-mode legacy runs: it keeps the run's installation-token credentials
// Secret fresh while its coordinator runs and reports when the next refresh
// pass is due. Only the CoordinatorLauncher in an App-mode legacy deployment
// implements it; nil outside that wiring leaves the settle byte-identical.
type LegacyGitCredentials interface {
	// EnsureInstallationToken maintains the run's installation-token
	// credentials Secret. It never creates one for a run whose coordinator
	// was not launched App-mode, and it never reads or overwrites a Secret
	// the run does not control.
	EnsureInstallationToken(ctx context.Context, run *courierv1alpha1.CoderRun) error
	// InstallationTokenRequeueDelay returns how long the reconciler may wait
	// before the pass that refreshes the tokens: the moment the earliest
	// document entry enters the refresh window, floored at the broker margin
	// so a near-expiry document still schedules a real pass (a zero
	// RequeueAfter is no requeue at all), or a bounded default when no usable
	// document exists.
	InstallationTokenRequeueDelay(ctx context.Context, run *courierv1alpha1.CoderRun) time.Duration
}

var _ LegacyGitCredentials = (*CoordinatorLauncher)(nil)

func (l *CoordinatorLauncher) installationGitNow() time.Time {
	return l.InstallationGit.now()
}

// legacyGitInstallationIdentity derives the run's base and work repositories
// the way the pod builder derives its git remotes — the work remote follows
// the persisted head repository, falling back to the base — and proves the
// mint identity before any token exists: both repositories must name the
// same owner, the minter must sign for exactly that owner, and the minter
// endpoint must be a usable https API endpoint. Every refusal here is
// permanent for this run and its deployment, not something a requeue clears.
func (l *CoordinatorLauncher) legacyGitInstallationIdentity(run *courierv1alpha1.CoderRun) (string, string, string, *executor.InstallationGitConfig, error) {
	base := run.Spec.Repo
	work := strings.TrimSpace(run.Status.HeadRepo)
	if work == "" {
		work = base
	}
	baseOwner, _, baseOK := splitOwnerName(base)
	workOwner, _, workOK := splitOwnerName(work)
	if !baseOK || !workOK || !strings.EqualFold(baseOwner, workOwner) {
		return "", "", "", nil, secureNeedsHuman("InstallationTokenOwner",
			"GitHub App installation-token git credentials require base and work repositories with the same owner; base %q, head %q", base, work)
	}
	minterOwner, minterEndpoint := l.InstallationGit.Minter.Identity()
	if !strings.EqualFold(minterOwner, baseOwner) {
		return "", "", "", nil, secureNeedsHuman("InstallationTokenOwner",
			"GitHub App installation identity %q does not own repository owner %q", minterOwner, baseOwner)
	}
	config := &executor.InstallationGitConfig{Owner: baseOwner, APIEndpoint: minterEndpoint, CredentialsSecret: topology.CredentialsSecretName(run.Name)}
	if err := config.Validate(); err != nil {
		return "", "", "", nil, secureNeedsHuman("InstallationTokenEndpoint",
			"GitHub App identity endpoint %q is not a usable API endpoint: %v", minterEndpoint, err)
	}
	return base, work, baseOwner, config, nil
}

// ensureLegacyInstallationTokenCredential maintains the run's installation-
// token credentials Secret, mirroring the secure path's contract: a live read
// decides existence, a deterministic name is trusted only once proven
// controlled by this exact run incarnation, a document that is still fresh
// for this identity is served without minting, and every mint is repository-
// scoped with only the git-transport permissions that repository needs. A
// create race is settled only against a winner proven owned and bound to this
// run's identity; anything else fails closed.
func (l *CoordinatorLauncher) ensureLegacyInstallationTokenCredential(ctx context.Context, run *courierv1alpha1.CoderRun, base, work, owner string, config *executor.InstallationGitConfig) error {
	key := types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}
	existing := &corev1.Secret{}
	err := l.collisionReader().Get(ctx, key, existing)
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return fmt.Errorf("legacy installation token: read credentials: %w", err)
	}
	now := l.installationGitNow()
	if err == nil {
		// A deterministic name is only trusted once proven controlled by this
		// exact run incarnation: a foreign or stale-owner object is never read
		// for its credential and never overwritten, so a name collision cannot
		// turn into a credential write to an object the run does not own.
		if !metav1.IsControlledBy(existing, run) {
			return secureNeedsHuman("InstallationTokenOwnership",
				"the credentials Secret %q is not controlled by this run; refusing to read or overwrite its installation token", key.Name)
		}
		if legacyInstallationDocumentFresh(existing.Data[topology.CredKeyInstallationToken], owner, config.APIEndpoint, []string{base, work}, now) {
			return nil
		}
	}

	entries, err := l.mintLegacyInstallationEntries(ctx, base, work, now)
	if err != nil {
		return err
	}
	document := topology.InstallationTokenDocument{
		Version:  topology.InstallationTokenDocumentVersion,
		Owner:    owner,
		Endpoint: config.APIEndpoint,
		Entries:  entries,
	}
	secret, err := topology.InstallationTokenCredentials(run, document)
	if err != nil {
		return fmt.Errorf("legacy installation token: render credentials: %w", err)
	}
	// A superseded installation token stays valid on GitHub until its own
	// expiry, and the coordinator pod held it while the previous document was
	// live, so it must remain recoverable for the evidence intake's re-scan.
	// The intake is the only reader of this key.
	if !missing {
		if superseded := installationSupersededEntries(existing.Data, entries, now); len(superseded) > 0 {
			raw, err := json.Marshal(superseded)
			if err != nil {
				return fmt.Errorf("legacy installation token: render superseded entries: %w", err)
			}
			secret.Data[topology.CredKeyInstallationTokenSuperseded] = raw
		}
	}
	if missing {
		if err := l.Client.Create(ctx, secret); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("legacy installation token: create credentials: %w", err)
			}
			// The create race is only settled once the winner is proven to be
			// this run's own object carrying a document for this run's
			// identity: a foreign winner fails closed, and a winner whose
			// document still needs attention is refreshed by the next pass.
			winner := &corev1.Secret{}
			if err := l.collisionReader().Get(ctx, key, winner); err != nil {
				return fmt.Errorf("legacy installation token: read credentials winner: %w", err)
			}
			if !metav1.IsControlledBy(winner, run) {
				return secureNeedsHuman("InstallationTokenOwnership",
					"the credentials Secret %q created under this run's name is not controlled by this run; refusing to trust it", key.Name)
			}
			var winnerDocument topology.InstallationTokenDocument
			if err := json.Unmarshal(winner.Data[topology.CredKeyInstallationToken], &winnerDocument); err != nil ||
				winnerDocument.Version != topology.InstallationTokenDocumentVersion ||
				!strings.EqualFold(winnerDocument.Owner, owner) ||
				!installationEndpointMatches(winnerDocument.Endpoint, config.APIEndpoint) ||
				!legacyInstallationDocumentCovers(winnerDocument, base, work) {
				return errors.New("legacy installation token: credentials winner carries a document for a different identity")
			}
			return nil
		}
		return nil
	}
	// Ownership is proven above, so the update replaces the credential whole
	// and converges the owner references to the rendered object rather than
	// preserving anything a previous writer left behind.
	existing.Data = secret.Data
	existing.OwnerReferences = secret.OwnerReferences
	if err := l.Client.Update(ctx, existing); err != nil {
		return fmt.Errorf("legacy installation token: update credentials: %w", err)
	}
	return nil
}

// installationSupersededEntries returns the not-yet-expired credentials the
// stored Secret carried before this rotation — the live document's entries and
// any already-superseded ones — deduplicated by token and excluding the
// replacement entries. A stored value that no longer parses holds no
// recoverable credential and contributes nothing; failing the scan on what it
// cannot parse is the intake's job, not the rotation's.
func installationSupersededEntries(stored map[string][]byte, replacement []topology.InstallationTokenEntry, now time.Time) []topology.InstallationTokenEntry {
	seen := make(map[string]bool, len(replacement))
	for _, entry := range replacement {
		seen[entry.Token] = true
	}
	var superseded []topology.InstallationTokenEntry
	collect := func(entries []topology.InstallationTokenEntry) {
		for _, entry := range entries {
			if entry.Token == "" || !entry.ExpiresAt.After(now) || seen[entry.Token] {
				continue
			}
			seen[entry.Token] = true
			superseded = append(superseded, entry)
		}
	}
	var document topology.InstallationTokenDocument
	if err := json.Unmarshal(stored[topology.CredKeyInstallationToken], &document); err == nil {
		collect(document.Entries)
	}
	var previous []topology.InstallationTokenEntry
	if err := json.Unmarshal(stored[topology.CredKeyInstallationTokenSuperseded], &previous); err == nil {
		collect(previous)
	}
	return superseded
}

// mintLegacyInstallationEntries mints one repository-scoped token per pinned
// repository with only the git-transport permissions that repository needs.
// A same-repository run (work == base) is the one case where both capability
// classes genuinely target a single repository, and its mint carries the
// merge of the two profiles. A credential that cannot outlive the refresh
// window turns every pass into a mint, so it is rejected as a transient
// failure rather than written.
func (l *CoordinatorLauncher) mintLegacyInstallationEntries(ctx context.Context, base, work string, now time.Time) ([]topology.InstallationTokenEntry, error) {
	type credentialCapability struct {
		repository  string
		permissions map[string]string
	}
	capabilities := []credentialCapability{{repository: base, permissions: cloneInstallationTokenPermissions(legacyBaseRepoGitPermissions)}}
	if !strings.EqualFold(work, base) {
		capabilities = append(capabilities, credentialCapability{repository: work, permissions: cloneInstallationTokenPermissions(legacyWorkRepoGitPermissions)})
	} else {
		for permission, level := range legacyWorkRepoGitPermissions {
			if level == "write" || capabilities[0].permissions[permission] == "" {
				capabilities[0].permissions[permission] = level
			}
		}
	}
	entries := make([]topology.InstallationTokenEntry, 0, len(capabilities))
	for _, capability := range capabilities {
		token, expiresAt, err := l.InstallationGit.Minter.Mint(ctx, []string{capability.repository}, capability.permissions)
		if err != nil {
			return nil, fmt.Errorf("legacy installation token: mint for %s: %w", capability.repository, err)
		}
		if token == "" || !expiresAt.After(now.Add(installationTokenRefreshLead+installationTokenBrokerMargin)) {
			return nil, fmt.Errorf("legacy installation token: mint for %s returned a credential shorter than the refresh window", capability.repository)
		}
		entries = append(entries, topology.InstallationTokenEntry{Repository: capability.repository, Token: token, ExpiresAt: expiresAt})
	}
	return entries, nil
}

// legacyInstallationDocumentFresh reports that the stored document is the
// supported version bound to this run's owner and endpoint, carries a token
// for each named repository, and every entry outlives the refresh window.
// Owner and endpoint are re-checked on the fast path so a document that
// drifted away from the run's identity is refreshed rather than served.
func legacyInstallationDocumentFresh(raw []byte, owner, endpoint string, repositories []string, now time.Time) bool {
	var document topology.InstallationTokenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return false
	}
	if document.Version != topology.InstallationTokenDocumentVersion ||
		!strings.EqualFold(document.Owner, owner) || !installationEndpointMatches(document.Endpoint, endpoint) {
		return false
	}
	for _, repository := range repositories {
		entry, ok := document.Entry(repository)
		if !ok || entry.Token == "" || !entry.ExpiresAt.After(now.Add(installationTokenRefreshLead)) {
			return false
		}
	}
	return true
}

// legacyInstallationDocumentCoversRepositories reports that the document
// carries a non-empty entry for every repository the run targets, ignoring
// expiry. The create race uses it so a winner owned by this run but missing a
// repository is refused rather than trusted until a later pass notices.
func legacyInstallationDocumentCovers(document topology.InstallationTokenDocument, repositories ...string) bool {
	for _, repository := range repositories {
		entry, ok := document.Entry(repository)
		if !ok || entry.Token == "" {
			return false
		}
	}
	return true
}

// ensureLegacyGitCredentialsResources provisions the run-scoped identity the
// App-mode pod's git egress guard reads its credentials Secret with: a
// ServiceAccount (only when the isolated observer is not already the pod's
// identity), a Role granting exactly one get on the run's credentials Secret,
// and a RoleBinding of that Role to the pod's effective ServiceAccount. The
// projected token that carries this identity is mounted only in the guard
// container (#902): the model container holds neither the credential nor a
// means to read it. All three are ensured and ownership-verified before the
// coordinator pod can begin execution, with the same fail-closed
// reuse-or-refuse semantics as the observer's identity.
func (l *CoordinatorLauncher) ensureLegacyGitCredentialsResources(ctx context.Context, run *courierv1alpha1.CoderRun, pod *corev1.Pod) error {
	name := legacyGitCredentialsResourceName(run.Name)
	effective := name
	if pod.Annotations[executor.ObserverContractAnnotation] == executor.ObserverContractVersion {
		// The isolated observer already put its run-scoped ServiceAccount on
		// the pod; the credential grant binds to that same identity rather
		// than introducing a second one the pod never uses.
		effective = legacyObserverResourceName(run.Name)
	} else {
		if err := l.ensureObserverObject(ctx, run.UID, &corev1.ServiceAccount{
			ObjectMeta:                   metav1.ObjectMeta{Namespace: run.Namespace, Name: name, OwnerReferences: l.observerOwner(run)},
			AutomountServiceAccountToken: boolPtr(false),
		}); err != nil {
			return err
		}
		// The identity must be on the pod before it is created; setting it
		// afterwards would not persist, and the credential helper would read
		// the namespace default ServiceAccount with no access to the Secret.
		pod.Spec.ServiceAccountName = name
	}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: name, OwnerReferences: l.observerOwner(run)}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{topology.CredentialsSecretName(run.Name)}, Verbs: []string{"get"}}}}
	if err := l.ensureObserverObject(ctx, run.UID, role); err != nil {
		return err
	}
	return l.ensureObserverObject(ctx, run.UID, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: name, OwnerReferences: l.observerOwner(run)}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}, Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: effective, Namespace: run.Namespace}}})
}

func legacyGitCredentialsResourceName(runName string) string {
	return "courier-gitcreds-" + executor.RunLabelValue(runName)
}

// EnsureInstallationToken maintains the App-mode legacy run's installation-
// token credentials Secret between launches. It only maintains Secrets that
// belong: a run-owned Secret is kept fresh; a missing Secret is recreated
// only when the run's coordinator pod was launched App-mode, so a pre-App
// static run never gains a Secret behind its back; and a foreign uncontrolled
// Secret is an error, never an overwrite.
func (l *CoordinatorLauncher) EnsureInstallationToken(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	if l == nil || l.Client == nil {
		return ErrLauncherClientRequired
	}
	if run == nil {
		return executor.ErrNilRun
	}
	if l.InstallationGit == nil || l.InstallationGit.Minter == nil {
		return errors.New("legacy installation token: no GitHub App minter is configured")
	}
	existing := &corev1.Secret{}
	err := l.collisionReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, existing)
	switch {
	case apierrors.IsNotFound(err):
		if !l.coordinatorUsesGitInstallation(ctx, run) {
			return nil
		}
	case err != nil:
		return fmt.Errorf("legacy installation token: read credentials: %w", err)
	default:
		if !metav1.IsControlledBy(existing, run) {
			return secureNeedsHuman("InstallationTokenOwnership",
				"the credentials Secret %q is not controlled by this run; refusing to read or overwrite its installation token", existing.Name)
		}
	}
	base, work, owner, config, err := l.legacyGitInstallationIdentity(run)
	if err != nil {
		return err
	}
	return l.ensureLegacyInstallationTokenCredential(ctx, run, base, work, owner, config)
}

// InstallationTokenRequeueDelay returns how long the reconciler should wait
// before the pass that refreshes the run's tokens: the moment the earliest
// current document entry enters the refresh window, floored at the broker
// margin so a document already inside the window still schedules a real
// rotation pass (a zero RequeueAfter is no requeue at all), or the bounded
// default when no usable document exists yet.
func (l *CoordinatorLauncher) InstallationTokenRequeueDelay(ctx context.Context, run *courierv1alpha1.CoderRun) time.Duration {
	secret := &corev1.Secret{}
	if err := l.collisionReader().Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, secret); err != nil {
		return legacyInstallationTokenAbsentRequeue
	}
	var document topology.InstallationTokenDocument
	if err := json.Unmarshal(secret.Data[topology.CredKeyInstallationToken], &document); err != nil ||
		document.Version != topology.InstallationTokenDocumentVersion || len(document.Entries) == 0 {
		return legacyInstallationTokenAbsentRequeue
	}
	now := l.installationGitNow()
	delay := time.Duration(-1)
	for _, entry := range document.Entries {
		entryDelay := entry.ExpiresAt.Add(-installationTokenRefreshLead).Sub(now)
		if entryDelay < 0 {
			entryDelay = 0
		}
		if delay < 0 || entryDelay < delay {
			delay = entryDelay
		}
	}
	if delay < 0 {
		return legacyInstallationTokenAbsentRequeue
	}
	if delay < installationTokenBrokerMargin {
		return installationTokenBrokerMargin
	}
	return delay
}

// coordinatorUsesGitInstallation reports whether the run's coordinator pod
// was rendered App-mode: a run-attributed, non-terminating coordinator whose
// pod carries the App-mode git credential contract, in either the guard
// shape or the pre-#902 askpass shape. The pod spec is authoritative here —
// an App-enabled pod never renders without it, and a static pod never
// renders with it.
func (l *CoordinatorLauncher) coordinatorUsesGitInstallation(ctx context.Context, run *courierv1alpha1.CoderRun) bool {
	var pods corev1.PodList
	if err := l.Client.List(ctx, &pods,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{executor.LabelRun: executor.RunLabelValue(run.Name)},
	); err != nil {
		return false
	}
	for i := range pods.Items {
		if coordinatorRendersGitInstallation(&pods.Items[i]) {
			return true
		}
	}
	return false
}

// coordinatorRendersGitInstallation reports whether pod is the run's
// coordinator rendered with the App-mode git credential contract: the #902
// egress-guard annotation and container, or — for pods launched before the
// guard existed — the coordinator container's installation-contract
// environment. Recognizing both keeps steady-state rotation running for an
// in-flight pre-guard pod across an operator upgrade.
func coordinatorRendersGitInstallation(pod *corev1.Pod) bool {
	if pod == nil || !hasCoordinatorContainer(pod) || pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Annotations[executor.GitEgressContractAnnotation] == executor.GitEgressContractVersion {
		return true
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != coordinatorContainerName {
			continue
		}
		for _, env := range container.Env {
			if env.Name == executor.EnvGitInstallation {
				return true
			}
		}
	}
	return false
}
