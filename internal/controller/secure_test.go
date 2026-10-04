package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
	"github.com/misospace/courier/internal/topology"
)

const secureTestNamespace = "courier-runs"

// fakeAdmissionForge implements the admission provider facets in memory.
type fakeAdmissionForge struct {
	repos   map[string]forge.Repository
	refs    map[string]forge.RefState
	prs     []forge.PullRequest
	readErr error
}

func (f *fakeAdmissionForge) ResolveRepository(_ context.Context, name string) (forge.Repository, error) {
	if f.readErr != nil {
		return forge.Repository{}, f.readErr
	}
	repo, ok := f.repos[name]
	if !ok {
		return forge.Repository{}, errors.New("unknown repository")
	}
	return repo, nil
}

func (f *fakeAdmissionForge) ReadRef(_ context.Context, repo forge.Repository, ref string) (forge.RefState, error) {
	if f.readErr != nil {
		return forge.RefState{}, f.readErr
	}
	return f.refs[repo.Canonical+":"+ref], nil
}

func (f *fakeAdmissionForge) ReadEffectiveProtection(context.Context, forge.Repository, string) (forge.Protection, error) {
	return forge.Protection{Protected: false}, nil
}

func (f *fakeAdmissionForge) CanWriteRepository(context.Context, forge.Repository) (bool, error) {
	return true, nil
}

func (f *fakeAdmissionForge) ReadWorkItem(context.Context, forge.WorkItemRef) (forge.WorkItem, error) {
	return forge.WorkItem{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) Config() forge.ProviderConfig {
	return forge.ProviderConfig{Name: "github", Endpoint: "https://api.github.com/"}
}

func (f *fakeAdmissionForge) Capabilities() forge.Capabilities {
	return forge.NewCapabilities(forge.CapabilityReadPullRequest, forge.CapabilityCreatePullRequest, forge.CapabilityUpdatePullRequest)
}

func (f *fakeAdmissionForge) ReadPullRequest(_ context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	for _, pr := range f.prs {
		if pr.Number == ref.Number && pr.BaseRepo == ref.Repo {
			return pr, nil
		}
	}
	return forge.PullRequest{}, errors.New("pull request not found")
}

func (f *fakeAdmissionForge) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) CreatePullRequest(context.Context, forge.CreatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) UpdatePullRequest(context.Context, forge.PullRequestRef, forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	return forge.Comment{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) ListPullRequestsByHead(_ context.Context, base forge.PullRequestRef, repo, ref string) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, pr := range f.prs {
		if pr.BaseRepo == base.Repo && pr.HeadRepo == repo && pr.HeadRef == ref {
			out = append(out, pr)
		}
	}
	return out, nil
}

func fakeProviderFactory(provider *fakeAdmissionForge) ProviderFactory {
	return func(context.Context, *forge.Registration) (AdmissionProvider, error) {
		return AdmissionProvider{Provider: provider, Policy: provider, HeadLister: provider}, nil
	}
}

func secureRegistry() *forge.Registry {
	data := []byte(`{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["*"]}]}`)
	registry, err := forge.Load(data, "github")
	if err != nil {
		panic(err)
	}
	return registry
}

func secureRun(name string) *courier.CoderRun {
	run := &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: secureTestNamespace,
			UID:       types.UID("uid-" + name),
		},
		Spec: courier.CoderRunSpec{
			Mode:       courier.ModeResolveIssue,
			Source:     "manual",
			WorkItemID: "item-1",
			Repo:       "acme/widgets",
			Ref:        7,
			Lane:       "local",
		},
	}
	return run
}

func secureForge() *fakeAdmissionForge {
	baseRepo := forge.Repository{ID: "repo-1", Canonical: "Acme/Widgets", DefaultRef: "main"}
	return &fakeAdmissionForge{
		repos: map[string]forge.Repository{"acme/widgets": baseRepo, "Acme/Widgets": baseRepo},
		refs: map[string]forge.RefState{
			"Acme/Widgets:main":                         {Ref: "main", OID: "baseoid", Exists: true},
			"Acme/Widgets:courier/Acme/Widgets/issue-7": {Ref: "courier/Acme/Widgets/issue-7", Exists: false},
		},
	}
}

func secureControl(t *testing.T, objects ...client.Object) (*SecureControl, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, networkingv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	// The lane every test run names; the launcher builds the control pod's
	// run context from it.
	lane := &courier.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: secureTestNamespace, Name: "local"},
		Spec: courier.LaneProfileSpec{
			Roles: map[string]string{
				"coordinator": "test/coordinator",
				"coder":       "test/coder",
			},
		},
	}
	objects = append([]client.Object{lane}, objects...)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courier.CoderRun{}).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				// Stand in for the API server: assign UIDs on create and
				// reject the known-bad dry-run specs the preflight uses to
				// prove restricted admission.
				if obj.GetUID() == "" {
					obj.SetUID(types.UID("generated-" + obj.GetName()))
				}
				if strings.Contains(obj.GetName(), "-bad-dryrun") {
					return errors.New("admission denied: known-bad pod spec")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	control := &SecureControl{
		Client:  c,
		Patcher: &CoderRunReconciler{Client: c, StatusWriter: fakeStatusWriter{client: c}},
		Config: SecureConfig{
			RunNamespace:      secureTestNamespace,
			Registry:          secureRegistry(),
			HarnessImage:      "harness:test",
			ProbeImage:        "busybox:test",
			LiveProbes:        false,
			OperatorNamespace: "courier-system",
			Providers:         fakeProviderFactory(secureForge()),
			Kubernetes:        fakeAccessReviews(),
			Now:               func() time.Time { return time.Now() },
		},
	}
	return control, c
}

// fakeAccessReviews answers every access review with allowed, standing in
// for the RBAC authorizer.
func fakeAccessReviews() *fakek8s.Clientset {
	clientset := fakek8s.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SelfSubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
		}, nil
	})
	clientset.Fake.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
		}, nil
	})
	return clientset
}

func TestResolveAdmissionResolveIssue(t *testing.T) {
	control, _ := secureControl(t)
	run := secureRun("resolve")
	registration, policy, err := control.ResolveAdmission(context.Background(), run)
	if err != nil {
		t.Fatalf("ResolveAdmission: %v", err)
	}
	if registration.Name != "github" {
		t.Fatalf("registration = %q", registration.Name)
	}
	if policy.RunUID != string(run.UID) || policy.ProviderConfigRef != "github" {
		t.Fatalf("policy identity = %+v", policy)
	}
	if policy.BaseRepo != "Acme/Widgets" {
		t.Fatalf("base repo must be canonical: %q", policy.BaseRepo)
	}
	if policy.BaseRef != "main" || policy.BaseOID != "baseoid" {
		t.Fatalf("base = %q/%q", policy.BaseRef, policy.BaseOID)
	}
	if policy.WorkRepo != "Acme/Widgets" || policy.WorkRef != "courier/Acme/Widgets/issue-7" {
		t.Fatalf("work = %q/%q", policy.WorkRepo, policy.WorkRef)
	}
	if !policy.WorkInitiallyAbsent || policy.WorkOID != "" {
		t.Fatalf("absent work ref must be recorded as initially absent: %+v", policy)
	}
	if policy.ProviderEndpoint != "https://api.github.com/" || policy.CredentialRefDigest == "" {
		t.Fatalf("provider binding fields = %q %q", policy.ProviderEndpoint, policy.CredentialRefDigest)
	}
}

func TestResolveAdmissionFixPR(t *testing.T) {
	control, _ := secureControl(t)
	provider := &fakeAdmissionForge{
		repos: map[string]forge.Repository{
			"acme/widgets": {ID: "repo-1", Canonical: "Acme/Widgets", DefaultRef: "main"},
			"contrib/fork": {ID: "repo-2", Canonical: "Contrib/Fork", DefaultRef: "main"},
		},
		refs: map[string]forge.RefState{
			"Acme/Widgets:main": {Ref: "main", OID: "baseoid", Exists: true},
		},
		prs: []forge.PullRequest{{
			Number: 7, BaseRepo: "Acme/Widgets", BaseRef: "main", BaseSHA: "baseoid",
			HeadRepo: "contrib/fork", HeadRef: "patch-1", HeadSHA: "anchoroid",
		}},
	}
	control.Config.Providers = fakeProviderFactory(provider)
	run := secureRun("fixpr")
	run.Spec.Mode = courier.ModeFixPR
	_, policy, err := control.ResolveAdmission(context.Background(), run)
	if err != nil {
		t.Fatalf("ResolveAdmission: %v", err)
	}
	if policy.WorkRepo != "Contrib/Fork" || policy.WorkRef != "patch-1" {
		t.Fatalf("work identity = %q/%q", policy.WorkRepo, policy.WorkRef)
	}
	if policy.PRNumber != 7 || policy.HeadAnchorOID != "anchoroid" || policy.WorkOID != "anchoroid" {
		t.Fatalf("fix-pr anchors = %+v", policy)
	}
	if policy.WorkInitiallyAbsent {
		t.Fatal("a fix-pr head always exists")
	}
}

func TestResolveAdmissionFailsClosed(t *testing.T) {
	t.Run("namespace boundary", func(t *testing.T) {
		control, _ := secureControl(t)
		run := secureRun("elsewhere")
		run.Namespace = "default"
		_, _, err := control.ResolveAdmission(context.Background(), run)
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "RunNamespace" {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("closed pull request on the head", func(t *testing.T) {
		control, _ := secureControl(t)
		provider := secureForge()
		provider.prs = []forge.PullRequest{{
			Number: 3, BaseRepo: "Acme/Widgets", HeadRepo: "Acme/Widgets",
			HeadRef: "courier/Acme/Widgets/issue-7", State: "closed", Merged: true,
		}}
		control.Config.Providers = fakeProviderFactory(provider)
		_, _, err := control.ResolveAdmission(context.Background(), secureRun("closedpr"))
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "HeadAlreadyMerged" {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("ambiguous selection", func(t *testing.T) {
		control, _ := secureControl(t)
		data := []byte(`{"providers":[
			{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["acme/*"]},
			{"name":"b","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*"]}
		]}`)
		registry, err := forge.Load(data, "github")
		if err != nil {
			t.Fatal(err)
		}
		control.Config.Registry = registry
		_, _, err = control.ResolveAdmission(context.Background(), secureRun("ambiguous"))
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "ProviderSelection" {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestVerifyPersistedPolicyFencesUID(t *testing.T) {
	run := secureRun("bound")
	run.Status.PublicationPolicy = &courier.PublicationPolicy{RunUID: string(run.UID), ProviderConfigRef: "github"}
	if _, err := VerifyPersistedPolicy(run); err != nil {
		t.Fatalf("persisted policy verified: %v", err)
	}
	run.Status.PublicationPolicy.RunUID = "another-incarnation"
	if _, err := VerifyPersistedPolicy(run); err == nil {
		t.Fatal("a policy bound to another UID must be refused")
	}
}

func TestLaunchSecureProvisionsTopology(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("topology")
	run.Status.Phase = courier.PhaseClaimed
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:              string(run.UID),
		ProviderConfigRef:   "github",
		ProviderEndpoint:    "https://api.github.com/",
		CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
		BaseRepo:            "Acme/Widgets",
		BaseRef:             "main",
		BaseOID:             "baseoid",
		WorkRepo:            "Acme/Widgets",
		WorkRef:             "courier/Acme/Widgets/issue-7",
		WorkInitiallyAbsent: true,
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// The dedicated run namespace with enforced restricted Pod Security.
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: secureTestNamespace,
			Labels: map[string]string{
				"pod-security.kubernetes.io/enforce": "restricted",
			},
		},
	}
	if err := c.Create(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	// Credential sources for the per-run copy.
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}
	if err := c.Create(context.Background(), creds); err != nil {
		t.Fatal(err)
	}

	result, err := control.LaunchSecure(context.Background(), run)
	if err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Fatalf("provisioning returned a requeue: %#v", result)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(secureTestNamespace)); err != nil {
		t.Fatal(err)
	}
	components := map[string]*corev1.Pod{}
	for i := range pods.Items {
		components[pods.Items[i].Labels["courier.misospace.dev/component"]] = &pods.Items[i]
	}
	for _, want := range []string{topology.ComponentCoordinator, topology.ComponentWorker, topology.ComponentBroker} {
		if components[want] == nil {
			t.Fatalf("topology is missing the %s pod", want)
		}
	}
	// The worker must carry no identity material and must be bound to the
	// control incarnation.
	worker := components[topology.ComponentWorker]
	for _, v := range worker.Spec.Volumes {
		if v.Secret != nil || v.Projected != nil {
			t.Fatalf("worker pod mounts %q", v.Name)
		}
	}
	incarnation := ""
	for _, env := range worker.Spec.Containers[0].Env {
		if env.Name == topology.EnvControlPodUID {
			incarnation = env.Value
		}
	}
	if incarnation == "" {
		t.Fatal("worker pod carries no control incarnation binding")
	}
	controlPod := components[topology.ComponentCoordinator]
	controlIncarnation := ""
	workerUID := ""
	for _, env := range controlPod.Spec.Containers[0].Env {
		if env.Name == topology.EnvControlPodUID {
			controlIncarnation = env.Value
		}
		if env.Name == topology.EnvWorkerPodUID {
			workerUID = env.Value
		}
	}
	if controlIncarnation != incarnation {
		t.Fatalf("control incarnation %q != worker binding %q", controlIncarnation, incarnation)
	}
	if workerUID != string(components[topology.ComponentWorker].UID) {
		t.Fatal("control pod is not bound to the live worker pod UID")
	}
	for _, env := range controlPod.Spec.Containers[0].Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("control pod env %q references a Secret", env.Name)
		}
	}
	// The finalizer was added.
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if !containsString(updated.Finalizers, topology.FinalizerName) {
		t.Fatal("secure topology finalizer missing")
	}
	// A second launch is a no-op: identities must not rotate on idempotent relaunch.
	_, err = control.LaunchSecure(context.Background(), &updated)
	if err != nil {
		t.Fatalf("idempotent relaunch: %v", err)
	}
	// The control pod carries the run context the native harness reconstructs.
	foundGoal := false
	for _, env := range controlPod.Spec.Containers[0].Env {
		if env.Name == "COURIER_GOAL" && env.Value != "" {
			foundGoal = true
		}
	}
	if !foundGoal {
		t.Fatal("control pod carries no run-context goal")
	}
}

// The deployment-level model gateway is rendered into trusted control only:
// the URL is env, the key is a per-run Secret copy mounted as a file, and
// runs without gateway configuration carry no gateway material at all.
func TestLaunchSecureRendersModelGateway(t *testing.T) {
	newLaunch := func(t *testing.T, control *SecureControl, c client.Client, name string) *corev1.Pod {
		t.Helper()
		run := secureRun(name)
		run.Status.Phase = courier.PhaseClaimed
		run.Status.PublicationPolicy = &courier.PublicationPolicy{
			RunUID:              string(run.UID),
			ProviderConfigRef:   "github",
			ProviderEndpoint:    "https://api.github.com/",
			CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
			BaseRepo:            "Acme/Widgets",
			BaseRef:             "main",
			BaseOID:             "baseoid",
			WorkRepo:            "Acme/Widgets",
			WorkRef:             "courier/Acme/Widgets/issue-7",
			WorkInitiallyAbsent: true,
		}
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		if err := c.Create(context.Background(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: secureTestNamespace, Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := c.Create(context.Background(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
			Data:       map[string][]byte{"token": []byte("tok-123")},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := control.LaunchSecure(context.Background(), run); err != nil {
			t.Fatalf("LaunchSecure: %v", err)
		}
		pods := &corev1.PodList{}
		if err := c.List(context.Background(), pods, client.InNamespace(secureTestNamespace)); err != nil {
			t.Fatal(err)
		}
		for i := range pods.Items {
			if pods.Items[i].Labels["courier.misospace.dev/component"] == topology.ComponentCoordinator {
				return &pods.Items[i]
			}
		}
		t.Fatal("no control pod provisioned")
		return nil
	}

	t.Run("gateway configured", func(t *testing.T) {
		control, c := secureControl(t)
		control.Config.GatewayURL = "http://gateway:4000/v1"
		control.Config.GatewayKeySecret = "courier-system/gateway-key"
		if err := c.Create(context.Background(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "gateway-key"},
			Data:       map[string][]byte{"key": []byte("gw-secret-value")},
		}); err != nil {
			t.Fatal(err)
		}
		controlPod := newLaunch(t, control, c, "gateway")
		url, keyFile := "", ""
		for _, env := range controlPod.Spec.Containers[0].Env {
			switch env.Name {
			case topology.EnvGatewayURL:
				url = env.Value
			case topology.EnvGatewayKeyFile:
				keyFile = env.Value
			}
		}
		if url != "http://gateway:4000/v1" || keyFile == "" {
			t.Fatalf("gateway env = url %q key file %q", url, keyFile)
		}
		gatewaySecret := &corev1.Secret{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: secureTestNamespace, Name: topology.GatewaySecretName("gateway")}, gatewaySecret); err != nil {
			t.Fatalf("per-run gateway key copy missing: %v", err)
		}
		if string(gatewaySecret.Data[topology.GatewayKeyName]) != "gw-secret-value" {
			t.Fatal("gateway key copy does not carry the deployment key value")
		}
	})

	t.Run("gateway unconfigured", func(t *testing.T) {
		control, c := secureControl(t)
		controlPod := newLaunch(t, control, c, "nogateway")
		for _, env := range controlPod.Spec.Containers[0].Env {
			if env.Name == topology.EnvGatewayURL {
				t.Fatal("gateway URL must be absent without gateway configuration")
			}
		}
		secrets := &corev1.SecretList{}
		if err := c.List(context.Background(), secrets, client.InNamespace(secureTestNamespace)); err != nil {
			t.Fatal(err)
		}
		for i := range secrets.Items {
			if secrets.Items[i].Name == topology.GatewaySecretName("nogateway") {
				t.Fatal("no gateway key copy may be provisioned without gateway configuration")
			}
		}
	})
}

func TestObserveTopologyFencesBrokenWorker(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("fence")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	workerPod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(context.Background(), workerPod); err != nil {
		t.Fatal(err)
	}

	pods := listRunPods(t, c, run)
	_, handled, err := control.ObserveTopology(context.Background(), run, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("a failed worker must be handled as infrastructure failure")
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 1 {
		t.Fatalf("fence relaunch state = phase %q restarts %d", updated.Status.Phase, updated.Status.Restarts)
	}
}

func TestObserveTopologyFencesWorkerOnControlTermination(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("term")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	controlPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2}},
	}}
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	_, handled, err := control.ObserveTopology(context.Background(), run, listRunPods(t, c, run))
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var worker corev1.Pod
	err = c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("worker pod must be fenced on control termination, got %v", err)
	}
}

func TestRevokeIsOrderedAndIdempotent(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("revoke")
	now := metav1.Now()
	run.DeletionTimestamp = &now
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	for _, pod := range []*corev1.Pod{healthyPod(run.Name, topology.ComponentCoordinator), healthyPod(run.Name, topology.ComponentWorker), healthyPod(run.Name, topology.ComponentBroker)} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	// The broker Service exists with the pod: revocation must delete it
	// before the pod (ingress first).
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	done, err := control.Revoke(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("revocation cannot be done while pods exist")
	}
	// Simulate kubelet completing pod termination: fake client removes them.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		if err := c.Delete(context.Background(), &pods.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		var err error
		done, err = control.Revoke(context.Background(), run)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("revocation did not complete within five passes")
	}
	// Everything must be gone, and a rerun stays done.
	remaining := &corev1.PodList{}
	if err := c.List(context.Background(), remaining, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(remaining.Items) != 0 {
		t.Fatalf("topology pods survived revocation: %d", len(remaining.Items))
	}
	services := &corev1.ServiceList{}
	if err := c.List(context.Background(), services, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(services.Items) != 0 {
		t.Fatalf("services survived revocation: %d", len(services.Items))
	}
	if done, err := control.Revoke(context.Background(), run); err != nil || !done {
		t.Fatalf("rerun of a completed revocation = done=%v err=%v", done, err)
	}
}

func healthyPod(runName, component string) *corev1.Pod {
	name := topology.ControlPodName(runName)
	switch component {
	case topology.ComponentWorker:
		name = topology.WorkerPodName(runName)
	case topology.ComponentBroker:
		name = topology.BrokerPodName(runName)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: secureTestNamespace,
			UID:       types.UID(name + "-uid"),
			Labels:    topology.Labels(runName, component),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

func listRunPods(t *testing.T, c client.Client, run *courier.CoderRun) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	out := make([]corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if pods.Items[i].Labels[topology.LabelRun] == run.Name {
			out = append(out, pods.Items[i])
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

var _ = ctrl.Result{}
var _ = strings.TrimSpace

func TestProbePodsAreExcludedFromWorkloadLookups(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("probe-exclude")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// A leftover probe pod from a crashed preflight pass: worker labels, but
	// marked as a probe.
	probe := topology.ProbePod(run, "node-a", "busybox:test", "true")
	if probe.Labels[topology.LabelProbe] != "true" {
		t.Fatal("probe pods must carry the probe label")
	}
	if err := c.Create(context.Background(), probe); err != nil {
		t.Fatal(err)
	}
	if pod, err := control.pod(context.Background(), run, topology.ComponentWorker); err != nil || pod != nil {
		t.Fatalf("a probe pod must not satisfy the worker lookup, got %v, %v", pod, err)
	}
	if control.topologyPodsExist(context.Background(), run) {
		t.Fatal("probe pods must not make the topology look provisioned")
	}
	// The real worker is still found.
	if err := c.Create(context.Background(), healthyPod(run.Name, topology.ComponentWorker)); err != nil {
		t.Fatal(err)
	}
	pod, err := control.pod(context.Background(), run, topology.ComponentWorker)
	if err != nil || pod == nil {
		t.Fatalf("real worker lookup failed: %v, %v", pod, err)
	}
}

func TestProbeCoverageIsIncompleteWithoutEveryNode(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("probe-coverage")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// Two eligible nodes; only node-a produced a complete result set (two
	// checks), node-b's probe crashed without a readable result.
	for _, node := range []string{"node-a", "node-b"} {
		if err := c.Create(context.Background(), topology.ProbePod(run, node, "busybox:test", "true")); err != nil {
			t.Fatal(err)
		}
	}
	a := topology.ProbePod(run, "node-a", "busybox:test", "true")
	a.Status.Phase = corev1.PodSucceeded
	a.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "probe",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "{\"name\":\"api-server\",\"got\":\"deny\"}\n{\"name\":\"cluster-dns\",\"got\":\"deny\"}\n"}},
	}}
	if err := c.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	b := topology.ProbePod(run, "node-b", "busybox:test", "true")
	b.Status.Phase = corev1.PodFailed
	b.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "probe",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: ""}},
	}}
	if err := c.Status().Update(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	_, pending, err := control.collectProbeResults(context.Background(), run, []string{"node-a", "node-b"}, 2)
	if err == nil {
		t.Fatal("incomplete probe coverage must not be treated as evidence")
	}
	if pending {
		t.Fatal("both probes are terminal, so nothing is pending")
	}
}

// TestPublicationPolicyPersistsEmptyToSetOnly drives resolvePersistSecurePolicy
// through the real production patch path — patchStatus into a
// status.KubePatchWriter issuing a JSON merge patch against the API client,
// exactly as cmd/main.go wires it — and proves the set-once property: a
// persisted policy is never rewritten or cleared by a later patch.
func TestPublicationPolicyPersistsEmptyToSetOnly(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, networkingv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courier.CoderRun{}).Build()
	control, _ := secureControl(t)
	reconciler := &CoderRunReconciler{
		Client:       c,
		Secure:       control,
		StatusWriter: status.KubePatchWriter{Client: c},
	}
	run := secureRun("policy-persist")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	// Empty→set: the resolved policy persists through the production writer.
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
		t.Fatalf("resolvePersistSecurePolicy: %v", err)
	}
	var persisted courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy == nil {
		t.Fatal("publication policy was never persisted")
	}
	if persisted.Status.PublicationPolicy.ProviderConfigRef != "github" || persisted.Status.PublicationPolicy.RunUID != string(run.UID) {
		t.Fatalf("persisted policy = %+v", persisted.Status.PublicationPolicy)
	}

	// Set→mutated: a later patch carrying a different policy must not
	// rewrite the persisted one.
	before := persisted.DeepCopy()
	mutated := persisted.DeepCopy()
	mutated.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:            string(run.UID),
		ProviderConfigRef: "evil-provider",
		ProviderEndpoint:  "https://evil.example.test/",
		BaseRepo:          "Acme/Widgets",
	}
	if err := reconciler.patchStatus(context.Background(), before, mutated); err != nil {
		t.Fatalf("patchStatus with a mutated policy: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy.ProviderConfigRef != "github" {
		t.Fatalf("persisted policy was rewritten to %q", persisted.Status.PublicationPolicy.ProviderConfigRef)
	}

	// Set→cleared: a patch that drops the in-memory policy must not clear it.
	before = persisted.DeepCopy()
	cleared := persisted.DeepCopy()
	cleared.Status.PublicationPolicy = nil
	if err := reconciler.patchStatus(context.Background(), before, cleared); err != nil {
		t.Fatalf("patchStatus with a cleared policy: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy == nil {
		t.Fatal("persisted policy was cleared")
	}

	// Re-resolving a persisted run verifies it instead of rewriting it.
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), &persisted); err != nil {
		t.Fatalf("re-resolve of a persisted policy: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy.ProviderConfigRef != "github" {
		t.Fatalf("re-resolve rewrote the persisted policy to %q", persisted.Status.PublicationPolicy.ProviderConfigRef)
	}
}

// TestRevokeDeletesBrokerServiceWhenBrokerPodIsGone covers the pod-absent
// regression: a partial pass that removed every pod but left the broker
// Service must not report done while broker ingress is still routed.
func TestRevokeDeletesBrokerServiceWhenBrokerPodIsGone(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("orphan-service")
	now := metav1.Now()
	run.DeletionTimestamp = &now
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// Every pod is already gone; only the broker Service survived.
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	done, err := control.Revoke(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("revocation must not report done on the pass that deletes the broker Service")
	}
	var service corev1.Service
	err = c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("broker Service must be deleted, got %v", err)
	}

	// The next pass confirms it gone and completes.
	done, err = control.Revoke(context.Background(), run)
	if err != nil || !done {
		t.Fatalf("second pass = done=%v err=%v", done, err)
	}
}

// TestReconcileFencesWorkerWhenControlTerminates is the reconcile-level
// regression for fence placement: the legacy coordinator-termination handler
// must not bypass the secure topology's teardown, or the untrusted worker and
// the credentialed broker outlive their supervisor — terminal runs are never
// reaped, so a later-reconcile fence would never run at all.
func TestReconcileFencesWorkerWhenControlTerminates(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("fence-reconcile")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	controlPod.Spec.Containers[0].Name = topology.ControlContainerName
	controlPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 2,
			Message:  `COURIER_TERMINATION {"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"capability probe failed"}`,
		}},
	}}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var worker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced in the same reconcile that terminalizes the run, got %v", err)
	}
	var broker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &broker); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be torn down with its control incarnation, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled with its control incarnation, got %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the control exit code", updated.Status.Phase)
	}
}
