package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReconcileAllCreatesMissingProfileWithOwnerLabel(t *testing.T) {
	kubeClient := newBootstrapClient(t)
	provider := &staticProvider{profiles: []Profile{{
		Name:         "local",
		RuntimeImage: "ghcr.io/misospace/coordinator:1.0.0",
		Concurrency:  3,
		Roles:        map[string]string{"coordinator": "claude-sonnet-4-5", "coder": "gpt-5"},
		Framing:      "One A100 GPU, no load tool.\nPrefer a single parallel worker.\n",
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "local"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Labels[OwnerLabelKey] != OwnerLabelValue {
		t.Fatalf("labels = %#v, want the owner label", got.Labels)
	}
	if got.Spec.RuntimeImage != "ghcr.io/misospace/coordinator:1.0.0" {
		t.Fatalf("runtimeImage = %q, want the configured image", got.Spec.RuntimeImage)
	}
	if got.Spec.Concurrency != 3 {
		t.Fatalf("concurrency = %d, want 3", got.Spec.Concurrency)
	}
	if got.Spec.Roles["coordinator"] != "claude-sonnet-4-5" || got.Spec.Roles["coder"] != "gpt-5" {
		t.Fatalf("roles = %#v, want both roles", got.Spec.Roles)
	}
	if got.Spec.Framing != "One A100 GPU, no load tool.\nPrefer a single parallel worker.\n" {
		t.Fatalf("framing = %q, want the multi-line framing round-tripped", got.Spec.Framing)
	}
}

func TestReconcileAllRestoresDriftedManagedProfile(t *testing.T) {
	drifted := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "courier",
			Name:      "local",
			Labels:    map[string]string{OwnerLabelKey: OwnerLabelValue},
		},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: 1,
			Roles:       map[string]string{"coordinator": "old-model", "coder": "old-model"},
		},
	}
	kubeClient := newBootstrapClient(t, drifted)
	provider := &staticProvider{profiles: []Profile{{
		Name:         "local",
		RuntimeImage: "ghcr.io/misospace/coordinator:2.0.0",
		Concurrency:  3,
		Roles:        map[string]string{"coordinator": "claude-opus-4-5", "coder": "gpt-5"},
		Framing:      "Restore the framing when any spec field drifts.\n",
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "local"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Concurrency != 3 {
		t.Fatalf("concurrency = %d, want 3", got.Spec.Concurrency)
	}
	if got.Spec.RuntimeImage != "ghcr.io/misospace/coordinator:2.0.0" {
		t.Fatalf("runtimeImage = %q, want the restored image", got.Spec.RuntimeImage)
	}
	if got.Spec.Roles["coordinator"] != "claude-opus-4-5" || got.Spec.Roles["coder"] != "gpt-5" {
		t.Fatalf("roles = %#v, want the desired roles", got.Spec.Roles)
	}
	if got.Spec.Framing != "Restore the framing when any spec field drifts.\n" {
		t.Fatalf("framing = %q, want the framing round-tripped on restore", got.Spec.Framing)
	}
}

func TestReconcileAllIsIdempotent(t *testing.T) {
	// The fake client has no cache-staleness fidelity, so the get-then-create
	// race is not exercised here; AlreadyExists/Conflict absorption is covered
	// by the interceptor tests.
	kubeClient := newBootstrapClient(t)
	provider := &staticProvider{profiles: []Profile{{
		Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "gpt-5"},
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var first courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "local"}, &first); err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var second courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "local"}, &second); err != nil {
		t.Fatal(err)
	}
	if second.ResourceVersion != first.ResourceVersion {
		t.Fatalf("resourceVersion changed %s -> %s, want no write on steady state", first.ResourceVersion, second.ResourceVersion)
	}
}

func TestReconcileAllBlockedByUnmanagedProfile(t *testing.T) {
	existing := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "local"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: 7,
			Roles:       map[string]string{"coder": "gpt-5"},
		},
	}
	kubeClient := newBootstrapClient(t, existing)
	provider := &staticProvider{profiles: []Profile{{
		Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "claude-sonnet-4-5"},
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	err := r.ReconcileAll(context.Background())
	if !errors.Is(err, ErrProfileUnmanaged) {
		t.Fatalf("error = %v, want ErrProfileUnmanaged", err)
	}
	var got courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "local"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Concurrency != 7 || got.Spec.Roles["coder"] != "gpt-5" {
		t.Fatalf("unmanaged spec was modified: %#v", got.Spec)
	}
	if got.Labels != nil {
		t.Fatalf("unmanaged labels were modified: %#v", got.Labels)
	}
}

func TestReconcileAllLeavesUnlistedProfilesUntouched(t *testing.T) {
	userLane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "user-lane"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: 5,
			Roles:       map[string]string{"coder": "gpt-5"},
		},
	}
	kubeClient := newBootstrapClient(t, userLane)
	provider := &staticProvider{profiles: []Profile{
		{Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "claude-sonnet-4-5"}},
		{Name: "managed", Concurrency: 2, Roles: map[string]string{"coder": "claude-sonnet-4-5"}},
	}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The config shrinks: "managed" is no longer listed.
	provider.profiles = []Profile{
		{Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "claude-sonnet-4-5"}},
	}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var managed courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "managed"}, &managed); err != nil {
		t.Fatalf("previously managed profile was deleted: %v", err)
	}
	if managed.Spec.Concurrency != 2 {
		t.Fatalf("left-behind managed profile was modified: %d", managed.Spec.Concurrency)
	}

	var user courierv1alpha1.LaneProfile
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: "courier", Name: "user-lane"}, &user); err != nil {
		t.Fatalf("user profile was deleted: %v", err)
	}
	if user.Spec.Concurrency != 5 {
		t.Fatalf("user profile spec was modified: %d", user.Spec.Concurrency)
	}
	if _, ok := user.Labels[OwnerLabelKey]; ok {
		t.Fatal("user profile was labeled as bootstrap-managed")
	}
}

func TestReconcileAllNoConfigIsNoop(t *testing.T) {
	kubeClient := newBootstrapClient(t)
	provider := &staticProvider{}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("ReconcileAll() = %v, want nil for an empty config", err)
	}
	var lanes courierv1alpha1.LaneProfileList
	if err := kubeClient.List(context.Background(), &lanes, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(lanes.Items) != 0 {
		t.Fatalf("created %d LaneProfiles, want none", len(lanes.Items))
	}
}

func TestReconcileAllWrapsProviderError(t *testing.T) {
	want := errors.New("config unavailable")
	kubeClient := newBootstrapClient(t)
	provider := &staticProvider{err: want}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	err := r.ReconcileAll(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped %v", err, want)
	}
}

func TestReconcilerStartValidation(t *testing.T) {
	kubeClient := newBootstrapClient(t)
	provider := &staticProvider{profiles: []Profile{{Name: "local", Roles: map[string]string{"coder": "gpt-5"}}}}
	// Validation errors return before the ticker loop, so an uncanceled
	// context is safe here.
	if err := (&Reconciler{Client: kubeClient, Provider: provider}).Start(context.Background()); err == nil {
		t.Fatal("missing namespace was accepted")
	}
	if err := (&Reconciler{Client: kubeClient, Namespace: "courier"}).Start(context.Background()); err == nil {
		t.Fatal("missing provider was accepted")
	}
}

func TestReconcilerStartRunsPassesAndReturnsOnContextDone(t *testing.T) {
	kubeClient := newBootstrapClient(t)
	provider := &staticProvider{profiles: []Profile{{Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "gpt-5"}}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider, Interval: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	// Wait for the immediate pass to create the profile.
	deadline := time.After(2 * time.Second)
	for {
		var got courierv1alpha1.LaneProfile
		if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: "courier", Name: "local"}, &got); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("immediate pass did not create the profile")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start() = %v, want nil on context done", err)
	}
}

func TestEnsureAbsorbsCreateAlreadyExists(t *testing.T) {
	laneResource := schema.GroupResource{
		Group:    "courier.misospace.dev",
		Resource: "laneprofiles",
	}
	// Only the write is intercepted; Get flows through to the fake client.
	kubeClient := fake.NewClientBuilder().
		WithScheme(bootstrapScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(laneResource, obj.GetName())
			},
		}).
		Build()
	provider := &staticProvider{profiles: []Profile{{
		Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "gpt-5"},
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("ReconcileAll() = %v, want nil (AlreadyExists absorbed)", err)
	}
}

func TestEnsureAbsorbsUpdateConflict(t *testing.T) {
	existing := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "courier",
			Name:      "local",
			Labels:    map[string]string{OwnerLabelKey: OwnerLabelValue},
		},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: 1,
			Roles:       map[string]string{"coder": "old-model"},
		},
	}
	laneResource := schema.GroupResource{
		Group:    "courier.misospace.dev",
		Resource: "laneprofiles",
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(bootstrapScheme()).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				return apierrors.NewConflict(laneResource, obj.GetName(), nil)
			},
		}).
		Build()
	provider := &staticProvider{profiles: []Profile{{
		Name: "local", Concurrency: 2, Roles: map[string]string{"coder": "gpt-5"},
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("ReconcileAll() = %v, want nil (Conflict absorbed)", err)
	}
}

func TestReconcileAllCollisionDedupKeepsReturningError(t *testing.T) {
	existing := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier", Name: "local"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: 7,
			Roles:       map[string]string{"coder": "gpt-5"},
		},
	}
	kubeClient := newBootstrapClient(t, existing)
	provider := &staticProvider{profiles: []Profile{{
		Name: "local", Concurrency: 1, Roles: map[string]string{"coder": "claude-sonnet-4-5"},
	}}}
	r := &Reconciler{Client: kubeClient, Namespace: "courier", Provider: provider, Interval: 20 * time.Millisecond}
	for pass := 1; pass <= 2; pass++ {
		err := r.ReconcileAll(context.Background())
		if !errors.Is(err, ErrProfileUnmanaged) {
			t.Fatalf("pass %d: error = %v, want ErrProfileUnmanaged", pass, err)
		}
	}

	// The fix guards Start's per-pass logging (logPassErrors): a persistent
	// collision must not make Start exit. Run Start for a few ticks with the
	// collision in place and confirm it stays alive and returns nil on context
	// done. No log capture: liveness is the contract.
	startCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(startCtx) }()
	select {
	case err := <-done:
		t.Fatalf("Start() = %v, want it to keep running on a persistent collision", err)
	case <-time.After(500 * time.Millisecond):
		cancel()
	}
	if err := <-done; err != nil {
		t.Fatalf("Start() = %v, want nil on context done", err)
	}
}

type staticProvider struct {
	profiles []Profile
	err      error
}

func (p *staticProvider) Profiles(context.Context) ([]Profile, error) {
	return p.profiles, p.err
}

func newBootstrapClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(bootstrapScheme()).WithObjects(objs...).Build()
}

func bootstrapScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = courierv1alpha1.AddToScheme(scheme)
	return scheme
}
