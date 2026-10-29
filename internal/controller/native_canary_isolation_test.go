package controller

import (
	"context"
	"testing"
	"time"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A second native controller must not settle another deployment's manual run.
func TestNativeCanaryDoesNotMutateForeignNamespaceRun(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	run := secureRun("foreign-production-run")
	run.Namespace = "production-v1"
	run.Status.Phase = courier.PhasePending
	lane := &courier.LaneProfile{ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: run.Namespace}, Spec: courier.LaneProfileSpec{Concurrency: 1}}
	if err := c.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Launch:       func(context.Context, *courier.CoderRun) error { return nil },
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	control.Patcher = reconciler
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhasePending || len(item.transitions) != 0 {
		t.Fatalf("foreign run mutated: phase=%s source transitions=%v; want Pending and no source transitions", updated.Status.Phase, item.transitions)
	}
}

// A legacy controller scoped to a different deployment's namespaces must be a
// strict no-op for a foreign run: no claim, no status write.
func TestLegacyControllerDoesNotMutateForeignNamespaceRun(t *testing.T) {
	ctx := context.Background()
	_, c := secureControl(t)
	run := secureRun("legacy-foreign-run")
	run.Namespace = "production-v1"
	run.Status.Phase = courier.PhasePending
	lane := &courier.LaneProfile{ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: run.Namespace}, Spec: courier.LaneProfileSpec{Concurrency: 1}}
	if err := c.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:          c,
		Launch:          func(context.Context, *courier.CoderRun) error { return nil },
		Sources:         NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter:    fakeStatusWriter{client: c},
		OwnedNamespaces: []string{"native-runs"},
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhasePending || len(item.claimed) != 0 || len(item.transitions) != 0 {
		t.Fatalf("foreign run mutated: phase=%s claimed=%v source transitions=%v; want Pending with no source activity", updated.Status.Phase, item.claimed, item.transitions)
	}
}

// A secure-mode controller owns exactly its configured run namespace, so a
// claimed foreign run is neither resumed nor launched: no transition, no
// launch, phase unchanged.
func TestForeignClaimedRunIsNotResumedOrLaunched(t *testing.T) {
	ctx := context.Background()
	run := secureRun("foreign-claimed-run")
	run.Namespace = "production-v1"
	run.Status.Phase = courier.PhaseClaimed
	run.Status.AdmittedAt = &metav1.Time{Time: reapClock}
	control, c := secureControl(t, run)
	item := &admissionSource{}
	launches := 0
	reconciler := &CoderRunReconciler{
		Client:       c,
		Launch:       func(context.Context, *courier.CoderRun) error { launches++; return nil },
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	control.Patcher = reconciler
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if launches != 0 || len(item.transitions) != 0 || len(item.claimed) != 0 || updated.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("foreign claimed run resumed: launches=%d claimed=%v transitions=%v phase=%s; want none and Claimed", launches, item.claimed, item.transitions, updated.Status.Phase)
	}
}

// A deleting secure run outside the controller's namespace keeps its
// finalizer: revocation and finalizer release are reserved for the
// deployment that owns the run.
func TestForeignDeletingSecureRunKeepsFinalizer(t *testing.T) {
	ctx := context.Background()
	run := secureRun("foreign-deleting-run")
	run.Namespace = "production-v1"
	run.Status.Phase = courier.PhaseRunning
	run.Finalizers = []string{secureTopologyFinalizer}
	control, c := secureControl(t, run)
	if err := c.Delete(ctx, run); err != nil {
		t.Fatal(err)
	}
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	control.Patcher = reconciler
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var terminating courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &terminating); err != nil {
		t.Fatalf("foreign deleting run vanished: %v", err)
	}
	if !containsString(terminating.Finalizers, secureTopologyFinalizer) {
		t.Fatalf("foreign run finalizers = %v; want %s kept", terminating.Finalizers, secureTopologyFinalizer)
	}
	if len(item.transitions) != 0 || len(item.reports) != 0 {
		t.Fatalf("foreign run drove source activity: transitions=%v reports=%v", item.transitions, item.reports)
	}
}

// The ownership boundary must not block the owning controller: a run inside
// the declared namespace is still admitted.
func TestOwnedRunIsStillAdmittedWhenScoped(t *testing.T) {
	ctx := context.Background()
	_, c := secureControl(t)
	run := secureRun("owned-scoped-run")
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:          c,
		Sources:         NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter:    fakeStatusWriter{client: c},
		OwnedNamespaces: []string{secureTestNamespace},
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if len(item.claimed) == 0 || updated.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("scoped controller withheld its own admission: claimed=%v phase=%s; want a claim and Claimed", item.claimed, updated.Status.Phase)
	}
}

// A foreign run whose source identity the registry cannot resolve is still
// ignored: the gate decides before source resolution, so an unknown identity
// never surfaces as a reconcile error.
func TestForeignRunWithUnknownSourceIdentityIsStillIgnored(t *testing.T) {
	ctx := context.Background()
	run := secureRun("foreign-unknown-source-run")
	run.Namespace = "production-v1"
	run.Spec.Source = "dispatch"
	run.Spec.SourceAgent = "native"
	run.Status.Phase = courier.PhasePending
	control, c := secureControl(t, run)
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	control.Patcher = reconciler
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatalf("Reconcile() error = %v; a foreign run is ignored regardless of source identity", err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhasePending || len(item.claimed) != 0 || len(item.transitions) != 0 {
		t.Fatalf("foreign run mutated: phase=%s claimed=%v transitions=%v; want Pending with no source activity", updated.Status.Phase, item.claimed, item.transitions)
	}
}

// A terminal run past its retention window is reaped only when it is in the
// controller's own namespace; the foreign counterpart of the same fixture
// survives the identical window.
func TestForeignTerminalRunIsNotReaped(t *testing.T) {
	ctx := context.Background()
	terminal := func(name, namespace string) *courier.CoderRun {
		run := terminalReapRun(name, courier.PhaseNeedsHuman, reapClock.Add(-2*time.Hour))
		run.Namespace = namespace
		return withReapMarker(run, needsHumanAtAnnotation, reapClock.Add(-2*time.Hour))
	}
	foreign := terminal("foreign-terminal-run", "production-v1")
	owned := terminal("owned-terminal-run", secureTestNamespace)
	control, c := secureControl(t, foreign, owned)
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:              c,
		Sources:             NewSourceRegistry(map[string]source.Adapter{"test": item, "manual": item}),
		StatusWriter:        fakeStatusWriter{client: c},
		Secure:              control,
		NeedsHumanRetention: time.Minute,
		TerminalAuditKeep:   -1,
		Now:                 func() time.Time { return reapClock },
	}
	control.Patcher = reconciler
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(foreign)}); err != nil {
		t.Fatal(err)
	}
	var surviving courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), &surviving); err != nil {
		t.Fatalf("foreign terminal run reaped past its window: %v", err)
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(owned)}); err != nil {
		t.Fatal(err)
	}
	err := c.Get(ctx, client.ObjectKeyFromObject(owned), &courier.CoderRun{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("owned terminal run Get error = %v; want NotFound proving the identical window reaps only owned runs", err)
	}
	if len(item.claimed) != 0 || len(item.transitions) != 0 || len(item.reports) != 0 {
		t.Fatalf("source activity on a terminal reap: claimed=%v transitions=%v reports=%v", item.claimed, item.transitions, item.reports)
	}
}
