package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

func observerLauncher(t *testing.T, c client.Client) *CoordinatorLauncher {
	t.Helper()
	return &CoordinatorLauncher{Client: c, Pod: executor.PodConfig{Image: "example/opencode:test"}, Executor: &executor.OpenCode{}}
}

func TestObserverLaunchBlocksOnForeignRunScopedIdentities(t *testing.T) {
	// A foreign same-named ServiceAccount, Role, or RoleBinding must stop
	// the launch before the coordinator pod exists: the API server admits
	// the pod under whatever ServiceAccount the name resolves to, and a
	// foreign Role/RoleBinding would silently grant or withhold the
	// observer's access. Ownership is proven before the pod can run (#327).
	observerName := legacyObserverResourceName("foreign")
	controller := true
	runObj := admissionRun("foreign", "local", courierv1alpha1.PhaseClaimed)
	runObj.UID = "uid-foreign-run"
	for _, tc := range []struct {
		name    string
		foreign []client.Object
		why     string
	}{
		{
			name: "service account",
			foreign: []client.Object{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
				Namespace: admissionNamespace, Name: observerName,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "other.example/v1", Kind: "Other", Name: "someone-else", UID: "uid-not-the-run", Controller: &controller}},
			}}},
			why: "ServiceAccount",
		},
		{
			name: "role",
			foreign: []client.Object{&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{
				Namespace: admissionNamespace, Name: observerName,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "other.example/v1", Kind: "Other", Name: "someone-else", UID: "uid-not-the-run", Controller: &controller}},
			}}},
			why: "Role",
		},
		{
			name: "role binding",
			foreign: []client.Object{&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{
				Namespace: admissionNamespace, Name: observerName,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "other.example/v1", Kind: "Other", Name: "someone-else", UID: "uid-not-the-run", Controller: &controller}},
			}}},
			why: "RoleBinding",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{admissionLaneWithCoordinator("local", 1), runObj}
			for _, o := range tc.foreign {
				objs = append(objs, o)
			}
			c := launcherClientForAdmission(t, objs...)
			launcher := observerLauncher(t, c)
			var run courierv1alpha1.CoderRun
			if err := c.Get(context.Background(), admissionKey("foreign"), &run); err != nil {
				t.Fatal(err)
			}
			err := launcher.Launch(context.Background(), &run)
			if err == nil || !strings.Contains(err.Error(), "not owned by run UID") {
				t.Fatalf("Launch with foreign %s = %v, want an ownership refusal", tc.why, err)
			}
			var pod corev1.Pod
			if getErr := c.Get(context.Background(), client.ObjectKey{Namespace: admissionNamespace, Name: "foreign-coordinator"}, &pod); !apierrors.IsNotFound(getErr) {
				t.Fatalf("coordinator pod exists after a foreign-%s refusal (get err %v); the pod must never be created", tc.why, getErr)
			}
		})
	}
}

func TestObserverProvisioningFailureLeavesNoClaimReleaseWithLivePod(t *testing.T) {
	// A Role create that fails transiently must not surface as a plain
	// launch error (which releases the source claim) while a coordinator
	// pod already sits live at the deterministic slot from an earlier
	// ambiguous attempt: with the claim released, another capacity could
	// be handed the same work while this writer keeps running.
	controller := true
	live := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "foreign-coordinator", Namespace: admissionNamespace,
		Labels: map[string]string{executor.LabelRun: executor.RunLabelValue("foreign")},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: courierv1alpha1.GroupVersion.String(), Kind: "CoderRun", Name: "foreign", UID: "uid-foreign-run", Controller: &controller,
		}},
	}}
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1))
	runObj := admissionRun("foreign", "local", courierv1alpha1.PhaseClaimed)
	runObj.UID = "uid-foreign-run"
	if err := c.Create(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), runObj); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected rbac outage")
	wrapped := fake.NewClientBuilder().
		WithScheme(schemeForAdmission(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(admissionLaneWithCoordinator("local", 1), live, runObj).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*rbacv1.Role); ok {
					return boom
				}
				if p, ok := obj.(*corev1.Pod); ok {
					t.Fatalf("coordinator pod create attempted while Role provisioning failed: %s", p.Name)
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).
		Build()
	launcher := observerLauncher(t, wrapped)
	var run courierv1alpha1.CoderRun
	if err := wrapped.Get(context.Background(), admissionKey("foreign"), &run); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(context.Background(), &run); !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch = %v, want ErrProvisioningInFlight while a live pod occupies the slot; the reconciler must stay Claimed, not release", err)
	}
}

func TestObserverProvisioningFailureOnEmptySlotStaysPlain(t *testing.T) {
	// With the slot proven empty, a provisioning failure keeps the plain
	// error contract: the reconciler releases the claim safely (no writer
	// can exist) and retries — partial provisioning from a prior attempt
	// remains owned and is re-verified, not duplicated or adopted.
	runObj := admissionRun("foreign", "local", courierv1alpha1.PhaseClaimed)
	runObj.UID = "uid-foreign-run"
	noAutomount := false
	boom := errors.New("injected rbac outage")
	wrapped := fake.NewClientBuilder().
		WithScheme(schemeForAdmission(t)).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(admissionLaneWithCoordinator("local", 1), runObj, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: admissionNamespace, Name: legacyObserverResourceName("foreign"), OwnerReferences: ownerRefsForTest("foreign", "uid-foreign-run")}, AutomountServiceAccountToken: &noAutomount}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*rbacv1.Role); ok {
					return boom
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).
		Build()
	launcher := observerLauncher(t, wrapped)
	var run courierv1alpha1.CoderRun
	if err := wrapped.Get(context.Background(), admissionKey("foreign"), &run); err != nil {
		t.Fatal(err)
	}
	err := launcher.Launch(context.Background(), &run)
	if err == nil || !errors.Is(err, boom) || errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch on an empty slot = %v, want the plain injected failure (claim release is safe with no writer)", err)
	}
	var saList corev1.ServiceAccountList
	if err := wrapped.List(context.Background(), &saList); err != nil {
		t.Fatal(err)
	}
	if len(saList.Items) != 1 {
		t.Fatalf("service accounts = %d, want the one correctly owned leftover reused, never duplicated", len(saList.Items))
	}
}

func TestObserverLaunchSucceedsWithOwnedResourcesAndIsIdempotent(t *testing.T) {
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), admissionRun("foreign", "local", courierv1alpha1.PhaseClaimed))
	launcher := observerLauncher(t, c)
	var run courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("foreign"), &run); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(context.Background(), &run); err != nil {
		t.Fatalf("first Launch: %v", err)
	}
	observerName := legacyObserverResourceName(run.Name)
	var sa corev1.ServiceAccount
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: admissionNamespace, Name: observerName}, &sa); err != nil {
		t.Fatalf("service account after launch: %v", err)
	}
	if !resourceOwnedByUID(&sa, run.UID) {
		t.Fatal("service account not owned by run UID")
	}
	var role rbacv1.Role
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: admissionNamespace, Name: observerName}, &role); err != nil {
		t.Fatalf("role after launch: %v", err)
	}
	if len(role.Rules) != 1 || len(role.Rules[0].ResourceNames) != 1 || role.Rules[0].ResourceNames[0] != run.Name+"-coordinator" {
		t.Fatalf("role grants more than get on this run's pod: %+v", role.Rules)
	}
	var binding rbacv1.RoleBinding
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: admissionNamespace, Name: observerName}, &binding); err != nil {
		t.Fatalf("role binding after launch: %v", err)
	}
	if binding.RoleRef.Name != observerName || len(binding.Subjects) != 1 || binding.Subjects[0].Name != observerName {
		t.Fatalf("role binding does not bind the run-scoped pair: %+v", binding)
	}
	var pod corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: admissionNamespace, Name: run.Name + "-coordinator"}, &pod); err != nil {
		t.Fatalf("coordinator pod after launch: %v", err)
	}
	if pod.Spec.ServiceAccountName != observerName {
		t.Fatalf("pod service account = %q, want %q", pod.Spec.ServiceAccountName, observerName)
	}
	// Retry with everything (including the pod) in place: fully owned
	// resources resolve idempotently, no second pod, no error.
	if err := launcher.Launch(context.Background(), &run); err != nil {
		t.Fatalf("idempotent Launch: %v", err)
	}
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want exactly the one coordinator", len(pods.Items))
	}
}

func TestObserverLaunchForeignTerminatingPodStaysInFlight(t *testing.T) {
	// The established collision contract is untouched by the reordering:
	// a terminating pod at the slot keeps ErrProvisioningInFlight even
	// when observer resources are fully owned and provisioned.
	now := metav1.Now()
	terminating := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "foreign-coordinator", Namespace: admissionNamespace,
		DeletionTimestamp: &now, Finalizers: []string{"keep.test/deletion"},
	}}
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), admissionRun("foreign", "local", courierv1alpha1.PhaseClaimed), terminating)
	launcher := observerLauncher(t, c)
	var run courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("foreign"), &run); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(context.Background(), &run); !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("Launch with a terminating slot pod = %v, want ErrProvisioningInFlight", err)
	}
}

func schemeForAdmission(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courierv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

func ownerRefsForTest(runName string, uid string) []metav1.OwnerReference {
	controller := true
	return []metav1.OwnerReference{{APIVersion: courierv1alpha1.GroupVersion.String(), Kind: "CoderRun", Name: runName, UID: types.UID(uid), Controller: &controller, BlockOwnerDeletion: &controller}}
}
