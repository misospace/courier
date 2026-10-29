package adhoc

import (
	"context"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// adhocEnvtestClient is the API server the package's envtest fixture
// talks to. It is nil when KUBEBUILDER_ASSETS is unset; the fixture then
// skips. Unlike the fake-client tests there is no fallback here, because
// the point of the fixture is to prove the created object passes the
// CRD's server-side admission validation.
var adhocEnvtestClient client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	server := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../../charts/courier/crd-manifests"},
		ErrorIfCRDPathMissing: true,
	}
	config, err := server.Start()
	if err != nil {
		panic(err)
	}
	adhocEnvtestClient, err = client.New(config, client.Options{Scheme: adhocTestScheme()})
	if err != nil {
		_ = server.Stop()
		panic(err)
	}
	exitCode := m.Run()
	if err := server.Stop(); err != nil && exitCode == 0 {
		exitCode = 1
	}
	os.Exit(exitCode)
}

// adhocTestScheme registers the API types the fixture serves: the
// Courier CRDs and the core namespace type it creates.
func adhocTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = courierv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

// TestAdmitAgainstEnvtest runs a full Admit plus a duplicate Admit
// against the real API server. It is the proof that the created
// contribute run satisfies the CRD's contribute-mode CEL rule (spec
// requires workRepo and forbids ref), which the fake-client tests cannot
// check. It skips when the envtest assets are unavailable.
func TestAdmitAgainstEnvtest(t *testing.T) {
	if adhocEnvtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set; envtest API server unavailable")
	}
	ctx := context.Background()
	in := validAdmission()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: in.Namespace}}
	if err := adhocEnvtestClient.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", in.Namespace, err)
	}

	a := newAdapter(adhocEnvtestClient)
	first, err := a.Admit(ctx, in)
	if err != nil {
		t.Fatalf("first Admit against envtest: %v", err)
	}
	if first.Resumed {
		t.Fatalf("first Admit resumed; want a new run")
	}
	second, err := a.Admit(ctx, in)
	if err != nil {
		t.Fatalf("duplicate Admit against envtest: %v", err)
	}
	if !second.Resumed {
		t.Fatalf("duplicate Admit did not resume; want Resumed=true")
	}
	if second.RunName != first.RunName || second.WorkItemID != first.WorkItemID {
		t.Fatalf("duplicate identity (RunName %q, WorkItemID %q) differs from first (RunName %q, WorkItemID %q)",
			second.RunName, second.WorkItemID, first.RunName, first.WorkItemID)
	}

	var list courierv1alpha1.CoderRunList
	if err := adhocEnvtestClient.List(ctx, &list, client.InNamespace(in.Namespace)); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("runs after duplicate Admit = %d; want 1", len(list.Items))
	}

	run := &courierv1alpha1.CoderRun{}
	if err := adhocEnvtestClient.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: first.RunName}, run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Spec.Mode != courierv1alpha1.ModeContribute {
		t.Errorf("Spec.Mode = %q; want %q", run.Spec.Mode, courierv1alpha1.ModeContribute)
	}
	if run.Spec.WorkRepo == "" {
		t.Error("Spec.WorkRepo empty; want the approved fork")
	}
	if run.Spec.Ref != 0 {
		t.Errorf("Spec.Ref = %d; want 0 for a contribute run", run.Spec.Ref)
	}
}
