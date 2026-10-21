package dispatch

import (
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// dispatchEnvtestClient is the API server the package's envtest fixtures poll
// through. It is nil when KUBEBUILDER_ASSETS is unset; the fixtures then fall
// back to the controller-runtime fake client so every assertion still runs in
// a plain `go test` instead of being skipped.
var dispatchEnvtestClient client.Client

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
	dispatchEnvtestClient, err = client.New(config, client.Options{Scheme: dispatchTestScheme()})
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

// dispatchTestScheme registers the API types the package's fixtures serve:
// the Courier CRDs and the core namespace type the envtest fixtures create.
func dispatchTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = courierv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}
