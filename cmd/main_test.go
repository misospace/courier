package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/source/dispatch"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type observerWithHeadResolver struct{}

func (observerWithHeadResolver) Observe(context.Context, string, controller.HeadRef) (controller.PRObservation, error) {
	return controller.PRObservation{}, nil
}

func (observerWithHeadResolver) ResolveHead(context.Context, *courierv1alpha1.CoderRun) (controller.HeadRef, error) {
	return controller.HeadRef{Repo: "acme/demo", Branch: "feature/existing"}, nil
}

func TestDispatchPRStateCheckerRequiresGitHubObserver(t *testing.T) {
	if _, err := dispatchPRStateChecker(nil); !errors.Is(err, dispatch.ErrPRStateCheckerNeeded) {
		t.Fatalf("dispatchPRStateChecker() error = %v, want checker-required", err)
	}
}

func TestExistingPRHeadResolver(t *testing.T) {
	observer := observerWithHeadResolver{}
	if got := existingPRHeadResolver(observer); got == nil {
		t.Fatal("existingPRHeadResolver() returned nil for a compatible observer")
	}
	if got := existingPRHeadResolver(nil); got != nil {
		t.Fatal("existingPRHeadResolver(nil) returned a resolver")
	}
}

func TestResolveDispatchBindings(t *testing.T) {
	tests := []struct {
		name      string
		queueLane string
		lane      string
		repeated  []string
		want      []dispatchBinding
		wantErr   bool
	}{
		{
			name:      "shorthand only",
			queueLane: "queue-a",
			lane:      "lane-a",
			want:      []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a"}},
		},
		{
			name:     "repeated only",
			repeated: []string{"queue-a:lane-a", "queue-b:lane-b"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a"}, {queueLane: "queue-b", laneProfile: "lane-b"}},
		},
		{
			name:      "shorthand and repeated mix rejected",
			queueLane: "queue-a",
			lane:      "lane-a",
			repeated:  []string{"queue-b:lane-b"},
			wantErr:   true,
		},
		{
			name:      "queue lane only rejected",
			queueLane: "queue-a",
			wantErr:   true,
		},
		{
			name:    "lane profile only rejected",
			lane:    "lane-a",
			wantErr: true,
		},
		{
			name:     "repeated without colon rejected",
			repeated: []string{"queue-a"},
			wantErr:  true,
		},
		{
			name:     "repeated empty half rejected",
			repeated: []string{"queue-a:"},
			wantErr:  true,
		},
		{
			name:     "duplicate queue lane rejected",
			repeated: []string{"queue-a:lane-a", "queue-a:lane-b"},
			wantErr:  true,
		},
		{
			name:     "duplicate lane profile accepted",
			repeated: []string{"queue-a:lane-x", "queue-b:lane-x"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-x"}, {queueLane: "queue-b", laneProfile: "lane-x"}},
		},
		{
			name:     "whitespace trimmed",
			repeated: []string{" queue-a : lane-a "},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a"}},
		},
		{
			name:     "repeated whitespace-only rejected",
			repeated: []string{" "},
			wantErr:  true,
		},
		{
			name:     "extra colon in lane profile rejected",
			repeated: []string{"queue-a:lane:a"},
			wantErr:  true,
		},
		{
			name:      "whitespace-only shorthand rejected",
			queueLane: " ",
			wantErr:   true,
		},
		{
			name: "all empty",
			want: []dispatchBinding{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveDispatchBindings(test.queueLane, test.lane, test.repeated)
			if test.wantErr {
				if err == nil {
					t.Fatalf("resolveDispatchBindings() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDispatchBindings() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("resolveDispatchBindings() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestBuildSecureControlRejectsTaintedLegacyCredential(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"courier-github","key":"token"}},"serves":["*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := buildSecureControl(secureConfig{
		providersFile:   registryFile,
		runNamespace:    "runs",
		harnessImage:    "harness:test",
		probeImage:      "busybox:test",
		legacyGitSecret: "courier-github",
		observer:        observerWithHeadResolver{},
		operatorNS:      "courier-system",
	})
	if err == nil || !strings.Contains(err.Error(), "tainted") {
		t.Fatalf("a registration referencing a legacy-exposed Secret must fail startup, got %v", err)
	}
}

func TestBuildSecureControlRequiresObserverCredential(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildSecureControl(secureConfig{
		providersFile: registryFile,
		runNamespace:  "runs",
	}); err == nil || !strings.Contains(err.Error(), "observer") {
		t.Fatalf("secure mode without an admission-read identity must fail startup, got %v", err)
	}
}

func TestValidateEvidenceFlags(t *testing.T) {
	tests := []struct {
		name      string
		keySecret string
		service   string
		bind      string
		wantErr   string // substring required in the error; empty means no error
	}{
		{
			name: "all empty disables wiring",
		},
		{
			name:      "key secret and service pair",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
		},
		{
			name:      "key secret, service, and bind",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
			bind:      "0.0.0.0:8443",
		},
		{
			name:      "key secret without service",
			keySecret: "courier-evidence-key",
			wantErr:   "both --evidence-intake-key-secret and --evidence-intake-service",
		},
		{
			name:    "service without key secret",
			service: "http://evidence-intake:8443",
			wantErr: "both --evidence-intake-key-secret and --evidence-intake-service",
		},
		{
			name:    "bind without key secret",
			bind:    "0.0.0.0:8443",
			wantErr: "--evidence-intake-bind requires --evidence-intake-key-secret",
		},
		{
			name:      "whitespace-only bind ignored",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
			bind:      "   ",
		},
		{
			// Settled decision: key+service without bind stays legal so
			// deployments that predate --evidence-intake-bind keep starting.
			// main() logs that capture POSTs will be refused.
			name:      "key secret and service without bind stays valid",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateEvidenceFlags(test.keySecret, test.service, test.bind)
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("validateEvidenceFlags() error = nil, want error mentioning %q", test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("validateEvidenceFlags() error = %q, want it to mention %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateEvidenceFlags() error = %v, want none", err)
			}
		})
	}
}

func TestLoadEvidenceKey(t *testing.T) {
	const keyValue = "a-test-hmac-key-of-some-length"

	tests := []struct {
		name       string
		namespace  string
		secretName string
		data       map[string][]byte // nil: no Secret in the namespace
		want       []byte
		wantErr    string // substring required in the error
	}{
		{
			name:       "happy path",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"key": []byte(keyValue)},
			want:       []byte(keyValue),
		},
		{
			name:       "secret missing",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			wantErr:    "not found",
		},
		{
			name:       "no key entry",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"other": []byte("not-the-key")},
			wantErr:    `no non-empty "key" entry`,
		},
		{
			name:       "whitespace-only key",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"key": []byte("   ")},
			wantErr:    `no non-empty "key" entry`,
		},
		{
			name:       "empty namespace",
			namespace:  "",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"key": []byte(keyValue)},
			wantErr:    "POD_NAMESPACE",
		},
		{
			name:       "empty name",
			namespace:  "courier-system",
			secretName: "",
			data:       map[string][]byte{"key": []byte(keyValue)},
			wantErr:    "name is required",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("add core scheme: %v", err)
			}
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if test.data != nil {
				builder = builder.WithRuntimeObjects(&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "courier-evidence-key",
						Namespace: "courier-system",
					},
					Data: test.data,
				})
			}
			fakeClient := builder.Build()

			got, err := loadEvidenceKey(context.Background(), fakeClient, test.namespace, test.secretName)
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("loadEvidenceKey() error = nil, want error mentioning %q", test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("loadEvidenceKey() error = %q, want it to mention %q", err, test.wantErr)
				}
				if strings.Contains(err.Error(), keyValue) {
					t.Fatalf("loadEvidenceKey() error = %q must not contain the key value", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadEvidenceKey() error = %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("loadEvidenceKey() = %q, want %q", got, test.want)
			}
		})
	}
}
