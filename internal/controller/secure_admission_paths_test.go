package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/topology"
)

// invalidPodRejection is the API server's answer for a pod spec it refuses on
// its face: the permanent, unfixable-by-requeue shape the preflight proof
// exists to surface.
func invalidPodRejection(name string) error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, name, field.ErrorList{
		field.Invalid(field.NewPath("spec"), "resources.claims is not honoured", "unsupported field"),
	})
}

// rejectPodCreates layers one more interceptor on a SecureControl's client,
// rejecting every create whose name reject matches.
func rejectPodCreates(t *testing.T, control *SecureControl, reject func(name string) error) {
	t.Helper()
	watcher, ok := control.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the fake client does not support watch")
	}
	control.Client = interceptor.NewClient(watcher, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := reject(obj.GetName()); err != nil {
				return err
			}
			return c.Create(ctx, obj, opts...)
		},
	})
}

// launcherDryRun rejects the specs the preflight proof creates for the pods
// the launcher really creates, leaving the known-bad specs the proof relies
// on alone.
func launcherDryRun(name string) error {
	if strings.HasSuffix(name, "-dryrun") && !strings.Contains(name, "-bad-dryrun") {
		return invalidPodRejection(name)
	}
	return nil
}

// TestPreflightDryRunProofCoversEveryCreatedPod pins the invariant that a
// rendered pod spec the API server refuses surfaces as NeedsHuman
// (PodAdmission) on every create path, not just the three workload pods. The
// probe-only case rejects a name nothing but the probe's dry-run carries, so
// dropping the probe from the proof fails here.
func TestPreflightDryRunProofCoversEveryCreatedPod(t *testing.T) {
	cases := map[string]struct {
		reject func(control *SecureControl, run *courier.CoderRun) func(string) error
		wantIn string
	}{
		"any launcher dry-run pod": {
			reject: func(*SecureControl, *courier.CoderRun) func(string) error {
				return launcherDryRun
			},
			wantIn: "worker",
		},
		"only the probe dry-run pod": {
			reject: func(control *SecureControl, run *courier.CoderRun) func(string) error {
				probe := topology.ProbePod(run, "dry-run-node", control.Config.ProbeImage, "true", courier.DefaultPodResources())
				dryRun := probe.Name + "-dryrun"
				return func(name string) error {
					if name == dryRun {
						return invalidPodRejection(name)
					}
					return nil
				}
			},
			wantIn: "probe",
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			control, c := secureControl(t)
			run := launchedSecureRun(t, c, "admission-proof", "local")
			rejectPodCreates(t, control, test.reject(control, run))

			_, err := control.LaunchSecure(context.Background(), run)
			var permanent *SecureNeedsHumanError
			if !errors.As(err, &permanent) {
				t.Fatalf("error = %v, want a *SecureNeedsHumanError the reconciler can terminalize", err)
			}
			if permanent.Reason != "PodAdmission" {
				t.Fatalf("reason = %q, want PodAdmission", permanent.Reason)
			}
			if !strings.Contains(permanent.Detail, test.wantIn) {
				t.Fatalf("detail = %q, want it to name the rejected %q pod", permanent.Detail, test.wantIn)
			}
		})
	}
}

// probeClusterFixture seeds what runLiveProbes reads before it creates
// anything: one eligible node and the three ClusterIPs the checks target.
func probeClusterFixture(t *testing.T, c client.Client, run *courier.CoderRun) {
	t.Helper()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		}}},
	}
	if err := c.Create(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	for _, service := range []*corev1.Service{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "kubernetes"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-dns"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.10"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.20"}},
	} {
		if err := c.Create(context.Background(), service); err != nil {
			t.Fatal(err)
		}
	}
}

// TestLiveProbeCreateClassifiesOnlyPermanentRejection is the backstop under
// the dry-run proof: the live probe create is the one create path the proof
// cannot make, so it classifies an Invalid itself. A Forbidden or quota denial
// stays an ordinary error, because a grant or a quota can still be raised.
func TestLiveProbeCreateClassifiesOnlyPermanentRejection(t *testing.T) {
	cases := map[string]struct {
		rejection     error
		wantPermanent bool
	}{
		"invalid": {
			rejection:     invalidPodRejection("probe"),
			wantPermanent: true,
		},
		"forbidden": {
			rejection:     apierrors.NewForbidden(corev1.Resource("pods"), "probe", errors.New("exceeded quota")),
			wantPermanent: false,
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			control, c := secureControl(t)
			run := secureRun("live-" + name)
			probeClusterFixture(t, c, run)
			rejectPodCreates(t, control, func(podName string) error {
				if strings.HasSuffix(podName, "-probe-node-a") {
					return test.rejection
				}
				return nil
			})

			err := control.runLiveProbes(context.Background(), run, nil, courier.DefaultPodResources())
			if err == nil {
				t.Fatal("runLiveProbes returned nil, want the create failure")
			}
			var permanent *SecureNeedsHumanError
			if test.wantPermanent {
				if !errors.As(err, &permanent) || permanent.Reason != "PodAdmission" {
					t.Fatalf("error = %v, want a *SecureNeedsHumanError reason PodAdmission", err)
				}
				if !strings.Contains(permanent.Detail, "node-a") {
					t.Fatalf("detail = %q, want it to name the node the probe would have run on", permanent.Detail)
				}
				return
			}
			if errors.As(err, &permanent) {
				t.Fatalf("error = %v, want an ordinary requeueable error", err)
			}
			if !strings.Contains(err.Error(), "create probe pod on node-a") {
				t.Fatalf("error = %v, want the unchanged probe create failure", err)
			}
		})
	}
}

// TestLaunchSecureResolvesLaneResourcesOncePerPass proves the dry-run proof
// and the pods actually created are the same artifact. The interceptor hands
// out the lane's real sizing on the first read and a different sizing on every
// read after it, standing in for a LaneProfile update landing between the proof
// and the create: a launcher that resolved sizing per step would create pods
// the proof never admitted, and would read the lane more than once per pass.
func TestLaunchSecureResolvesLaneResourcesOncePerPass(t *testing.T) {
	sizing := func(cpuRequest, memoryRequest, cpuLimit, memoryLimit string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpuRequest),
				corev1.ResourceMemory: resource.MustParse(memoryRequest),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpuLimit),
				corev1.ResourceMemory: resource.MustParse(memoryLimit),
			},
		}
	}
	control, c := secureControl(t, &courier.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: secureTestNamespace, Name: "sized"},
		Spec: courier.LaneProfileSpec{
			Roles:     map[string]string{"coordinator": "test/coordinator", "coder": "test/coder"},
			Resources: sizing("750m", "1Gi", "3", "6Gi"),
		},
	})
	run := launchedSecureRun(t, c, "one-pass", "sized")

	reads := 0
	watcher, ok := control.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the fake client does not support watch")
	}
	control.Client = interceptor.NewClient(watcher, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			lane, ok := obj.(*courier.LaneProfile)
			if !ok {
				return nil
			}
			reads++
			if reads > 1 {
				// A concurrent lane update: same roles, different sizing.
				lane.Spec.Resources = sizing("2", "2Gi", "8", "8Gi")
			}
			return nil
		},
	})

	if _, err := control.LaunchSecure(context.Background(), run); err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	// One sizing resolution plus the control pod's run-context read; any more
	// means the lane is resolved again mid-pass.
	if reads != 2 {
		t.Fatalf("LaneProfile reads = %d, want 2: one sizing resolution plus the control pod's run-context read", reads)
	}
	for component, podName := range map[string]string{
		topology.ComponentWorker:      topology.WorkerPodName(run.Name),
		topology.ComponentBroker:      topology.BrokerPodName(run.Name),
		topology.ComponentCoordinator: topology.ControlPodName(run.Name),
	} {
		var pod corev1.Pod
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: podName}, &pod); err != nil {
			t.Fatalf("%s pod: %v", component, err)
		}
		assertSized(t, component+" pod", pod.Spec.Containers[0].Resources)
	}
}

// TestLaunchSecurePermanentRejectionTerminalizesRun is the reconciler half of
// the invariant: a permanent rejection from either LaunchSecure call site
// terminalizes the run NeedsHuman with the SecurePreflightFailed condition
// instead of returning it to Pending and cycling claim and release. It drives
// both entry phases — admission of a Pending run and resume of a Claimed one —
// because the two call sites classify separately.
func TestLaunchSecurePermanentRejectionTerminalizesRun(t *testing.T) {
	cases := map[string]courier.Phase{
		"admission": courier.PhasePending,
		"resume":    courier.PhaseClaimed,
	}
	for name, phase := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			control, c := secureControl(t)
			runName := "terminalizes-" + name
			var run *courier.CoderRun
			if phase == courier.PhaseClaimed {
				// A resumed run already carries its persisted policy, so
				// LaunchSecure is the first secure call it makes.
				run = launchedSecureRun(t, c, runName, "local")
			} else {
				run = secureRun(runName)
				run.Status.Phase = courier.PhasePending
				if err := c.Create(ctx, run); err != nil {
					t.Fatal(err)
				}
				restrictNamespaceAndCredentials(t, c, run)
			}
			rejectPodCreates(t, control, launcherDryRun)

			item := &admissionSource{}
			reconciler := &CoderRunReconciler{
				Client: c,
				// The launch hook must be present for admission to reach the
				// launcher; Secure takes precedence over it.
				Launch:       func(context.Context, *courier.CoderRun) error { return nil },
				Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
				StatusWriter: fakeStatusWriter{client: c},
				Secure:       control,
			}
			control.Patcher = reconciler

			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			var updated courier.CoderRun
			if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.Phase != courier.PhaseNeedsHuman {
				t.Fatalf("phase = %q, want NeedsHuman; a permanent rejection must not cycle back to Pending", updated.Status.Phase)
			}
			condition := apimeta.FindStatusCondition(updated.Status.Conditions, "SecurePreflightFailed")
			if condition == nil {
				t.Fatal("the NeedsHuman terminalization must record SecurePreflightFailed")
			}
			if !strings.Contains(condition.Message, "rejected the rendered topology pod") {
				t.Fatalf("condition message = %q, want it to name the pod admission rejection", condition.Message)
			}
			// The secure hand-off settles the source with a NeedsHuman
			// transition rather than a release, exactly as every other
			// SecureControl terminalization does.
			if len(item.transitions) == 0 || item.transitions[len(item.transitions)-1] != source.StateNeedsHuman {
				t.Fatalf("transitions = %#v, want the source settled NeedsHuman", item.transitions)
			}
		})
	}
}

// restrictNamespaceAndCredentials provisions the two objects the secure
// preflight reads before it proves admission: the restricted run namespace and
// the deployment secret the per-run credential copy reads.
func restrictNamespaceAndCredentials(t *testing.T, c client.Client, run *courier.CoderRun) {
	t.Helper()
	ctx := context.Background()
	if err := c.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   run.Namespace,
			Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestLegacyLaunchPermanentRejectionTerminalizesRun is the reconciler half of
// the legacy path: with no SecureControl wired, a permanent launcher failure
// terminalizes the run NeedsHuman rather than releasing the claim back to
// Pending forever. The claim is released once, the cause reaches the source's
// terminal report, and a later reconcile neither re-claims nor re-releases.
func TestLegacyLaunchPermanentRejectionTerminalizesRun(t *testing.T) {
	ctx := context.Background()
	item := &admissionSource{}
	run := admissionRun("legacy-terminal", "local", courier.PhasePending)
	c := phaseClient(t, admissionLane("local", 1), run)
	reconciler := &CoderRunReconciler{
		Client: c,
		Launch: func(context.Context, *courier.CoderRun) error {
			return secureNeedsHuman("PodAdmission",
				"the API server rejected the coordinator pod %s: refused by admission", run.Name+"-coordinator")
		},
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: c},
	}
	if _, err := reconciler.Reconcile(ctx, admissionRequest("legacy-terminal")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, admissionKey("legacy-terminal"), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if len(item.released) != 1 {
		t.Fatalf("released = %#v, want exactly one release of the source claim", item.released)
	}
	if len(item.reports) != 1 || !strings.Contains(item.reports[0].Error, "rejected the coordinator pod") {
		t.Fatalf("reports = %#v, want the terminal report to name the pod admission rejection", item.reports)
	}

	// The run is parked, not re-admitted: a later reconcile claims nothing and
	// releases nothing further.
	if _, err := reconciler.Reconcile(ctx, admissionRequest("legacy-terminal")); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if len(item.claimed) != 1 || len(item.released) != 1 {
		t.Fatalf("claimed = %#v released = %#v, want the parked run to cycle neither", item.claimed, item.released)
	}
}
