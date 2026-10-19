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

// TestPreflightDryRunProofCoversEveryCreatedPod pins which pod specs the
// dry-run proof must admit before the launcher creates anything: the three
// launcher pods always, and the probe pod on exactly the passes that create
// probes live. A launcher pod left out of the proof would be created unproven;
// a probe pod proven on a pass that creates no probe would let a deployment's
// probe configuration — a probe image this pass never needs — reject a run
// that never asks for one.
func TestPreflightDryRunProofCoversEveryCreatedPod(t *testing.T) {
	cases := map[string]struct {
		reject        func(control *SecureControl, run *courier.CoderRun) func(string) error
		wantIn        string
		liveProbes    bool
		relaunch      bool
		wantPermanent bool
	}{
		"a launcher dry-run pod is proven on every pass": {
			reject: func(*SecureControl, *courier.CoderRun) func(string) error {
				return launcherDryRun
			},
			wantIn:        "worker",
			wantPermanent: true,
		},
		"the probe dry-run pod is proven on a pass that creates probes": {
			reject:        rejectProbeDryRun,
			wantIn:        "probe",
			liveProbes:    true,
			wantPermanent: true,
		},
		"the probe is unproven when the deployment disables probes": {
			reject: rejectProbeDryRun,
		},
		"the probe is unproven on a relaunch, which creates no probes": {
			reject:     rejectProbeDryRun,
			liveProbes: true,
			relaunch:   true,
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			control, c := secureControl(t)
			control.Config.LiveProbes = test.liveProbes
			run := launchedSecureRun(t, c, "admission-proof", "local")
			if test.relaunch {
				// Any surviving topology pod makes this a relaunch, which
				// re-verifies the static guarantees but runs no live probes.
				if err := c.Create(context.Background(), healthyPod(run.Name, topology.ComponentBroker)); err != nil {
					t.Fatal(err)
				}
			}
			rejectPodCreates(t, control, test.reject(control, run))

			_, err := control.LaunchSecure(context.Background(), run)
			var permanent *SecureNeedsHumanError
			if !test.wantPermanent {
				if errors.As(err, &permanent) {
					t.Fatalf("error = %v (%s), want no terminalizable permanent rejection on a pass that creates no probe", err, permanent.Reason)
				}
				return
			}
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

// rejectProbeDryRun rejects only the probe pod's dry-run proof — the spec the
// live probe phase creates outside the proof loop. Only the probe's
// deterministic name is read here, so the sizing it renders with is arbitrary.
func rejectProbeDryRun(control *SecureControl, run *courier.CoderRun) func(string) error {
	probe := topology.ProbePod(run, "dry-run-node", control.Config.ProbeImage, "true", courier.DefaultPodResources())
	dryRun := probe.Name + "-dryrun"
	return func(name string) error {
		if name == dryRun {
			return invalidPodRejection(name)
		}
		return nil
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

// TestLiveProbeCreateClassifiesOnlyPermanentRejection pins the live probe
// create's own classification: the dry-run proof admits the probe spec on the
// passes that create probes, but a dry run is not a create, so the live create
// is the last point where the API can refuse that spec — and it classifies an
// Invalid itself rather than requeueing forever. A Forbidden or quota denial
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
			probe := topology.ProbePod(run, "node-a", control.Config.ProbeImage, "true", courier.DefaultPodResources()).Name
			rejectPodCreates(t, control, func(podName string) error {
				if podName == probe {
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
	proven := []*corev1.Pod{}
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
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if pod, isPod := obj.(*corev1.Pod); isPod && strings.HasSuffix(pod.Name, "-dryrun") {
				proven = append(proven, pod.DeepCopy())
			}
			return c.Create(ctx, obj, opts...)
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
	if len(proven) == 0 {
		t.Fatal("no dry-run pod was created, so the proof proved nothing")
	}
	for _, pod := range proven {
		assertSized(t, "dry-run proof of "+pod.Name, pod.Spec.Containers[0].Resources)
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
// the invariant: a pod-admission rejection from either LaunchSecure call site
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

// TestProbeIdentityMismatchTerminalizesRun is the reconciler half of the
// probe-identity verdict (#449): a probe pod whose deterministic name resolves
// to a pod pinned elsewhere is refused terminally, so the run ends NeedsHuman
// naming the probe and both nodes instead of releasing the claim back to
// Pending and cycling probe phases around a mismatch whose persistent causes
// a requeue cannot clear.
func TestProbeIdentityMismatchTerminalizesRun(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	control.Config.LiveProbes = true
	run := launchedSecureRun(t, c, "probe-identity-terminalizes", "local")
	eligibleProbeCluster(t, c, run, "node-a")
	probe := topology.ProbePod(run, "node-a", control.Config.ProbeImage, "true", courier.DefaultPodResources()).Name
	// Stand in for the collision outcome: the pod the probe name resolves to
	// survived pinned to a node the pass never asked about. The dry-run probe
	// copy is left alone — only the live create is misplaced.
	watcher, ok := control.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the fake client does not support watch")
	}
	control.Client = interceptor.NewClient(watcher, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			pod, isProbe := obj.(*corev1.Pod)
			if !isProbe || pod.Name != probe {
				return nil
			}
			misplaced := pod.DeepCopy()
			misplaced.Spec.NodeName = "node-b"
			return c.Update(ctx, misplaced)
		},
	})

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
		t.Fatalf("Reconcile: %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman; a probe-identity mismatch must not cycle back to Pending", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, "SecurePreflightFailed")
	if condition == nil {
		t.Fatal("the NeedsHuman terminalization must record SecurePreflightFailed")
	}
	if !strings.Contains(condition.Message, probe) ||
		!strings.Contains(condition.Message, "scheduled on node node-b, not node-a") {
		t.Fatalf("condition message = %q, want it to name the probe %q, the node it is pinned to, and the node under test", condition.Message, probe)
	}
	// The secure hand-off settles the source with a NeedsHuman transition,
	// not a release back to Pending.
	if len(item.released) != 0 {
		t.Fatalf("released = %#v, want the claim settled by the transition, never released", item.released)
	}
	if len(item.transitions) == 0 || item.transitions[len(item.transitions)-1] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want the source settled NeedsHuman", item.transitions)
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
// terminal report, and a later reconcile neither re-claims nor re-releases. It
// drives both entry phases — admission of a Pending run and resume of a Claimed
// one — because the two call sites classify separately.
func TestLegacyLaunchPermanentRejectionTerminalizesRun(t *testing.T) {
	cases := map[string]courier.Phase{
		"admission": courier.PhasePending,
		"resume":    courier.PhaseClaimed,
	}
	for name, phase := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			item := &admissionSource{}
			runName := "legacy-terminal-" + name
			run := admissionRun(runName, "local", phase)
			c := phaseClient(t, admissionLane("local", 1), run)
			reconciler := &CoderRunReconciler{
				Client: c,
				Launch: func(context.Context, *courier.CoderRun) error {
					return secureNeedsHuman("PodAdmission",
						"the API server rejected the coordinator pod %s: refused by admission", runName+"-coordinator")
				},
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
				StatusWriter: fakeStatusWriter{client: c},
			}
			if _, err := reconciler.Reconcile(ctx, admissionRequest(runName)); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			var updated courier.CoderRun
			if err := c.Get(ctx, admissionKey(runName), &updated); err != nil {
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

			// The run is parked, not re-admitted: a later reconcile claims
			// nothing and releases nothing further.
			claims, releases := len(item.claimed), len(item.released)
			if _, err := reconciler.Reconcile(ctx, admissionRequest(runName)); err != nil {
				t.Fatalf("second Reconcile: %v", err)
			}
			if len(item.claimed) != claims || len(item.released) != releases {
				t.Fatalf("claimed = %#v released = %#v, want the parked run to cycle neither", item.claimed, item.released)
			}
		})
	}
}

// TestLegacyPodAdmissionTerminalizationKeepsPullRequestLink pins that the
// shared terminalization leaves the run's branch identity alone. A fix-pr run
// that a pod-admission rejection takes down still has a head to observe, and
// the terminal PR enrichment reads Status.Branch to find it: clearing the
// branch here — as the missing-head path does, for good reason, because its head
// is genuinely gone — would silently drop the run's pull request link while the
// secure path's identical outcome keeps it.
func TestLegacyPodAdmissionTerminalizationKeepsPullRequestLink(t *testing.T) {
	ctx := context.Background()
	item := &admissionSource{}
	runName := "legacy-fix-pr"
	run := admissionRun(runName, "local", courier.PhaseClaimed)
	run.Spec.Mode = courier.ModeFixPR
	run.Spec.Ref = 42
	run.Status.Branch = "courier/Acme/Widgets/issue-42"
	run.Status.HeadRepo = "Acme/Widgets"
	run.Status.HeadSHA = "headsha"
	c := phaseClient(t, admissionLane("local", 1), run)
	calls := 0
	observer := &fakeWorldObserver{observation: PRObservation{PR: "https://forge/acme/widgets/pull/42"}, calls: &calls}
	reconciler := &CoderRunReconciler{
		Client: c,
		Launch: func(context.Context, *courier.CoderRun) error {
			return secureNeedsHuman("PodAdmission",
				"the API server rejected the coordinator pod %s: refused by admission", runName+"-coordinator")
		},
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: fakeStatusWriter{client: c},
		Observer:     observer,
	}
	if _, err := reconciler.Reconcile(ctx, admissionRequest(runName)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, admissionKey(runName), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
	}
	if updated.Status.Branch != run.Status.Branch || updated.Status.HeadRepo != run.Status.HeadRepo {
		t.Fatalf("head identity = %q@%q, want the run's own branch kept", updated.Status.HeadRepo, updated.Status.Branch)
	}
	if observer.calls == nil || *observer.calls == 0 {
		t.Fatal("the terminal PR enrichment never observed the run's head")
	}
	if updated.Status.PR != "https://forge/acme/widgets/pull/42" {
		t.Fatalf("PR = %q, want the observed pull request recorded on the terminal run", updated.Status.PR)
	}
}

// TestPodSecurityRejectionStaysRequeueable pins the narrowing of the launch
// path's permanent classification to the pod-admission class. It drives the
// PodSecurity class end to end — the run namespace missing the restricted
// enforce label — because that verdict, like a live probe that outran the
// just-written NetworkPolicy or an RBAC read off a stale informer cache, is one
// an operator fixes on the live world and a later pass then admits. Terminalizing
// it would strand the run over a label, so the reconciler keeps its requeue
// behaviour: the claim is released back to Pending and the error is returned.
func TestPodSecurityRejectionStaysRequeueable(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	run := secureRun("pod-security-requeue")
	run.Status.Phase = courier.PhasePending
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	// The restricted namespace, then the label this class trips over removed.
	restrictNamespaceAndCredentials(t, c, run)
	var namespace corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: secureTestNamespace}, &namespace); err != nil {
		t.Fatal(err)
	}
	namespace.Labels = map[string]string{"pod-security.kubernetes.io/audit": "restricted"}
	if err := c.Update(ctx, &namespace); err != nil {
		t.Fatal(err)
	}

	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client: c,
		Launch: func(context.Context, *courier.CoderRun) error { return nil },
		Sources: NewSourceRegistry(map[string]source.Adapter{
			"manual": item,
		}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	control.Patcher = reconciler

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err == nil {
		t.Fatal("Reconcile returned nil, want the pod-security verdict requeued")
	}
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != "PodSecurity" {
		t.Fatalf("error = %v, want the unchanged pod-security failure to be requeued", err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhasePending {
		t.Fatalf("phase = %q, want Pending: only the pod-admission class terminalizes", updated.Status.Phase)
	}
	if len(item.released) != 1 {
		t.Fatalf("released = %#v, want the claim released back to Pending", item.released)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, "SecurePreflightFailed"); condition != nil {
		t.Fatalf("condition = %#v, want no terminalizing condition on a requeueable failure", condition)
	}
}

// TestPodAdmissionRejectionFencesRoundBeforeTerminalizing proves the secure
// terminalization is fenced: a resumed launch reaches the launcher with a live
// worker and a credentialed broker already provisioned, and a NeedsHuman run is
// no longer observed, so a fence deferred past that reconcile would leave the
// untrusted executor running and the broker holding credentials until the
// retention window elapses. A fence that fails does not terminalize — the next
// pass retries it.
func TestPodAdmissionRejectionFencesRoundBeforeTerminalizing(t *testing.T) {
	cases := map[string]struct {
		fenceErr  error
		wantPhase courier.Phase
	}{
		"fence-succeeds": {wantPhase: courier.PhaseNeedsHuman},
		"fence-fails":    {fenceErr: errors.New("worker pod delete forbidden"), wantPhase: courier.PhasePending},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			control, c := secureControl(t)
			run := launchedSecureRun(t, c, "fenced-"+name, "local")
			run.Status.Phase = courier.PhaseClaimed
			if err := c.Status().Update(ctx, run); err != nil {
				t.Fatal(err)
			}
			// A round already up: the resumed launch fails its dry-run
			// proof with the worker and the credentialed broker live.
			for _, pod := range []*corev1.Pod{
				healthyPod(run.Name, topology.ComponentWorker),
				healthyPod(run.Name, topology.ComponentBroker),
			} {
				if err := c.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			workerName := topology.WorkerPodName(run.Name)
			watcher, ok := control.Client.(client.WithWatch)
			if !ok {
				t.Fatal("the fake client does not support watch")
			}
			control.Client = interceptor.NewClient(watcher, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if err := launcherDryRun(obj.GetName()); err != nil {
						return err
					}
					return c.Create(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if test.fenceErr != nil && obj.GetName() == workerName {
						return test.fenceErr
					}
					return c.Delete(ctx, obj, opts...)
				},
			})

			item := &admissionSource{}
			reconciler := &CoderRunReconciler{
				Client:       c,
				Launch:       func(context.Context, *courier.CoderRun) error { return nil },
				Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
				StatusWriter: fakeStatusWriter{client: c},
				Secure:       control,
			}
			control.Patcher = reconciler

			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			if test.fenceErr != nil {
				if err == nil || !strings.Contains(err.Error(), test.fenceErr.Error()) {
					t.Fatalf("Reconcile error = %v, want the fence failure surfaced for a retry", err)
				}
			} else if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			var updated courier.CoderRun
			if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.Phase != test.wantPhase {
				t.Fatalf("phase = %q, want %q", updated.Status.Phase, test.wantPhase)
			}
			for _, fenced := range []struct {
				what   string
				name   string
				object client.Object
			}{
				{"worker pod", workerName, &corev1.Pod{}},
				{"broker pod", topology.BrokerPodName(run.Name), &corev1.Pod{}},
				{"broker Service", topology.BrokerServiceName(run.Name), &corev1.Service{}},
			} {
				err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: fenced.name}, fenced.object)
				if test.fenceErr != nil {
					if err != nil {
						t.Fatalf("read %s after the failed fence: %v", fenced.what, err)
					}
					continue
				}
				if !apierrors.IsNotFound(err) {
					t.Fatalf("read %s = %v, want NotFound: the round must be fenced before the run terminalizes", fenced.what, err)
				}
			}
		})
	}
}
