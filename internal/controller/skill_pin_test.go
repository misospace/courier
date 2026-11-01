package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/skills"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/manual"
	"github.com/misospace/courier/internal/status"
)

// skillBundleRegistry registers one real bundle of the given content under
// name through the production loader, returning the registry and the
// bundle's content digest. The tests below compare and drift these digests,
// so they must be the loader's own deterministic content identity.
func skillBundleRegistry(t *testing.T, name, body string) (*skills.Registry, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("bundles:\n  - name: %s\n    source: %s\n", name, dir)
	path := filepath.Join(root, "bundles.yaml")
	if err := os.WriteFile(path, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	registry, err := skills.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	bundle, ok := registry.Lookup(name)
	if !ok {
		t.Fatalf("bundle %q did not register", name)
	}
	return registry, bundle.Digest
}

// skillPinReconciler builds the reconciler the skill-pin set-once tests
// drive through the real production patch path — patchStatus into a
// status.KubePatchWriter issuing a JSON merge patch against the API client —
// with the given skill registry wired into the secure config and the given
// lane present for the pin resolution to read. Every emitted patch document
// is recorded for exact-shape assertions.
func skillPinReconciler(t *testing.T, registry *skills.Registry, lane *courier.LaneProfile) (*CoderRunReconciler, client.Client, *recordingPatchWriter) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courier.CoderRun{}).WithObjects(lane).Build()
	control, _ := secureControl(t)
	control.Config.Skills = registry
	recorder := &recordingPatchWriter{inner: status.KubePatchWriter{Client: c}}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Secure:       control,
		StatusWriter: recorder,
	}
	return reconciler, c, recorder
}

// recordingPatchWriter wraps the production writer and records every patch
// document, so a test can assert the exact merge patch the operator emitted.
// It is needed because the fake client's merge-patch application drops an
// explicit empty array, while the real API server honors it.
type recordingPatchWriter struct {
	inner   status.PatchWriter
	patches [][]byte
}

func (w *recordingPatchWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte) error {
	w.patches = append(w.patches, patch)
	return w.inner.PatchStatus(ctx, name, patch)
}

// skillSelectionLane is the fixture lane selecting the given skill names.
func skillSelectionLane(names ...string) *courier.LaneProfile {
	lane := &courier.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: secureTestNamespace, Name: "local"},
		Spec: courier.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "test/coordinator"},
		},
	}
	lane.Spec.Skills = names
	return lane
}

// TestSkillPinsPersistBesidePolicy drives the skill pin through the same
// set-once lifecycle as the publication policy (#658): first resolution
// persists both in one patch, and a later pass re-verifies instead of
// rewriting.
func TestSkillPinsPersistBesidePolicy(t *testing.T) {
	registry, digest := skillBundleRegistry(t, "repo-tour", "tour the repository first.")
	lane := skillSelectionLane("repo-tour")
	reconciler, c, _ := skillPinReconciler(t, registry, lane)
	run := secureRun("skill-persist")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}

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
	pins := persisted.Status.SkillPins
	if len(pins) != 1 || pins[0].Name != "repo-tour" || pins[0].Digest != digest {
		t.Fatalf("persisted skill pins = %+v, want repo-tour at digest %q", pins, digest)
	}

	// Re-resolving a pinned run re-verifies instead of rewriting: the same
	// registry passes and the pin survives untouched.
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), persisted.DeepCopy()); err != nil {
		t.Fatalf("re-resolve of a persisted pin: %v", err)
	}
	var after courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &after); err != nil {
		t.Fatal(err)
	}
	if len(after.Status.SkillPins) != 1 || after.Status.SkillPins[0] != pins[0] {
		t.Fatalf("re-resolve rewrote the persisted pin to %+v", after.Status.SkillPins)
	}
}

// TestPreContractRunPinsFromLiveLane covers the retained-run shape: a run
// with a persisted policy but no pin (admitted before #658) resolves the pin
// from its live lane and patches it alone, leaving the policy untouched.
func TestPreContractRunPinsFromLiveLane(t *testing.T) {
	registry, digest := skillBundleRegistry(t, "simple-english", "write plainly.")
	lane := skillSelectionLane("simple-english")
	reconciler, c, _ := skillPinReconciler(t, registry, lane)
	run := secureRun("pre-contract")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// First resolve persists policy and pin together...
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
		t.Fatalf("resolvePersistSecurePolicy: %v", err)
	}
	var persisted courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	policy := persisted.Status.PublicationPolicy.DeepCopy()
	// ...then rewind the pin to simulate a run admitted before the pin
	// contract, and re-resolve.
	persisted.Status.SkillPins = nil
	if err := c.Status().Update(context.Background(), &persisted); err != nil {
		t.Fatal(err)
	}
	var retained courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), &retained); err != nil {
		t.Fatalf("resolvePersistSecurePolicy on a pre-contract run: %v", err)
	}
	var after courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &after); err != nil {
		t.Fatal(err)
	}
	if len(after.Status.SkillPins) != 1 || after.Status.SkillPins[0].Name != "simple-english" || after.Status.SkillPins[0].Digest != digest {
		t.Fatalf("backfilled pins = %+v, want simple-english at digest %q", after.Status.SkillPins, digest)
	}
	if after.Status.PublicationPolicy == nil || !equalityPolicy(policy, after.Status.PublicationPolicy) {
		t.Fatalf("backfill rewrote the persisted policy:\n got %+v\nwant %+v", after.Status.PublicationPolicy, policy)
	}
}

func equalityPolicy(a, b *courier.PublicationPolicy) bool {
	if a == nil || b == nil {
		return a == b
	}
	return reflect.DeepEqual(*a, *b)
}

// TestSkillPinRelaunchVerifiesAgainstRegistry pins the registry-drift
// fail-closed: a resumed run whose pinned identity is no longer
// materializable terminalizes instead of substituting newer content.
func TestSkillPinRelaunchVerifiesAgainstRegistry(t *testing.T) {
	registry, _ := skillBundleRegistry(t, "repo-tour", "tour the repository first.")
	lane := skillSelectionLane("repo-tour")
	reconciler, c, _ := skillPinReconciler(t, registry, lane)
	run := secureRun("pin-verify")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
		t.Fatalf("resolvePersistSecurePolicy: %v", err)
	}
	var persisted courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}

	t.Run("same registry verifies", func(t *testing.T) {
		if err := reconciler.resolvePersistSecurePolicy(context.Background(), persisted.DeepCopy()); err != nil {
			t.Fatalf("re-resolve with the same registry: %v", err)
		}
	})

	t.Run("drifted digest fails closed", func(t *testing.T) {
		drifted, _ := skillBundleRegistry(t, "repo-tour", "tour the repository differently.")
		reconciler.Secure.Config.Skills = drifted
		err := reconciler.resolvePersistSecurePolicy(context.Background(), persisted.DeepCopy())
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "SkillPinConflict" {
			t.Fatalf("drifted digest error = %v, want *SecureNeedsHumanError reason SkillPinConflict", err)
		}
	})

	t.Run("dropped name fails closed", func(t *testing.T) {
		reconciler.Secure.Config.Skills = nil
		err := reconciler.resolvePersistSecurePolicy(context.Background(), persisted.DeepCopy())
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "SkillPinConflict" {
			t.Fatalf("dropped name error = %v, want *SecureNeedsHumanError reason SkillPinConflict", err)
		}
	})
}

// TestSkillSelectionFailsClosedAtFirstResolution pins the admission-side
// rejects: an unknown lane skill name and a deployment registering no
// bundles both fail closed as permanent lane misconfiguration, and a lane
// selecting nothing pins an explicit empty set.
func TestSkillSelectionFailsClosedAtFirstResolution(t *testing.T) {
	registry, _ := skillBundleRegistry(t, "repo-tour", "tour the repository first.")

	t.Run("unknown name", func(t *testing.T) {
		lane := skillSelectionLane("ghost-skill")
		reconciler, c, _ := skillPinReconciler(t, registry, lane)
		run := secureRun("unknown-skill")
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		err := reconciler.resolvePersistSecurePolicy(context.Background(), run)
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "LaneConfig" {
			t.Fatalf("unknown skill error = %v, want *SecureNeedsHumanError reason LaneConfig", err)
		}
	})

	t.Run("nil registry", func(t *testing.T) {
		lane := skillSelectionLane("repo-tour")
		reconciler, c, _ := skillPinReconciler(t, nil, lane)
		run := secureRun("no-registry")
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		err := reconciler.resolvePersistSecurePolicy(context.Background(), run)
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "LaneConfig" {
			t.Fatalf("nil-registry error = %v, want *SecureNeedsHumanError reason LaneConfig", err)
		}
	})

	t.Run("empty selection pins an empty set", func(t *testing.T) {
		lane := skillSelectionLane()
		reconciler, c, recorder := skillPinReconciler(t, nil, lane)
		run := secureRun("no-skills")
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
			t.Fatalf("resolvePersistSecurePolicy: %v", err)
		}
		// The run pins an explicit empty set, and the patch carries it as an
		// empty array. The persisted-object assertion lives on the recorded
		// patch document rather than the fake client's stored object, whose
		// merge-patch application drops an explicit empty array the real API
		// server honors.
		if run.Status.SkillPins == nil || len(run.Status.SkillPins) != 0 {
			t.Fatalf("resolved pins = %#v, want an empty non-nil set", run.Status.SkillPins)
		}
		var payload struct {
			Status map[string]json.RawMessage `json:"status"`
		}
		if err := json.Unmarshal(recorder.patches[len(recorder.patches)-1], &payload); err != nil {
			t.Fatal(err)
		}
		raw, ok := payload.Status["skillPins"]
		if !ok || strings.TrimSpace(string(raw)) != "[]" {
			t.Fatalf("patch = %s, want an explicit empty skillPins array", recorder.patches[len(recorder.patches)-1])
		}
	})
}

// TestLaneEditAfterPinDoesNotChangePins pins that the verify path never
// re-reads the lane: the pin resolved at admission is authoritative, so a
// lane edit after admission never alters a retained run's resolved skills.
func TestLaneEditAfterPinDoesNotChangePins(t *testing.T) {
	registry, _ := skillBundleRegistry(t, "repo-tour", "tour the repository first.")
	lane := skillSelectionLane("repo-tour")
	reconciler, c, _ := skillPinReconciler(t, registry, lane)
	run := secureRun("lane-edit")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
		t.Fatalf("resolvePersistSecurePolicy: %v", err)
	}
	var persisted courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	pinned := persisted.Status.SkillPins

	// Rewrite the lane's selection after admission; the retained run's pins
	// must survive the next pass untouched.
	var live courier.LaneProfile
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(lane), &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Skills = []string{"simple-english"}
	if err := c.Update(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), persisted.DeepCopy()); err != nil {
		t.Fatalf("re-resolve after a lane edit: %v", err)
	}
	var after courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &after); err != nil {
		t.Fatal(err)
	}
	if len(after.Status.SkillPins) != 1 || after.Status.SkillPins[0] != pinned[0] {
		t.Fatalf("pins after a lane edit = %+v, want the admitted pin %+v", after.Status.SkillPins, pinned)
	}
}

// TestRelaunchFunnelVerifiesSkillPins pins the funnel: every secure
// relaunch re-enters through resumeClaimed (infra loss and broken topology
// both charge a run back to Claimed), so pin verification and pre-contract
// backfill must fire there — not only on the Pending admission path.
func TestRelaunchFunnelVerifiesSkillPins(t *testing.T) {
	t.Run("drifted registry fails closed on relaunch", func(t *testing.T) {
		registry, _ := skillBundleRegistry(t, "repo-tour", "tour the repository first.")
		lane := skillSelectionLane("repo-tour")
		reconciler, c, _ := skillPinReconciler(t, registry, lane)
		run := secureRun("funnel-drift")
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
			t.Fatalf("resolvePersistSecurePolicy: %v", err)
		}
		var persisted courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
			t.Fatal(err)
		}
		drifted, _ := skillBundleRegistry(t, "repo-tour", "tour the repository differently.")
		reconciler.Secure.Config.Skills = drifted
		// Terminalization walks the run's source work item, so the funnel's
		// needs-human hand-off wires the reconciler as its own patcher with a
		// source registry, the same shape the broken-topology tests use.
		reconciler.Sources = NewSourceRegistry(map[string]source.Adapter{"manual": manual.New(nil)})
		reconciler.Secure.Patcher = reconciler
		if _, err := reconciler.resumeClaimed(context.Background(), persisted.DeepCopy()); err != nil {
			t.Fatalf("resumeClaimed() on a drifted registry: %v", err)
		}
		var terminal courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &terminal); err != nil {
			t.Fatal(err)
		}
		if terminal.Status.Phase != courier.PhaseNeedsHuman {
			t.Fatalf("phase after a drifted registry = %q, want NeedsHuman", terminal.Status.Phase)
		}
		for _, condition := range terminal.Status.Conditions {
			if condition.Type == "SecurePreflightFailed" && strings.Contains(condition.Message, "pinned digest") {
				return
			}
		}
		t.Fatal("no SecurePreflightFailed condition carries the pin-drift failure detail")
	})

	t.Run("pre-contract run backfills its pin on relaunch", func(t *testing.T) {
		registry, digest := skillBundleRegistry(t, "repo-tour", "tour the repository first.")
		lane := skillSelectionLane("repo-tour")
		reconciler, c, _ := skillPinReconciler(t, registry, lane)
		reconciler.Sources = NewSourceRegistry(map[string]source.Adapter{"manual": manual.New(nil)})
		run := secureRun("funnel-backfill")
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		if err := reconciler.resolvePersistSecurePolicy(context.Background(), run); err != nil {
			t.Fatalf("resolvePersistSecurePolicy: %v", err)
		}
		var persisted courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
			t.Fatal(err)
		}
		persisted.Status.SkillPins = nil
		if err := c.Status().Update(context.Background(), &persisted); err != nil {
			t.Fatal(err)
		}
		var retained courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
			t.Fatal(err)
		}
		// The funnel backfills the missing pin from the live lane; the run
		// then proceeds toward launch and stops cleanly at the nil legacy
		// launcher seam this fixture carries.
		if _, err := reconciler.resumeClaimed(context.Background(), &retained); err != nil {
			t.Fatalf("resumeClaimed() on a pre-contract run: %v", err)
		}
		var after courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &after); err != nil {
			t.Fatal(err)
		}
		if len(after.Status.SkillPins) != 1 || after.Status.SkillPins[0].Name != "repo-tour" || after.Status.SkillPins[0].Digest != digest {
			t.Fatalf("backfilled pins = %+v, want repo-tour at digest %q", after.Status.SkillPins, digest)
		}
	})
}

// TestControlInvocationCarriesPinnedSkills pins that the control pod's
// invocation carries the pinned skill envelope from persisted status, never
// the live lane, and that a run pinning nothing carries no envelope.
func TestControlInvocationCarriesPinnedSkills(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("invocation-skills")
	run.Status.SkillPins = []courier.SkillPin{
		{Name: "repo-tour", Digest: "sha256:" + strings.Repeat("b", 64)},
		{Name: "simple-english", Digest: "sha256:" + strings.Repeat("a", 64)},
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	invocation, err := control.controlInvocation(context.Background(), run)
	if err != nil {
		t.Fatalf("controlInvocation: %v", err)
	}
	want := `[{"name":"repo-tour","digest":"sha256:` + strings.Repeat("b", 64) + `"},{"name":"simple-english","digest":"sha256:` + strings.Repeat("a", 64) + `"}]`
	if invocation.Skills != want {
		t.Fatalf("invocation skills = %q, want the pinned envelope %q", invocation.Skills, want)
	}

	// An empty pin set is still a pin, but it carries no envelope: absence
	// stays meaningful to the reconstruction boundary.
	empty := secureRun("invocation-skillless")
	empty.Status.SkillPins = []courier.SkillPin{}
	if err := c.Create(context.Background(), empty); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), empty); err != nil {
		t.Fatal(err)
	}
	invocation, err = control.controlInvocation(context.Background(), empty)
	if err != nil {
		t.Fatalf("controlInvocation for a skillless run: %v", err)
	}
	if invocation.Skills != "" {
		t.Fatalf("a skillless run's invocation carries skills %q, want empty", invocation.Skills)
	}
}

// TestLegacyLauncherRejectsSkillSelectingLane pins the legacy path's
// fail-closed counterpart: a lane selecting skills requires the secure
// path's registry seam, so the legacy launcher terminalizes the run as a
// permanent lane misconfiguration instead of launching skillless work.
func TestLegacyLauncherRejectsSkillSelectingLane(t *testing.T) {
	run := &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: types.UID("run-uid")},
		Spec: courier.CoderRunSpec{
			Mode: courier.ModeResolveIssue, Repo: "acme/widgets", Ref: 1, Lane: "skilled",
		},
		Status: courier.CoderRunStatus{Phase: courier.PhaseClaimed},
	}
	lane := &courier.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "skilled", Namespace: "default"},
		Spec: courier.LaneProfileSpec{
			Roles:  map[string]string{"coordinator": "test-model"},
			Skills: []string{"repo-tour", "simple-english"},
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(launcherScheme(t)).
		WithStatusSubresource(&courier.CoderRun{}).
		WithRuntimeObjects(run, lane).
		Build()
	launcher := &CoordinatorLauncher{
		Client: fakeClient,
		Pod:    executor.PodConfig{Image: "example/opencode:test"},
	}
	err := launcher.Launch(context.Background(), run)
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != "LaneConfig" {
		t.Fatalf("Launch() error = %v, want a *SecureNeedsHumanError with reason LaneConfig", err)
	}
	if !strings.Contains(permanent.Detail, "repo-tour, simple-english") {
		t.Fatalf("rejection detail = %q, want the selected skill names", permanent.Detail)
	}
	var pod corev1.Pod
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "run-1-coordinator"}, &pod); err == nil {
		t.Fatal("coordinator pod created despite a skill-selecting lane")
	}
}
