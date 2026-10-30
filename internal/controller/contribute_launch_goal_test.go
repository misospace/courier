package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/adhoc"
	"github.com/misospace/courier/internal/source/manual"
)

// The contribution goal bridge (#624): an admitted ad-hoc contribution must
// reach the coordinator as a rendered goal carried by the ordinary
// Invocation -> COURIER_GOAL path, with no second prompt channel and no
// source-owned context ever decoded by the generic renderer.

const contributeGoal = `implement X for the login flow: handle "retry", add 支持, then document it
and cover it with tests`

func contributeTestClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add courier scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func contributeAdmission() adhoc.Admission {
	return adhoc.Admission{
		Namespace: "contrib",
		Lane:      "local",
		BaseRepo:  "upstream/project",
		BaseRef:   "trunk",
		WorkRepo:  "contributor/project",
		Goal:      contributeGoal,
	}
}

func contributeLane() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "contrib"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "litellm/coordinator"},
		},
	}
}

// admitContributeRun admits one contribution and returns the durable run the
// source materialized, exactly as a later launch would read it back.
func admitContributeRun(t *testing.T, c client.Client) *courierv1alpha1.CoderRun {
	t.Helper()
	admitted, err := adhoc.New(c, c).Admit(context.Background(), contributeAdmission())
	if err != nil {
		t.Fatalf("admit contribution: %v", err)
	}
	run := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: contributeAdmission().Namespace, Name: admitted.RunName}, run); err != nil {
		t.Fatalf("read admitted run %q: %v", admitted.RunName, err)
	}
	return run
}

func contributeRegistry(c client.Client) *SourceRegistry {
	return NewSourceRegistry(map[string]source.Adapter{
		"adhoc":  adhoc.New(c, c),
		"manual": manual.New(nil),
	})
}

// contributeEnvMap renders the run-context environment contract as a lookup.
func contributeEnvMap(inv executor.Invocation) map[string]string {
	values := map[string]string{}
	for _, variable := range executor.RunContextEnvironment(inv) {
		values[variable.Name] = variable.Value
	}
	return values
}

// TestContributeAdmissionReachesCoordinatorGoal walks the whole bridge:
// Admit(goal="implement X") -> contribute-mode CoderRun with source-owned
// context -> the owning adapter's source-neutral execution input -> the
// rendered coordinator goal -> the Invocation's COURIER_GOAL, and back through
// the environment contract the control pod reconstructs. It pins that the
// admitted objective arrives byte-for-byte, that a relaunch regenerates the
// identical goal from durable state, and that a run which cannot be given its
// objective never reaches goal rendering.
func TestContributeAdmissionReachesCoordinatorGoal(t *testing.T) {
	c := contributeTestClient(t)
	run := admitContributeRun(t, c)

	if run.Spec.Mode != courierv1alpha1.ModeContribute {
		t.Fatalf("admitted run mode = %q, want %q", run.Spec.Mode, courierv1alpha1.ModeContribute)
	}
	if run.Spec.Ref != 0 {
		t.Fatalf("admitted run ref = %d, want 0", run.Spec.Ref)
	}

	registry := contributeRegistry(c)
	contribution, err := launchContribution(registry, run)
	if err != nil {
		t.Fatalf("launch contribution: %v", err)
	}
	if contribution.Goal != contributeGoal {
		t.Fatalf("decoded goal drifted from the admitted objective:\n got %q\nwant %q", contribution.Goal, contributeGoal)
	}
	if contribution.BaseRef != "trunk" {
		t.Fatalf("decoded base ref = %q, want %q", contribution.BaseRef, "trunk")
	}

	// Resume and relaunch regenerate the same goal from durable run state.
	relaunched, err := launchContribution(registry, run)
	if err != nil {
		t.Fatalf("relaunch contribution: %v", err)
	}
	if relaunched != contribution {
		t.Fatalf("relaunch regenerated a different execution input:\n got %+v\nwant %+v", relaunched, contribution)
	}

	goal, err := executor.GoalWithContribution(run, contribution)
	if err != nil {
		t.Fatalf("render contribute goal: %v", err)
	}
	if !strings.Contains(goal, contributeGoal) {
		t.Fatalf("rendered goal lost the admitted objective verbatim:\n%s", goal)
	}
	for _, want := range []string{"upstream/project", "trunk", "untrusted task data"} {
		if !strings.Contains(goal, want) {
			t.Fatalf("rendered goal omits %q:\n%s", want, goal)
		}
	}

	invocation, err := executor.NewInvocationWithContribution(run, contributeLane(), "/workspace", contribution)
	if err != nil {
		t.Fatalf("build invocation: %v", err)
	}
	if invocation.Goal != goal {
		t.Fatalf("invocation goal differs from the rendered goal")
	}
	env := contributeEnvMap(invocation)
	if env["COURIER_GOAL"] != goal {
		t.Fatalf("COURIER_GOAL differs from the rendered goal:\n%s", env["COURIER_GOAL"])
	}
	if !strings.Contains(env["COURIER_GOAL"], "implement X") {
		t.Fatalf("COURIER_GOAL omits the admitted objective:\n%s", env["COURIER_GOAL"])
	}
	if env["COURIER_MODE"] != string(courierv1alpha1.ModeContribute) {
		t.Fatalf("COURIER_MODE = %q, want %q", env["COURIER_MODE"], courierv1alpha1.ModeContribute)
	}
	if env["COURIER_REF"] != "0" {
		t.Fatalf("COURIER_REF = %q, want 0 for a contribute run", env["COURIER_REF"])
	}
	rebuilt, err := executor.InvocationFromEnv(func(name string) string { return env[name] })
	if err != nil {
		t.Fatalf("reconstruct invocation from the run-context environment: %v", err)
	}
	if rebuilt.Goal != goal || rebuilt.Mode != courierv1alpha1.ModeContribute || rebuilt.Ref != 0 {
		t.Fatalf("environment round trip changed the run's goal or identity: %+v", rebuilt)
	}
}

// TestSecureControlInvocationCarriesAdmittedGoal pins the secure launch path
// itself: the control pod's invocation is built with the run's own source input,
// so the trusted control binary reconstructs the same COURIER_GOAL the operator
// rendered rather than an empty or invented one.
func TestSecureControlInvocationCarriesAdmittedGoal(t *testing.T) {
	c := contributeTestClient(t)
	run := admitContributeRun(t, c)
	if err := c.Create(context.Background(), contributeLane()); err != nil {
		t.Fatalf("create lane profile: %v", err)
	}
	secure := &SecureControl{Client: c, Sources: contributeRegistry(c)}
	invocation, err := secure.controlInvocation(context.Background(), run)
	if err != nil {
		t.Fatalf("secure control invocation: %v", err)
	}
	if !strings.Contains(invocation.Goal, "implement X") {
		t.Fatalf("control invocation goal omits the admitted objective:\n%s", invocation.Goal)
	}
	if invocation.Mode != courierv1alpha1.ModeContribute || invocation.Ref != 0 {
		t.Fatalf("control invocation identity = mode %q ref %d, want contribute with ref 0", invocation.Mode, invocation.Ref)
	}
	if invocation.Criteria != "" {
		t.Fatalf("a contribution pins no acceptance criteria, got envelope %q", invocation.Criteria)
	}
}

// TestContributeLaunchFailsClosedWithoutAdmittedContext pins that a contribute
// run never launches a coordinator it cannot give an objective to, and pins
// which failures the reconciler treats as permanent: a context the owning
// source refuses is immutable spec data, so it must be classified as a
// human-fixable launch rejection (which terminalizes NeedsHuman and fences any
// provisioned topology) rather than churned through claim and release forever.
// A failure to reach an owning adapter at all stays retryable, because a later
// deployment can wire it.
func TestContributeLaunchFailsClosedWithoutAdmittedContext(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		context   string
		permanent bool
		registry  func(c client.Client) *SourceRegistry
	}{
		{name: "empty context", source: "adhoc", context: "", permanent: true, registry: contributeRegistry},
		{name: "malformed context", source: "adhoc", context: `{"adhoc.courier.misospace.dev/v1":`, permanent: true, registry: contributeRegistry},
		{name: "foreign context key", source: "adhoc", context: `{"other.example/v1":{"goal":"x","baseRef":"main"}}`, permanent: true, registry: contributeRegistry},
		{name: "context without goal", source: "adhoc", context: `{"adhoc.courier.misospace.dev/v1":{"baseRef":"main"}}`, permanent: true, registry: contributeRegistry},
		{name: "context with a control character", source: "adhoc", context: `{"adhoc.courier.misospace.dev/v1":{"goal":"x\u0001y","baseRef":"main"}}`, permanent: true, registry: contributeRegistry},
		{name: "source without the surface", source: "manual", context: `{"goal":"x"}`, registry: contributeRegistry},
		{name: "unregistered source", source: "cron", context: `{"goal":"x"}`, registry: contributeRegistry},
		{name: "unwired registry", source: "adhoc", context: `{"adhoc.courier.misospace.dev/v1":{"goal":"x","baseRef":"main"}}`, registry: func(client.Client) *SourceRegistry { return NewSourceRegistry(nil) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := contributeTestClient(t)
			run := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: "courier-contrib", Namespace: "contrib"},
				Spec: courierv1alpha1.CoderRunSpec{
					Mode:          courierv1alpha1.ModeContribute,
					Source:        tc.source,
					WorkItemID:    "adhoc-abcdef123456",
					SourceContext: tc.context,
					Repo:          "upstream/project",
					WorkRepo:      "contributor/project",
					Lane:          "local",
				},
			}
			_, err := launchContribution(tc.registry(c), run)
			if err == nil {
				t.Fatalf("launch accepted a contribute run with %s", tc.name)
			}
			var needsHuman *SecureNeedsHumanError
			if tc.permanent {
				if !errors.As(err, &needsHuman) || needsHuman.Reason != contributionContextReason {
					t.Fatalf("error = %v, want a permanent rejection with reason %q", err, contributionContextReason)
				}
				// This is the reconciler's own gate: without it the run would
				// release its claim and re-launch on every pass.
				if classified := launchRejection(err); classified == nil || classified.Reason != contributionContextReason {
					t.Fatalf("launchRejection(%v) = %+v, want the contribution-context verdict", err, classified)
				}
			} else {
				if errors.As(err, &needsHuman) {
					t.Fatalf("error = %v, want a retryable failure, not a permanent NeedsHuman verdict", err)
				}
				if classified := launchRejection(err); classified != nil {
					t.Fatalf("launchRejection(%v) = %+v, want nil so the launch stays retryable", err, classified)
				}
			}
			// The renderer cannot substitute a goal of its own.
			if _, err := executor.Goal(run); !errors.Is(err, executor.ErrMissingContributionGoal) {
				t.Fatalf("goal without the admitted input = %v, want %v", err, executor.ErrMissingContributionGoal)
			}
		})
	}
}
