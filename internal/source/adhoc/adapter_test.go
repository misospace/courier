package adhoc

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// mintedKeyRe is the settled shape of a minted idempotency key.
var mintedKeyRe = regexp.MustCompile(`^adhoc-[0-9a-f]{12}$`)

// newTestCluster builds a fresh controller-runtime fake client with the
// Courier API types registered.
func newTestCluster(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add courier scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

// newAdapter builds an adapter whose write client and direct reader are
// the same client, the shape plain unit tests need.
func newAdapter(c client.Client) Adapter {
	return New(c, c)
}

// validAdmission is the baseline well-formed admission; tests mutate a
// copy per case. It carries no upstream issue so the happy path can assert
// the JSON omits that key.
func validAdmission() Admission {
	return Admission{
		Namespace: "contrib",
		Lane:      "local",
		BaseRepo:  "upstream/project",
		BaseRef:   "main",
		WorkRepo:  "contributor/project",
		Goal:      "fix the login flow",
	}
}

// listRuns returns every CoderRun in namespace.
func listRuns(t *testing.T, c client.Client, namespace string) []courierv1alpha1.CoderRun {
	t.Helper()
	var list courierv1alpha1.CoderRunList
	if err := c.List(context.Background(), &list, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list coder runs: %v", err)
	}
	return list.Items
}

// getRun fetches one CoderRun by namespace and name.
func getRun(t *testing.T, c client.Client, namespace, name string) *courierv1alpha1.CoderRun {
	t.Helper()
	run := &courierv1alpha1.CoderRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, run); err != nil {
		t.Fatalf("get coder run %s/%s: %v", namespace, name, err)
	}
	return run
}

// decodeContext decodes a run's SourceContext and returns the payload
// under the source-owned key.
func decodeContext(t *testing.T, raw string) contextPayload {
	t.Helper()
	var envelope map[string]contextPayload
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		t.Fatalf("decode source context %q: %v", raw, err)
	}
	payload, ok := envelope[contextKey]
	if !ok {
		t.Fatalf("source context %q has no %q key", raw, contextKey)
	}
	return payload
}

// TestAdmitCreatesContributeRun is the happy path: one well-formed
// admission materializes exactly one contribute-mode CoderRun with the
// settled spec shape and an unresumable identity.
func TestAdmitCreatesContributeRun(t *testing.T) {
	c := newTestCluster(t)
	a := newAdapter(c)

	admitted, err := a.Admit(context.Background(), validAdmission())
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if admitted.Resumed {
		t.Errorf("Resumed = true; want false for a new run")
	}
	if !strings.HasPrefix(admitted.RunName, "courier-") {
		t.Errorf("RunName = %q; want the courier- prefix", admitted.RunName)
	}
	if !mintedKeyRe.MatchString(admitted.WorkItemID) {
		t.Errorf("WorkItemID = %q; want the adhoc-<12 hex> shape", admitted.WorkItemID)
	}

	if items := listRuns(t, c, validAdmission().Namespace); len(items) != 1 {
		t.Fatalf("runs after Admit = %d; want 1", len(items))
	}
	run := getRun(t, c, validAdmission().Namespace, admitted.RunName)
	if run.Spec.Mode != courierv1alpha1.ModeContribute {
		t.Errorf("Spec.Mode = %q; want %q", run.Spec.Mode, courierv1alpha1.ModeContribute)
	}
	if run.Spec.Source != SourceName {
		t.Errorf("Spec.Source = %q; want %q", run.Spec.Source, SourceName)
	}
	if run.Spec.Repo != validAdmission().BaseRepo {
		t.Errorf("Spec.Repo = %q; want %q", run.Spec.Repo, validAdmission().BaseRepo)
	}
	if run.Spec.WorkRepo != validAdmission().WorkRepo {
		t.Errorf("Spec.WorkRepo = %q; want %q", run.Spec.WorkRepo, validAdmission().WorkRepo)
	}
	if run.Spec.Ref != 0 {
		t.Errorf("Spec.Ref = %d; want 0 (contribute forbids it)", run.Spec.Ref)
	}
	if run.Spec.Lane != validAdmission().Lane {
		t.Errorf("Spec.Lane = %q; want %q", run.Spec.Lane, validAdmission().Lane)
	}
	if run.Spec.WorkItemID != admitted.WorkItemID {
		t.Errorf("Spec.WorkItemID = %q; want the admitted key %q", run.Spec.WorkItemID, admitted.WorkItemID)
	}
	if run.Spec.Debug {
		t.Error("Spec.Debug = true; want false when the admission does not request it")
	}
	if run.Spec.SourceAgent != "" || run.Spec.AssignmentKind != "" || len(run.Spec.AssignmentReasons) != 0 {
		t.Errorf("unexpected assignment fields: agent %q kind %q reasons %v",
			run.Spec.SourceAgent, run.Spec.AssignmentKind, run.Spec.AssignmentReasons)
	}

	payload := decodeContext(t, run.Spec.SourceContext)
	if payload.Goal != validAdmission().Goal {
		t.Errorf("context goal = %q; want %q", payload.Goal, validAdmission().Goal)
	}
	if payload.BaseRef != validAdmission().BaseRef {
		t.Errorf("context baseRef = %q; want %q", payload.BaseRef, validAdmission().BaseRef)
	}
	if strings.Contains(run.Spec.SourceContext, "upstreamIssue") {
		t.Errorf("source context carries upstreamIssue when unset: %s", run.Spec.SourceContext)
	}

	t.Run("debug-flag-carried", func(t *testing.T) {
		in := validAdmission()
		in.Debug = true
		in.IdempotencyKey = "debug-run-1"
		admitted, err := a.Admit(context.Background(), in)
		if err != nil {
			t.Fatalf("Admit(debug): %v", err)
		}
		if admitted.Resumed {
			t.Fatal("second run resumed; want a new run under its own key")
		}
		if got := getRun(t, c, in.Namespace, admitted.RunName).Spec.Debug; !got {
			t.Error("Spec.Debug = false; want true")
		}
	})
}

// TestAdmitNilClient checks that a clientless adapter fails Admit with
// the sentinel, not a panic.
func TestAdmitNilClient(t *testing.T) {
	_, err := newAdapter(nil).Admit(context.Background(), validAdmission())
	if !errors.Is(err, ErrClientRequired) {
		t.Fatalf("err = %v; want ErrClientRequired", err)
	}
}

// TestAdmitKeyMinting pins the idempotency-key contract: identical
// admissions mint identical keys across independent adapters and
// clusters, every digest input participates, a supplied key is used
// verbatim, and malformed supplied keys are rejected with no object.
func TestAdmitKeyMinting(t *testing.T) {
	ctx := context.Background()
	base := validAdmission()

	t.Run("identical-inputs-mint-identical-key", func(t *testing.T) {
		first, err := newAdapter(newTestCluster(t)).Admit(ctx, base)
		if err != nil {
			t.Fatalf("first Admit: %v", err)
		}
		second, err := newAdapter(newTestCluster(t)).Admit(ctx, base)
		if err != nil {
			t.Fatalf("second Admit: %v", err)
		}
		if first.WorkItemID != second.WorkItemID {
			t.Fatalf("identical admissions minted different keys: %q vs %q", first.WorkItemID, second.WorkItemID)
		}
		if !mintedKeyRe.MatchString(first.WorkItemID) {
			t.Fatalf("key %q does not match the adhoc-<12 hex> shape", first.WorkItemID)
		}
	})

	t.Run("each-digest-input-changes-the-key", func(t *testing.T) {
		keyOf := func(mut func(*Admission)) string {
			t.Helper()
			in := base
			mut(&in)
			admitted, err := newAdapter(newTestCluster(t)).Admit(ctx, in)
			if err != nil {
				t.Fatalf("Admit: %v", err)
			}
			return admitted.WorkItemID
		}
		baseline := mintKey(base.BaseRepo, base.BaseRef, base.Goal, base.WorkRepo)
		if got := keyOf(func(*Admission) {}); got != baseline {
			t.Fatalf("minted key %q differs from the package derivation %q", got, baseline)
		}
		mutations := map[string]func(*Admission){
			"base repo": func(in *Admission) { in.BaseRepo = "other/repo" },
			"base ref":  func(in *Admission) { in.BaseRef = "develop" },
			"goal":      func(in *Admission) { in.Goal = "write a new feature" },
			"work repo": func(in *Admission) { in.WorkRepo = "contributor/other" },
		}
		for name, mut := range mutations {
			if got := keyOf(mut); got == baseline {
				t.Errorf("changing %s kept key %q; want a different key", name, got)
			}
		}
	})

	t.Run("supplied-key-used-verbatim", func(t *testing.T) {
		in := base
		in.IdempotencyKey = "issue-42-attempt-1"
		c := newTestCluster(t)
		admitted, err := newAdapter(c).Admit(ctx, in)
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if admitted.WorkItemID != in.IdempotencyKey {
			t.Fatalf("WorkItemID = %q; want the supplied key %q verbatim", admitted.WorkItemID, in.IdempotencyKey)
		}
		if got := getRun(t, c, in.Namespace, admitted.RunName).Spec.WorkItemID; got != in.IdempotencyKey {
			t.Fatalf("Spec.WorkItemID = %q; want %q", got, in.IdempotencyKey)
		}
	})

	t.Run("invalid-supplied-keys-rejected", func(t *testing.T) {
		c := newTestCluster(t)
		a := newAdapter(c)
		keys := []struct {
			name string
			key  string
		}{
			{"blank", "   "},
			{"over-256", strings.Repeat("a", 257)},
			{"leading-dash", "-lead"},
			{"slash", "a/b"},
			{"hash", "a#b"},
		}
		for _, tc := range keys {
			t.Run(tc.name, func(t *testing.T) {
				in := base
				in.IdempotencyKey = tc.key
				_, err := a.Admit(ctx, in)
				if !errors.Is(err, ErrInvalidAdmission) {
					t.Fatalf("err = %v; want ErrInvalidAdmission", err)
				}
			})
		}
		if items := listRuns(t, c, base.Namespace); len(items) != 0 {
			t.Fatalf("invalid keys created %d runs; want 0", len(items))
		}
	})
}

// TestAdmitDuplicateResumes pins the resume contract: a second Admit
// with the same identity returns the existing run's identity, never a
// second object — whether it comes from the same adapter or a fresh one
// (the lost-race shape).
func TestAdmitDuplicateResumes(t *testing.T) {
	ctx := context.Background()
	in := validAdmission()
	c := newTestCluster(t)

	first, err := newAdapter(c).Admit(ctx, in)
	if err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	if first.Resumed {
		t.Fatal("first Admit resumed; want a new run")
	}

	t.Run("same-adapter", func(t *testing.T) {
		a := newAdapter(c)
		second, err := a.Admit(ctx, in)
		if err != nil {
			t.Fatalf("duplicate Admit: %v", err)
		}
		if !second.Resumed {
			t.Error("duplicate Admit did not resume")
		}
		if second.RunName != first.RunName || second.WorkItemID != first.WorkItemID {
			t.Errorf("duplicate identity (%s, %s) differs from first (%s, %s)",
				second.RunName, second.WorkItemID, first.RunName, first.WorkItemID)
		}
		if items := listRuns(t, c, in.Namespace); len(items) != 1 {
			t.Fatalf("runs after duplicate = %d; want 1", len(items))
		}
	})

	t.Run("fresh-adapter", func(t *testing.T) {
		second, err := newAdapter(c).Admit(ctx, in)
		if err != nil {
			t.Fatalf("fresh-adapter Admit: %v", err)
		}
		if !second.Resumed {
			t.Error("fresh-adapter Admit did not resume the pre-seeded run")
		}
		if second.RunName != first.RunName || second.WorkItemID != first.WorkItemID {
			t.Errorf("fresh-adapter identity (%s, %s) differs from first (%s, %s)",
				second.RunName, second.WorkItemID, first.RunName, first.WorkItemID)
		}
	})
}

// TestAdmitCreateRaceResumes pins the lost-race branch: the pre-create
// Get saw nothing, Create answered AlreadyExists against a concurrent
// admission's winner, and the re-read follows the same rules as the
// resume path — the winner resumes when it owns the key, fails closed
// with ErrForeignRun when it belongs to someone else, and fails closed
// when it vanished before the re-read.
func TestAdmitCreateRaceResumes(t *testing.T) {
	ctx := context.Background()
	in := validAdmission()
	key := mintKey(in.BaseRepo, in.BaseRef, in.Goal, in.WorkRepo)
	name := runName(key)
	resource := schema.GroupResource{Group: "courier.misospace.dev", Resource: "coderruns"}

	raceCluster := func(t *testing.T, winner *courierv1alpha1.CoderRun, hideFirstGet bool) client.Client {
		t.Helper()
		scheme := runtime.NewScheme()
		if err := courierv1alpha1.AddToScheme(scheme); err != nil {
			t.Fatalf("add courier scheme: %v", err)
		}
		var gets int
		builder := fake.NewClientBuilder().WithScheme(scheme)
		if winner != nil {
			builder = builder.WithObjects(winner)
		}
		return builder.WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				gets++
				if hideFirstGet && gets == 1 {
					return apierrors.NewNotFound(resource, name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(resource, name)
			},
		}).Build()
	}
	ownedWinner := func() *courierv1alpha1.CoderRun {
		return &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: in.Namespace},
			Spec: courierv1alpha1.CoderRunSpec{
				Mode:       courierv1alpha1.ModeContribute,
				Source:     SourceName,
				WorkItemID: key,
				Repo:       in.BaseRepo,
				WorkRepo:   in.WorkRepo,
				Lane:       in.Lane,
			},
		}
	}

	t.Run("winner-owns-key-resumes", func(t *testing.T) {
		c := raceCluster(t, ownedWinner(), true)
		admitted, err := newAdapter(c).Admit(ctx, in)
		if err != nil {
			t.Fatalf("Admit into lost race: %v", err)
		}
		if !admitted.Resumed || admitted.RunName != name || admitted.WorkItemID != key {
			t.Fatalf("race outcome %+v; want resume of (%s, %s)", admitted, name, key)
		}
		if items := listRuns(t, c, in.Namespace); len(items) != 1 {
			t.Fatalf("runs after lost race = %d; want 1", len(items))
		}
	})

	t.Run("foreign-winner-fails-closed", func(t *testing.T) {
		foreign := ownedWinner()
		foreign.Spec.Source = "dispatch"
		c := raceCluster(t, foreign, true)
		if _, err := newAdapter(c).Admit(ctx, in); !errors.Is(err, ErrForeignRun) {
			t.Fatalf("err = %v; want ErrForeignRun", err)
		}
		if got := getRun(t, c, in.Namespace, name); got.Spec.Source != "dispatch" {
			t.Fatalf("foreign winner was mutated: source %q", got.Spec.Source)
		}
	})

	t.Run("winner-vanished-fails-closed", func(t *testing.T) {
		c := raceCluster(t, nil, true)
		_, err := newAdapter(c).Admit(ctx, in)
		if err == nil {
			t.Fatal("Admit succeeded with a winner that vanished; want failure")
		}
		if errors.Is(err, ErrForeignRun) || errors.Is(err, ErrInvalidAdmission) {
			t.Fatalf("err = %v; want a plain re-read failure", err)
		}
	})
}

// TestAdmitForeignRunFailsClosed checks the name-collision guard: a
// CoderRun already occupying the derived name under another source or
// key is an error, and the foreign object survives untouched.
func TestAdmitForeignRunFailsClosed(t *testing.T) {
	ctx := context.Background()
	in := validAdmission()
	key := mintKey(in.BaseRepo, in.BaseRef, in.Goal, in.WorkRepo)
	name := runName(key)

	for _, tc := range []struct {
		name   string
		mutate func(spec *courierv1alpha1.CoderRunSpec)
	}{
		{"foreign-source", func(s *courierv1alpha1.CoderRunSpec) { s.Source = "dispatch" }},
		{"foreign-work-item", func(s *courierv1alpha1.CoderRunSpec) { s.WorkItemID = "someone-else-key" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCluster(t)
			foreign := &courierv1alpha1.CoderRun{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: in.Namespace},
				Spec: courierv1alpha1.CoderRunSpec{
					Mode:       courierv1alpha1.ModeResolveIssue,
					Source:     SourceName,
					WorkItemID: key,
					Repo:       in.BaseRepo,
					Ref:        7,
					Lane:       "some-lane",
				},
			}
			tc.mutate(&foreign.Spec)
			if err := c.Create(ctx, foreign); err != nil {
				t.Fatalf("create foreign run: %v", err)
			}
			before := foreign.Spec

			if _, err := newAdapter(c).Admit(ctx, in); !errors.Is(err, ErrForeignRun) {
				t.Fatalf("err = %v; want ErrForeignRun", err)
			}

			after := getRun(t, c, in.Namespace, name)
			if !reflect.DeepEqual(before, after.Spec) {
				t.Fatalf("foreign run was mutated: before %+v, after %+v", before, after.Spec)
			}
		})
	}
}

// TestAdmitGoalContextShaping pins how untrusted context is carried:
// the upstream issue is verbatim, an oversized goal is rejected before
// any object exists, and all inputs are trimmed before they feed the
// digest and the context.
func TestAdmitGoalContextShaping(t *testing.T) {
	ctx := context.Background()

	t.Run("upstream-issue-carried-verbatim", func(t *testing.T) {
		in := validAdmission()
		in.UpstreamIssue = "https://github.com/upstream/project/issues/42"
		c := newTestCluster(t)
		admitted, err := newAdapter(c).Admit(ctx, in)
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		payload := decodeContext(t, getRun(t, c, in.Namespace, admitted.RunName).Spec.SourceContext)
		if payload.UpstreamIssue != in.UpstreamIssue {
			t.Fatalf("context upstreamIssue = %q; want %q", payload.UpstreamIssue, in.UpstreamIssue)
		}
	})

	t.Run("oversized-goal-rejected-without-object", func(t *testing.T) {
		in := validAdmission()
		in.Goal = strings.Repeat("x", 20000)
		c := newTestCluster(t)
		_, err := newAdapter(c).Admit(ctx, in)
		if !errors.Is(err, ErrInvalidAdmission) {
			t.Fatalf("err = %v; want ErrInvalidAdmission", err)
		}
		if items := listRuns(t, c, in.Namespace); len(items) != 0 {
			t.Fatalf("oversized goal created %d runs; want 0", len(items))
		}
	})

	t.Run("inputs-trimmed-before-digest-and-context", func(t *testing.T) {
		padded := validAdmission()
		padded.Namespace = "  contrib  "
		padded.Lane = " local "
		padded.BaseRepo = " upstream/project "
		padded.BaseRef = " main "
		padded.WorkRepo = " contributor/project "
		padded.Goal = " fix the login flow "

		c := newTestCluster(t)
		paddedAdmitted, err := newAdapter(c).Admit(ctx, padded)
		if err != nil {
			t.Fatalf("Admit(padded): %v", err)
		}
		plainAdmitted, err := newAdapter(newTestCluster(t)).Admit(ctx, validAdmission())
		if err != nil {
			t.Fatalf("Admit(plain): %v", err)
		}
		if paddedAdmitted.WorkItemID != plainAdmitted.WorkItemID {
			t.Fatalf("padded and plain admissions minted different keys (%q vs %q); trimming must precede the digest",
				paddedAdmitted.WorkItemID, plainAdmitted.WorkItemID)
		}

		run := getRun(t, c, "contrib", paddedAdmitted.RunName)
		if run.Namespace != "contrib" || run.Spec.Lane != "local" {
			t.Errorf("namespace %q / lane %q; want the trimmed values", run.Namespace, run.Spec.Lane)
		}
		if run.Spec.Repo != "upstream/project" || run.Spec.WorkRepo != "contributor/project" {
			t.Errorf("repo %q / workRepo %q; want the trimmed values", run.Spec.Repo, run.Spec.WorkRepo)
		}
		payload := decodeContext(t, run.Spec.SourceContext)
		if payload.Goal != "fix the login flow" || payload.BaseRef != "main" {
			t.Errorf("context goal %q / baseRef %q; want the trimmed values", payload.Goal, payload.BaseRef)
		}
	})
}

// TestAdmitValidation is the admission validation table: every failure
// mode is rejected with ErrInvalidAdmission and creates nothing.
func TestAdmitValidation(t *testing.T) {
	ctx := context.Background()
	const namespace = "contrib"
	cases := []struct {
		name   string
		mutate func(*Admission)
	}{
		{"missing-namespace", func(in *Admission) { in.Namespace = "" }},
		{"blank-namespace", func(in *Admission) { in.Namespace = "   " }},
		{"missing-lane", func(in *Admission) { in.Lane = "" }},
		{"blank-lane", func(in *Admission) { in.Lane = " \t " }},
		{"missing-goal", func(in *Admission) { in.Goal = "" }},
		{"blank-goal", func(in *Admission) { in.Goal = "   " }},
		{"missing-base-ref", func(in *Admission) { in.BaseRef = "" }},
		{"blank-base-ref", func(in *Admission) { in.BaseRef = " " }},
		{"overlong-base-ref", func(in *Admission) { in.BaseRef = strings.Repeat("r", 256) }},
		{"whitespace-base-ref", func(in *Admission) { in.BaseRef = "main branch" }},
		{"leading-slash-base-ref", func(in *Admission) { in.BaseRef = "/main" }},
		{"trailing-slash-base-ref", func(in *Admission) { in.BaseRef = "main/" }},
		{"base-repo-no-slash", func(in *Admission) { in.BaseRepo = "noslash" }},
		{"base-repo-two-slashes", func(in *Admission) { in.BaseRepo = "a/b/c" }},
		{"base-repo-trailing-slash", func(in *Admission) { in.BaseRepo = "a/" }},
		{"work-repo-no-slash", func(in *Admission) { in.WorkRepo = "noslash" }},
		{"work-repo-two-slashes", func(in *Admission) { in.WorkRepo = "a/b/c" }},
		{"work-repo-trailing-slash", func(in *Admission) { in.WorkRepo = "a/" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCluster(t)
			in := validAdmission()
			tc.mutate(&in)
			_, err := newAdapter(c).Admit(ctx, in)
			if !errors.Is(err, ErrInvalidAdmission) {
				t.Fatalf("err = %v; want ErrInvalidAdmission", err)
			}
			if items := listRuns(t, c, namespace); len(items) != 0 {
				t.Fatalf("invalid admission created %d runs; want 0", len(items))
			}
		})
	}
}

// TestAdapterLifecycleNoOps pins the push-based contract: discovery
// yields nothing and every lifecycle verb is a side-effect-free no-op,
// even on a clientless adapter.
func TestAdapterLifecycleNoOps(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(nil)
	item := source.WorkItem{ID: "adhoc-000000000000"}

	items, err := a.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if items != nil {
		t.Errorf("Discover returned %d items; want nil", len(items))
	}
	if err := a.Claim(ctx, item); err != nil {
		t.Errorf("Claim: %v", err)
	}
	if err := a.Release(ctx, item); err != nil {
		t.Errorf("Release: %v", err)
	}
	for _, state := range []source.State{source.StateInProgress, source.StateInReview, source.StateNeedsHuman} {
		if err := a.Transition(ctx, item, state); err != nil {
			t.Errorf("Transition(%s): %v", state, err)
		}
	}
	if err := a.Resolve(ctx, item); err != nil {
		t.Errorf("Resolve: %v", err)
	}
}
