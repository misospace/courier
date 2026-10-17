package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
	"github.com/misospace/courier/internal/topology"
)

const secureTestNamespace = "courier-runs"

// fakeAdmissionForge implements the admission provider facets in memory.
type fakeAdmissionForge struct {
	repos        map[string]forge.Repository
	refs         map[string]forge.RefState
	prs          []forge.PullRequest
	issues       map[string]forge.WorkItem
	readErr      error
	issueReadErr error
}

func (f *fakeAdmissionForge) ResolveRepository(_ context.Context, name string) (forge.Repository, error) {
	if f.readErr != nil {
		return forge.Repository{}, f.readErr
	}
	repo, ok := f.repos[name]
	if !ok {
		return forge.Repository{}, errors.New("unknown repository")
	}
	return repo, nil
}

func (f *fakeAdmissionForge) ReadRef(_ context.Context, repo forge.Repository, ref string) (forge.RefState, error) {
	if f.readErr != nil {
		return forge.RefState{}, f.readErr
	}
	return f.refs[repo.Canonical+":"+ref], nil
}

func (f *fakeAdmissionForge) ReadEffectiveProtection(context.Context, forge.Repository, string) (forge.Protection, error) {
	return forge.Protection{Protected: false}, nil
}

func (f *fakeAdmissionForge) CanWriteRepository(context.Context, forge.Repository) (bool, error) {
	return true, nil
}

func (f *fakeAdmissionForge) ReadWorkItem(_ context.Context, ref forge.WorkItemRef) (forge.WorkItem, error) {
	if f.issueReadErr != nil {
		return forge.WorkItem{}, f.issueReadErr
	}
	item, ok := f.issues[ref.Repo+"#"+ref.ID]
	if !ok {
		return forge.WorkItem{}, errors.New("work item not found")
	}
	return item, nil
}

func (f *fakeAdmissionForge) Config() forge.ProviderConfig {
	return forge.ProviderConfig{Name: "github", Endpoint: "https://api.github.com/"}
}

func (f *fakeAdmissionForge) Capabilities() forge.Capabilities {
	return forge.NewCapabilities(forge.CapabilityReadPullRequest, forge.CapabilityCreatePullRequest, forge.CapabilityUpdatePullRequest)
}

func (f *fakeAdmissionForge) ReadPullRequest(_ context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	for _, pr := range f.prs {
		if pr.Number == ref.Number && pr.BaseRepo == ref.Repo {
			return pr, nil
		}
	}
	return forge.PullRequest{}, errors.New("pull request not found")
}

func (f *fakeAdmissionForge) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) CreatePullRequest(context.Context, forge.CreatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) UpdatePullRequest(context.Context, forge.PullRequestRef, forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	return forge.Comment{}, forge.ErrUnsupported
}

func (f *fakeAdmissionForge) ListPullRequestsByHead(_ context.Context, base forge.PullRequestRef, repo, ref string) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, pr := range f.prs {
		if pr.BaseRepo == base.Repo && pr.HeadRepo == repo && pr.HeadRef == ref {
			out = append(out, pr)
		}
	}
	return out, nil
}

func fakeProviderFactory(provider *fakeAdmissionForge) ProviderFactory {
	return func(context.Context, *forge.Registration) (AdmissionProvider, error) {
		return AdmissionProvider{Provider: provider, Policy: provider, HeadLister: provider}, nil
	}
}

func secureRegistry() *forge.Registry {
	data := []byte(`{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["*"]}]}`)
	registry, err := forge.Load(data, "github")
	if err != nil {
		panic(err)
	}
	return registry
}

func secureRun(name string) *courier.CoderRun {
	run := &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: secureTestNamespace,
			UID:       types.UID("uid-" + name),
		},
		Spec: courier.CoderRunSpec{
			Mode:       courier.ModeResolveIssue,
			Source:     "manual",
			WorkItemID: "item-1",
			Repo:       "acme/widgets",
			Ref:        7,
			Lane:       "local",
		},
	}
	return run
}

func secureForge() *fakeAdmissionForge {
	baseRepo := forge.Repository{ID: "repo-1", Canonical: "Acme/Widgets", DefaultRef: "main"}
	return &fakeAdmissionForge{
		repos: map[string]forge.Repository{"acme/widgets": baseRepo, "Acme/Widgets": baseRepo},
		refs: map[string]forge.RefState{
			"Acme/Widgets:main":                         {Ref: "main", OID: "baseoid", Exists: true},
			"Acme/Widgets:courier/Acme/Widgets/issue-7": {Ref: "courier/Acme/Widgets/issue-7", Exists: false},
		},
		issues: map[string]forge.WorkItem{
			"Acme/Widgets#7": {ID: "7", Repo: "Acme/Widgets", Title: "Add the widget", Body: "## Problem\nThe widget is missing.", State: "open"},
		},
	}
}

func secureControl(t *testing.T, objects ...client.Object) (*SecureControl, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, networkingv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	// The lane every test run names; the launcher builds the control pod's
	// run context from it.
	lane := &courier.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: secureTestNamespace, Name: "local"},
		Spec: courier.LaneProfileSpec{
			Roles: map[string]string{
				"coordinator": "test/coordinator",
				"coder":       "test/coder",
			},
		},
	}
	objects = append([]client.Object{lane}, objects...)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courier.CoderRun{}).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				// Stand in for the API server: assign UIDs on create and
				// reject the known-bad dry-run specs the preflight uses to
				// prove restricted admission.
				if obj.GetUID() == "" {
					obj.SetUID(types.UID("generated-" + obj.GetName()))
				}
				if strings.Contains(obj.GetName(), "-bad-dryrun") {
					return errors.New("admission denied: known-bad pod spec")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	control := &SecureControl{
		Client:  c,
		Patcher: &CoderRunReconciler{Client: c, StatusWriter: fakeStatusWriter{client: c}},
		Config: SecureConfig{
			RunNamespace:      secureTestNamespace,
			Registry:          secureRegistry(),
			HarnessImage:      "harness:test",
			ProbeImage:        "busybox:test",
			LiveProbes:        false,
			OperatorNamespace: "courier-system",
			Providers:         fakeProviderFactory(secureForge()),
			Kubernetes:        fakeAccessReviews(),
			Now:               func() time.Time { return time.Now() },
		},
	}
	return control, c
}

// fakeAccessReviews answers every access review with allowed, standing in
// for the RBAC authorizer.
func fakeAccessReviews() *fakek8s.Clientset {
	clientset := fakek8s.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SelfSubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
		}, nil
	})
	clientset.Fake.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
		}, nil
	})
	return clientset
}

func TestResolveAdmissionResolveIssue(t *testing.T) {
	control, _ := secureControl(t)
	run := secureRun("resolve")
	registration, policy, err := control.ResolveAdmission(context.Background(), run)
	if err != nil {
		t.Fatalf("ResolveAdmission: %v", err)
	}
	if registration.Name != "github" {
		t.Fatalf("registration = %q", registration.Name)
	}
	if policy.RunUID != string(run.UID) || policy.ProviderConfigRef != "github" {
		t.Fatalf("policy identity = %+v", policy)
	}
	if policy.BaseRepo != "Acme/Widgets" {
		t.Fatalf("base repo must be canonical: %q", policy.BaseRepo)
	}
	if policy.BaseRef != "main" || policy.BaseOID != "baseoid" {
		t.Fatalf("base = %q/%q", policy.BaseRef, policy.BaseOID)
	}
	if policy.WorkRepo != "Acme/Widgets" || policy.WorkRef != "courier/Acme/Widgets/issue-7" {
		t.Fatalf("work = %q/%q", policy.WorkRepo, policy.WorkRef)
	}
	if !policy.WorkInitiallyAbsent || policy.WorkOID != "" {
		t.Fatalf("absent work ref must be recorded as initially absent: %+v", policy)
	}
	if policy.ProviderEndpoint != "https://api.github.com/" || policy.CredentialRefDigest == "" {
		t.Fatalf("provider binding fields = %q %q", policy.ProviderEndpoint, policy.CredentialRefDigest)
	}
	// The trusted source-issue read pins the identity and an explicit empty
	// set: this issue body carries no Acceptance section.
	if policy.SourceIssue == nil || policy.SourceIssue.Owner != "Acme" || policy.SourceIssue.Name != "Widgets" || policy.SourceIssue.Number != 7 {
		t.Fatalf("source issue pin = %+v, want Acme/Widgets#7", policy.SourceIssue)
	}
	if len(policy.AcceptanceCriteria) != 0 || policy.AcceptanceCriteriaDigest == "" {
		t.Fatalf("criteria pin = %+v digest %q, want an explicit empty set with a digest", policy.AcceptanceCriteria, policy.AcceptanceCriteriaDigest)
	}
}

func TestResolveAdmissionFixPR(t *testing.T) {
	control, _ := secureControl(t)
	provider := &fakeAdmissionForge{
		repos: map[string]forge.Repository{
			"acme/widgets": {ID: "repo-1", Canonical: "Acme/Widgets", DefaultRef: "main"},
			"contrib/fork": {ID: "repo-2", Canonical: "Contrib/Fork", DefaultRef: "main"},
		},
		refs: map[string]forge.RefState{
			"Acme/Widgets:main": {Ref: "main", OID: "baseoid", Exists: true},
		},
		prs: []forge.PullRequest{{
			Number: 7, BaseRepo: "Acme/Widgets", BaseRef: "main", BaseSHA: "baseoid",
			HeadRepo: "contrib/fork", HeadRef: "patch-1", HeadSHA: "anchoroid",
		}},
	}
	control.Config.Providers = fakeProviderFactory(provider)
	run := secureRun("fixpr")
	run.Spec.Mode = courier.ModeFixPR
	_, policy, err := control.ResolveAdmission(context.Background(), run)
	if err != nil {
		t.Fatalf("ResolveAdmission: %v", err)
	}
	if policy.WorkRepo != "Contrib/Fork" || policy.WorkRef != "patch-1" {
		t.Fatalf("work identity = %q/%q", policy.WorkRepo, policy.WorkRef)
	}
	if policy.PRNumber != 7 || policy.HeadAnchorOID != "anchoroid" || policy.WorkOID != "anchoroid" {
		t.Fatalf("fix-pr anchors = %+v", policy)
	}
	if policy.WorkInitiallyAbsent {
		t.Fatal("a fix-pr head always exists")
	}
	if policy.SourceIssue != nil || len(policy.AcceptanceCriteria) > 0 || policy.AcceptanceCriteriaDigest != "" {
		t.Fatalf("a fix-pr run pins nothing: %+v", policy)
	}
}

// criteriaForge serves a resolve-issue run against the miso-gallery#502
// fixture body so admission tests pin the real dogfood material (#280).
func criteriaForge(t *testing.T, body string) *fakeAdmissionForge {
	t.Helper()
	repo := forge.Repository{ID: "repo-1", Canonical: "misospace/miso-gallery", DefaultRef: "main"}
	return &fakeAdmissionForge{
		repos: map[string]forge.Repository{"misospace/miso-gallery": repo},
		refs: map[string]forge.RefState{
			"misospace/miso-gallery:main":                                     {Ref: "main", OID: "baseoid", Exists: true},
			"misospace/miso-gallery:courier/misospace/miso-gallery/issue-502": {Ref: "courier/misospace/miso-gallery/issue-502", Exists: false},
		},
		issues: map[string]forge.WorkItem{
			"misospace/miso-gallery#502": {ID: "502", Repo: "misospace/miso-gallery", Title: "Package the tracked assets directory", Body: body, State: "open"},
		},
	}
}

func criteriaRun() *courier.CoderRun {
	run := secureRun("criteria")
	run.Spec.Repo = "misospace/miso-gallery"
	run.Spec.Ref = 502
	return run
}

func criteriaControl(t *testing.T, forge *fakeAdmissionForge) *SecureControl {
	t.Helper()
	control, _ := secureControl(t)
	control.Config.Providers = fakeProviderFactory(forge)
	return control
}

// Admission pins the live miso-gallery#502 issue body exactly: the three
// Acceptance bullets become AC-1..AC-3 with digests the shared pure
// extraction re-derives, and re-running admission on the same issue revision
// yields the same set digest (restart idempotency).
func TestResolveAdmissionPinsCriteria(t *testing.T) {
	body, err := os.ReadFile("../../internal/broker/testdata/miso-gallery-502-issue.md")
	if err != nil {
		t.Fatalf("read 502 fixture: %v", err)
	}
	control := criteriaControl(t, criteriaForge(t, string(body)))
	run := criteriaRun()

	_, policy, err := control.ResolveAdmission(context.Background(), run)
	if err != nil {
		t.Fatalf("ResolveAdmission: %v", err)
	}
	if policy.SourceIssue == nil || policy.SourceIssue.Owner != "misospace" || policy.SourceIssue.Name != "miso-gallery" || policy.SourceIssue.Number != 502 {
		t.Fatalf("source issue pin = %+v, want misospace/miso-gallery#502", policy.SourceIssue)
	}
	expected, err := broker.ExtractCriteria(string(body), broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502})
	if err != nil {
		t.Fatalf("re-derive expected set: %v", err)
	}
	if policy.AcceptanceCriteriaDigest != expected.Digest {
		t.Fatalf("set digest = %q, want the pure extraction's %q", policy.AcceptanceCriteriaDigest, expected.Digest)
	}
	if len(policy.AcceptanceCriteria) != len(expected.Criteria) {
		t.Fatalf("pinned %d criteria, want %d", len(policy.AcceptanceCriteria), len(expected.Criteria))
	}
	for i, criterion := range policy.AcceptanceCriteria {
		want := expected.Criteria[i]
		if criterion.ID != want.ID || criterion.Text != want.Text || criterion.Digest != want.Digest {
			t.Fatalf("criterion %d = %+v, want %+v", i, criterion, want)
		}
	}
	if len(policy.AcceptanceCriteria) != 3 || policy.AcceptanceCriteria[0].ID != "AC-1" || policy.AcceptanceCriteria[2].ID != "AC-3" {
		t.Fatalf("502 must pin AC-1..AC-3, got %+v", policy.AcceptanceCriteria)
	}

	// The same issue revision admitted again pins the identical digest.
	_, again, err := control.ResolveAdmission(context.Background(), criteriaRun())
	if err != nil {
		t.Fatalf("re-admission: %v", err)
	}
	if again.AcceptanceCriteriaDigest != policy.AcceptanceCriteriaDigest {
		t.Fatalf("re-admission digest = %q, want the stable %q", again.AcceptanceCriteriaDigest, policy.AcceptanceCriteriaDigest)
	}
}

// An issue whose body carries no Acceptance section (or a bullet-less one)
// pins an explicit empty set — a legitimate pin, not a failure.
func TestResolveAdmissionPinsEmptyCriteriaSet(t *testing.T) {
	tests := map[string]string{
		"no acceptance section": "## Problem\nSomething is broken.\n\n## Evidence\n- observed locally\n",
		"bullet-less section":   "## Acceptance\nProse only, no bullets to pin.\n",
		"empty body":            "",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			control := criteriaControl(t, criteriaForge(t, body))
			_, policy, err := control.ResolveAdmission(context.Background(), criteriaRun())
			if err != nil {
				t.Fatalf("ResolveAdmission: %v", err)
			}
			if policy.SourceIssue == nil {
				t.Fatal("the source-issue identity is pinned even for an empty set")
			}
			if len(policy.AcceptanceCriteria) != 0 || policy.AcceptanceCriteriaDigest == "" {
				t.Fatalf("criteria = %+v digest %q, want an empty set with a digest", policy.AcceptanceCriteria, policy.AcceptanceCriteriaDigest)
			}
		})
	}
}

// Ambiguous or over-bound bodies fail closed at admission: an unpinnable
// contract must not become an unchecked one, and an over-bound set is never
// truncated.
func TestResolveAdmissionCriteriaFailClosed(t *testing.T) {
	tooMany := &strings.Builder{}
	tooMany.WriteString("## Acceptance\n")
	for i := 0; i < broker.MaxCriteria+1; i++ {
		fmt.Fprintf(tooMany, "- criterion %d\n", i+1)
	}
	overLong := "## Acceptance\n- " + strings.Repeat("x", broker.MaxCriterionTextBytes+1) + "\n"

	tests := map[string]struct {
		body   string
		reason string
	}{
		"ambiguous sections": {
			body:   "## Acceptance\n- one\n\n## Notes\n\n## Acceptance\n- two\n",
			reason: "CriteriaUnpinnable",
		},
		"over-bound set": {
			body:   tooMany.String(),
			reason: "CriteriaUnpinnable",
		},
		"over-long criterion": {
			body:   overLong,
			reason: "CriteriaUnpinnable",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			control := criteriaControl(t, criteriaForge(t, test.body))
			_, _, err := control.ResolveAdmission(context.Background(), criteriaRun())
			var permanent *SecureNeedsHumanError
			if !errors.As(err, &permanent) || permanent.Reason != test.reason {
				t.Fatalf("error = %v, want NeedsHuman %s", err, test.reason)
			}
			if !strings.Contains(permanent.Detail, "criteria extraction") {
				t.Fatalf("NeedsHuman detail must name why: %q", permanent.Detail)
			}
		})
	}

	t.Run("issue read failure is transient", func(t *testing.T) {
		provider := criteriaForge(t, "## Acceptance\n- one\n")
		provider.issueReadErr = errors.New("transport hiccup")
		control := criteriaControl(t, provider)
		_, _, err := control.ResolveAdmission(context.Background(), criteriaRun())
		if err == nil {
			t.Fatal("a failed trusted read must fail admission")
		}
		var permanent *SecureNeedsHumanError
		if errors.As(err, &permanent) {
			t.Fatalf("transport failure must be transient, got NeedsHuman: %v", err)
		}
	})

	t.Run("unsupported issue read is a capability failure", func(t *testing.T) {
		provider := criteriaForge(t, "## Acceptance\n- one\n")
		provider.issueReadErr = forge.ErrUnsupported
		control := criteriaControl(t, provider)
		_, _, err := control.ResolveAdmission(context.Background(), criteriaRun())
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "ProviderCapability" {
			t.Fatalf("error = %v, want NeedsHuman ProviderCapability", err)
		}
	})

	// The response's identity is checked against the request before
	// anything is pinned: a provider returning another issue's number, a
	// number-less answer, or a different repository must never stand in for
	// the trusted source issue.
	identityMismatch := map[string]forge.WorkItem{
		"wrong issue number": {ID: "13", Repo: "misospace/miso-gallery", Body: "## Acceptance\n- someone else's criterion\n"},
		"number-less answer": {ID: "", Repo: "misospace/miso-gallery", Body: "## Acceptance\n- someone else's criterion\n"},
		"wrong repository":   {ID: "502", Repo: "other/repo", Body: "## Acceptance\n- someone else's criterion\n"},
	}
	for name, item := range identityMismatch {
		t.Run(name, func(t *testing.T) {
			provider := criteriaForge(t, "## Acceptance\n- one\n")
			provider.issues["misospace/miso-gallery#502"] = item
			control := criteriaControl(t, provider)
			_, _, err := control.ResolveAdmission(context.Background(), criteriaRun())
			var permanent *SecureNeedsHumanError
			if !errors.As(err, &permanent) || permanent.Reason != "ProviderRead" {
				t.Fatalf("error = %v, want NeedsHuman ProviderRead", err)
			}
		})
	}
}

func TestResolveAdmissionFailsClosed(t *testing.T) {
	t.Run("namespace boundary", func(t *testing.T) {
		control, _ := secureControl(t)
		run := secureRun("elsewhere")
		run.Namespace = "default"
		_, _, err := control.ResolveAdmission(context.Background(), run)
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "RunNamespace" {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("closed pull request on the head", func(t *testing.T) {
		control, _ := secureControl(t)
		provider := secureForge()
		provider.prs = []forge.PullRequest{{
			Number: 3, BaseRepo: "Acme/Widgets", HeadRepo: "Acme/Widgets",
			HeadRef: "courier/Acme/Widgets/issue-7", State: "closed", Merged: true,
		}}
		control.Config.Providers = fakeProviderFactory(provider)
		_, _, err := control.ResolveAdmission(context.Background(), secureRun("closedpr"))
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "HeadAlreadyMerged" {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("ambiguous selection", func(t *testing.T) {
		control, _ := secureControl(t)
		data := []byte(`{"providers":[
			{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["acme/*"]},
			{"name":"b","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*"]}
		]}`)
		registry, err := forge.Load(data, "github")
		if err != nil {
			t.Fatal(err)
		}
		control.Config.Registry = registry
		_, _, err = control.ResolveAdmission(context.Background(), secureRun("ambiguous"))
		var permanent *SecureNeedsHumanError
		if !errors.As(err, &permanent) || permanent.Reason != "ProviderSelection" {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestVerifyPersistedPolicyFencesUID(t *testing.T) {
	run := secureRun("bound")
	run.Status.PublicationPolicy = &courier.PublicationPolicy{RunUID: string(run.UID), ProviderConfigRef: "github"}
	if _, err := VerifyPersistedPolicy(run); err != nil {
		t.Fatalf("persisted policy verified: %v", err)
	}
	run.Status.PublicationPolicy.RunUID = "another-incarnation"
	if _, err := VerifyPersistedPolicy(run); err == nil {
		t.Fatal("a policy bound to another UID must be refused")
	}
}

func TestLaunchSecureProvisionsTopology(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("topology")
	run.Status.Phase = courier.PhaseClaimed
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:              string(run.UID),
		ProviderConfigRef:   "github",
		ProviderEndpoint:    "https://api.github.com/",
		CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
		BaseRepo:            "Acme/Widgets",
		BaseRef:             "main",
		BaseOID:             "baseoid",
		WorkRepo:            "Acme/Widgets",
		WorkRef:             "courier/Acme/Widgets/issue-7",
		WorkInitiallyAbsent: true,
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// The dedicated run namespace with enforced restricted Pod Security.
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: secureTestNamespace,
			Labels: map[string]string{
				"pod-security.kubernetes.io/enforce": "restricted",
			},
		},
	}
	if err := c.Create(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	// Credential sources for the per-run copy.
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}
	if err := c.Create(context.Background(), creds); err != nil {
		t.Fatal(err)
	}

	result, err := control.LaunchSecure(context.Background(), run)
	if err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Fatalf("provisioning returned a requeue: %#v", result)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(secureTestNamespace)); err != nil {
		t.Fatal(err)
	}
	components := map[string]*corev1.Pod{}
	for i := range pods.Items {
		components[pods.Items[i].Labels["courier.misospace.dev/component"]] = &pods.Items[i]
	}
	for _, want := range []string{topology.ComponentCoordinator, topology.ComponentWorker, topology.ComponentBroker} {
		if components[want] == nil {
			t.Fatalf("topology is missing the %s pod", want)
		}
	}
	// The worker must carry no identity material and must be bound to the
	// control incarnation.
	worker := components[topology.ComponentWorker]
	for _, v := range worker.Spec.Volumes {
		if v.Secret != nil || v.Projected != nil {
			t.Fatalf("worker pod mounts %q", v.Name)
		}
	}
	incarnation := ""
	for _, env := range worker.Spec.Containers[0].Env {
		if env.Name == topology.EnvControlPodUID {
			incarnation = env.Value
		}
	}
	if incarnation == "" {
		t.Fatal("worker pod carries no control incarnation binding")
	}
	controlPod := components[topology.ComponentCoordinator]
	controlIncarnation := ""
	workerUID := ""
	for _, env := range controlPod.Spec.Containers[0].Env {
		if env.Name == topology.EnvControlPodUID {
			controlIncarnation = env.Value
		}
		if env.Name == topology.EnvWorkerPodUID {
			workerUID = env.Value
		}
	}
	if controlIncarnation != incarnation {
		t.Fatalf("control incarnation %q != worker binding %q", controlIncarnation, incarnation)
	}
	if workerUID != string(components[topology.ComponentWorker].UID) {
		t.Fatal("control pod is not bound to the live worker pod UID")
	}
	for _, env := range controlPod.Spec.Containers[0].Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("control pod env %q references a Secret", env.Name)
		}
	}
	// The finalizer was added.
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if !containsString(updated.Finalizers, topology.FinalizerName) {
		t.Fatal("secure topology finalizer missing")
	}
	// A second launch is a no-op: identities must not rotate on idempotent relaunch.
	_, err = control.LaunchSecure(context.Background(), &updated)
	if err != nil {
		t.Fatalf("idempotent relaunch: %v", err)
	}
	// The control pod carries the run context the native harness reconstructs.
	foundGoal := false
	for _, env := range controlPod.Spec.Containers[0].Env {
		if env.Name == "COURIER_GOAL" && env.Value != "" {
			foundGoal = true
		}
	}
	if !foundGoal {
		t.Fatal("control pod carries no run-context goal")
	}
}

// The deployment-level model gateway is rendered into trusted control only:
// the URL is env, the key is a per-run Secret copy mounted as a file, and
// runs without gateway configuration carry no gateway material at all.
func TestLaunchSecureRendersModelGateway(t *testing.T) {
	newLaunch := func(t *testing.T, control *SecureControl, c client.Client, name string) *corev1.Pod {
		t.Helper()
		run := secureRun(name)
		run.Status.Phase = courier.PhaseClaimed
		run.Status.PublicationPolicy = &courier.PublicationPolicy{
			RunUID:              string(run.UID),
			ProviderConfigRef:   "github",
			ProviderEndpoint:    "https://api.github.com/",
			CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
			BaseRepo:            "Acme/Widgets",
			BaseRef:             "main",
			BaseOID:             "baseoid",
			WorkRepo:            "Acme/Widgets",
			WorkRef:             "courier/Acme/Widgets/issue-7",
			WorkInitiallyAbsent: true,
		}
		if err := c.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		if err := c.Create(context.Background(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: secureTestNamespace, Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := c.Create(context.Background(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
			Data:       map[string][]byte{"token": []byte("tok-123")},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := control.LaunchSecure(context.Background(), run); err != nil {
			t.Fatalf("LaunchSecure: %v", err)
		}
		pods := &corev1.PodList{}
		if err := c.List(context.Background(), pods, client.InNamespace(secureTestNamespace)); err != nil {
			t.Fatal(err)
		}
		for i := range pods.Items {
			if pods.Items[i].Labels["courier.misospace.dev/component"] == topology.ComponentCoordinator {
				return &pods.Items[i]
			}
		}
		t.Fatal("no control pod provisioned")
		return nil
	}

	t.Run("gateway configured", func(t *testing.T) {
		control, c := secureControl(t)
		control.Config.GatewayURL = "http://gateway:4000/v1"
		control.Config.GatewayKeySecret = "courier-system/gateway-key"
		if err := c.Create(context.Background(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "gateway-key"},
			Data:       map[string][]byte{"key": []byte("gw-secret-value")},
		}); err != nil {
			t.Fatal(err)
		}
		controlPod := newLaunch(t, control, c, "gateway")
		url, keyFile := "", ""
		for _, env := range controlPod.Spec.Containers[0].Env {
			switch env.Name {
			case topology.EnvGatewayURL:
				url = env.Value
			case topology.EnvGatewayKeyFile:
				keyFile = env.Value
			}
		}
		if url != "http://gateway:4000/v1" || keyFile == "" {
			t.Fatalf("gateway env = url %q key file %q", url, keyFile)
		}
		gatewaySecret := &corev1.Secret{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: secureTestNamespace, Name: topology.GatewaySecretName("gateway")}, gatewaySecret); err != nil {
			t.Fatalf("per-run gateway key copy missing: %v", err)
		}
		if string(gatewaySecret.Data[topology.GatewayKeyName]) != "gw-secret-value" {
			t.Fatal("gateway key copy does not carry the deployment key value")
		}
	})

	t.Run("gateway unconfigured", func(t *testing.T) {
		control, c := secureControl(t)
		controlPod := newLaunch(t, control, c, "nogateway")
		for _, env := range controlPod.Spec.Containers[0].Env {
			if env.Name == topology.EnvGatewayURL {
				t.Fatal("gateway URL must be absent without gateway configuration")
			}
		}
		secrets := &corev1.SecretList{}
		if err := c.List(context.Background(), secrets, client.InNamespace(secureTestNamespace)); err != nil {
			t.Fatal(err)
		}
		for i := range secrets.Items {
			if secrets.Items[i].Name == topology.GatewaySecretName("nogateway") {
				t.Fatal("no gateway key copy may be provisioned without gateway configuration")
			}
		}
	})
}

func TestObserveTopologyFencesBrokenWorker(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("fence")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	workerPod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(context.Background(), workerPod); err != nil {
		t.Fatal(err)
	}

	pods := listRunPods(t, c, run)
	_, handled, err := control.ObserveTopology(context.Background(), run, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("a failed worker must be handled as infrastructure failure")
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 1 {
		t.Fatalf("fence relaunch state = phase %q restarts %d", updated.Status.Phase, updated.Status.Restarts)
	}
}

// brokenWorkerSecureRound stands up a Running secure round whose worker has
// definitively failed — the fixture a broken-topology relaunch is charged
// from — and returns it with its control and client before any observation.
func brokenWorkerSecureRound(t *testing.T, name string, restarts int) (*SecureControl, client.Client, *courier.CoderRun) {
	t.Helper()
	control, c := secureControl(t)
	run := secureRun(name)
	run.Status.Phase = courier.PhaseRunning
	run.Status.Restarts = restarts
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	workerPod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(context.Background(), workerPod); err != nil {
		t.Fatal(err)
	}
	return control, c, run
}

// TestObserveTopologyBrokenWorkerCeilingIsConfigured pins #216: the secure
// broken-topology relaunch bounds against the configured ceiling, not the
// package default. A configured ceiling of 1 terminalizes a repeatedly
// broken topology the package default would still relaunch; with no
// configured ceiling the same round relaunches, so the terminalization can
// only come from the configured value.
func TestObserveTopologyBrokenWorkerCeilingIsConfigured(t *testing.T) {
	t.Run("ConfiguredCeilingTerminalizesBelowPackageDefault", func(t *testing.T) {
		control, c, run := brokenWorkerSecureRound(t, "ceiling-configured", 1)
		control.MaxRestarts = 1 // below defaultMaxRestarts, at the run's counter.
		// The ceiling hand-off terminalizes through the patcher, which needs
		// the source registry the plain secure helper does not wire.
		control.Patcher = &CoderRunReconciler{
			Client:       c,
			Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
			StatusWriter: fakeStatusWriter{client: c},
		}
		_, handled, err := control.ObserveTopology(context.Background(), run, listRunPods(t, c, run))
		if err != nil || !handled {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
		var updated courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.Status.Phase != courier.PhaseNeedsHuman {
			t.Fatalf("phase = %q, want NeedsHuman from the configured ceiling; the package default must not bound this path", updated.Status.Phase)
		}
		if updated.Status.Restarts != 1 {
			t.Fatalf("restarts = %d, want 1 (ceiling is preserved, not incremented)", updated.Status.Restarts)
		}
	})
	t.Run("PackageDefaultRelaunchesAboveConfiguredCounter", func(t *testing.T) {
		control, c, run := brokenWorkerSecureRound(t, "ceiling-default", 1)
		_, handled, err := control.ObserveTopology(context.Background(), run, listRunPods(t, c, run))
		if err != nil || !handled {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
		var updated courier.CoderRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 2 {
			t.Fatalf("relaunch state = phase %q restarts %d, want Claimed with the counter incremented under the package default", updated.Status.Phase, updated.Status.Restarts)
		}
	})
}

func TestObserveTopologyFencesWorkerOnControlTermination(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("term")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	controlPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2}},
	}}
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	_, handled, err := control.ObserveTopology(context.Background(), run, listRunPods(t, c, run))
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	var worker corev1.Pod
	err = c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("worker pod must be fenced on control termination, got %v", err)
	}
}

func TestRevokeIsOrderedAndIdempotent(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("revoke")
	now := metav1.Now()
	run.DeletionTimestamp = &now
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	for _, pod := range []*corev1.Pod{healthyPod(run.Name, topology.ComponentCoordinator), healthyPod(run.Name, topology.ComponentWorker), healthyPod(run.Name, topology.ComponentBroker)} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	// The broker Service exists with the pod: revocation must delete it
	// before the pod (ingress first).
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}
	// The per-run gateway credential copy exists: revocation must remove it
	// with the rest of the run-only material — a model-provider credential
	// may never outlive the run.
	gatewaySecret, err := topology.GatewaySecret(run, []byte("gw-secret-value"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), gatewaySecret); err != nil {
		t.Fatal(err)
	}

	done, err := control.Revoke(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("revocation cannot be done while pods exist")
	}
	// Simulate kubelet completing pod termination: fake client removes them.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		if err := c.Delete(context.Background(), &pods.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		var err error
		done, err = control.Revoke(context.Background(), run)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("revocation did not complete within five passes")
	}
	// Everything must be gone, and a rerun stays done.
	remaining := &corev1.PodList{}
	if err := c.List(context.Background(), remaining, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(remaining.Items) != 0 {
		t.Fatalf("topology pods survived revocation: %d", len(remaining.Items))
	}
	services := &corev1.ServiceList{}
	if err := c.List(context.Background(), services, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(services.Items) != 0 {
		t.Fatalf("services survived revocation: %d", len(services.Items))
	}
	// The model-provider gateway credential copy must be gone with the rest
	// of the run-only material.
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: topology.GatewaySecretName(run.Name)}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("gateway credential copy survived revocation: %v", err)
	}
	if done, err := control.Revoke(context.Background(), run); err != nil || !done {
		t.Fatalf("rerun of a completed revocation = done=%v err=%v", done, err)
	}
}

func healthyPod(runName, component string) *corev1.Pod {
	name := topology.ControlPodName(runName)
	switch component {
	case topology.ComponentWorker:
		name = topology.WorkerPodName(runName)
	case topology.ComponentBroker:
		name = topology.BrokerPodName(runName)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: secureTestNamespace,
			UID:       types.UID(name + "-uid"),
			Labels:    topology.Labels(runName, component),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

func listRunPods(t *testing.T, c client.Client, run *courier.CoderRun) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	out := make([]corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if pods.Items[i].Labels[topology.LabelRun] == run.Name {
			out = append(out, pods.Items[i])
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

var _ = ctrl.Result{}
var _ = strings.TrimSpace

func TestProbePodsAreExcludedFromWorkloadLookups(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("probe-exclude")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// A leftover probe pod from a crashed preflight pass: worker labels, but
	// marked as a probe.
	probe := topology.ProbePod(run, "node-a", "busybox:test", "true")
	if probe.Labels[topology.LabelProbe] != "true" {
		t.Fatal("probe pods must carry the probe label")
	}
	if err := c.Create(context.Background(), probe); err != nil {
		t.Fatal(err)
	}
	if pod, err := control.pod(context.Background(), run, topology.ComponentWorker); err != nil || pod != nil {
		t.Fatalf("a probe pod must not satisfy the worker lookup, got %v, %v", pod, err)
	}
	if control.topologyPodsExist(context.Background(), run) {
		t.Fatal("probe pods must not make the topology look provisioned")
	}
	// The real worker is still found.
	if err := c.Create(context.Background(), healthyPod(run.Name, topology.ComponentWorker)); err != nil {
		t.Fatal(err)
	}
	pod, err := control.pod(context.Background(), run, topology.ComponentWorker)
	if err != nil || pod == nil {
		t.Fatalf("real worker lookup failed: %v, %v", pod, err)
	}
}

func TestProbeCoverageIsIncompleteWithoutEveryNode(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("probe-coverage")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// Two eligible nodes; only node-a produced a complete result set (two
	// checks), node-b's probe crashed without a readable result.
	for _, node := range []string{"node-a", "node-b"} {
		if err := c.Create(context.Background(), topology.ProbePod(run, node, "busybox:test", "true")); err != nil {
			t.Fatal(err)
		}
	}
	a := topology.ProbePod(run, "node-a", "busybox:test", "true")
	a.Status.Phase = corev1.PodSucceeded
	a.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "probe",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "{\"name\":\"api-server\",\"got\":\"deny\"}\n{\"name\":\"cluster-dns\",\"got\":\"deny\"}\n"}},
	}}
	if err := c.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	b := topology.ProbePod(run, "node-b", "busybox:test", "true")
	b.Status.Phase = corev1.PodFailed
	b.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "probe",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: ""}},
	}}
	if err := c.Status().Update(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	_, pending, err := control.collectProbeResults(context.Background(), run, []string{"node-a", "node-b"}, 2)
	if err == nil {
		t.Fatal("incomplete probe coverage must not be treated as evidence")
	}
	if pending {
		t.Fatal("both probes are terminal, so nothing is pending")
	}
}

// TestPublicationPolicyPersistsEmptyToSetOnly drives resolvePersistSecurePolicy
// through the real production patch path — patchStatus into a
// status.KubePatchWriter issuing a JSON merge patch against the API client,
// exactly as cmd/main.go wires it — and proves the set-once property: a
// persisted policy is never rewritten or cleared by a later patch.
func TestPublicationPolicyPersistsEmptyToSetOnly(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, networkingv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courier.CoderRun{}).Build()
	control, _ := secureControl(t)
	reconciler := &CoderRunReconciler{
		Client:       c,
		Secure:       control,
		StatusWriter: status.KubePatchWriter{Client: c},
	}
	run := secureRun("policy-persist")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	// Empty→set: the resolved policy persists through the production writer.
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
	if persisted.Status.PublicationPolicy.ProviderConfigRef != "github" || persisted.Status.PublicationPolicy.RunUID != string(run.UID) {
		t.Fatalf("persisted policy = %+v", persisted.Status.PublicationPolicy)
	}

	// Set→mutated: a later patch carrying a different policy must not
	// rewrite the persisted one.
	before := persisted.DeepCopy()
	mutated := persisted.DeepCopy()
	mutated.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:            string(run.UID),
		ProviderConfigRef: "evil-provider",
		ProviderEndpoint:  "https://evil.example.test/",
		BaseRepo:          "Acme/Widgets",
	}
	if err := reconciler.patchStatus(context.Background(), before, mutated); err != nil {
		t.Fatalf("patchStatus with a mutated policy: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy.ProviderConfigRef != "github" {
		t.Fatalf("persisted policy was rewritten to %q", persisted.Status.PublicationPolicy.ProviderConfigRef)
	}

	// Set→cleared: a patch that drops the in-memory policy must not clear it.
	before = persisted.DeepCopy()
	cleared := persisted.DeepCopy()
	cleared.Status.PublicationPolicy = nil
	if err := reconciler.patchStatus(context.Background(), before, cleared); err != nil {
		t.Fatalf("patchStatus with a cleared policy: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy == nil {
		t.Fatal("persisted policy was cleared")
	}

	// Re-resolving a persisted run verifies it instead of rewriting it.
	if err := reconciler.resolvePersistSecurePolicy(context.Background(), &persisted); err != nil {
		t.Fatalf("re-resolve of a persisted policy: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.PublicationPolicy.ProviderConfigRef != "github" {
		t.Fatalf("re-resolve rewrote the persisted policy to %q", persisted.Status.PublicationPolicy.ProviderConfigRef)
	}
}

// TestRevokeDeletesBrokerServiceWhenBrokerPodIsGone covers the pod-absent
// regression: a partial pass that removed every pod but left the broker
// Service must not report done while broker ingress is still routed.
func TestRevokeDeletesBrokerServiceWhenBrokerPodIsGone(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("orphan-service")
	now := metav1.Now()
	run.DeletionTimestamp = &now
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// Every pod is already gone; only the broker Service survived.
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	done, err := control.Revoke(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("revocation must not report done on the pass that deletes the broker Service")
	}
	var service corev1.Service
	err = c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("broker Service must be deleted, got %v", err)
	}

	// The next pass confirms it gone and completes.
	done, err = control.Revoke(context.Background(), run)
	if err != nil || !done {
		t.Fatalf("second pass = done=%v err=%v", done, err)
	}
}

// TestReconcileFencesWorkerWhenControlTerminates is the reconcile-level
// regression for fence placement: the legacy coordinator-termination handler
// must not bypass the secure topology's teardown, or the untrusted worker and
// the credentialed broker outlive their supervisor — a terminal run is never
// relaunched (and an AwaitingReview, HandedOff, or BlockedExternal run is never reaped), so a
// later-reconcile fence would never run at all.
func TestReconcileFencesWorkerWhenControlTerminates(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("fence-reconcile")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	controlPod.Spec.Containers[0].Name = topology.ControlContainerName
	controlPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 2,
			Message:  `COURIER_TERMINATION {"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":"capability probe failed"}`,
		}},
	}}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var worker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced in the same reconcile that terminalizes the run, got %v", err)
	}
	var broker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &broker); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be torn down with its control incarnation, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled with its control incarnation, got %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the control exit code", updated.Status.Phase)
	}
}

// TestReconcileControlLossReturnsToClaimed is the secure-topology mirror of
// the legacy pod-loss backstop: a vanished control pod with the worker and
// broker of the round still running is coordinator loss, so the run returns
// to Claimed for a relaunch from its checkpoint. The detection reconcile
// must also fence the surviving round — delete the worker, disable broker
// ingress, delete the broker pod — so the untrusted executor and the
// credentialed broker never outlive the missing supervisor. (#215)
func TestReconcileControlLossReturnsToClaimed(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("control-loss")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// The control pod is gone from both readers; the worker and broker of
	// the round survive at the detection reconcile.
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	// Broker ingress is a Service in front of the broker pod; it must be
	// torn down before the broker pod so the Service stops routing before
	// the pod stops serving (HARNESS.md §3).
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}
	reconciler := &CoderRunReconciler{
		Client:       c,
		APIReader:    c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a lost control pod must relaunch the round")
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
	// The surviving round was fenced in the detection reconcile, so the
	// next Claimed reconcile re-provisions rather than tolerating a
	// surviving worker and broker from a missing supervisor.
	var worker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on control-loss detection, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress Service must be disabled on control-loss detection, got %v", err)
	}
	var broker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &broker); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be torn down with its missing control on detection, got %v", err)
	}
}

// TestReconcileControlLossWithoutStatusReturnsToClaimed is the secure mirror
// of the unobservable-dead-coordinator backstop: a control pod evicted
// without ever reporting container statuses is coordinator loss. The inert
// object is deleted so the replacement control pod can carry the run's name,
// and the detection reconcile also fences the surviving round — delete the
// worker, disable broker ingress, delete the broker pod — so a replacement
// cannot attach to a surviving broker carrying the old incarnation's
// credentials. (#215)
func TestReconcileControlLossWithoutStatusReturnsToClaimed(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("control-no-status")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	// The control pod exists but is terminal with no container statuses: the
	// eviction left no exit state to observe.
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	controlPod.Spec.Containers[0].Name = topology.ControlContainerName
	controlPod.Status = corev1.PodStatus{
		Phase:   corev1.PodFailed,
		Reason:  "Evicted",
		Message: "The node was low on resource: ephemeral-storage.",
	}
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}
	reconciler := &CoderRunReconciler{
		Client:       c,
		APIReader:    c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; an unobservable dead control pod must relaunch the round")
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", updated.Status.Restarts)
	}
	// The inert coordinator object is gone so the replacement control pod
	// can carry the run's name; the surviving round is fenced the same
	// reconcile so the untrusted worker and the credentialed broker never
	// outlive the missing supervisor.
	var lost corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}, &lost); !apierrors.IsNotFound(err) {
		t.Fatalf("control pod must be deleted so the name is free for the replacement, got %v", err)
	}
	var worker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on control-loss detection, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress Service must be disabled on control-loss detection, got %v", err)
	}
	var broker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &broker); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be torn down with its dead control on detection, got %v", err)
	}
}

// TestReconcileControlLossAtCeilingFencesThenNeedsHuman pins the at-ceiling
// arm of #215: when the control loss lands on a run whose restart counter
// has already reached the ceiling, the detection reconcile still fences the
// surviving round before terminalizing to NeedsHuman. A terminal run is
// never relaunched (and an AwaitingReview, HandedOff, or BlockedExternal run is never reaped),
// so a fence deferred past the detection reconcile would never run, and the
// run's hand-off must keep the only intact objects of the round safe from the
// missing supervisor.
func TestReconcileControlLossAtCeilingFencesThenNeedsHuman(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("control-loss-ceiling")
	run.Status.Phase = courier.PhaseRunning
	run.Status.Restarts = 3 // matches defaultMaxRestarts; the next pod loss hands off to a human.
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}
	reconciler := &CoderRunReconciler{
		Client:       c,
		APIReader:    c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
		Secure:       control,
	}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want no requeue; the ceiling hand-off terminalizes in this reconcile", result)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the ceiling", updated.Status.Phase)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (ceiling is preserved, not incremented)", updated.Status.Restarts)
	}
	// Fence still happens — the NeedsHuman hand-off keeps no replacement
	// running, so a surviving worker/broker would dangle past the human
	// decision the run is waiting on.
	var worker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &worker); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on the ceiling reconcile, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress Service must be disabled on the ceiling reconcile, got %v", err)
	}
	var broker corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &broker); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be torn down on the ceiling reconcile, got %v", err)
	}
}

func TestObserveTopologyFencesRoundWhenWorkerVanishes(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("vanish")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// A live control whose worker is gone (deleted post-launch): the worker
	// death ends the run's operations, and recovery is the operator's own
	// infrastructure decision, independent of any heartbeat.
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	_, handled, err := control.ObserveTopology(context.Background(), run, listRunPods(t, c, run))
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("a vanished worker with a live control must be handled as infrastructure failure")
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 1 {
		t.Fatalf("fence relaunch state = phase %q restarts %d", updated.Status.Phase, updated.Status.Restarts)
	}
}

// TestObserveTopologyTerminatedControlWithVanishedWorkerIsNilSafe pins the
// ordering guard: a terminated control can coexist with an already-vanished
// worker or broker in one pods snapshot (worker eviction, then control
// exit, both landing between reconciles). The fence must treat an absent
// pod as nothing-to-delete, never dereference it.
func TestObserveTopologyTerminatedControlWithVanishedWorkerIsNilSafe(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("vanishterm")
	run.Status.Phase = courier.PhaseRunning
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// The control container has exited; the worker pod is absent entirely.
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	controlPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
	}}
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	_, handled, err := control.ObserveTopology(context.Background(), run, listRunPods(t, c, run))
	if err != nil {
		t.Fatalf("ObserveTopology() error = %v; a vanished worker must not panic the fence", err)
	}
	if handled {
		t.Fatal("handled = true, want the exit-code mapping to proceed")
	}
	// The broker and its ingress were still fenced.
	var updated corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(brokerPod), &updated); err == nil && updated.DeletionTimestamp == nil {
		t.Fatal("broker pod survived with no deletion in flight; the terminated control must fence it")
	}
}

// wedgedControlPod is a secure round's control pod past the liveness window
// with no fresh heartbeat: a silent wedge, not a termination — the container
// is still Running, so the exit-code mapping and ObserveTopology leave the
// decision to liveness.
func wedgedControlPod(run *courier.CoderRun, now time.Time) *corev1.Pod {
	pod := healthyPod(run.Name, topology.ComponentCoordinator)
	pod.Spec.Containers[0].Name = topology.ControlContainerName
	pod.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: topology.ControlContainerName,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
			StartedAt: metav1.NewTime(now.Add(-2 * time.Hour)),
		}},
	}}
	return pod
}

func secureLivenessReconciler(c client.Client, control *SecureControl, now time.Time) *CoderRunReconciler {
	return &CoderRunReconciler{
		Client:         c,
		APIReader:      c,
		Sources:        NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter:   fakeStatusWriter{client: c},
		Secure:         control,
		LivenessWindow: 5 * time.Minute,
		MaxRestarts:    3,
		Now:            func() time.Time { return now },
	}
}

// TestWedgeOfSecureRunReapsOnlyCoordinatorAndFencesRound pins #222: a
// stale-heartbeat wedge of a secure run charges exactly one crashloop credit,
// the wedge reap set is the coordinator pod, and the surviving round is torn
// down by SecureControl's fence — Service before broker pod (HARNESS.md §3) —
// on the same reconcile, exactly as the disappearance backstop does (#215).
func TestWedgeOfSecureRunReapsOnlyCoordinatorAndFencesRound(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("wedge-secure")
	run.Status.Phase = courier.PhaseRunning
	controlPod := wedgedControlPod(run, livenessClock)
	run.Status.Heartbeat = &courier.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-10 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: string(controlPod.UID),
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := secureLivenessReconciler(c, control, livenessClock)
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("Requeue = false, want true; a wedged control must relaunch the round")
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		t.Fatalf("restarts = %d, want exactly 1; one coordinator wedge is one credit", updated.Status.Restarts)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("control pod must be reaped, got %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced by SecureControl on the wedge reconcile, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled with the wedge, got %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced with the wedge, got %v", err)
	}
}

// TestWedgeOfSecureRunAtCeilingFencesThenNeedsHuman pins the at-ceiling arm:
// the wedge hand-off terminalizes in the detection reconcile, so the fence
// must run there — a terminal run is never relaunched and would otherwise
// leave the untrusted executor and the credentialed broker alive past the
// human decision.
func TestWedgeOfSecureRunAtCeilingFencesThenNeedsHuman(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("wedge-secure-ceiling")
	run.Status.Phase = courier.PhaseRunning
	run.Status.Restarts = 3
	controlPod := wedgedControlPod(run, livenessClock)
	run.Status.Heartbeat = &courier.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-10 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: string(controlPod.UID),
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := secureLivenessReconciler(c, control, livenessClock)
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want no requeue; the ceiling hand-off terminalizes in this reconcile", result)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the ceiling", updated.Status.Phase)
	}
	if updated.Status.Restarts != 3 {
		t.Fatalf("restarts = %d, want 3 (ceiling is preserved, not incremented)", updated.Status.Restarts)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on the ceiling reconcile, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled on the ceiling reconcile, got %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced on the ceiling reconcile, got %v", err)
	}
}

// readerWithoutControlPod answers pod lists from the wrapped client with the
// run's control pod removed, standing in for the informer cache disagreeing
// with the API server: the cache still shows a live control the API server
// does not have.
type readerWithoutControlPod struct {
	client.Client
	controlName string
}

func (w readerWithoutControlPod) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := w.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		kept := make([]corev1.Pod, 0, len(pods.Items))
		for _, pod := range pods.Items {
			if pod.Name != w.controlName {
				kept = append(kept, pod)
			}
		}
		pods.Items = kept
	}
	return nil
}

// TestWedgeViewDisagreementAtCeilingFencesThenNeedsHuman pins the fence on
// the cached-view-disagrees hand-off: the live read shows no control pod
// while the cache still shows a live one, and the ceiling hand-off
// terminalizes the run in this reconcile, so the surviving round must be
// fenced here — a terminal run is never torn down again.
func TestWedgeViewDisagreementAtCeilingFencesThenNeedsHuman(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("wedge-disagree")
	run.Status.Phase = courier.PhaseRunning
	run.Status.Restarts = 3
	controlPod := wedgedControlPod(run, livenessClock)
	run.Status.Heartbeat = &courier.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-10 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: string(controlPod.UID),
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := secureLivenessReconciler(c, control, livenessClock)
	reconciler.APIReader = readerWithoutControlPod{Client: c, controlName: topology.ControlPodName(run.Name)}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the ceiling hand-off", updated.Status.Phase)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on the disagrees hand-off reconcile, got %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced on the disagrees hand-off reconcile, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled on the disagrees hand-off reconcile, got %v", err)
	}
}

// TestWedgeAtCeilingPreservesUnrecoverableControlAndFencesRound pins the
// #106 evidence rule inside the secure topology: an evicted control object
// at the ceiling is the NeedsHuman hand-off's only record of the cause and
// survives, while the surviving round is still fenced — terminal worker or
// broker objects are never coordinator evidence, and a terminal run must
// not leave them standing.
func TestWedgeAtCeilingPreservesUnrecoverableControlAndFencesRound(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("wedge-secure-preserved")
	run.Status.Phase = courier.PhaseRunning
	run.Status.Restarts = 3
	controlPod := healthyPod(run.Name, topology.ComponentCoordinator)
	controlPod.Spec.Containers[0].Name = topology.ControlContainerName
	controlPod.CreationTimestamp = metav1.NewTime(livenessClock.Add(-2 * time.Hour))
	controlPod.Status = corev1.PodStatus{
		Phase:   corev1.PodFailed,
		Reason:  "Evicted",
		Message: "The node was low on resource: ephemeral-storage.",
	}
	run.Status.Heartbeat = &courier.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-10 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: string(controlPod.UID),
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := secureLivenessReconciler(c, control, livenessClock)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the ceiling", updated.Status.Phase)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}, &got); err != nil {
		t.Fatalf("the unrecoverable control object must survive as the hand-off's evidence: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced even when the control is preserved, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled even when the control is preserved, got %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced even when the control is preserved, got %v", err)
	}
}

// TestWedgeOfSecureRunAtConfiguredCeilingFencesThenNeedsHuman pins the
// configured-ceiling path (#216): with `MaxRestarts` set below the package
// default, the wedge detection reconcile terminalizes a run at the configured
// counter — not the default — and the secure round is fenced on that
// detection reconcile. A wedge at `Restarts == configured` must hand off
// to a human; a wedge below the configured counter would relaunch.
func TestWedgeOfSecureRunAtConfiguredCeilingFencesThenNeedsHuman(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("wedge-secure-configured-ceiling")
	run.Status.Phase = courier.PhaseRunning
	const configuredCeiling = 2
	run.Status.Restarts = configuredCeiling
	controlPod := wedgedControlPod(run, livenessClock)
	run.Status.Heartbeat = &courier.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-10 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: string(controlPod.UID),
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := secureLivenessReconciler(c, control, livenessClock)
	reconciler.MaxRestarts = configuredCeiling
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want no requeue; the configured-ceiling hand-off terminalizes in this reconcile", result)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the configured ceiling (%d), not the package default", updated.Status.Phase, configuredCeiling)
	}
	if updated.Status.Restarts != configuredCeiling {
		t.Fatalf("restarts = %d, want %d (configured ceiling is preserved, not incremented)", updated.Status.Restarts, configuredCeiling)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on the configured-ceiling reconcile, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled on the configured-ceiling reconcile, got %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced on the configured-ceiling reconcile, got %v", err)
	}
}

// TestWedgeDeferredChargeDoesNotFenceYet pins the sequencing the deferred
// charge depends on: when the coordinator delete is still re-confirming
// (the object survives the live read), the detection reconcile takes no
// charge and leaves the round standing — the disappearance backstop, which
// fences when it takes the deferred charge, owns that teardown.
func TestWedgeDeferredChargeDoesNotFenceYet(t *testing.T) {
	control, c := secureControl(t)
	run := secureRun("wedge-deferred")
	run.Status.Phase = courier.PhaseRunning
	controlPod := wedgedControlPod(run, livenessClock)
	// A finalizer is what keeps the deleted object present for the live
	// re-confirmation; the fake client refuses a DeletionTimestamp without one.
	controlPod.Finalizers = []string{"courier.misospace.dev/test"}
	run.Status.Heartbeat = &courier.Heartbeat{
		At:                metav1.NewTime(livenessClock.Add(-10 * time.Minute)),
		Kind:              "stream",
		CoordinatorPodUID: string(controlPod.UID),
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	workerPod := healthyPod(run.Name, topology.ComponentWorker)
	brokerPod := healthyPod(run.Name, topology.ComponentBroker)
	for _, pod := range []*corev1.Pod{controlPod, workerPod, brokerPod} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(context.Background(), topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	reconciler := secureLivenessReconciler(c, control, livenessClock)
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != observationRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a delete still re-confirming defers", result.RequeueAfter, observationRequeueDelay)
	}
	var updated courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Restarts != 0 || updated.Status.Phase != courier.PhaseRunning {
		t.Fatalf("state = phase %q restarts %d, want Running/0 while the charge is deferred", updated.Status.Phase, updated.Status.Restarts)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); err != nil {
		t.Fatalf("worker must still stand while the charge defers to the backstop: %v", err)
	}
	var service corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); err != nil {
		t.Fatalf("broker ingress must still stand while the charge defers to the backstop: %v", err)
	}
}

// recordingReader is a client.Reader that reports whether it was consulted.
// The repo's convention for live reads is a reader distinct from the cached
// client (liveness_test.go's `api` readers); pointing APIReader at the very
// same fake client as Client would make the uncached read indistinguishable
// from the cached one, so a test using this wrapper pins which reader the
// secure relaunch's re-confirmation actually goes through.
type recordingReader struct {
	client.Reader
	consulted bool
}

func (r *recordingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.consulted = true
	return r.Reader.List(ctx, list, opts...)
}

func (r *recordingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	r.consulted = true
	return r.Reader.Get(ctx, key, object, opts...)
}

// runRecordingReader records the CoderRun reads that reach it. recordingReader
// alone cannot say which read a path made uncached — any Get or List satisfies
// it, and the relaunch reads both the run and the pod — so this pins the
// counter read the crashloop ceiling is judged from.
type runRecordingReader struct {
	client.Reader
	runReads int
}

func (r *runRecordingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return r.Reader.List(ctx, list, opts...)
}

func (r *runRecordingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, ok := object.(*courier.CoderRun); ok {
		r.runReads++
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

// inertControlPod turns a live secure control pod into the shape the secure
// relaunch treats as evidence: a terminal object Kubernetes never recorded a
// coordinator exit for (an eviction), so it can never report a run outcome.
// healthyPod's fixture names its container "main"; the live re-confirmation
// recognizes the coordinator object by its container name, so that is set here
// too — the production coupling `topology.ControlPod` renders, pinned rather
// than patched around.
func inertControlPod(t *testing.T, c client.Client, run *courier.CoderRun) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	if err := c.Get(context.Background(), key, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Containers[0].Name = topology.ControlContainerName
	// The spec change goes through a full update and the status through the
	// status subresource; the fake client will not carry a spec edit on a
	// status write.
	if err := c.Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = corev1.PodStatus{
		Phase:   corev1.PodFailed,
		Reason:  "Evicted",
		Message: "The node was low on resource: ephemeral-storage.",
	}
	if err := c.Status().Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, &pod); err != nil {
		t.Fatal(err)
	}
	if controlTerminated(&pod) {
		t.Fatal("the inert control fixture must not read as a terminated coordinator, or ObserveTopology's control-terminated branch would intercept it")
	}
	if !hasCoordinatorContainer(&pod) {
		t.Fatalf("the control fixture must declare the %q container, or the live re-confirmation cannot recognize it as a coordinator object", coordinatorContainerName)
	}
	if !coordinatorPodUnrecoverable(&pod) {
		t.Fatalf("control phase %v with %d container statuses must read as unrecoverable", pod.Status.Phase, len(pod.Status.ContainerStatuses))
	}
	return &pod
}

// TestSecureBrokenTopologyDefersChargeUntilControlObjectIsGone pins #223's core
// arm: the secure broken-topology relaunch obeys "a delete is not a
// disappearance" for the control object that carries evidence. A delete the API
// server has only marked terminating — held by a finalizer — is not a
// disappearance, so the charge is deferred and the round is left standing; the
// disappearance backstop takes both the fence and the single charge once the
// object is provably gone, so one physical loss is charged once.
func TestSecureBrokenTopologyDefersChargeUntilControlObjectIsGone(t *testing.T) {
	ctx := context.Background()
	control, c, run := brokenWorkerSecureRound(t, "secure-deferred-charge", 0)
	// The live re-confirmation reads through the uncached API reader, as the
	// manager wires it in production.
	api := &recordingReader{Reader: c}
	control.APIReader = api

	controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	controlPod := inertControlPod(t, c, run)
	// A finalizer is what keeps the deleted object present for the live
	// re-confirmation; the fake client refuses a DeletionTimestamp without one.
	controlPod.Finalizers = []string{"courier.misospace.dev/test"}
	if err := c.Update(ctx, controlPod); err != nil {
		t.Fatal(err)
	}

	result, handled, err := control.ObserveTopology(ctx, run, listRunPods(t, c, run))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
	}
	if !api.consulted {
		t.Fatal("the re-confirmation must read through the uncached APIReader, not the cached Client")
	}
	if result.RequeueAfter != secureRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v; a delete still re-confirming defers", result.RequeueAfter, secureRequeueDelay)
	}
	if result.Requeue {
		t.Fatal("Requeue = true, want false; a deferred charge must not relaunch")
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseRunning || updated.Status.Restarts != 0 {
		t.Fatalf("state = phase %q restarts %d, want Running/0; a delete that only set a deletionTimestamp is not a loss to charge",
			updated.Status.Phase, updated.Status.Restarts)
	}
	// The control object is still there, terminating: the delete was issued and
	// is in flight, not confirmed.
	var still corev1.Pod
	if err := c.Get(ctx, controlKey, &still); err != nil {
		t.Fatalf("the deleted control object must survive as terminating: %v", err)
	}
	if still.DeletionTimestamp == nil {
		t.Fatal("deletionTimestamp = nil, want set; the delete must be in flight")
	}
	var got corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); err != nil {
		t.Fatalf("worker must still stand while the charge defers: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); err != nil {
		t.Fatalf("broker pod must still stand while the charge defers: %v", err)
	}

	// Release the finalizer so the object is actually gone, then reconcile: the
	// disappearance backstop now confirms the loss and takes the deferred charge,
	// fencing the round on the same reconcile.
	still.Finalizers = nil
	if err := c.Update(ctx, &still); err != nil {
		t.Fatal(err)
	}
	reconciler := secureLivenessReconciler(c, control, livenessClock)
	result, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatal("Requeue = false, want true; a confirmed-gone loss must relaunch")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed; the loss is charged once the control object is gone", updated.Status.Phase)
	}
	if updated.Status.Restarts != 1 {
		// A second charge here is the double-charge this issue is about: the
		// relaunch path tolerates AlreadyExists, so a replacement round could
		// attach to the dying object and one physical loss be paid for twice.
		t.Fatalf("restarts = %d, want 1; one physical loss is charged exactly once", updated.Status.Restarts)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("the backstop must fence the worker when it takes the deferred charge, got %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("the backstop must fence the broker pod when it takes the deferred charge, got %v", err)
	}
}

// TestSecureBrokenTopologyChargesRecoverableControlInPlace pins the boundary of
// the #223 deferral, and is the regression guard for it. Only the inert control
// object — the one that can never report an outcome — may defer its charge. A
// still-recoverable control object is charged in place: a deferred charge would
// leave the run Running while the pod is torn down, and the next reconcile's
// exit-code mapping would read that teardown's exit code and terminalize the
// run (Failed) instead of relaunching it. A finalizer holding the recoverable
// object therefore costs a bounded re-observation at the launch path, never a
// lost charge and never a premature terminal transition.
func TestSecureBrokenTopologyChargesRecoverableControlInPlace(t *testing.T) {
	ctx := context.Background()
	control, c, run := brokenWorkerSecureRound(t, "secure-recoverable-control", 0)
	control.APIReader = c

	controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	var controlPod corev1.Pod
	if err := c.Get(ctx, controlKey, &controlPod); err != nil {
		t.Fatal(err)
	}
	controlPod.Spec.Containers[0].Name = topology.ControlContainerName
	// The same finalizer-held delete as the inert case above.
	controlPod.Finalizers = []string{"courier.misospace.dev/test"}
	if err := c.Update(ctx, &controlPod); err != nil {
		t.Fatal(err)
	}
	if coordinatorPodUnrecoverable(&controlPod) {
		t.Fatal("a running control object is recoverable and must not take the inert path")
	}

	result, handled, err := control.ObserveTopology(ctx, run, listRunPods(t, c, run))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
	}
	if result.RequeueAfter != 0 || !result.Requeue {
		t.Fatalf("result = %+v, want Requeue with no requeue delay; a recoverable control object is charged in place, not deferred", result)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 1 {
		t.Fatalf("state = phase %q restarts %d, want Claimed/1 charged in place", updated.Status.Phase, updated.Status.Restarts)
	}
	// The round is fenced and the object deleted even though the finalizer keeps
	// the control pod alive: the run has left Running, so nothing can read the
	// teardown's exit code as a run outcome.
	var got corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced on the in-place charge, got %v", err)
	}
	if err := c.Get(ctx, controlKey, &got); err != nil {
		t.Fatalf("the recoverable control object must still be deleted: %v", err)
	}
	// The finalizer keeps the object readable after the delete, so its mere
	// presence proves nothing: the delete must have been issued. Without it the
	// deterministic name stays occupied, and the launch path's AlreadyExists
	// tolerance would let the replacement round attach to this stale object.
	if got.DeletionTimestamp == nil {
		t.Fatal("deletionTimestamp = nil, want set; the recoverable control object must be deleted below the ceiling, not preserved")
	}
}

// TestSecureBrokenTopologyAtCeilingPreservesUnrecoverableControl pins #223's
// other arm: at the crashloop ceiling an unrecoverable control object is the
// NeedsHuman hand-off's only record of the cause and is preserved rather than
// deleted, while the round is still fenced. A terminal run is never relaunched,
// so deleting the evidence would destroy the only record a human gets (#106).
func TestSecureBrokenTopologyAtCeilingPreservesUnrecoverableControl(t *testing.T) {
	ctx := context.Background()
	// The package default ceiling: the run's counter already sits on it.
	control, c, run := brokenWorkerSecureRound(t, "secure-ceiling-preserved", defaultMaxRestarts)
	// The ceiling hand-off terminalizes through the patcher, which needs the
	// source registry the plain secure helper does not wire.
	control.Patcher = &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
	}
	if err := c.Create(ctx, topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}
	controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	inertControlPod(t, c, run)

	_, handled, err := control.ObserveTopology(ctx, run, listRunPods(t, c, run))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the ceiling", updated.Status.Phase)
	}
	if updated.Status.Restarts != defaultMaxRestarts {
		t.Fatalf("restarts = %d, want %d (the ceiling is preserved, not incremented)", updated.Status.Restarts, defaultMaxRestarts)
	}
	var got corev1.Pod
	if err := c.Get(ctx, controlKey, &got); err != nil {
		t.Fatalf("the unrecoverable control object must survive as the hand-off's evidence: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced even when the control is preserved, got %v", err)
	}
	var service corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("broker ingress must be disabled even when the control is preserved, got %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced even when the control is preserved, got %v", err)
	}
}

// TestSecureBrokenTopologyAtCeilingStillDeletesRecoverableControl pins the
// qualifier on the arm above: preservation at the ceiling is for the inert
// evidence object only. A still-recoverable control object records a live round,
// not a cause, so it is deleted at the ceiling exactly as below it — nothing is
// withheld from a replacement that will never come, and a live object is not the
// hand-off's record.
func TestSecureBrokenTopologyAtCeilingStillDeletesRecoverableControl(t *testing.T) {
	ctx := context.Background()
	control, c, run := brokenWorkerSecureRound(t, "secure-ceiling-recoverable", defaultMaxRestarts)
	control.Patcher = &CoderRunReconciler{
		Client:       c,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: c},
	}
	controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	var controlPod corev1.Pod
	if err := c.Get(ctx, controlKey, &controlPod); err != nil {
		t.Fatal(err)
	}
	controlPod.Spec.Containers[0].Name = topology.ControlContainerName
	if err := c.Update(ctx, &controlPod); err != nil {
		t.Fatal(err)
	}

	if _, handled, err := control.ObserveTopology(ctx, run, listRunPods(t, c, run)); err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman from the ceiling", updated.Status.Phase)
	}
	var got corev1.Pod
	if err := c.Get(ctx, controlKey, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("a recoverable control object must be deleted at the ceiling, got %v", err)
	}
	// Preservation is the qualifier, not the fence: the round is still torn down
	// on the ceiling reconcile either way, because a terminal run is never
	// relaunched and a fence deferred past this reconcile would never run.
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced at the ceiling, got %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced at the ceiling, got %v", err)
	}
}

// TestFenceAndRelaunchToleratesControlPodVanishingBeforeReread pins the
// nil-safety of the relaunch's own control-pod re-read. ObserveTopology decides
// from the pod list the reconcile collected; fenceAndRelaunch lists again, so a
// control pod that vanishes in between — eviction, node drain, a human deleting
// it — arrives as no control pod at all. That is a loss the disappearance
// backstop owns, not a panic: the relaunch must delete nothing, preserve
// nothing, and charge exactly as it would for any absent control pod.
func TestFenceAndRelaunchToleratesControlPodVanishingBeforeReread(t *testing.T) {
	ctx := context.Background()
	control, c, run := brokenWorkerSecureRound(t, "secure-control-vanished", 0)
	control.APIReader = c
	controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	// Gone by the time the relaunch re-reads it.
	if err := c.Delete(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: controlKey.Namespace, Name: controlKey.Name},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, topology.BrokerService(run)); err != nil {
		t.Fatal(err)
	}

	result, handled, err := control.fenceAndRelaunch(ctx, run)
	if err != nil {
		t.Fatalf("fenceAndRelaunch: %v", err)
	}
	if !handled || !result.Requeue {
		t.Fatalf("handled=%v result=%+v, want the relaunch handled with a requeue", handled, result)
	}
	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 1 {
		t.Fatalf("state = phase %q restarts %d, want Claimed/1; an absent control pod is charged like any other", updated.Status.Phase, updated.Status.Restarts)
	}
	var got corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("worker must be fenced, got %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod must be fenced, got %v", err)
	}
}

// TestSecureBrokenTopologyReobservesWhenControlPodWasReplaced pins #370's UID
// arm inside the secure topology: the control object the broken-topology
// relaunch tears down is the one it observed, never the name it holds. A control
// pod replaced between the live read and the delete conflicts instead of being
// deleted by name, and the conflict is a re-observation — no fence, no charge —
// the same discipline checkLiveness applies before it reaps a replaced
// coordinator (#105; the liveness precedent is
// TestReapDeleteConflictsWhenPodWasReplaced).
func TestSecureBrokenTopologyReobservesWhenControlPodWasReplaced(t *testing.T) {
	ctx := context.Background()
	const (
		replacementControlUID = types.UID("replacement-control-uid")
		observedControlUID    = types.UID("observed-control-uid")
	)
	control, store, run := brokenWorkerSecureRound(t, "secure-replaced-control", 0)
	controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}
	// The store already holds the replacement control pod — the delete/recreate
	// happened before this reconcile — while the live read it makes still
	// observes the previous incarnation's UID.
	var replacement corev1.Pod
	if err := store.Get(ctx, controlKey, &replacement); err != nil {
		t.Fatal(err)
	}
	replacement.UID = replacementControlUID
	// Naming the coordinator container is what makes the live reader's UID
	// rewrite apply, standing in for a server whose coordinator object still
	// carries the UID this reconcile observed.
	replacement.Spec.Containers[0].Name = topology.ControlContainerName
	if err := store.Update(ctx, &replacement); err != nil {
		t.Fatal(err)
	}
	enforcing := &uidEnforcingClient{Client: store}
	control.Client = enforcing
	control.APIReader = uidRewriteReader{Client: store, visibleUID: observedControlUID}

	result, handled, err := control.ObserveTopology(ctx, run, listRunPods(t, store, run))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
	}
	if result.RequeueAfter != secureRequeueDelay || result.Requeue {
		t.Fatalf("result = %+v, want a bounded re-observation after the conflict; a conflicted delete must not act", result)
	}
	if len(enforcing.preconditions) == 0 {
		t.Fatal("the relaunch delete carried no UID precondition; a pod must never be deleted by name alone")
	}
	// The precondition must be the UID this reconcile observed, not merely some
	// precondition: a delete that named the live object's UID would delete the
	// replacement this arm is about.
	if got := enforcing.preconditions[0]; got != observedControlUID {
		t.Fatalf("delete precondition UID = %q, want the observed control pod's UID %q", got, observedControlUID)
	}
	var updated courier.CoderRun
	if err := store.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != courier.PhaseRunning || updated.Status.Restarts != 0 {
		t.Fatalf("state = phase %q restarts %d, want no charge; the decision was made on a world that no longer exists",
			updated.Status.Phase, updated.Status.Restarts)
	}
	var survived corev1.Pod
	if err := store.Get(ctx, controlKey, &survived); err != nil {
		t.Fatalf("the replacement control pod must survive the stale decision: %v", err)
	}
	if survived.UID != replacementControlUID {
		t.Fatalf("control pod UID = %q, want the store's own object %q untouched by the stale decision", survived.UID, replacementControlUID)
	}
	if survived.DeletionTimestamp != nil {
		t.Fatal("the replacement control pod carries a deletion; a conflicting delete must tear nothing down")
	}
	var got corev1.Pod
	if err := store.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.WorkerPodName(run.Name)}, &got); err != nil {
		t.Fatalf("worker must still stand; a conflicted delete does not fence the round: %v", err)
	}
	if err := store.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerPodName(run.Name)}, &got); err != nil {
		t.Fatalf("broker pod must still stand; a conflicted delete does not fence the round: %v", err)
	}
}

// TestSecureBrokenTopologyJudgesCeilingFromLiveRun pins #370's counter arm: the
// crashloop ceiling the secure relaunch consults is the API server's own counter
// on the live run, never the cached status the reconcile happens to carry. The
// cached run's counter can read low (a run that already sits on the ceiling
// would relaunch one more round past it) or high (a run the world has reset
// would hand off to a human over a counter nobody holds). Both directions are
// bounded by the world counter shared with the other relaunch paths (#216), and
// the read itself goes through the uncached reader (§6).
func TestSecureBrokenTopologyJudgesCeilingFromLiveRun(t *testing.T) {
	ctx := context.Background()

	t.Run("StaleLowCachedCounterTerminalizesAtWorldCeiling", func(t *testing.T) {
		control, c, run := brokenWorkerSecureRound(t, "secure-live-ceiling", 0)
		control.MaxRestarts = 1
		// The ceiling hand-off terminalizes through the patcher, which needs the
		// source registry the plain secure helper does not wire.
		control.Patcher = &CoderRunReconciler{
			Client:       c,
			Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
			StatusWriter: fakeStatusWriter{client: c},
		}
		// The world's counter already sits on the configured ceiling; the run this
		// reconcile carries is stale and reads 0.
		var stored courier.CoderRun
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), &stored); err != nil {
			t.Fatal(err)
		}
		stored.Status.Restarts = 1
		if err := c.Status().Update(ctx, &stored); err != nil {
			t.Fatal(err)
		}
		api := &runRecordingReader{Reader: c}
		control.APIReader = api
		if err := c.Create(ctx, topology.BrokerService(run)); err != nil {
			t.Fatal(err)
		}
		controlKey := types.NamespacedName{Namespace: run.Namespace, Name: topology.ControlPodName(run.Name)}

		_, handled, err := control.ObserveTopology(ctx, run, listRunPods(t, c, run))
		if err != nil || !handled {
			t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
		}
		if api.runReads == 0 {
			t.Fatal("the ceiling judgement must read the run through the uncached APIReader, not the cached Client")
		}
		var updated courier.CoderRun
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.Status.Phase != courier.PhaseNeedsHuman {
			t.Fatalf("phase = %q, want NeedsHuman: the world's counter sits on the configured ceiling (%d) whatever the cached run carries",
				updated.Status.Phase, control.MaxRestarts)
		}
		if updated.Status.Restarts != 1 {
			t.Fatalf("restarts = %d, want 1 (the world's ceiling is preserved, not incremented)", updated.Status.Restarts)
		}
		// The control object is still recoverable, so it is deleted at the ceiling:
		// nothing withholds its deterministic name from a replacement that will
		// never come.
		var got corev1.Pod
		if err := c.Get(ctx, controlKey, &got); !apierrors.IsNotFound(err) {
			t.Fatalf("a recoverable control object must be deleted at the ceiling, got %v", err)
		}
	})

	t.Run("StaleHighCachedCounterRelaunchesAgainstWorldCounter", func(t *testing.T) {
		control, c, run := brokenWorkerSecureRound(t, "secure-live-counter", 0)
		// The cached run's counter sits on the package default ceiling while the
		// world's counter is 0: the world wins, so the round relaunches instead of
		// handing off over a counter nobody holds.
		cached := run.DeepCopy()
		cached.Status.Restarts = defaultMaxRestarts
		api := &runRecordingReader{Reader: c}
		control.APIReader = api

		result, handled, err := control.ObserveTopology(ctx, cached, listRunPods(t, c, cached))
		if err != nil || !handled {
			t.Fatalf("handled=%v err=%v, want the broken topology handled", handled, err)
		}
		if api.runReads == 0 {
			t.Fatal("the ceiling judgement must read the run through the uncached APIReader, not the cached Client")
		}
		if !result.Requeue || result.RequeueAfter != 0 {
			t.Fatalf("result = %+v, want an immediate relaunch below the world's ceiling", result)
		}
		var updated courier.CoderRun
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.Status.Phase != courier.PhaseClaimed || updated.Status.Restarts != 1 {
			t.Fatalf("state = phase %q restarts %d, want Claimed/1 charged as the world's counter plus one; a stale high counter must not hand off",
				updated.Status.Phase, updated.Status.Restarts)
		}
	})
}
