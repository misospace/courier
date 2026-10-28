package label

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
)

// fakeProvider is a minimal in-memory forge provider built for adapter
// tests. It satisfies the forge contract's identity surface and the two
// typed seams the label adapter consumes, and records every label removal.
type fakeProvider struct {
	caps forge.Capabilities
	// failRepo makes every listing for that one repository fail, so the
	// whole-poll failure contract can be exercised against a multi-repo
	// config.
	failRepo string
	failErr  error
	byLabel  map[labelQuery][]forge.WorkItem
	removed  []labelRemoval
}

type labelQuery struct {
	repo  string
	label string
	kind  forge.WorkItemKind
}

type labelRemoval struct {
	repo  string
	id    string
	label string
}

var _ forge.Provider = (*fakeProvider)(nil)
var _ forge.LabeledWorkItemLister = (*fakeProvider)(nil)
var _ forge.WorkItemLabelMutator = (*fakeProvider)(nil)

func (f *fakeProvider) Config() forge.ProviderConfig { return forge.ProviderConfig{Name: "fake"} }

func (f *fakeProvider) Capabilities() forge.Capabilities { return f.caps }

func (f *fakeProvider) ReadWorkItem(context.Context, forge.WorkItemRef) (forge.WorkItem, error) {
	return forge.WorkItem{}, forge.ErrUnsupported
}

func (f *fakeProvider) ReadPullRequest(context.Context, forge.PullRequestRef) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeProvider) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeProvider) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeProvider) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	return nil, forge.ErrUnsupported
}

func (f *fakeProvider) CreatePullRequest(context.Context, forge.CreatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeProvider) UpdatePullRequest(context.Context, forge.PullRequestRef, forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (f *fakeProvider) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	return forge.Comment{}, forge.ErrUnsupported
}

func (f *fakeProvider) ListWorkItemsByLabel(_ context.Context, repo, label string, kind forge.WorkItemKind) ([]forge.WorkItem, error) {
	if !f.caps.Has(forge.CapabilityListWorkItemsByLabel) {
		return nil, forge.ErrUnsupported
	}
	if repo == f.failRepo {
		return nil, f.failErr
	}
	return f.byLabel[labelQuery{repo: repo, label: label, kind: kind}], nil
}

func (f *fakeProvider) RemoveWorkItemLabel(_ context.Context, ref forge.WorkItemRef, label string) error {
	if !f.caps.Has(forge.CapabilityRemoveWorkItemLabel) {
		return forge.ErrUnsupported
	}
	f.removed = append(f.removed, labelRemoval{repo: ref.Repo, id: ref.ID, label: label})
	return nil
}

func testAdapter(provider forge.Provider, config Config) Adapter {
	a, err := New(provider, config, nil)
	if err != nil {
		panic(fmt.Sprintf("New: unexpected error: %v", err))
	}
	return a
}

// plainProvider implements only the forge.Provider identity surface, so the
// construction refusals for missing optional seams can be exercised against
// a type that genuinely lacks them.
type plainProvider struct{}

var _ forge.Provider = plainProvider{}

func (plainProvider) Config() forge.ProviderConfig { return forge.ProviderConfig{Name: "plain"} }

func (plainProvider) Capabilities() forge.Capabilities { return forge.Capabilities{} }

func (plainProvider) ReadWorkItem(context.Context, forge.WorkItemRef) (forge.WorkItem, error) {
	return forge.WorkItem{}, forge.ErrUnsupported
}

func (plainProvider) ReadPullRequest(context.Context, forge.PullRequestRef) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (plainProvider) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}

func (plainProvider) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}

func (plainProvider) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	return nil, forge.ErrUnsupported
}

func (plainProvider) CreatePullRequest(context.Context, forge.CreatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (plainProvider) UpdatePullRequest(context.Context, forge.PullRequestRef, forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	return forge.PullRequest{}, forge.ErrUnsupported
}

func (plainProvider) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	return forge.Comment{}, forge.ErrUnsupported
}

// listOnlyProvider adds only the listing seam to plainProvider, so the
// missing-mutator refusal has a genuinely partial type.
type listOnlyProvider struct{ plainProvider }

var _ forge.LabeledWorkItemLister = listOnlyProvider{}

func (listOnlyProvider) ListWorkItemsByLabel(context.Context, string, string, forge.WorkItemKind) ([]forge.WorkItem, error) {
	return nil, forge.ErrUnsupported
}

func TestNewValidatesConfigAndProvider(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:   "defaults applied",
			config: Config{Repos: []string{"owner/repo"}},
		},
		{
			name:    "no repositories",
			config:  Config{},
			wantErr: "at least one repository",
		},
		{
			name:    "blank repository entry",
			config:  Config{Repos: []string{"owner/repo", "  "}},
			wantErr: "canonical owner/name",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
			a, err := New(provider, tc.config, nil)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("New() error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("New() unexpected error: %v", err)
			}
			if a.config.TriggerLabel != DefaultTriggerLabel {
				t.Errorf("TriggerLabel = %q, want default %q", a.config.TriggerLabel, DefaultTriggerLabel)
			}
			if a.config.FixLabel != DefaultFixLabel {
				t.Errorf("FixLabel = %q, want default %q", a.config.FixLabel, DefaultFixLabel)
			}
		})
	}
	t.Run("invalid repository shape refused", func(t *testing.T) {
		t.Parallel()
		provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
		for _, repo := range []string{"owner", "owner/repo/extra", "/repo", "owner/",
			"../repo", "repo/..", "./repo", "repo/.", "owner/./repo",
			"own\x00er/repo", "owner/re\x00po", "owner/re\tpo"} {
			if _, err := New(provider, Config{Repos: []string{repo}}, nil); err == nil || !strings.Contains(err.Error(), "canonical owner/name") {
				t.Fatalf("New() with repository %q error = %v, want canonical owner/name refusal", repo, err)
			}
		}
	})
	t.Run("duplicate repositories collapse", func(t *testing.T) {
		t.Parallel()
		provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
		a, err := New(provider, Config{Repos: []string{"owner/a", "owner/a"}}, nil)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		if len(a.config.Repos) != 1 {
			t.Fatalf("repos = %v, want one deduplicated entry", a.config.Repos)
		}
	})
	t.Run("pathological labels refused", func(t *testing.T) {
		t.Parallel()
		provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
		for _, label := range []string{".", "..", "a,b"} {
			if _, err := New(provider, Config{Repos: []string{"owner/a"}, TriggerLabel: label}, nil); err == nil || !strings.Contains(err.Error(), "not a usable label") {
				t.Fatalf("New() with trigger label %q error = %v, want usable-label refusal", label, err)
			}
		}
	})
	t.Run("provider without lister refused", func(t *testing.T) {
		t.Parallel()
		var provider forge.Provider = plainProvider{}
		_, err := New(provider, Config{Repos: []string{"owner/repo"}}, nil)
		if err == nil || !strings.Contains(err.Error(), "does not list work items by label") {
			t.Fatalf("New() error = %v, want missing-lister refusal", err)
		}
	})
	t.Run("provider without mutator refused", func(t *testing.T) {
		t.Parallel()
		var provider forge.Provider = listOnlyProvider{}
		_, err := New(provider, Config{Repos: []string{"owner/repo"}}, nil)
		if err == nil || !strings.Contains(err.Error(), "does not remove work item labels") {
			t.Fatalf("New() error = %v, want missing-mutator refusal", err)
		}
	})
	t.Run("nil provider refused", func(t *testing.T) {
		t.Parallel()
		if _, err := New(nil, Config{Repos: []string{"owner/repo"}}, nil); err == nil {
			t.Fatal("New(nil, ...) succeeded, want error")
		}
	})
}

func TestDiscoverMapsLabelsToModes(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{
		caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel),
		byLabel: map[labelQuery][]forge.WorkItem{
			{repo: "owner/a", label: DefaultTriggerLabel, kind: forge.WorkItemKindIssue}: {
				{ID: "7", Repo: "owner/a", Title: "Fix the thing", Body: "It is broken.", URL: "https://forge/owner/a/issues/7", State: "open"},
			},
			{repo: "owner/a", label: DefaultFixLabel, kind: forge.WorkItemKindPullRequest}: {
				{ID: "9", Repo: "owner/a", Title: "Fix the thing", Body: "Attempt one.", URL: "https://forge/owner/a/pull/9", State: "open"},
			},
			{repo: "owner/b", label: DefaultTriggerLabel, kind: forge.WorkItemKindIssue}: {
				{ID: "2", Repo: "owner/b", Title: "Second repo", State: "open"},
			},
		},
	}
	a := testAdapter(provider, Config{Repos: []string{"owner/b", "owner/a"}, Lane: "frontier"})

	items, err := a.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() unexpected error: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("Discover() returned %d items, want 3: %+v", len(items), items)
	}
	if items[0].ID != "owner/a#7" || items[0].Mode != resolveIssueMode || items[0].Ref != 7 || items[0].Lane != "frontier" {
		t.Errorf("items[0] = %+v, want owner/a#7 resolve-issue", items[0])
	}
	if items[0].Assignment.Kind != source.AssignmentOther {
		t.Errorf("resolve-issue assignment kind = %q, want %q", items[0].Assignment.Kind, source.AssignmentOther)
	}
	if !strings.Contains(items[0].Context, "Fix the thing") || !strings.Contains(items[0].Context, "It is broken.") || !strings.Contains(items[0].Context, "Source: https://forge/owner/a/issues/7") {
		t.Errorf("resolve-issue context missing title, body, or source link: %q", items[0].Context)
	}
	// The fix item sorts between the two trigger items: owner/a before
	// owner/b, and within owner/a the lower number wins.
	if items[1].ID != "owner/a#9" || items[1].Mode != fixPRMode || items[1].Ref != 9 {
		t.Errorf("items[1] = %+v, want owner/a#9 fix-pr", items[1])
	}
	if items[1].Assignment.Kind != source.AssignmentReviewFeedback {
		t.Errorf("fix-pr assignment kind = %q, want %q", items[1].Assignment.Kind, source.AssignmentReviewFeedback)
	}
	if len(items[1].Assignment.Reasons) != 1 || !strings.Contains(items[1].Assignment.Reasons[0], DefaultFixLabel) {
		t.Errorf("fix-pr reasons = %+v, want one bounded reason naming the fix label", items[1].Assignment.Reasons)
	}
	if items[2].ID != "owner/b#2" {
		t.Errorf("items[2] = %+v, want owner/b#2", items[2])
	}
}

func TestDiscoverContextIsBounded(t *testing.T) {
	t.Parallel()
	huge := strings.Repeat("x", source.MaxSourceContextBytes*2)
	provider := &fakeProvider{
		caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel),
		byLabel: map[labelQuery][]forge.WorkItem{
			{repo: "owner/a", label: DefaultTriggerLabel, kind: forge.WorkItemKindIssue}: {
				{ID: "5", Repo: "owner/a", Title: huge, Body: huge, URL: "https://forge/owner/a/issues/5", State: "open"},
			},
		},
	}
	a := testAdapter(provider, Config{Repos: []string{"owner/a"}})

	items, err := a.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Discover() returned %d items, want 1", len(items))
	}
	if got := len(items[0].Context); got > source.MaxSourceContextBytes {
		t.Errorf("context length %d exceeds the %d-byte source budget", got, source.MaxSourceContextBytes)
	}
}

func TestDiscoverExcludingDropsRetainedWork(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{
		caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel),
		byLabel: map[labelQuery][]forge.WorkItem{
			{repo: "owner/a", label: DefaultTriggerLabel, kind: forge.WorkItemKindIssue}: {
				{ID: "1", Repo: "owner/a", State: "open"},
				{ID: "2", Repo: "owner/a", State: "open"},
			},
		},
	}
	a := testAdapter(provider, Config{Repos: []string{"owner/a"}})

	excluded := []source.WorkItem{{ID: "owner/a#2"}}
	items, err := a.DiscoverExcluding(context.Background(), excluded)
	if err != nil {
		t.Fatalf("DiscoverExcluding() unexpected error: %v", err)
	}
	if len(items) != 1 || items[0].ID != "owner/a#1" {
		t.Fatalf("DiscoverExcluding() = %+v, want only owner/a#1", items)
	}
}

func TestDiscoverRejectsMalformedProviderID(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{
		caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel),
		byLabel: map[labelQuery][]forge.WorkItem{
			{repo: "owner/a", label: DefaultTriggerLabel, kind: forge.WorkItemKindIssue}: {
				{ID: "not-a-number", Repo: "owner/a", State: "open"},
			},
		},
	}
	a := testAdapter(provider, Config{Repos: []string{"owner/a"}})

	if _, err := a.Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "malformed work item ID") {
		t.Fatalf("Discover() error = %v, want malformed work item ID refusal", err)
	}
}

func TestDiscoverListFailureFailsWholePoll(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{
		caps:     forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel),
		failRepo: "owner/b",
		failErr:  errors.New("forge down"),
	}
	a := testAdapter(provider, Config{Repos: []string{"owner/a", "owner/b"}})

	if _, err := a.Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "forge down") {
		t.Fatalf("Discover() error = %v, want the listing failure", err)
	}
}

func TestResolveRemovesModeMatchedLabel(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
	a := testAdapter(provider, Config{Repos: []string{"owner/a"}})

	cases := []struct {
		name     string
		item     source.WorkItem
		wantRepo string
		wantID   string
		wantLab  string
	}{
		{
			name:     "resolve-issue removes trigger label",
			item:     source.WorkItem{ID: "owner/a#7", Repo: "owner/a", Mode: resolveIssueMode},
			wantRepo: "owner/a",
			wantID:   "7",
			wantLab:  DefaultTriggerLabel,
		},
		{
			name:     "fix-pr removes fix label",
			item:     source.WorkItem{ID: "owner/a#9", Repo: "owner/a", Mode: fixPRMode},
			wantRepo: "owner/a",
			wantID:   "9",
			wantLab:  DefaultFixLabel,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := a.Resolve(context.Background(), tc.item); err != nil {
				t.Fatalf("Resolve() unexpected error: %v", err)
			}
			if len(provider.removed) != 1 {
				t.Fatalf("removals = %+v, want exactly one", provider.removed)
			}
			got := provider.removed[0]
			if got.repo != tc.wantRepo || got.id != tc.wantID || got.label != tc.wantLab {
				t.Errorf("removal = %+v, want repo %q id %q label %q", got, tc.wantRepo, tc.wantID, tc.wantLab)
			}
			provider.removed = nil
		})
	}
	t.Run("unsupported mode refused", func(t *testing.T) {
		if err := a.Resolve(context.Background(), source.WorkItem{ID: "owner/a#7", Mode: "other"}); err == nil {
			t.Fatal("Resolve() with unsupported mode succeeded, want error")
		}
	})
	t.Run("malformed identity refused", func(t *testing.T) {
		if err := a.Resolve(context.Background(), source.WorkItem{ID: "owner/a", Mode: resolveIssueMode}); err == nil {
			t.Fatal("Resolve() with malformed ID succeeded, want error")
		}
	})
	t.Run("cross-repo identity refused", func(t *testing.T) {
		if err := a.Resolve(context.Background(), source.WorkItem{ID: "owner/a#7", Repo: "owner/b", Mode: resolveIssueMode}); err == nil {
			t.Fatal("Resolve() with mismatched repository succeeded, want error")
		}
	})
}

func TestWorkIdentityIsStable(t *testing.T) {
	t.Parallel()
	a := testAdapter(&fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}, Config{Repos: []string{"owner/a"}})
	if got := a.WorkIdentity("owner/a#7"); got != "owner/a#7" {
		t.Errorf("WorkIdentity() = %q, want unchanged", got)
	}
}

func TestNoOpLifecycle(t *testing.T) {
	t.Parallel()
	a := testAdapter(&fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}, Config{Repos: []string{"owner/a"}})
	item := source.WorkItem{ID: "owner/a#7"}
	if err := a.Claim(context.Background(), item); err != nil {
		t.Errorf("Claim() unexpected error: %v", err)
	}
	if err := a.Release(context.Background(), item); err != nil {
		t.Errorf("Release() unexpected error: %v", err)
	}
	if err := a.Transition(context.Background(), item, source.StateInReview); err != nil {
		t.Errorf("Transition() unexpected error: %v", err)
	}
}

func TestDiagnosticsEnvelope(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
	reg := prometheus.NewRegistry()
	e := connector.NewEmitter(reg, nil)
	if _, err := New(provider, Config{Repos: []string{"owner/a"}}, e); err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}
	states := map[string]string{
		connector.CapabilityDiscovery:             connector.CapabilitySupported,
		connector.CapabilityDiscoveryExclusions:   connector.CapabilitySupported,
		connector.CapabilityWorkIdentity:          connector.CapabilitySupported,
		connector.CapabilityAvailability:          connector.CapabilitySupported,
		connector.CapabilityLifecycleReport:       connector.CapabilityUnsupported,
		connector.CapabilityPreLaunchRevalidation: connector.CapabilityUnsupported,
		connector.CapabilityCIParkLifecycle:       connector.CapabilityUnsupported,
	}
	for capability, state := range states {
		got, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": connectorName, "capability": capability, "state": state,
		})
		if !ok || got != 1 {
			t.Errorf("capability %q state %q = %v, ok=%v; want 1", capability, state, got, ok)
		}
	}
}

func findSeries(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
					break
				}
			}
			if match {
				switch {
				case m.GetCounter() != nil:
					return m.GetCounter().GetValue(), true
				case m.GetGauge() != nil:
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func TestDiscoverAvailabilityDiagnostics(t *testing.T) {
	t.Parallel()
	t.Run("available-on-success", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		e := connector.NewEmitter(reg, nil)
		provider := &fakeProvider{caps: forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel)}
		a, err := New(provider, Config{Repos: []string{"owner/a"}}, e)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		if _, err := a.Discover(context.Background()); err != nil {
			t.Fatalf("Discover() unexpected error: %v", err)
		}
		if got, ok := findSeries(t, reg, connector.MetricDiagnosticsTotal, map[string]string{
			"connector": connectorName, "signal": connector.SignalAvailability, "reason": reasonAvailable,
		}); !ok || got != 1 {
			t.Fatalf("availability/available counter = %v, ok=%v; want 1", got, ok)
		}
	})
	t.Run("unreachable-on-failure", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		e := connector.NewEmitter(reg, nil)
		provider := &fakeProvider{
			caps:     forge.NewCapabilities(forge.CapabilityListWorkItemsByLabel, forge.CapabilityRemoveWorkItemLabel),
			failRepo: "owner/a",
			failErr:  errors.New("forge down"),
		}
		a, err := New(provider, Config{Repos: []string{"owner/a"}}, e)
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		// The runner prefers the exclusion path whenever the adapter offers
		// it, so the emission is exercised through DiscoverExcluding, the
		// path production actually takes.
		if _, err := a.DiscoverExcluding(context.Background(), nil); err == nil {
			t.Fatal("DiscoverExcluding() succeeded, want the listing failure")
		}
		if got, ok := findSeries(t, reg, connector.MetricDiagnosticsTotal, map[string]string{
			"connector": connectorName, "signal": connector.SignalAvailability, "reason": reasonUnreachable,
		}); !ok || got != 1 {
			t.Fatalf("availability/unreachable counter = %v, ok=%v; want 1", got, ok)
		}
	})
}
