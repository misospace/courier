package broker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/forge"
	couriergit "github.com/misospace/courier/internal/git"
)

// These tests join the real git broker, pinned transport, forge adapter, and
// policy engine; only the provider's forge API is simulated.
type integrationForge struct {
	cfg       forge.ProviderConfig
	repos     map[string]forge.Repository
	paths     map[string]string
	protected map[string]bool
	prs       []forge.PullRequest
	nextPR    int
	racePR    bool
}

func (f *integrationForge) Config() forge.ProviderConfig { return f.cfg }
func (f *integrationForge) Capabilities() forge.Capabilities {
	return forge.NewCapabilities(forge.CapabilityReadPullRequest, forge.CapabilityCreatePullRequest, forge.CapabilityUpdatePullRequest)
}
func (f *integrationForge) ResolveRepository(_ context.Context, name string) (forge.Repository, error) {
	r, ok := f.repos[name]
	if !ok {
		return forge.Repository{}, errors.New("unknown repository")
	}
	return r, nil
}
func (f *integrationForge) ReadRef(_ context.Context, repo forge.Repository, ref string) (forge.RefState, error) {
	out, err := exec.Command("git", "--git-dir", f.paths[repo.Canonical], "rev-parse", "--verify", "refs/heads/"+ref).CombinedOutput()
	if err != nil {
		return forge.RefState{Ref: ref}, nil
	}
	return forge.RefState{Ref: ref, OID: strings.TrimSpace(string(out)), Exists: true}, nil
}
func (f *integrationForge) ReadEffectiveProtection(_ context.Context, repo forge.Repository, ref string) (forge.Protection, error) {
	return forge.Protection{Protected: f.protected[repo.Canonical+":"+ref]}, nil
}
func (f *integrationForge) CanWriteRepository(context.Context, forge.Repository) (bool, error) {
	return true, nil
}
func (f *integrationForge) ReadWorkItem(context.Context, forge.WorkItemRef) (forge.WorkItem, error) {
	return forge.WorkItem{}, forge.ErrUnsupported
}
func (f *integrationForge) ReadPullRequest(_ context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	for _, pr := range f.prs {
		if pr.Number == ref.Number && pr.BaseRepo == ref.Repo {
			if tip := f.tip(pr.HeadRepo, pr.HeadRef); tip != "" {
				pr.HeadSHA = tip
			}
			return pr, nil
		}
	}
	return forge.PullRequest{}, errors.New("pull request not found")
}
func (f *integrationForge) ListReviews(context.Context, forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}
func (f *integrationForge) ListComments(context.Context, forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}
func (f *integrationForge) ReadChecks(context.Context, forge.PullRequestRef, string) ([]forge.Check, error) {
	return nil, forge.ErrUnsupported
}
func (f *integrationForge) CreatePullRequest(_ context.Context, in forge.CreatePullRequestInput) (forge.PullRequest, error) {
	if f.racePR {
		f.nextPR++
		other := forge.PullRequest{Number: f.nextPR, Repo: in.Repo, BaseRepo: in.Repo, BaseRef: in.Base, BaseSHA: f.tip(in.Repo, in.Base), HeadRepo: in.HeadRepo, HeadRef: in.HeadRef, HeadSHA: f.tip(in.HeadRepo, in.HeadRef), State: "open", Title: "concurrent"}
		f.prs = append(f.prs, other)
	}
	f.nextPR++
	pr := forge.PullRequest{Number: f.nextPR, Repo: in.Repo, BaseRepo: in.Repo, BaseRef: in.Base, BaseSHA: f.tip(in.Repo, in.Base), HeadRepo: in.HeadRepo, HeadRef: in.HeadRef, HeadSHA: f.tip(in.HeadRepo, in.HeadRef), State: "open", Title: in.Title, Body: in.Body, Draft: in.Draft}
	f.prs = append(f.prs, pr)
	return pr, nil
}
func (f *integrationForge) UpdatePullRequest(_ context.Context, ref forge.PullRequestRef, in forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	for i := range f.prs {
		if f.prs[i].Number == ref.Number {
			if in.Title != "" {
				f.prs[i].Title = in.Title
			}
			if in.Body != "" {
				f.prs[i].Body = in.Body
			}
			return f.prs[i], nil
		}
	}
	return forge.PullRequest{}, errors.New("pull request not found")
}
func (f *integrationForge) CommentPullRequest(context.Context, forge.PullRequestRef, string) (forge.Comment, error) {
	return forge.Comment{}, forge.ErrUnsupported
}
func (f *integrationForge) ListPullRequestsByHead(_ context.Context, base forge.PullRequestRef, repo, ref string) ([]forge.PullRequest, error) {
	var result []forge.PullRequest
	for _, pr := range f.prs {
		if pr.BaseRepo == base.Repo && pr.HeadRepo == repo && pr.HeadRef == ref {
			if tip := f.tip(pr.HeadRepo, pr.HeadRef); tip != "" {
				pr.HeadSHA = tip
			}
			result = append(result, pr)
		}
	}
	return result, nil
}
func (f *integrationForge) tip(repo, ref string) string {
	if f.paths[repo] == "" {
		return ""
	}
	out, err := exec.Command("git", "--git-dir", f.paths[repo], "rev-parse", "--verify", "refs/heads/"+ref).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (f *integrationForge) Endpoint(_ context.Context, identity string) (string, error) {
	path := f.paths[identity]
	if path == "" {
		return "", errors.New("unknown endpoint")
	}
	return path, nil
}

var _ forge.Provider = (*integrationForge)(nil)
var _ forge.RepositoryPolicyProvider = (*integrationForge)(nil)
var _ forge.PullRequestHeadLister = (*integrationForge)(nil)
var _ GitEndpointResolver = (*integrationForge)(nil)

type integrationFixture struct {
	root                                            string
	baseRepo, workRepo                              forge.Repository
	base, anchor, proposed, nonFF, workRef, baseRef string
	provider                                        *integrationForge
	engine                                          *PolicyEngine
}

func newIntegrationFixture(t *testing.T, mode Mode) *integrationFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	basePath, workPath, source := filepath.Join(root, "base.git"), filepath.Join(root, "fork.git"), filepath.Join(root, "source")
	igGit(t, "", "init", "--bare", basePath)
	igGit(t, "", "init", "--bare", workPath)
	igGit(t, "", "init", source)
	igGit(t, source, "config", "user.email", "test@example.invalid")
	igGit(t, source, "config", "user.name", "test")
	igWrite(t, filepath.Join(source, "file"), "base\n")
	igGit(t, source, "add", "file")
	igGit(t, source, "commit", "-m", "base")
	igGit(t, source, "branch", "-M", "main")
	baseOID := igOID(t, source, "HEAD")
	igGit(t, source, "push", basePath, "HEAD:refs/heads/main")
	igGit(t, source, "push", workPath, "HEAD:refs/heads/main")
	workRef := "courier/acme/project/issue-122"
	anchor := baseOID
	if mode == ModeFixPR {
		igWrite(t, filepath.Join(source, "file"), "fork anchor\n")
		igGit(t, source, "commit", "-am", "fork anchor")
		anchor = igOID(t, source, "HEAD")
		igGit(t, source, "push", workPath, "HEAD:refs/heads/feature")
		igGit(t, source, "push", basePath, "HEAD:refs/heads/feature")
		workRef = "feature"
	}
	igWrite(t, filepath.Join(source, "file"), "proposed\n")
	igGit(t, source, "commit", "-am", "proposed")
	proposed := igOID(t, source, "HEAD")
	igGit(t, source, "checkout", "--orphan", "unrelated")
	igGit(t, source, "rm", "-rf", ".")
	igWrite(t, filepath.Join(source, "other"), "unrelated\n")
	igGit(t, source, "add", "other")
	igGit(t, source, "commit", "-m", "unrelated")
	nonFF := igOID(t, source, "HEAD")
	bundle := filepath.Join(root, "proposal.bundle")
	igGit(t, source, "bundle", "create", bundle, "--all")
	baseRepo := forge.Repository{ID: "base-id", Canonical: "acme/project", DefaultRef: "main"}
	workRepo := baseRepo
	workCanonical := baseRepo.Canonical
	if mode == ModeFixPR {
		workRepo = forge.Repository{ID: "fork-id", Canonical: "contributor/project", DefaultRef: "main"}
		workCanonical = workRepo.Canonical
	}
	provider := &integrationForge{cfg: forge.ProviderConfig{Name: "test"}, repos: map[string]forge.Repository{baseRepo.Canonical: baseRepo, workRepo.Canonical: workRepo}, paths: map[string]string{baseRepo.Canonical: basePath, workRepo.Canonical: workPath}, protected: map[string]bool{}, nextPR: 10}
	policy := Policy{RunUID: "run-uid", Mode: mode, Provider: "test", BaseRepo: baseRepo.Canonical, BaseRef: "main", BaseOID: baseOID, WorkRepo: workCanonical, WorkRef: workRef}
	if mode == ModeResolveIssue {
		policy.WorkInitiallyAbsent = true
		// The trusted source identity for the resolve-issue integration
		// tests is the base repository plus a stable issue number
		// that every PR body must reference for linkage to pass.
		policy.SourceIssue = SourceIssue{Owner: "acme", Name: "project", Number: 122}
	} else {
		policy.PRNumber, policy.HeadAnchorOID, policy.WorkAnchorOID = 7, anchor, anchor
		provider.prs = []forge.PullRequest{{Number: 7, Repo: baseRepo.Canonical, BaseRepo: baseRepo.Canonical, BaseRef: "main", BaseSHA: baseOID, HeadRepo: workRepo.Canonical, HeadRef: workRef, HeadSHA: anchor, State: "open", Title: "before", Body: "old"}}
	}
	adapter, err := NewForgeAdapter(policy, provider, provider, baseRepo, workRepo)
	if err != nil {
		t.Fatal(err)
	}
	gb, err := couriergit.NewBroker(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(gb.Directory()) })
	if err := gb.ImportBundle(ctx, bundle, proposed, anchor); err != nil {
		t.Fatalf("import proposal: %v", err)
	}
	// Import unrelated ancestry without asserting it descends from the anchor.
	if err := gb.ImportBundle(ctx, bundle, nonFF, ""); err != nil {
		t.Fatalf("import non-FF: %v", err)
	}
	pusher, err := NewGitPusher(ctx, GitTransportConfig{Policy: policy, Git: gb, Resolver: provider})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewPolicyEngine(policy, adapter, pusher)
	if err != nil {
		t.Fatal(err)
	}
	return &integrationFixture{root: root, baseRepo: baseRepo, workRepo: workRepo, base: baseOID, anchor: anchor, proposed: proposed, nonFF: nonFF, workRef: workRef, baseRef: "main", provider: provider, engine: engine}
}

func TestBrokerIntegrationResolvePublishAndCreatePR(t *testing.T) {
	f := newIntegrationFixture(t, ModeResolveIssue)
	ctx := context.Background()
	result, err := f.engine.Publish(ctx, PublicationRequest{RunUID: "run-uid", ProposedOID: f.proposed})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.OID != f.proposed {
		t.Fatalf("published OID = %s", result.OID)
	}
	pr, err := f.engine.CreatePullRequest(ctx, f.proposed, "issue 122", "Closes acme/project#122.", false)
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if pr.Number != 11 || pr.BaseRepo != f.baseRepo.Canonical || pr.BaseRef != f.baseRef || pr.HeadRepo != f.baseRepo.Canonical || pr.HeadRef != f.workRef || pr.HeadOID != f.proposed {
		t.Fatalf("created/read-back PR = %#v", pr)
	}
	if got := f.provider.tip(f.baseRepo.Canonical, f.workRef); got != f.proposed {
		t.Fatalf("remote work tip = %s", got)
	}
}

func TestBrokerIntegrationFixPRPublishesForkAndUpdatesMetadata(t *testing.T) {
	f := newIntegrationFixture(t, ModeFixPR)
	ctx := context.Background()
	baseCollision := f.provider.tip(f.baseRepo.Canonical, f.workRef)
	result, err := f.engine.Publish(ctx, PublicationRequest{RunUID: "run-uid", ExpectedWorkOID: f.anchor, ProposedOID: f.proposed})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.OID != f.proposed {
		t.Fatalf("published OID = %s", result.OID)
	}
	if got := f.provider.tip(f.baseRepo.Canonical, f.workRef); got != baseCollision {
		t.Fatalf("same-named base ref changed: %s -> %s", baseCollision, got)
	}
	if got := f.provider.tip(f.workRepo.Canonical, f.workRef); got != f.proposed {
		t.Fatalf("fork head = %s", got)
	}
	if err := f.engine.UpdateFixPR(ctx, UpdatePullRequest{Title: "after", Body: "updated"}); err != nil {
		t.Fatalf("UpdateFixPR: %v", err)
	}
	pr, err := f.provider.ReadPullRequest(ctx, forge.PullRequestRef{Repo: f.baseRepo.Canonical, Number: 7})
	if err != nil {
		t.Fatal(err)
	}
	if pr.Title != "after" || pr.Body != "updated" || pr.HeadRepo != f.workRepo.Canonical || pr.HeadSHA != f.proposed {
		t.Fatalf("updated PR = %#v", pr)
	}
}

func TestBrokerIntegrationDeniesProtectedAndDefaultWorkRefs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		protect bool
	}{{"default", false}, {"protected", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIntegrationFixture(t, ModeResolveIssue)
			if tc.protect {
				f.provider.protected[f.workRepo.Canonical+":"+f.workRef] = true
			}
			// Make the derived work ref equal the provider's default for the default case.
			if !tc.protect {
				f.provider.repos[f.workRepo.Canonical] = forge.Repository{ID: f.workRepo.ID, Canonical: f.workRepo.Canonical, DefaultRef: f.workRef}
			}
			if _, err := f.engine.Publish(context.Background(), PublicationRequest{RunUID: "run-uid", ProposedOID: f.proposed}); err == nil {
				t.Fatal("publication unexpectedly allowed")
			}
			if got := f.provider.tip(f.workRepo.Canonical, f.workRef); got != "" {
				t.Fatalf("denied ref was pushed: %s", got)
			}
		})
	}
}

func TestBrokerIntegrationDeniesForeignHeadAndNonFastForward(t *testing.T) {
	t.Run("foreign fork head", func(t *testing.T) {
		f := newIntegrationFixture(t, ModeFixPR)
		// Move fork and PR together to a commit not admitted or confirmed by this run.
		foreign := filepath.Join(f.root, "foreign")
		igGit(t, "", "clone", f.provider.paths[f.workRepo.Canonical], foreign)
		igGit(t, foreign, "config", "user.email", "foreign@example.invalid")
		igGit(t, foreign, "config", "user.name", "foreign")
		igWrite(t, filepath.Join(foreign, "foreign"), "foreign\n")
		igGit(t, foreign, "add", "foreign")
		igGit(t, foreign, "commit", "-m", "foreign")
		igGit(t, foreign, "push", "--force", "origin", "HEAD:refs/heads/"+f.workRef)
		if _, err := f.engine.Publish(context.Background(), PublicationRequest{RunUID: "run-uid", ExpectedWorkOID: f.anchor, ProposedOID: f.proposed}); err == nil {
			t.Fatal("foreign fork head accepted")
		}
	})
	t.Run("non-fast-forward proposal", func(t *testing.T) {
		f := newIntegrationFixture(t, ModeResolveIssue)
		if _, err := f.engine.Publish(context.Background(), PublicationRequest{RunUID: "run-uid", ProposedOID: f.nonFF}); err == nil {
			t.Fatal("non-fast-forward proposal accepted")
		}
		if got := f.provider.tip(f.workRepo.Canonical, f.workRef); got != "" {
			t.Fatalf("non-FF ref was pushed: %s", got)
		}
	})
}

func TestBrokerIntegrationDeniesConcurrentDuplicatePR(t *testing.T) {
	f := newIntegrationFixture(t, ModeResolveIssue)
	if _, err := f.engine.Publish(context.Background(), PublicationRequest{RunUID: "run-uid", ProposedOID: f.proposed}); err != nil {
		t.Fatal(err)
	}
	f.provider.racePR = true
	if _, err := f.engine.CreatePullRequest(context.Background(), f.proposed, "title", "Closes acme/project#122.", false); err == nil {
		t.Fatal("concurrent duplicate PR accepted")
	}
}

func igGit(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}
func igWrite(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}
func igOID(t *testing.T, dir, rev string) string {
	t.Helper()
	return strings.TrimSpace(string(igGit(t, dir, "rev-parse", rev)))
}
