package forgetest_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/forge/forgetest"
)

func TestOpRecordingOrderAndFields(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Repositories: []forge.Repository{
			{ID: "acme/web", Canonical: "github.com/acme/web", DefaultRef: "main"},
		},
		Refs: map[string]map[string]string{
			"github.com/acme/web": {"main": "aaa", "feat": "bbb"},
		},
		WorkItems: map[forge.WorkItemRef]forge.WorkItem{
			{Repo: "github.com/acme/web", ID: "1"}: {Repo: "github.com/acme/web", ID: "1", Title: "do it"},
		},
		PullRequests: []forge.PullRequest{
			{Number: 7, Repo: "github.com/acme/web", BaseRepo: "github.com/acme/web", BaseRef: "main",
				HeadRepo: "fork/acme/web", HeadRef: "feat", HeadSHA: "bbb", Title: "t", State: "open"},
		},
		Reviews: map[forge.PullRequestRef][]forge.Review{
			{Repo: "github.com/acme/web", Number: 7}: {{ID: "r1", Author: "a", State: "approved"}},
		},
		Comments: map[forge.PullRequestRef][]forge.Comment{
			{Repo: "github.com/acme/web", Number: 7}: {{ID: "c1", Author: "a", Body: "hi"}},
		},
		Checks: map[forge.PullRequestRef][]forge.Check{
			{Repo: "github.com/acme/web", Number: 7}: {{Name: "ci", State: "completed", Conclusion: "success"}},
		},
	})
	ctx := context.Background()
	repo, err := f.ResolveRepository(ctx, "acme/web")
	if err != nil || repo.Canonical != "github.com/acme/web" {
		t.Fatalf("ResolveRepository = (%+v, %v), want canonical github.com/acme/web", repo, err)
	}
	if state, err := f.ReadRef(ctx, repo, "main"); err != nil || !state.Exists || state.OID != "aaa" {
		t.Fatalf("ReadRef(main) = (%+v, %v), want exists aaa", state, err)
	}
	if state, err := f.ReadRef(ctx, repo, "missing"); err != nil || state.Exists || state.Ref != "missing" {
		t.Fatalf("ReadRef(missing) = (%+v, %v), want not exists", state, err)
	}
	if prot, err := f.ReadEffectiveProtection(ctx, repo, "main"); err != nil || prot.Protected {
		t.Fatalf("ReadEffectiveProtection = (%+v, %v), want default not protected", prot, err)
	}
	if writable, err := f.CanWriteRepository(ctx, repo); err != nil || !writable {
		t.Fatalf("CanWriteRepository = (%v, %v), want default writable", writable, err)
	}
	wi, err := f.ReadWorkItem(ctx, forge.WorkItemRef{Repo: "github.com/acme/web", ID: "1"})
	if err != nil || wi.Title != "do it" {
		t.Fatalf("ReadWorkItem = (%+v, %v), want do it", wi, err)
	}
	wi, err = f.ReadWorkItem(ctx, forge.WorkItemRef{Repo: "github.com/acme/web", ID: "404"})
	if !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadWorkItem(404) err = %v, want ErrNotFound", err)
	}
	pr, err := f.ReadPullRequest(ctx, forge.PullRequestRef{Repo: "github.com/acme/web", Number: 7})
	if err != nil || pr.Title != "t" {
		t.Fatalf("ReadPullRequest = (%+v, %v), want t", pr, err)
	}
	reviews, err := f.ListReviews(ctx, forge.PullRequestRef{Repo: "github.com/acme/web", Number: 7})
	if err != nil || len(reviews) != 1 || reviews[0].ID != "r1" {
		t.Fatalf("ListReviews = (%+v, %v), want one r1", reviews, err)
	}
	comments, err := f.ListComments(ctx, forge.PullRequestRef{Repo: "github.com/acme/web", Number: 7})
	if err != nil || len(comments) != 1 || comments[0].ID != "c1" {
		t.Fatalf("ListComments = (%+v, %v), want one c1", comments, err)
	}
	checks, err := f.ReadChecks(ctx, forge.PullRequestRef{Repo: "github.com/acme/web", Number: 7}, "bbb")
	if err != nil || len(checks) != 1 || checks[0].Name != "ci" {
		t.Fatalf("ReadChecks = (%+v, %v), want one ci", checks, err)
	}
	base := forge.PullRequestRef{Repo: "github.com/acme/web", Number: 7}
	list, err := f.ListPullRequestsByHead(ctx, base, "fork/acme/web", "feat")
	if err != nil || len(list) != 1 || list[0].Number != 7 {
		t.Fatalf("ListPullRequestsByHead = (%+v, %v), want one 7", list, err)
	}

	ref := forge.PullRequestRef{Repo: "github.com/acme/web", Number: 7}
	want := []forgetest.Op{
		{Kind: forgetest.OpResolveRepository, Repo: "acme/web"},
		{Kind: forgetest.OpReadRef, Repo: "github.com/acme/web", Ref: "main"},
		{Kind: forgetest.OpReadRef, Repo: "github.com/acme/web", Ref: "missing"},
		{Kind: forgetest.OpReadEffectiveProtection, Repo: "github.com/acme/web", Ref: "main"},
		{Kind: forgetest.OpCanWriteRepository, Repo: "github.com/acme/web"},
		{Kind: forgetest.OpReadWorkItem, Repo: "github.com/acme/web", WorkItem: forge.WorkItemRef{Repo: "github.com/acme/web", ID: "1"}},
		{Kind: forgetest.OpReadWorkItem, Repo: "github.com/acme/web", WorkItem: forge.WorkItemRef{Repo: "github.com/acme/web", ID: "404"}},
		{Kind: forgetest.OpReadPullRequest, Repo: "github.com/acme/web", PR: ref},
		{Kind: forgetest.OpListReviews, Repo: "github.com/acme/web", PR: ref},
		{Kind: forgetest.OpListComments, Repo: "github.com/acme/web", PR: ref},
		{Kind: forgetest.OpReadChecks, Repo: "github.com/acme/web", PR: ref, Head: "bbb"},
		{Kind: forgetest.OpListPullRequestsByHead, Repo: "fork/acme/web", PR: base, Base: "github.com/acme/web", HeadRepo: "fork/acme/web", Ref: "feat"},
	}
	if got := f.Ops(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Ops =\n%+v\nwant\n%+v", got, want)
	}
	// Config and Capabilities are not recorded.
	if got := f.Ops(); len(got) != len(want) {
		t.Fatalf("ops after Config/Capabilities = %d, want %d", len(got), len(want))
	}
	f.Config()
	f.Capabilities()
	if got := f.Ops(); len(got) != len(want) {
		t.Fatalf("ops after Config/Capabilities = %d, want %d", len(got), len(want))
	}
}

func TestWriteOpsFiltersToWritesOnly(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		PullRequests: []forge.PullRequest{
			{Number: 5, Repo: "r", BaseRepo: "r", BaseRef: "main", HeadRepo: "h", HeadRef: "b", Title: "t", State: "open"},
		},
	})
	ctx := context.Background()
	ref := forge.PullRequestRef{Repo: "r", Number: 5}
	if _, err := f.ReadWorkItem(ctx, forge.WorkItemRef{Repo: "r", ID: "1"}); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadWorkItem err = %v, want ErrNotFound", err)
	}
	if _, err := f.UpdatePullRequest(ctx, ref, forge.UpdatePullRequestInput{Title: "t2"}); err != nil {
		t.Fatalf("UpdatePullRequest: %v", err)
	}
	if _, err := f.CommentPullRequest(ctx, ref, "hello"); err != nil {
		t.Fatalf("CommentPullRequest: %v", err)
	}
	in := forge.CreatePullRequestInput{Repo: "r", Title: "n", Head: "b", HeadRepo: "h", HeadRef: "b", Base: "main", Body: "bb"}
	if _, err := f.CreatePullRequest(ctx, in); err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if _, err := f.ReadChecks(ctx, ref, "sha"); err != nil {
		t.Fatalf("ReadChecks: %v", err)
	}

	want := []forgetest.Op{
		{Kind: forgetest.OpUpdatePullRequest, Repo: "r", PR: ref, Update: forge.UpdatePullRequestInput{Title: "t2"}},
		{Kind: forgetest.OpCommentPullRequest, Repo: "r", PR: ref, Body: "hello"},
		{Kind: forgetest.OpCreatePullRequest, Repo: "r", Input: in},
	}
	if got := f.WriteOps(); !reflect.DeepEqual(got, want) {
		t.Fatalf("WriteOps =\n%+v\nwant\n%+v", got, want)
	}
	if got := f.WriteCount(); got != 3 {
		t.Fatalf("WriteCount = %d, want 3 (reads excluded)", got)
	}
}

func TestCreatePullRequestEffect(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Refs: map[string]map[string]string{
			"base/repo": {"main": "base-oid"},
			"fork/repo": {"branch": "head-oid"},
		},
		PullRequests: []forge.PullRequest{
			{Number: 9, Repo: "base/repo", BaseRepo: "base/repo", BaseRef: "main",
				HeadRepo: "fork/other", HeadRef: "other", State: "open", Title: "seeded"},
		},
	})
	ctx := context.Background()
	in := forge.CreatePullRequestInput{
		Repo: "base/repo", Title: "new", Head: "branch",
		HeadRepo: "fork/repo", HeadRef: "branch", Base: "main", Body: "b", Draft: true,
	}
	pr, err := f.CreatePullRequest(ctx, in)
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if pr.Number != 101 {
		t.Fatalf("Number = %d, want 101", pr.Number)
	}
	if pr.BaseSHA != "base-oid" || pr.HeadSHA != "head-oid" {
		t.Fatalf("SHAs = (%q, %q), want (base-oid, head-oid)", pr.BaseSHA, pr.HeadSHA)
	}
	if pr.State != "open" || !pr.Draft || pr.Title != "new" || pr.Body != "b" {
		t.Fatalf("PR fields = %+v, want open/draft/new/b", pr)
	}
	if want := "https://forge.invalid/base/repo/pulls/101"; pr.URL != want {
		t.Fatalf("URL = %q, want %q", pr.URL, want)
	}

	all := f.PullRequests()
	if len(all) != 2 || all[0].Number != 9 || all[1].Number != 101 {
		t.Fatalf("PullRequests numbers = %+v, want [9 101]", all)
	}

	// The live head SHA is refreshed on each list call.
	f.SetRef("fork/repo", "branch", "head-oid-2")
	list, err := f.ListPullRequestsByHead(ctx, forge.PullRequestRef{Repo: "base/repo", Number: 101}, "fork/repo", "branch")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListPullRequestsByHead = (%+v, %v), want one result", list, err)
	}
	if list[0].Number != 101 || list[0].HeadSHA != "head-oid-2" {
		t.Fatalf("list[0] = %+v, want number 101 with live head-oid-2", list[0])
	}
	if pr.HeadSHA != "head-oid" {
		t.Fatalf("earlier returned PR mutated: HeadSHA = %q, want head-oid", pr.HeadSHA)
	}
	// A different head ref lists nothing for this base.
	list, err = f.ListPullRequestsByHead(ctx, forge.PullRequestRef{Repo: "base/repo", Number: 101}, "fork/repo", "other")
	if err != nil || len(list) != 0 {
		t.Fatalf("ListPullRequestsByHead(other) = (%+v, %v), want none", list, err)
	}

	// Numbering keeps incrementing.
	next, err := f.CreatePullRequest(ctx, in)
	if err != nil || next.Number != 102 {
		t.Fatalf("second create = (%+v, %v), want number 102", next, err)
	}
}

func TestFailNextReadRef(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Repositories: []forge.Repository{{ID: "r", Canonical: "r", DefaultRef: "main"}},
		Refs:         map[string]map[string]string{"r": {"main": "oid"}},
	})
	ctx := context.Background()
	repo := forge.Repository{ID: "r", Canonical: "r"}
	injected := errors.New("injected read failure")
	f.FailNext(forgetest.OpReadRef, injected)

	state, err := f.ReadRef(ctx, repo, "main")
	if !errors.Is(err, injected) {
		t.Fatalf("ReadRef err = %v, want injected", err)
	}
	if state != (forge.RefState{}) {
		t.Fatalf("ReadRef response = %+v, want lost (zero value)", state)
	}
	// The failure is consumed: the next call succeeds against unchanged state.
	state, err = f.ReadRef(ctx, repo, "main")
	if err != nil {
		t.Fatalf("second ReadRef: %v", err)
	}
	if state.OID != "oid" || !state.Exists {
		t.Fatalf("second ReadRef = %+v, want oid/exists", state)
	}
	if got := f.OpsOf(forgetest.OpReadRef); len(got) != 2 {
		t.Fatalf("OpReadRef ops = %d, want 2", len(got))
	}
}

func TestFailNextResolveRepository(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Repositories: []forge.Repository{{ID: "r", Canonical: "r", DefaultRef: "main"}},
	})
	ctx := context.Background()
	injected := errors.New("injected lookup failure")
	f.FailNext(forgetest.OpResolveRepository, injected)

	repo, err := f.ResolveRepository(ctx, "r")
	if !errors.Is(err, injected) {
		t.Fatalf("ResolveRepository err = %v, want injected", err)
	}
	if repo != (forge.Repository{}) {
		t.Fatalf("ResolveRepository response = %+v, want lost (zero value)", repo)
	}
	// The failure is consumed: the next lookup succeeds against unchanged state.
	repo, err = f.ResolveRepository(ctx, "r")
	if err != nil || repo.Canonical != "r" {
		t.Fatalf("second ResolveRepository = (%+v, %v), want r/nil", repo, err)
	}
	if got := f.OpsOf(forgetest.OpResolveRepository); len(got) != 2 {
		t.Fatalf("OpResolveRepository ops = %d, want 2", len(got))
	}
}

func TestFailNextAfterEffectCreatePullRequest(t *testing.T) {
	f := forgetest.New(forgetest.Config{})
	ctx := context.Background()
	injected := errors.New("network blip after write landed")
	f.FailNextAfterEffect(forgetest.OpCreatePullRequest, injected)
	in := forge.CreatePullRequestInput{Repo: "r", Title: "t", Head: "b", HeadRepo: "r", HeadRef: "b", Base: "main"}

	pr, err := f.CreatePullRequest(ctx, in)
	if !errors.Is(err, injected) {
		t.Fatalf("CreatePullRequest err = %v, want injected", err)
	}
	if pr != (forge.PullRequest{}) {
		t.Fatalf("response = %+v, want lost (zero value)", pr)
	}
	// The write landed exactly once.
	all := f.PullRequests()
	if len(all) != 1 {
		t.Fatalf("PullRequests = %d, want exactly 1", len(all))
	}
	if all[0].Number != 101 || all[0].Title != "t" || all[0].Repo != "r" || all[0].State != "open" {
		t.Fatalf("stored PR = %+v, want 101/t/r/open", all[0])
	}
	if got := f.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d, want 1", got)
	}
	// Numbering advanced despite the lost response.
	next, err := f.CreatePullRequest(ctx, in)
	if err != nil || next.Number != 102 {
		t.Fatalf("second create = (%+v, %v), want number 102", next, err)
	}
}

func TestUnregisteredCreateCapability(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Capabilities: []forge.Capability{forge.CapabilityReadWorkItem, forge.CapabilityComment},
	})
	ctx := context.Background()

	caps := f.Capabilities()
	if !caps.Has(forge.CapabilityReadWorkItem) || !caps.Has(forge.CapabilityComment) {
		t.Fatalf("Capabilities = %+v, want exactly the listed two", caps.List())
	}
	if caps.Has(forge.CapabilityCreatePullRequest) {
		t.Fatalf("Capabilities must not include create")
	}
	// The returned set is a clone: mutating it does not affect the fake.
	caps.Register(forge.CapabilityReadWorkItem, forge.Availability{Available: false, Detail: "down"})
	if got := f.Capabilities().Available(forge.CapabilityReadWorkItem).Available; !got {
		t.Fatalf("Capabilities clone leaked into the fake")
	}

	_, err := f.CreatePullRequest(ctx, forge.CreatePullRequestInput{Repo: "r", Title: "t", Head: "b", HeadRepo: "r", HeadRef: "b", Base: "main"})
	if !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("CreatePullRequest err = %v, want ErrUnsupported", err)
	}
	if n := len(f.PullRequests()); n != 0 {
		t.Fatalf("PullRequests = %d, want none created", n)
	}
	// The ErrUnsupported path still records the op before the behavior.
	if ops := f.OpsOf(forgetest.OpCreatePullRequest); len(ops) != 1 {
		t.Fatalf("OpCreatePullRequest ops = %d, want 1", len(ops))
	}
}

func TestConcurrentCreateAndComment(t *testing.T) {
	f := forgetest.New(forgetest.Config{})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := forge.CreatePullRequestInput{
				Repo: "r", Title: fmt.Sprintf("pr-%d", i), Head: "b",
				HeadRepo: "r", HeadRef: "b", Base: "main",
			}
			pr, err := f.CreatePullRequest(ctx, in)
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ref := forge.PullRequestRef{Repo: pr.Repo, Number: pr.Number}
			if _, err := f.CommentPullRequest(ctx, ref, "hello"); err != nil {
				t.Errorf("comment: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := len(f.Ops()); got != 16 {
		t.Fatalf("Ops = %d, want exactly 16", got)
	}
	if got := len(f.OpsOf(forgetest.OpCreatePullRequest)); got != 8 {
		t.Fatalf("create ops = %d, want 8", got)
	}
	if got := len(f.OpsOf(forgetest.OpCommentPullRequest)); got != 8 {
		t.Fatalf("comment ops = %d, want 8", got)
	}
	if got := f.WriteCount(); got != 16 {
		t.Fatalf("WriteCount = %d, want 16", got)
	}
	prs := f.PullRequests()
	if len(prs) != 8 {
		t.Fatalf("PullRequests = %d, want 8", len(prs))
	}
	seen := map[int]bool{}
	for _, pr := range prs {
		if pr.Number < 101 || pr.Number > 108 || seen[pr.Number] {
			t.Fatalf("numbers not exactly 101..108: %+v", prs)
		}
		seen[pr.Number] = true
	}
	for _, pr := range prs {
		got, err := f.ListComments(ctx, forge.PullRequestRef{Repo: pr.Repo, Number: pr.Number})
		if err != nil || len(got) != 1 || got[0].Author != "forgetest" || got[0].Body != "hello" {
			t.Fatalf("comments for %d = (%+v, %v), want one forgetest/hello", pr.Number, got, err)
		}
	}
}

func TestNewDeepCopiesSeeds(t *testing.T) {
	trueValue := true
	cfg := forgetest.Config{
		Provider:     forge.ProviderConfig{Name: "p", Endpoint: "e", CredentialRef: "c"},
		Repositories: []forge.Repository{{ID: "r", Canonical: "r", DefaultRef: "main"}},
		Refs:         map[string]map[string]string{"r": {"main": "oid"}},
		Protected:    map[string]bool{"r": true},
		Writable:     map[string]bool{"r": false},
		WorkItems: map[forge.WorkItemRef]forge.WorkItem{
			{Repo: "r", ID: "1"}: {Repo: "r", ID: "1", Title: "t", Labels: []string{"a"}},
		},
		PullRequests: []forge.PullRequest{
			{Number: 3, Repo: "r", BaseRepo: "r", BaseRef: "main", HeadRepo: "h", HeadRef: "b",
				HeadSHA: "s", Title: "t", State: "open", Mergeable: &trueValue},
		},
		Reviews: map[forge.PullRequestRef][]forge.Review{
			{Repo: "r", Number: 3}: {{ID: "r1"}},
		},
	}
	f := forgetest.New(cfg)

	// Mutate every seed after New; none of it may reach the fake.
	cfg.Provider.CredentialRef = "mutated"
	cfg.Repositories[0].Canonical = "mutated"
	cfg.Refs["r"]["main"] = "mutated"
	cfg.Protected["r"] = false
	cfg.Writable["r"] = true
	wi := cfg.WorkItems[forge.WorkItemRef{Repo: "r", ID: "1"}]
	wi.Labels = append(wi.Labels, "b")
	cfg.WorkItems[forge.WorkItemRef{Repo: "r", ID: "1"}] = wi
	cfg.PullRequests[0].Title = "mutated"
	*cfg.PullRequests[0].Mergeable = false
	cfg.Reviews[forge.PullRequestRef{Repo: "r", Number: 3}][0].ID = "mutated"

	ctx := context.Background()
	if got := f.Config().CredentialRef; got != "c" {
		t.Fatalf("Config = %q, want c", got)
	}
	repo, err := f.ResolveRepository(ctx, "r")
	if err != nil || repo.Canonical != "r" {
		t.Fatalf("ResolveRepository = (%+v, %v), want canonical r", repo, err)
	}
	if state, err := f.ReadRef(ctx, repo, "main"); err != nil || state.OID != "oid" {
		t.Fatalf("ReadRef = (%+v, %v), want oid", state, err)
	}
	if prot, err := f.ReadEffectiveProtection(ctx, repo, "main"); err != nil || !prot.Protected {
		t.Fatalf("ReadEffectiveProtection = (%+v, %v), want protected", prot, err)
	}
	if writable, err := f.CanWriteRepository(ctx, repo); err != nil || writable {
		t.Fatalf("CanWriteRepository = (%v, %v), want not writable", writable, err)
	}
	wi, err = f.ReadWorkItem(ctx, forge.WorkItemRef{Repo: "r", ID: "1"})
	if err != nil || len(wi.Labels) != 1 || wi.Labels[0] != "a" {
		t.Fatalf("ReadWorkItem = (%+v, %v), want labels [a]", wi, err)
	}
	prs := f.PullRequests()
	if len(prs) != 1 || prs[0].Title != "t" || prs[0].Mergeable == nil || !*prs[0].Mergeable {
		t.Fatalf("PullRequests = %+v, want title t and Mergeable true", prs)
	}
	reviews, err := f.ListReviews(ctx, forge.PullRequestRef{Repo: "r", Number: 3})
	if err != nil || len(reviews) != 1 || reviews[0].ID != "r1" {
		t.Fatalf("ListReviews = (%+v, %v), want r1", reviews, err)
	}
}

func TestUnregisteredVerbsRefuseAllOperations(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Capabilities: []forge.Capability{forge.CapabilityReadWorkItem},
		PullRequests: []forge.PullRequest{
			{Number: 5, Repo: "r", BaseRepo: "r", BaseRef: "main", HeadRepo: "h", HeadRef: "b", State: "open"},
		},
	})
	ctx := context.Background()
	ref := forge.PullRequestRef{Repo: "r", Number: 5}
	if _, err := f.CreatePullRequest(ctx, forge.CreatePullRequestInput{Repo: "r", Head: "b", HeadRepo: "h", HeadRef: "b", Base: "main"}); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("CreatePullRequest err = %v, want ErrUnsupported", err)
	}
	if _, err := f.UpdatePullRequest(ctx, ref, forge.UpdatePullRequestInput{Title: "x"}); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("UpdatePullRequest err = %v, want ErrUnsupported", err)
	}
	if _, err := f.CommentPullRequest(ctx, ref, "x"); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("CommentPullRequest err = %v, want ErrUnsupported", err)
	}
	if _, err := f.ReadPullRequest(ctx, ref); !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("ReadPullRequest err = %v, want ErrUnsupported", err)
	}
	// Nothing performed: no write landed and the stored request is unchanged.
	if got := len(f.PullRequests()); got != 1 {
		t.Fatalf("PullRequests = %d, want only the seeded 5", got)
	}
	if f.PullRequests()[0].Title != "" {
		t.Fatalf("stored request mutated: %+v", f.PullRequests()[0])
	}
	// Refused calls are still recorded.
	if got := f.WriteCount(); got != 3 {
		t.Fatalf("WriteCount = %d, want 3 recorded attempts", got)
	}
}

func TestUnavailableCapabilityReportsDetailAndRefuses(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Unavailable: map[forge.Capability]string{forge.CapabilityCreatePullRequest: "rate limited"},
	})
	caps := f.Capabilities()
	if !caps.Has(forge.CapabilityCreatePullRequest) {
		t.Fatalf("create capability must stay registered when unavailable")
	}
	if got := caps.Available(forge.CapabilityCreatePullRequest); got.Available || got.Detail != "rate limited" {
		t.Fatalf("availability = %+v, want unavailable with detail rate limited", got)
	}
	_, err := f.CreatePullRequest(context.Background(), forge.CreatePullRequestInput{Repo: "r", Head: "b", HeadRepo: "r", HeadRef: "b", Base: "main"})
	if !errors.Is(err, forge.ErrUnsupported) {
		t.Fatalf("CreatePullRequest err = %v, want ErrUnsupported", err)
	}
}

func TestNumberingStartsAboveSeededPullRequests(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		PullRequests: []forge.PullRequest{
			{Number: 105, Repo: "r", BaseRepo: "r", BaseRef: "main", HeadRepo: "h", HeadRef: "b", State: "open"},
		},
	})
	created, err := f.CreatePullRequest(context.Background(), forge.CreatePullRequestInput{Repo: "r", Head: "b", HeadRepo: "h", HeadRef: "b", Base: "main"})
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if created.Number != 106 {
		t.Fatalf("Number = %d, want 106 (above the highest seeded request)", created.Number)
	}
	if got := len(f.PullRequests()); got != 2 {
		t.Fatalf("PullRequests = %d, want seeded 105 plus created 106", got)
	}
	// A request added after New also moves the counter.
	f.AddPullRequest(forge.PullRequest{Number: 200, Repo: "r", BaseRepo: "r", BaseRef: "main", HeadRepo: "h", HeadRef: "b", State: "open"})
	created, err = f.CreatePullRequest(context.Background(), forge.CreatePullRequestInput{Repo: "r", Head: "b", HeadRepo: "h", HeadRef: "b", Base: "main"})
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if created.Number != 201 {
		t.Fatalf("Number = %d, want 201 (above the added request)", created.Number)
	}
}

func TestAfterEffectInjectionSurvivesRefusedCall(t *testing.T) {
	f := forgetest.New(forgetest.Config{})
	ctx := context.Background()
	ref := forge.PullRequestRef{Repo: "r", Number: 7}
	injected := errors.New("lost response")
	f.FailNextAfterEffect(forgetest.OpUpdatePullRequest, injected)

	// The update cannot run: the request does not exist. The injection stays
	// queued rather than being consumed with no effect.
	if _, err := f.UpdatePullRequest(ctx, ref, forge.UpdatePullRequestInput{Title: "x"}); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("UpdatePullRequest err = %v, want ErrNotFound", err)
	}
	f.AddPullRequest(forge.PullRequest{Number: 7, Repo: "r", BaseRepo: "r", BaseRef: "main", HeadRepo: "h", HeadRef: "b", State: "open"})

	// Now the effect runs and the queued injection delivers: the update
	// landed, the reply is lost.
	if _, err := f.UpdatePullRequest(ctx, ref, forge.UpdatePullRequestInput{Title: "landed"}); !errors.Is(err, injected) {
		t.Fatalf("second UpdatePullRequest err = %v, want injected", err)
	}
	if got := f.PullRequests(); len(got) != 1 || got[0].Title != "landed" {
		t.Fatalf("stored requests = %+v, want one with title landed", got)
	}
}

func TestClosedHeadStaysFrozenWhenBranchMoves(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Repositories: []forge.Repository{{ID: "base", Canonical: "base", DefaultRef: "main"}},
		Refs:         map[string]map[string]string{"head": {"b": "oid-1"}},
		PullRequests: []forge.PullRequest{
			{Number: 1, Repo: "base", BaseRepo: "base", BaseRef: "main", HeadRepo: "head", HeadRef: "b", HeadSHA: "oid-1", State: "open"},
			{Number: 2, Repo: "base", BaseRepo: "base", BaseRef: "main", HeadRepo: "head", HeadRef: "b", HeadSHA: "oid-1", State: "closed"},
		},
	})
	f.SetRef("head", "b", "oid-2")
	list, err := f.ListPullRequestsByHead(context.Background(), forge.PullRequestRef{Repo: "base"}, "head", "b")
	if err != nil || len(list) != 2 {
		t.Fatalf("ListPullRequestsByHead = (%+v, %v), want two", list, err)
	}
	if list[0].Number != 1 || list[0].HeadSHA != "oid-2" {
		t.Fatalf("open request = %+v, want head oid-2 (live)", list[0])
	}
	if list[1].Number != 2 || list[1].HeadSHA != "oid-1" {
		t.Fatalf("closed request = %+v, want head frozen at oid-1", list[1])
	}
}

func TestReadRefOnUnknownRepositoryIsFixtureError(t *testing.T) {
	f := forgetest.New(forgetest.Config{
		Repositories: []forge.Repository{{ID: "r", Canonical: "r", DefaultRef: "main"}},
		Refs:         map[string]map[string]string{"r": {"main": "oid"}},
	})
	ctx := context.Background()
	if _, err := f.ReadRef(ctx, forge.Repository{ID: "typo", Canonical: "typo"}, "main"); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadRef on unknown repository err = %v, want ErrNotFound", err)
	}
	if _, err := f.ReadEffectiveProtection(ctx, forge.Repository{ID: "typo", Canonical: "typo"}, "main"); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadEffectiveProtection on unknown repository err = %v, want ErrNotFound", err)
	}
	if _, err := f.CanWriteRepository(ctx, forge.Repository{ID: "typo", Canonical: "typo"}); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("CanWriteRepository on unknown repository err = %v, want ErrNotFound", err)
	}
}
