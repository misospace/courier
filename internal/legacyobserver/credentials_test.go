package legacyobserver

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestSeedResolvesProviderCredentialsPerRemote(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	upstream, _, _ := remoteWithBranches(t, root, "upstream", true, false)
	fork, _, workTip := remoteWithBranches(t, root, "fork", false, true)
	var mu sync.Mutex
	var calls []string
	provider := func(_ context.Context, remoteURL string) (string, string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, remoteURL)
		return "provider-user", "provider-token", nil
	}
	tip, err := Seed(ctx, Options{
		RemoteURL: fork, BaseRemoteURL: upstream, BaseBranch: "main", Branch: "work",
		Scratch: filepath.Join(root, "s1"), Username: "ignored", Token: "ignored", Credentials: provider,
	})
	if err != nil {
		t.Fatalf("Seed with provider: %v", err)
	}
	if tip != workTip {
		t.Fatalf("baseline = %q, want fork work tip %q", tip, workTip)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) == 0 {
		t.Fatal("provider was never consulted")
	}
	for _, call := range calls {
		if call != fork {
			t.Fatalf("provider resolved unrelated remote %q, want only the work remote %q", call, fork)
		}
	}
}

func TestSeedProviderRefusalFailsFetchWithoutStaticFallback(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	upstream, _, _ := remoteWithBranches(t, root, "upstream", true, false)
	fork, _, _ := remoteWithBranches(t, root, "fork-empty", false, false)
	refuse := errors.New("provider refuses this remote")
	provider := func(_ context.Context, remoteURL string) (string, string, error) {
		if remoteURL == upstream {
			return "", "", refuse
		}
		return "provider-user", "provider-token", nil
	}
	_, err := Seed(ctx, Options{
		RemoteURL: fork, BaseRemoteURL: upstream, BaseBranch: "main", Branch: "work",
		Scratch: filepath.Join(root, "scratch"), Username: "static-user", Token: "static-token", Credentials: provider,
	})
	if !errors.Is(err, errObserverCredentials) {
		t.Fatalf("Seed error = %v, want a credential refusal", err)
	}
}

func TestObserveProviderResolvesEachRemoteURL(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote, baseline, _ := remoteWithBranches(t, root, "mono", true, false)
	workspace := filepath.Join(root, "workspace")
	gitTest(t, root, "clone", "--quiet", "--branch", "main", remote, workspace)
	gitTest(t, workspace, "config", "user.name", "test")
	gitTest(t, workspace, "config", "user.email", "test@example.invalid")
	gitTest(t, workspace, "checkout", "-q", "-b", "work")
	var mu sync.Mutex
	var calls []string
	provider := func(_ context.Context, remoteURL string) (string, string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, remoteURL)
		return "provider-user", "provider-token", nil
	}
	ev, err := Observe(ctx, Options{
		Workspace: workspace, Scratch: filepath.Join(root, "scratch"), RemoteURL: remote,
		BaseRemoteURL: remote, BaseBranch: "main", Branch: "work", Baseline: baseline, Credentials: provider,
	})
	if err != nil {
		t.Fatalf("Observe with provider: %v", err)
	}
	if ev.State != "empty" {
		t.Fatalf("state = %q, want empty (branch absent, head at baseline)", ev.State)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) < 2 {
		t.Fatalf("provider consulted %d time(s), want one resolution per fetch", len(calls))
	}
	for _, call := range calls {
		if call != remote {
			t.Fatalf("provider resolved unrelated remote %q", call)
		}
	}
}
