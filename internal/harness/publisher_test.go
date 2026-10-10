package harness

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misospace/courier/internal/broker"
	couriergit "github.com/misospace/courier/internal/git"
)

// --- world-truthful forge observer ------------------------------------------

// remoteObserver reads the live tip from the local bare remotes, so the
// broker's policy engine sees the world the test's git pushes actually
// create. Protection and writability are configured flags.
type remoteObserver struct {
	mu          sync.Mutex
	basePath    string
	workPath    string
	baseRef     string
	workRef     string
	workProtect bool
}

func (o *remoteObserver) tip(path, ref string) (string, bool, error) {
	out, err := couriergit.Hardened(context.Background(), "", "ls-remote", "--heads", "--", path, "refs/heads/"+ref)
	if err != nil {
		return "", false, fmt.Errorf("ls-remote %s: %w", path, err)
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		return "", false, nil
	}
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return "", false, fmt.Errorf("ls-remote output = %q", line)
	}
	return strings.ToLower(fields[0]), true, nil
}

func (o *remoteObserver) Repository(_ context.Context, repo, ref string) (broker.RepositoryState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	state := broker.RepositoryState{Repo: repo, Ref: ref}
	if repo != "org/repo" {
		return broker.RepositoryState{}, errors.New("unknown repository")
	}
	oid, exists, err := o.tip(o.basePath, ref)
	if err != nil {
		return broker.RepositoryState{}, err
	}
	if ref == o.baseRef {
		state.Exists, state.OID, state.Default, state.Protected, state.ProtectionKnown = exists, oid, true, true, true
		state.WriteKnown = true
		return state, nil
	}
	if ref == o.workRef {
		state.Exists, state.OID = exists, oid
		state.Default, state.Protected, state.ProtectionKnown = false, o.workProtect, true
		state.Writable, state.WriteKnown = !o.workProtect, true
		return state, nil
	}
	return broker.RepositoryState{}, errors.New("unknown ref")
}

func (o *remoteObserver) liveWorkTip() (string, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.tip(o.workPath, o.workRef)
}

func (o *remoteObserver) PullRequest(context.Context, int) (broker.PullRequestState, error) {
	return broker.PullRequestState{}, errors.New("not used in this fixture")
}

func (o *remoteObserver) FindPullRequest(context.Context, string, string) ([]broker.PullRequestState, error) {
	return nil, errors.New("not used in this fixture")
}

func (o *remoteObserver) CreatePullRequest(context.Context, broker.CreatePullRequest) (int, error) {
	return 0, errors.New("not used in this fixture")
}

func (o *remoteObserver) UpdatePullRequest(context.Context, int, broker.UpdatePullRequest) error {
	return errors.New("not used in this fixture")
}

// --- broker fixture ----------------------------------------------------------

type acceptAuthenticator struct{}

func (acceptAuthenticator) Authenticate(_ context.Context, token string) (broker.Identity, error) {
	if token != "valid-token" {
		return broker.Identity{}, errors.New("unauthorized")
	}
	return broker.Identity{RunUID: "run-uid"}, nil
}

type fileEndpointResolver struct {
	endpoints map[string]string
}

func (r fileEndpointResolver) Endpoint(_ context.Context, identity string) (string, error) {
	endpoint, ok := r.endpoints[identity]
	if !ok {
		return "", fmt.Errorf("repository %q is not pinned", identity)
	}
	return endpoint, nil
}

// testCertFiles writes a self-signed TLS certificate and key for the broker
// server constructor.
func testCertFiles(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "broker-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// observerWorkRef is the derived resolve-issue work branch the fixtures use.
const observerWorkRef = "courier/org/repo/issue-7"

// brokerFixture stands up the real broker: policy engine over the real git
// transport, real private bundle store, real (local) bare remotes, and a
// world-truthful observer. The work ref starts initially absent.
type brokerFixture struct {
	t        *testing.T
	server   *httptest.Server
	observer *remoteObserver
	policy   broker.Policy
	basePath string
	workPath string
	baseRef  string
	workRef  string
	baseOID  string
}

// rebuiltBroker stands up a fresh broker process over the same policy and
// the same live remotes: its in-process confirmation evidence is empty, the
// world is whatever the remotes now hold.
func (f *brokerFixture) rebuiltBroker() *BrokerClient {
	f.t.Helper()
	gitStore, err := couriergit.NewBroker(context.Background(), f.t.TempDir())
	if err != nil {
		f.t.Fatal(err)
	}
	transport, err := broker.NewGitPusher(context.Background(), broker.GitTransportConfig{
		Policy:   f.policy,
		Git:      gitStore,
		Resolver: fileEndpointResolver{endpoints: map[string]string{"org/repo": f.basePath}},
	})
	if err != nil {
		f.t.Fatal(err)
	}
	engine, err := broker.NewPolicyEngineWithSnapshotter(f.policy, f.observer, transport, transport)
	if err != nil {
		f.t.Fatal(err)
	}
	certFile, keyFile := testCertFiles(f.t)
	server, err := broker.NewServer(broker.ServerConfig{
		Policy:        engine,
		Authenticator: acceptAuthenticator{},
		Importer:      gitStore,
		Snapshot:      engine,
		ScratchDir:    f.t.TempDir(),
		TLSCertFile:   certFile,
		TLSKeyFile:    keyFile,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	f.t.Cleanup(httpServer.Close)
	return &BrokerClient{BaseURL: httpServer.URL, Token: []byte("valid-token"), HTTP: httpServer.Client()}
}

func newBrokerFixture(t *testing.T) *brokerFixture {
	t.Helper()
	return newBrokerFixtureWithWorkRef(t, false)
}

// newBrokerFixtureWithWorkRef optionally seeds the work branch with one
// commit (a prior run's publication) before the broker's policy resolves,
// so the initially-existing variant of the publication flow runs against
// the same real chain.
func newBrokerFixtureWithWorkRef(t *testing.T, existingWorkRef bool) *brokerFixture {
	t.Helper()
	ctx := context.Background()
	// The pinned repository is one identity ("org/repo") serving both the
	// base ref and the derived work ref: one bare remote.
	remotes := t.TempDir()
	basePath := filepath.Join(remotes, "repo.git")
	workPath := basePath
	if _, err := couriergit.Hardened(ctx, "", "init", "--bare", "--quiet", "--initial-branch", "main", basePath); err != nil {
		t.Fatalf("init bare remote: %v", err)
	}
	// Seed the base ref with one commit.
	seed := t.TempDir()
	gitCmd(t, seed, "init", "--quiet", "--initial-branch", "main")
	gitCmd(t, seed, "config", "gc.auto", "0")
	writeFile(t, filepath.Join(seed, "a.txt"), "a\n")
	gitCmd(t, seed, "add", "a.txt")
	gitCommit(t, seed, "base")
	baseOID := gitRev(t, seed, "rev-parse", "HEAD")
	gitCmd(t, seed, "push", "--quiet", "--", basePath, "refs/heads/main:refs/heads/main")

	// The initially-existing variant: the work branch carries one commit
	// when this run is admitted, and the policy anchors to it.
	workAnchor := ""
	if existingWorkRef {
		writeFile(t, filepath.Join(seed, "prior.txt"), "prior work\n")
		gitCmd(t, seed, "add", "prior.txt")
		gitCommit(t, seed, "prior run publication")
		workAnchor = gitRev(t, seed, "rev-parse", "HEAD")
		gitCmd(t, seed, "push", "--quiet", "--", basePath, "refs/heads/main:refs/heads/"+observerWorkRef)
	}

	observer := &remoteObserver{
		basePath: basePath, workPath: workPath,
		baseRef: "main", workRef: observerWorkRef,
	}
	policy := broker.Policy{
		RunUID: "run-uid", Mode: broker.ModeResolveIssue, Provider: "test",
		BaseRepo: "org/repo", BaseRef: "main", BaseOID: baseOID,
		WorkRepo: "org/repo", WorkRef: observer.workRef,
		WorkInitiallyAbsent: !existingWorkRef,
		WorkAnchorOID:       workAnchor,
		SourceIssue:         broker.SourceIssue{Owner: "org", Name: "repo", Number: 1},
	}
	gitStore, err := couriergit.NewBroker(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	transport, err := broker.NewGitPusher(ctx, broker.GitTransportConfig{
		Policy:   policy,
		Git:      gitStore,
		Resolver: fileEndpointResolver{endpoints: map[string]string{"org/repo": basePath}},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := broker.NewPolicyEngineWithSnapshotter(policy, observer, transport, transport)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := testCertFiles(t)
	server, err := broker.NewServer(broker.ServerConfig{
		Policy:        engine,
		Authenticator: acceptAuthenticator{},
		Importer:      gitStore,
		Snapshot:      engine,
		ScratchDir:    t.TempDir(),
		TLSCertFile:   certFile,
		TLSKeyFile:    keyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return &brokerFixture{
		t: t, server: httpServer, observer: observer, policy: policy,
		basePath: basePath, workPath: workPath,
		baseRef: observer.baseRef, workRef: observer.workRef, baseOID: baseOID,
	}
}

func (f *brokerFixture) client() *BrokerClient {
	return &BrokerClient{BaseURL: f.server.URL, Token: []byte("valid-token"), HTTP: f.server.Client()}
}

// remoteWorkTip reads the live work ref tip from the bare remote.
func (f *brokerFixture) remoteWorkTip() (string, bool, error) {
	return f.observer.liveWorkTip()
}

// engineFor builds a fresh control-side publication engine over a fresh
// integration tree, the way a new control incarnation would.
func (f *brokerFixture) engineFor() *PublicationEngine {
	f.t.Helper()
	tree := NewIntegrator(filepath.Join(f.t.TempDir(), "integration"), nil)
	engine, err := NewPublicationEngine(f.client(), tree)
	if err != nil {
		f.t.Fatal(err)
	}
	return engine
}

func (f *brokerFixture) prepare(t *testing.T, engine *PublicationEngine) string {
	t.Helper()
	snap, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatalf("prepare snapshot: %v", err)
	}
	return snap.Tip
}

// workerBundle renders one brief's artifact bundle on top of the dispatched
// tip, the way the worker's fixed unpack and pack tasks would.
func (f *brokerFixture) workerBundle(dispatchedTip, briefID, file, content string, engine *PublicationEngine) []byte {
	f.t.Helper()
	snapshot, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	if snapshot.Tip != dispatchedTip {
		f.t.Fatalf("snapshot tip = %s, want the dispatched tip %s", snapshot.Tip, dispatchedTip)
	}
	return briefBundleFromSnapshot(f.t, snapshot.Data, briefID, file, content)
}

func (f *brokerFixture) request(briefID, dispatchedTip string, bundle []byte) IntegrationRequest {
	return IntegrationRequest{
		BriefID:       briefID,
		DispatchedTip: dispatchedTip,
		Bundle:        bundle,
		Brief:         Brief{ID: briefID, Role: "coder", Objective: "work", SuccessCheck: "ok"},
		Summary:       "summary of " + briefID,
	}
}

// briefBundleFromSnapshot simulates the worker: unpack the snapshot bundle,
// commit the mutation, pack the full-closure result bundle.
func briefBundleFromSnapshot(t *testing.T, snapshot []byte, briefID, file, content string) []byte {
	t.Helper()
	workspace := t.TempDir()
	gitCmd(t, workspace, "init", "--quiet", "--initial-branch=integration", ".")
	gitCmd(t, workspace, "config", "gc.auto", "0")
	snapshotPath := filepath.Join(workspace, "snapshot.pack")
	if err := os.WriteFile(snapshotPath, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, workspace, "fetch", "--quiet", "--no-tags", snapshotPath,
		"+refs/courier/snapshot/work:refs/heads/work")
	gitCmd(t, workspace, "symbolic-ref", "HEAD", "refs/heads/work")
	gitCmd(t, workspace, "reset", "--quiet", "--hard", "refs/heads/work")
	writeFile(t, filepath.Join(workspace, file), content)
	gitCmd(t, workspace, "add", file)
	gitCommit(t, workspace, "brief "+briefID)
	gitCmd(t, workspace, "update-ref", ResultRefPrefix+briefID, "HEAD")
	bundlePath := filepath.Join(workspace, "artifact.bundle")
	if err := couriergit.BundleCreate(context.Background(), workspace, bundlePath, ResultRefPrefix+briefID); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// The full control→broker→remote chain: seed, integrate, publish,
// idempotent re-publish, stack a second brief.
func TestPublicationChainEndToEnd(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	if tip != f.baseOID {
		t.Fatalf("seeded tip = %s, want the base tip %s (work ref initially absent)", tip, f.baseOID)
	}

	// Brief 1: integrate and publish; the initially-absent work ref is
	// created with exactly the proposed commit.
	bundle := f.workerBundle(tip, "b1", "c.txt", "work one\n", engine)
	integration, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle))
	if err != nil {
		t.Fatalf("integrate b1: %v", err)
	}
	if !integration.Changed {
		t.Fatal("brief 1 must integrate")
	}
	publication, err := engine.Publish(context.Background())
	if err != nil {
		t.Fatalf("publish b1: %v", err)
	}
	if publication.AlreadyPublished || publication.OID != integration.Commit {
		t.Fatalf("publication = %+v, want the fresh integration commit", publication)
	}
	remote, exists, err := f.remoteWorkTip()
	if err != nil || !exists || remote != integration.Commit {
		t.Fatalf("remote work tip = %q (exists %v, err %v), want the published commit", remote, exists, err)
	}

	// An idempotent re-publish confirms without a second push.
	again, err := engine.Publish(context.Background())
	if err != nil {
		t.Fatalf("re-publish: %v", err)
	}
	if !again.AlreadyPublished || again.OID != integration.Commit {
		t.Fatalf("re-publication = %+v, want idempotent confirmation", again)
	}
	if remote2, _, _ := f.remoteWorkTip(); remote2 != integration.Commit {
		t.Fatalf("remote moved during the idempotent confirm: %s", remote2)
	}

	// Brief 2: a fresh snapshot carries brief 1's integrated work.
	snapshot, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tip != integration.Commit {
		t.Fatalf("second snapshot tip = %s, want the published commit", snapshot.Tip)
	}
	bundle2 := f.workerBundle(snapshot.Tip, "b2", "d.txt", "work two\n", engine)
	integration2, err := engine.Integrate(context.Background(), f.request("b2", snapshot.Tip, bundle2))
	if err != nil {
		t.Fatalf("integrate b2: %v", err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatalf("publish b2: %v", err)
	}
	remote2, _, err := f.remoteWorkTip()
	if err != nil || remote2 != integration2.Commit {
		t.Fatalf("remote work tip = %s (err %v), want the second publication", remote2, err)
	}
}

// Crash after a successful push: a new incarnation reconstructs from the
// remote and must not duplicate the publication.
func TestPublicationCrashAfterPushRecoversFromRemote(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	integration, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	published := integration.Commit
	// The crash: everything control-side is replaced; the broker process
	// keeps its confirmed evidence.
	recovered := f.engineFor()
	snap, err := recovered.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatalf("recovered seed: %v", err)
	}
	if snap.Tip != published {
		t.Fatalf("recovered tip = %s, want the remote publication %s", snap.Tip, published)
	}
	// Declaring changes with no new work confirms idempotently: the seed
	// sits beyond the admission anchor on a tip the broker still vouches
	// for, so the publication is confirmed from the remote without a
	// duplicate push.
	confirmed, err := recovered.Publish(context.Background())
	if err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}
	if !confirmed.AlreadyPublished || confirmed.OID != published {
		t.Fatalf("publish after recovery = %+v, want the idempotent confirmation of %s", confirmed, published)
	}
	// New work builds on the remote state and fast-forwards it; nothing is
	// duplicated.
	snapshot, err := recovered.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle2 := briefBundleFromSnapshot(t, snapshot.Data, "b2", "d.txt", "more work\n")
	integration2, err := recovered.Integrate(context.Background(), f.request("b2", snapshot.Tip, bundle2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Publish(context.Background()); err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}
	remote, _, err := f.remoteWorkTip()
	if err != nil || remote != integration2.Commit {
		t.Fatalf("remote tip = %s (err %v), want exactly the second publication", remote, err)
	}
	remoteCommits := gitRev(t, f.workPath, "rev-list", "--count", f.baseOID+".."+remote)
	if remoteCommits != "2" {
		t.Fatalf("remote commits beyond base = %s, want exactly 2 (no duplicates)", remoteCommits)
	}
}

// A foreign tip on the work ref is never adopted: publication stops with a
// blocked classification and the remote is left alone.
func TestPublicationForeignTipIsBlocked(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	integration, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Another actor advances the run's work ref.
	foreign := t.TempDir()
	gitCmd(t, foreign, "init", "--quiet", "--initial-branch=main")
	gitCmd(t, foreign, "config", "gc.auto", "0")
	gitCmd(t, foreign, "fetch", "--quiet", "--", f.workPath, "+refs/heads/"+f.workRef+":refs/remotes/up")
	gitCmd(t, foreign, "reset", "--quiet", "--hard", "refs/remotes/up")
	writeFile(t, filepath.Join(foreign, "foreign.txt"), "someone else\n")
	gitCmd(t, foreign, "add", "-A")
	gitCommit(t, foreign, "foreign advance")
	gitCmd(t, foreign, "push", "--quiet", "--", f.workPath, "refs/heads/main:refs/heads/"+f.workRef)

	// New work integrates locally; publication must refuse.
	snapshot, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = snapshot
	bundle2 := f.workerBundle(integration.Commit, "b2", "d.txt", "more\n", engine)
	if _, err := engine.Integrate(context.Background(), f.request("b2", integration.Commit, bundle2)); err != nil {
		t.Fatal(err)
	}
	_, err = engine.Publish(context.Background())
	if err == nil {
		t.Fatal("publication over a foreign tip must be blocked")
	}
	var blocked *PublicationBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("error = %v, want PublicationBlockedError", err)
	}
	foreignTip, _, err := f.remoteWorkTip()
	if err != nil {
		t.Fatal(err)
	}
	// The foreign tip's commit message is still the last one on the ref:
	// the run never pushed over it.
	subject := gitRev(t, f.workPath, "log", "--format=%s", "-1", foreignTip)
	if subject != "foreign advance" {
		t.Fatalf("remote tip subject = %q, the foreign tip must be untouched", subject)
	}
}

// A base advance is recovered by re-syncing the integration with the live
// base and retrying — §4's "re-sync and retry" row.
func TestPublicationBaseAdvanceResync(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	if _, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The base advances on the remote.
	seed := t.TempDir()
	gitCmd(t, seed, "init", "--quiet", "--initial-branch=main")
	gitCmd(t, seed, "config", "gc.auto", "0")
	gitCmd(t, seed, "fetch", "--quiet", "--", f.basePath, "+refs/heads/main:refs/remotes/up")
	gitCmd(t, seed, "reset", "--quiet", "--hard", "refs/remotes/up")
	writeFile(t, filepath.Join(seed, "base-advance.txt"), "new base work\n")
	gitCmd(t, seed, "add", "-A")
	gitCommit(t, seed, "base advance")
	gitCmd(t, seed, "push", "--quiet", "--", f.basePath, "refs/heads/main:refs/heads/main")

	// New work integrates from the stale base; publication re-syncs.
	snapshot, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle2 := briefBundleFromSnapshot(t, snapshot.Data, "b2", "d.txt", "more\n")
	integration2, err := engine.Integrate(context.Background(), f.request("b2", snapshot.Tip, bundle2))
	if err != nil {
		t.Fatal(err)
	}
	publication, err := engine.Publish(context.Background())
	if err != nil {
		t.Fatalf("publish after base advance: %v", err)
	}
	remote, _, err := f.remoteWorkTip()
	if err != nil || remote != publication.OID {
		t.Fatalf("remote tip = %s (err %v), want the re-synced publication %s", remote, err, publication.OID)
	}
	// The published commit is a merge carrying the live base tip and the
	// run's own work.
	liveBase := gitRev(t, f.basePath, "rev-parse", "refs/heads/main")
	parents := gitRev(t, f.workPath, "log", "--format=%P", "-1", remote)
	if !strings.Contains(parents, liveBase) {
		t.Fatalf("published commit parents = %q, want the live base tip %s", parents, liveBase)
	}
	if !strings.Contains(parents, integration2.Commit) {
		t.Fatalf("published commit parents = %q, want the run's integration commit %s", parents, integration2.Commit)
	}
}

// A broker that keeps refusing is classified: a definite denial is blocked,
// transport failure is transient after the bounded retries.
func TestPublicationDenialClassification(t *testing.T) {
	t.Run("definite denial", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "operation denied", http.StatusUnprocessableEntity)
		}))
		defer server.Close()
		engine := denialEngine(t, server)
		_, err := engine.Publish(context.Background())
		var blocked *PublicationBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("error = %v, want PublicationBlockedError", err)
		}
	})
	t.Run("transport exhaustion", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		engine := denialEngine(t, server)
		_, err := engine.Publish(context.Background())
		var transient *TransientPublicationError
		if !errors.As(err, &transient) {
			t.Fatalf("error = %v, want TransientPublicationError", err)
		}
	})
}

// seedTipEngine seeds a tree locally and points the engine at the given
// broker with its head exactly at the seed tip of an existing work ref that
// is not the admission anchor: the crash-after-push confirmation state.
func seedTipEngine(t *testing.T, client *BrokerClient) *PublicationEngine {
	t.Helper()
	ctx := context.Background()
	dir, _ := seedHarnessRepo(t, t.TempDir())
	gitCmd(t, dir, "update-ref", "refs/courier/snapshot/work", "HEAD")
	gitCmd(t, dir, "update-ref", "refs/courier/snapshot/base", "HEAD")
	bundlePath := filepath.Join(t.TempDir(), "s.bundle")
	if err := couriergit.BundleCreate(ctx, dir, bundlePath, "refs/courier/snapshot/work", "refs/courier/snapshot/base"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	tree := NewIntegrator(filepath.Join(t.TempDir(), "integration"), nil)
	tip, err := tree.Seed(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewPublicationEngine(client, tree)
	if err != nil {
		t.Fatal(err)
	}
	engine.seeded = true
	engine.seedTip = tip
	engine.seedWorkRefExist = true
	engine.seedAtAnchor = false
	engine.lastConfirmed = tip
	return engine
}

// denialEngine is a seed-tip engine with one real integration commit, so
// Publish proposes something beyond the tip.
func denialEngine(t *testing.T, server *httptest.Server) *PublicationEngine {
	t.Helper()
	engine := seedTipEngine(t, &BrokerClient{BaseURL: server.URL, Token: []byte("x"), HTTP: server.Client()})
	writeFile(t, filepath.Join(engine.tree.Dir, "z.txt"), "z\n")
	gitCmd(t, engine.tree.Dir, "add", "z.txt")
	gitCommit(t, engine.tree.Dir, "work")
	return engine
}

// The seed-tip confirmation branch classifies its terminal broker errors
// like the rest of the engine: a definite denial is a blocked world, an
// exhausted retryable/transport failure is a relaunchable transient — a
// temporary broker outage during crash-after-push recovery never sends the
// run to NeedsHuman.
func TestPublicationSeedTipConfirmClassification(t *testing.T) {
	t.Run("definite denial is blocked", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "operation denied", http.StatusUnprocessableEntity)
		}))
		defer server.Close()
		engine := seedTipEngine(t, &BrokerClient{BaseURL: server.URL, Token: []byte("x"), HTTP: server.Client()})
		_, err := engine.Publish(context.Background())
		var blocked *PublicationBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("definite denial = %v, want PublicationBlockedError", err)
		}
	})
	t.Run("transport exhaustion is transient", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		engine := seedTipEngine(t, &BrokerClient{BaseURL: server.URL, Token: []byte("x"), HTTP: server.Client()})
		_, err := engine.Publish(context.Background())
		var transient *TransientPublicationError
		if !errors.As(err, &transient) {
			t.Fatalf("transport exhaustion = %v, want TransientPublicationError", err)
		}
	})
}

func seedHarnessRepo(t *testing.T, dir string) (string, string) {
	t.Helper()
	gitCmd(t, dir, "init", "--quiet", "--initial-branch", "main")
	gitCmd(t, dir, "config", "gc.auto", "0")
	writeFile(t, filepath.Join(dir, "a.txt"), "a\n")
	gitCmd(t, dir, "add", "a.txt")
	gitCommit(t, dir, "base")
	return dir, gitRev(t, dir, "rev-parse", "HEAD")
}

// A fresh world with an initially-absent work ref and no integrated work has
// nothing to publish: publishing would create the ref at the base tip.
func TestPublicationNothingToPublishWithoutWork(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	f.prepare(t, engine)
	if _, err := engine.Publish(context.Background()); !errors.Is(err, ErrNothingToPublish) {
		t.Fatalf("publish without work = %v, want ErrNothingToPublish", err)
	}
	// An engine that never seeded has the same verdict, deterministically
	// rather than as an infrastructure failure.
	if _, err := f.engineFor().Publish(context.Background()); !errors.Is(err, ErrNothingToPublish) {
		t.Fatalf("unseeded publish = %v, want ErrNothingToPublish", err)
	}
}

// An initially-existing work ref with no integrated work has nothing to
// publish either: the no-work verdict must not depend on the seed-time ref
// state, and it must not burn reconciliation rounds first.
func TestPublicationNothingToPublishWithExistingWorkRef(t *testing.T) {
	f := newBrokerFixtureWithWorkRef(t, true)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	if tip == f.baseOID {
		t.Fatal("the seed must sit on the existing work ref, not the base")
	}
	if _, err := engine.Publish(context.Background()); !errors.Is(err, ErrNothingToPublish) {
		t.Fatalf("publish with an existing work ref and no work = %v, want ErrNothingToPublish", err)
	}
}

// A broker that cannot vouch for the seeded tip never lets the live OID
// stand in for ownership: the replacement-broker confirm is refused and the
// verdict is a blocked world, not a claimed publication.
func TestPublicationReplacedBrokerNeverInfersOwnership(t *testing.T) {
	f := newBrokerFixtureWithWorkRef(t, true)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	integration, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	published := integration.Commit

	// The broker is replaced: its confirmation evidence is gone, the remote
	// still carries the publication.
	replacement := f.rebuiltBroker()
	tree := NewIntegrator(filepath.Join(t.TempDir(), "integration"), nil)
	recovered, err := NewPublicationEngine(replacement, tree)
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := recovered.PrepareSnapshot(context.Background()); err != nil || snap.Tip != published {
		t.Fatalf("recovered seed = %+v (err %v), want the remote publication", snap, err)
	}
	_, err = recovered.Publish(context.Background())
	var blocked *PublicationBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("publish against a replaced broker = %v, want a blocked world", err)
	}
	// The remote is untouched: no push, no duplicate, no rewind.
	remote, _, err := f.remoteWorkTip()
	if err != nil || remote != published {
		t.Fatalf("remote tip = %s (err %v), want the untouched publication", remote, err)
	}
}

// A foreign work tip that arrives together with a base advance blocks
// before any base re-sync merge is attempted: an unadoptable tip is never
// re-pathed around, whatever its ancestry.
func TestPublicationForeignTipWithBaseMoveBlocksBeforeResync(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	if _, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A foreign actor rewinds the work ref to an unrelated commit while the
	// base advances.
	foreign := t.TempDir()
	gitCmd(t, foreign, "init", "--quiet", "--initial-branch=main")
	gitCmd(t, foreign, "config", "gc.auto", "0")
	writeFile(t, filepath.Join(foreign, "foreign.txt"), "someone else\n")
	gitCmd(t, foreign, "add", "-A")
	gitCommit(t, foreign, "foreign tip")
	gitCmd(t, foreign, "push", "--quiet", "--force", "--", f.basePath, "refs/heads/main:refs/heads/"+f.workRef)
	seed := t.TempDir()
	gitCmd(t, seed, "init", "--quiet", "--initial-branch=main")
	gitCmd(t, seed, "config", "gc.auto", "0")
	gitCmd(t, seed, "fetch", "--quiet", "--", f.basePath, "+refs/heads/main:refs/remotes/up")
	gitCmd(t, seed, "reset", "--quiet", "--hard", "refs/remotes/up")
	writeFile(t, filepath.Join(seed, "base-advance.txt"), "new base\n")
	gitCmd(t, seed, "add", "-A")
	gitCommit(t, seed, "base advance")
	gitCmd(t, seed, "push", "--quiet", "--", f.basePath, "refs/heads/main:refs/heads/main")

	snapshot, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle2 := briefBundleFromSnapshot(t, snapshot.Data, "b2", "d.txt", "more\n")
	if _, err := engine.Integrate(context.Background(), f.request("b2", snapshot.Tip, bundle2)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err == nil {
		t.Fatal("publication over a foreign rewound tip must be blocked")
	}
	// The blocked verdict came before any base re-sync: no merge commit was
	// created and no merge is in progress.
	if inProgress, err := couriergit.MergeInProgress(context.Background(), engine.tree.Dir); err != nil || inProgress {
		t.Fatalf("merge state after the foreign-tip block: %v (%v)", inProgress, err)
	}
	merges := gitRev(t, engine.tree.Dir, "log", "--merges", "--format=%H")
	if strings.TrimSpace(merges) != "" {
		t.Fatalf("a base re-sync merge was attempted for an unadoptable tip: %s", merges)
	}
}

// A conflicted base re-sync leaves the integration tree usable: the merge
// state is aborted, and the next brief integrates as exactly one normal
// commit — never a bogus two-parent merge.
func TestPublicationConflictedResyncLeavesTreeUsable(t *testing.T) {
	f := newBrokerFixture(t)
	engine := f.engineFor()
	tip := f.prepare(t, engine)
	bundle := f.workerBundle(tip, "b1", "c.txt", "work\n", engine)
	if _, err := engine.Integrate(context.Background(), f.request("b1", tip, bundle)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Advance the base with a change that will conflict with the run's work:
	// both touch c.txt.
	seed := t.TempDir()
	gitCmd(t, seed, "init", "--quiet", "--initial-branch=main")
	gitCmd(t, seed, "config", "gc.auto", "0")
	gitCmd(t, seed, "fetch", "--quiet", "--", f.basePath, "+refs/heads/main:refs/remotes/up")
	gitCmd(t, seed, "reset", "--quiet", "--hard", "refs/remotes/up")
	writeFile(t, filepath.Join(seed, "c.txt"), "conflicting base change\n")
	gitCmd(t, seed, "add", "-A")
	gitCommit(t, seed, "base advance")
	gitCmd(t, seed, "push", "--quiet", "--", f.basePath, "refs/heads/main:refs/heads/main")

	snapshot, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle2 := briefBundleFromSnapshot(t, snapshot.Data, "b2", "d.txt", "more\n")
	if _, err := engine.Integrate(context.Background(), f.request("b2", snapshot.Tip, bundle2)); err != nil {
		t.Fatal(err)
	}
	_, err = engine.Publish(context.Background())
	var blocked *PublicationBlockedError
	if !errors.As(err, &blocked) || !strings.Contains(blocked.Reason, "conflicts") {
		t.Fatalf("publish error = %v, want a conflict-classified block", err)
	}
	// The tree is clean: the next brief integrates as one ordinary commit.
	if inProgress, err := couriergit.MergeInProgress(context.Background(), engine.tree.Dir); err != nil || inProgress {
		t.Fatalf("merge still in progress after the refused re-sync: %v (%v)", inProgress, err)
	}
	snapshot3, err := engine.PrepareSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle3 := briefBundleFromSnapshot(t, snapshot3.Data, "b3", "e.txt", "later work\n")
	integration3, err := engine.Integrate(context.Background(), f.request("b3", snapshot3.Tip, bundle3))
	if err != nil {
		t.Fatalf("integrate after refused re-sync: %v", err)
	}
	if !integration3.Changed {
		t.Fatal("the post-conflict brief must integrate")
	}
	parents := gitRev(t, engine.tree.Dir, "log", "--format=%P", "-1", integration3.Commit)
	if strings.Count(strings.TrimSpace(parents), " ") != 0 {
		t.Fatalf("post-conflict integration commit has parents %q, want exactly one parent", parents)
	}
}
