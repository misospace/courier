package broker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type raceSnapshotter struct{}

func (raceSnapshotter) Snapshot(context.Context, SnapshotRequest) ([]byte, error) {
	return nil, ErrSnapshotTipMoved
}

// A pinned ref that keeps moving while the snapshot renders exhausts the
// bounded re-observe loop and must surface as the retryable race sentinel —
// not as a plain policy denial — so the server's 503 retry path stays real.
func TestSnapshotRaceExhaustionPreservesRetryableSentinel(t *testing.T) {
	policy := Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test",
		BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40),
		WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-7", WorkInitiallyAbsent: true}
	observer := importTestObserver{}
	pusher := serverPusher{}
	engine, err := NewPolicyEngineWithSnapshotter(policy, observer, pusher, raceSnapshotter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SnapshotForControl(context.Background()); !errors.Is(err, ErrSnapshotTipMoved) {
		t.Fatalf("exhausted render race = %v, want ErrSnapshotTipMoved preserved", err)
	}
}

// The server maps the exhausted race sentinel to 503 so control classifies
// it as a retryable availability failure, never a blocked world.
func TestServerSnapshotRaceReturns503(t *testing.T) {
	cert, key := testTLSFiles(t)
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test",
		BaseRepo: "org/repo", BaseRef: "main", BaseOID: strings.Repeat("a", 40),
		WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-7", WorkInitiallyAbsent: true},
		importTestObserver{}, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	handler := &stubSnapshotHandler{err: fmt.Errorf("publication denied: pinned refs kept moving while rendering the snapshot (3 attempts): %w", ErrSnapshotTipMoved)}
	server, err := NewServer(ServerConfig{Policy: engine, Authenticator: &testAuthenticator{}, Snapshot: handler, ScratchDir: t.TempDir(), TLSCertFile: cert, TLSKeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, PathSnapshot, nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("exhausted race status = %d, want 503", w.Code)
	}
}
