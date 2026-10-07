package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/protocol"
)

// The coordinator's fixed worker scripts run against the real honest worker
// listener: the snapshot unpack materializes the workspace, and the pack
// task returns a bundle that trusted control validates and integrates. Mock
// workers answer these scripts from scripted fields, so only the real
// listener exercises their actual content.
func TestWorkerScriptsEndToEndWithRealWorker(t *testing.T) {
	ctx := context.Background()
	world := newArtifactWorld(t, nil)

	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	uid := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	workspace := t.TempDir()
	realWorker, err := protocol.NewWorker(protocol.WorkerConfig{
		RunUID: "run", ControlPodUID: "control", WorkerPodUID: uid, PublicKey: pub,
		WorkspaceDir: workspace,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(realWorker.Handler())
	defer server.Close()
	client := &protocol.Client{BaseURL: server.URL}

	// Upload the snapshot exactly like the coordinator's prepare does.
	snapshot, err := world.integrator.Render(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.UploadSnapshot(ctx, snapshot.Data); err != nil {
		t.Fatal(err)
	}

	// The unpack task runs the coordinator's real fixed script.
	unpackOp := "unpack.test.0001"
	task := protocol.Task{Command: []string{"sh", "-c", workerUnpackScript()}}
	if _, err := dispatchForTest(ctx, client, priv, uid, unpackOp, task); err != nil {
		t.Fatal(err)
	}
	result := awaitForTest(t, client, unpackOp)
	if result.ExitCode != 0 || strings.TrimSpace(result.StdoutTail) != snapshot.Tip {
		t.Fatalf("unpack result = %+v, want tip %s echoed", result, snapshot.Tip)
	}
	if _, err := os.Stat(filepath.Join(workspace, "a.txt")); err != nil {
		t.Fatalf("unpacked workspace lacks the snapshot content: %v", err)
	}

	// The model's work: commit on the worker workspace (the shell path).
	writeFile(t, filepath.Join(workspace, "c.txt"), "model work\n")
	gitCmd(t, workspace, "add", "c.txt")
	gitCommit(t, workspace, "model work")

	// The pack task renders the brief's bundle through the real script.
	packOp := "pack.b1.0001"
	packTask := protocol.Task{
		Command:      []string{"sh", "-c", workerPackScript("b1", snapshot.Tip)},
		ArtifactPath: "artifact.b1.bundle",
	}
	if _, err := dispatchForTest(ctx, client, priv, uid, packOp, packTask); err != nil {
		t.Fatal(err)
	}
	packResult := awaitForTest(t, client, packOp)
	if packResult.ExitCode != 0 || len(packResult.Artifact) == 0 {
		t.Fatalf("pack result = %+v, want a bundle artifact", packResult)
	}

	// Trusted control validates and integrates the artifact.
	integration, err := world.integrator.Integrate(ctx, IntegrationRequest{
		BriefID:       "b1",
		DispatchedTip: snapshot.Tip,
		Bundle:        packResult.Artifact,
		Brief:         Brief{ID: "b1", Role: "coder", Objective: "work", SuccessCheck: "ok"},
		Summary:       "model work",
	})
	if err != nil {
		t.Fatalf("integrate: %v", err)
	}
	if !integration.Changed || integration.Commit == "" {
		t.Fatalf("integration = %+v", integration)
	}
	if _, err := os.Stat(filepath.Join(world.integrator.Dir, "c.txt")); err != nil {
		t.Fatalf("integrated file missing: %v", err)
	}
}

func dispatchForTest(ctx context.Context, client *protocol.Client, key []byte, workerUID string, opID string, task protocol.Task) (string, error) {
	payload, err := json.Marshal(task)
	if err != nil {
		return "", err
	}
	envelope := protocol.NewEnvelope(time.Now(), 10*time.Minute, payload)
	envelope.Kind = protocol.KindDispatch
	envelope.RunUID = "run"
	envelope.ControlPodUID = "control"
	envelope.WorkerPodUID = workerUID
	envelope.BriefID = "b1"
	envelope.OpID = opID
	return client.Dispatch(ctx, key, envelope, payload)
}

func awaitForTest(t *testing.T, client *protocol.Client, opID string) protocol.Result {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for {
		state, err := client.Result(ctx, opID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Result != nil {
			return *state.Result
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s did not complete", opID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
