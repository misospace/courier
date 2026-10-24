package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misospace/courier/internal/evidence"
)

// TestEvidenceGateCleanRemoteConfirmed proves the gate stays silent for a
// clean, remote-held worktree: the run commits and pushes its work, so the
// terminal is confirmed on the remote, and although capture is armed the gate
// finds nothing unrecoverable, so it delivers nothing and emits no event.
func TestEvidenceGateCleanRemoteConfirmed(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	prServer, prState := fakeForgePullRequestServer(t)
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'completed\n' > completed.txt
git add --all -- . && git commit -m done && git push origin "$COURIER_BRANCH" >/dev/null 2>&1 || true
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`+writePullRequestScript(prState, pullRequestFixtureJSON(12, "open", false, "Closes #7. The pushed turn opened the pull request.")))
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, prServer.URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitSuccess {
		t.Fatalf("run exit code = %d, want 0; stderr=%q stdout=%q", code, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Verifying"`) {
		t.Fatalf("run should end verifying: %q", output.String())
	}
	noEvidenceCapture(t, snapshot, &output)
}

// TestEvidenceRelaunchedClean proves a relaunch that adopts a run branch and
// finds nothing to recover is silent: the worktree is clean and holds no
// commits, so the gate's WorkState is none and the run ends NoChangeNeeded
// without capturing or emitting, even with capture armed.
func TestEvidenceRelaunchedClean(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithBranchAtMainTip(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"no_change_needed","evidence":"already verified"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitNoChangeNeeded {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitNoChangeNeeded, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"NoChangeNeeded"`) {
		t.Fatalf("run should end no-change-needed: %q", output.String())
	}
	noEvidenceCapture(t, snapshot, &output)
}

// TestEvidenceDirtyTerminalCaptures proves a dirty terminal is preserved: the
// run leaves an uncommitted file and declares changes, so the gate finds
// unrecoverable state, captures the worktree, delivers one authenticated
// bundle, and emits a single evidence.capture event. A second run of the same
// scenario with capture unconfigured must produce an identical terminal
// handoff, proving the capture never changes the run's ending.
func TestEvidenceDirtyTerminalCaptures(t *testing.T) {
	runScenario := func(t *testing.T, armed bool) (int, string, func() []intakeRecord) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		var snapshot func() []intakeRecord
		if armed {
			intake, snap := newEvidenceIntake(t)
			snapshot = snap
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			armEvidence(t, intake.URL)
		} else {
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String(), snapshot
	}

	code, armedOut, snapshot := runScenario(t, true)
	if code != exitFailed {
		t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(armedOut, `"phase":"Failed"`) {
		t.Fatalf("armed run should end failed: %q", armedOut)
	}

	t.Run("armed captures and delivers", func(t *testing.T) {
		records := snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want 1 for a dirty terminal", len(records))
		}
		rec := records[0]
		if rec.authorization != "Bearer "+testEvidenceToken {
			t.Fatalf("authorization = %q, want Bearer <testEvidenceToken>", rec.authorization)
		}
		if rec.manifest.Trigger != "terminal" {
			t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
		}
		if rec.manifest.Run.Name != "coderrun-it-7" {
			t.Fatalf("manifest run name = %q, want coderrun-it-7", rec.manifest.Run.Name)
		}
		if rec.manifest.Workspace.Branch == "" {
			t.Fatal("manifest workspace branch is empty")
		}
		if !isCommitSHA(rec.manifest.Workspace.StartSHA) {
			t.Fatalf("manifest start sha = %q, want a 40-hex commit", rec.manifest.Workspace.StartSHA)
		}
		if rec.manifest.Totals.Files < 1 {
			t.Fatalf("manifest totals files = %d, want at least 1", rec.manifest.Totals.Files)
		}
		content, ok := rec.archiveFiles["evidence-target.txt"]
		if !ok {
			t.Fatalf("archive members = %v, want evidence-target.txt", rec.archiveFiles)
		}
		if content != "uncovered work\n" {
			t.Fatalf("archive content = %q, want the uncommitted file's content", content)
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		capture := findEvent(t, events, "evidence.capture")
		detail := eventDetail(t, capture)
		if detail["trigger"] != "terminal" {
			t.Fatalf("event trigger = %v, want terminal", detail["trigger"])
		}
		if detail["outcome"] != "delivered" {
			t.Fatalf("event outcome = %v, want delivered", detail["outcome"])
		}
	})

	t.Run("unconfigured leaves the ending unchanged", func(t *testing.T) {
		disarmedCode, disarmedOut, _ := runScenario(t, false)
		if disarmedCode != exitFailed {
			t.Fatalf("disarmed run exit code = %d, want %d", disarmedCode, exitFailed)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, disarmedOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with capture unconfigured:\n armed=%v\n disarmed=%v", want, got)
		}
	})
}

// isCommitSHA reports whether v is a 40-character hexadecimal commit.
func isCommitSHA(v string) bool {
	if len(v) != 40 {
		return false
	}
	for _, r := range v {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

// TestEvidenceLocalCommitsNotOnRemoteCaptures proves a local commit the
// remote run branch does not hold is preserved: the run commits work but does
// not push, the remote has no run branch so the gate's bounded fetch fails,
// and the run declares changes. The pre-handoff observation (#247) then
// confirms the head was never published and fails the handoff closed to
// NeedsHuman. The gate finds the same unrecoverable state, captures the
// local commit, delivers one authenticated bundle, and emits a single
// evidence.capture event. A second run of the same scenario with capture
// unconfigured must produce the same handoff, proving the capture never
// changes the run's ending.
func TestEvidenceLocalCommitsNotOnRemoteCaptures(t *testing.T) {
	runScenario := func(t *testing.T, armed bool) (int, string, func() []intakeRecord) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithBaseOnly(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'local work\n' > local-work.txt
git add --all -- .
git commit -m 'work: local commit' >/dev/null
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		var snapshot func() []intakeRecord
		if armed {
			intake, snap := newEvidenceIntake(t)
			snapshot = snap
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			armEvidence(t, intake.URL)
		} else {
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String(), snapshot
	}

	code, armedOut, snapshot := runScenario(t, true)
	if code != exitNeedsHuman {
		t.Fatalf("armed run exit code = %d, want %d (unpublished head fails closed, #247)", code, exitNeedsHuman)
	}
	if !strings.Contains(armedOut, `"phase":"NeedsHuman"`) {
		t.Fatalf("armed run should end needs-human: %q", armedOut)
	}
	if !strings.Contains(armedOut, "is not published on the remote") {
		t.Fatalf("armed run should name the unpublished head: %q", armedOut)
	}

	t.Run("armed captures and delivers", func(t *testing.T) {
		records := snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want 1 for a local commit the remote lacks", len(records))
		}
		rec := records[0]
		if rec.authorization != "Bearer "+testEvidenceToken {
			t.Fatalf("authorization = %q, want Bearer <testEvidenceToken>", rec.authorization)
		}
		if rec.manifest.Trigger != "terminal" {
			t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
		}
		if rec.manifest.Run.Name != "coderrun-it-7" {
			t.Fatalf("manifest run name = %q, want coderrun-it-7", rec.manifest.Run.Name)
		}
		commitEntries := 0
		for name := range rec.archiveFiles {
			if strings.HasPrefix(name, "commits/") {
				commitEntries++
			}
		}
		if commitEntries != 1 {
			t.Fatalf("archive members = %v, want exactly one commits/ entry", rec.archiveFiles)
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		capture := findEvent(t, events, "evidence.capture")
		detail := eventDetail(t, capture)
		if detail["trigger"] != "terminal" {
			t.Fatalf("event trigger = %v, want terminal", detail["trigger"])
		}
		if detail["outcome"] != "delivered" {
			t.Fatalf("event outcome = %v, want delivered", detail["outcome"])
		}
	})

	t.Run("unconfigured leaves the ending unchanged", func(t *testing.T) {
		disarmedCode, disarmedOut, _ := runScenario(t, false)
		if disarmedCode != exitNeedsHuman {
			t.Fatalf("disarmed run exit code = %d, want %d (unpublished head fails closed, #247)", disarmedCode, exitNeedsHuman)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, disarmedOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with capture unconfigured:\n armed=%v\n disarmed=%v", want, got)
		}
	})
}

// TestEvidenceDeliveryAttempts proves the delivery budget on a failing dirty
// terminal: with capture armed the run makes at most two POST attempts at
// the intake. An intake that never accepts records exactly two attempts and a
// "lost" outcome, and its terminal handoff stays identical to the same run
// with capture unconfigured, so an undeliverable bundle never changes the
// ending. An intake that rejects the first attempt and accepts the second
// records two attempts and a "delivered" outcome.
func TestEvidenceDeliveryAttempts(t *testing.T) {
	// newStatusIntake is newEvidenceIntake with a per-call status code, so a
	// subtest can fail the first attempt or all of them.
	newStatusIntake := func(t *testing.T, statusFor func(call int) int) (*httptest.Server, func() []intakeRecord) {
		t.Helper()
		var mu sync.Mutex
		var records []intakeRecord
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			rec := intakeRecord{
				authorization: r.Header.Get("Authorization"),
				archiveFiles:  map[string]string{},
			}
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, "multipart: "+err.Error(), http.StatusBadRequest)
				return
			}
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, "multipart part: "+err.Error(), http.StatusBadRequest)
					return
				}
				data, err := io.ReadAll(part)
				if err != nil {
					http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
					return
				}
				switch part.FormName() {
				case "manifest":
					if err := json.Unmarshal(data, &rec.manifest); err != nil {
						http.Error(w, "manifest: "+err.Error(), http.StatusBadRequest)
						return
					}
				case "archive":
					files, err := readTarGZ(data)
					if err != nil {
						http.Error(w, "archive: "+err.Error(), http.StatusBadRequest)
						return
					}
					rec.archiveFiles = files
				}
			}
			mu.Lock()
			status := statusFor(len(records) + 1)
			records = append(records, rec)
			mu.Unlock()
			w.WriteHeader(status)
		}))
		t.Cleanup(server.Close)
		return server, func() []intakeRecord {
			mu.Lock()
			defer mu.Unlock()
			out := make([]intakeRecord, len(records))
			copy(out, records)
			return out
		}
	}

	// runDirty plays the TestEvidenceDirtyTerminalCaptures scenario — the
	// fake leaves an uncommitted file and declares changes, so the run ends
	// Failed with a dirty worktree. An empty evidenceURL leaves capture
	// unconfigured, the baseline for the handoff comparison.
	runDirty := func(t *testing.T, evidenceURL string) (int, string) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
		t.Setenv("COURIER_REF", "7")
		t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
		t.Setenv("GITHUB_TOKEN", "github-api-token")
		if evidenceURL == "" {
			t.Setenv("COURIER_EVIDENCE_URL", "")
			t.Setenv("COURIER_EVIDENCE_TOKEN", "")
		} else {
			armEvidence(t, evidenceURL)
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String()
	}

	t.Run("exhausted", func(t *testing.T) {
		intake, snapshot := newStatusIntake(t, func(int) int { return http.StatusInternalServerError })
		code, armedOut := runDirty(t, intake.URL)
		if code != exitFailed {
			t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
		}
		if !strings.Contains(armedOut, `"phase":"Failed"`) {
			t.Fatalf("armed run should end failed: %q", armedOut)
		}
		if records := snapshot(); len(records) != 2 {
			t.Fatalf("intake received %d deliveries, want 2 (the delivery budget)", len(records))
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		if n := countEvents(t, events, "evidence.capture"); n != 1 {
			t.Fatalf("evidence.capture events = %d, want 1", n)
		}
		detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
		if detail["outcome"] != "lost" {
			t.Fatalf("event outcome = %v, want lost", detail["outcome"])
		}
		baselineCode, baselineOut := runDirty(t, "")
		if baselineCode != exitFailed {
			t.Fatalf("baseline exit code = %d, want %d", baselineCode, exitFailed)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, baselineOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with an undeliverable bundle:\n armed=%v\n baseline=%v", want, got)
		}
	})

	t.Run("retry_succeeds", func(t *testing.T) {
		intake, snapshot := newStatusIntake(t, func(call int) int {
			if call == 1 {
				return http.StatusInternalServerError
			}
			return http.StatusOK
		})
		code, armedOut := runDirty(t, intake.URL)
		if code != exitFailed {
			t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
		}
		if records := snapshot(); len(records) != 2 {
			t.Fatalf("intake received %d deliveries, want 2 (one rejected, one accepted)", len(records))
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		if n := countEvents(t, events, "evidence.capture"); n != 1 {
			t.Fatalf("evidence.capture events = %d, want 1", n)
		}
		detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
		if detail["outcome"] != "delivered" {
			t.Fatalf("event outcome = %v, want delivered", detail["outcome"])
		}
	})
}

// TestEvidenceDisabledWithoutEnv proves capture is all-or-nothing: with only
// one of the two variables present the failing dirty terminal is exactly as
// silent as with none — no POST reaches the intake, no evidence.capture
// event is emitted, and the terminal handoff equals the evidence-unset
// baseline, so partial configuration behaves like absence.
func TestEvidenceDisabledWithoutEnv(t *testing.T) {
	// runPartial plays the same dirty terminal as TestEvidenceDeliveryAttempts
	// under an arbitrary evidence configuration: an empty argument leaves the
	// variable unset for that run.
	runPartial := func(t *testing.T, evidenceURL, evidenceToken string) (int, string) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
		t.Setenv("COURIER_REF", "7")
		t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
		t.Setenv("GITHUB_TOKEN", "github-api-token")
		t.Setenv("COURIER_EVIDENCE_URL", evidenceURL)
		t.Setenv("COURIER_EVIDENCE_TOKEN", evidenceToken)
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String()
	}

	for _, tc := range []struct {
		name   string
		urlSet bool
		token  string
	}{
		{name: "url_without_token", urlSet: true},
		{name: "token_without_url", token: testEvidenceToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The subtest's intake records any POST the present variable
			// might still provoke; in the absent-URL variant it stands by
			// unused.
			intake, snapshot := newEvidenceIntake(t)
			var url string
			if tc.urlSet {
				url = intake.URL
			}
			code, out := runPartial(t, url, tc.token)
			if code != exitFailed {
				t.Fatalf("run exit code = %d, want %d", code, exitFailed)
			}
			if !strings.Contains(out, `"phase":"Failed"`) {
				t.Fatalf("run should end failed: %q", out)
			}
			noEvidenceCapture(t, snapshot, bytes.NewBufferString(out))
			baselineCode, baselineOut := runPartial(t, "", "")
			if baselineCode != exitFailed {
				t.Fatalf("baseline exit code = %d, want %d", baselineCode, exitFailed)
			}
			noEvidenceCapture(t, snapshot, bytes.NewBufferString(baselineOut))
			want := terminationLineJSON(t, out)
			got := terminationLineJSON(t, baselineOut)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("terminal handoff changed with partial capture configuration:\n partial=%v\n baseline=%v", want, got)
			}
		})
	}
}

// TestEvidenceFailedWorldReadDegradesToManifest proves a failed world read
// degrades the capture to the manifest alone: the fake coordinator declares
// changes and then strips every permission from the workspace's .git tree, so
// the run's post-exit WorkState read fails with the state unknown. The gate
// still finds the moment unrecoverable, but the capture's own git reads fail
// as well, so the run delivers exactly one manifest-only bundle — a "manifest"
// part with no "archive" part, trigger "terminal" — and records a "degraded"
// outcome. The terminal handoff must equal the same run with capture
// unconfigured, so a failed world read ends the run the same way with or
// without the capture.
func TestEvidenceFailedWorldReadDegradesToManifest(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission stripping does not apply to root")
	}
	// delivery is one POST the part-recording intake received: the form-part
	// names in arrival order plus the decoded manifest, so a manifest-only
	// delivery is told apart from one that carries an archive.
	type delivery struct {
		parts    []string
		manifest evidence.Manifest
	}
	newPartsIntake := func(t *testing.T) (*httptest.Server, func() []delivery) {
		t.Helper()
		var mu sync.Mutex
		var records []delivery
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			rec := delivery{parts: []string{}}
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, "multipart: "+err.Error(), http.StatusBadRequest)
				return
			}
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, "multipart part: "+err.Error(), http.StatusBadRequest)
					return
				}
				rec.parts = append(rec.parts, part.FormName())
				if part.FormName() != "manifest" {
					if _, err := io.Copy(io.Discard, part); err != nil {
						http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
						return
					}
					continue
				}
				data, err := io.ReadAll(part)
				if err != nil {
					http.Error(w, "multipart part read: "+err.Error(), http.StatusBadRequest)
					return
				}
				if err := json.Unmarshal(data, &rec.manifest); err != nil {
					http.Error(w, "manifest: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
			mu.Lock()
			records = append(records, rec)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)
		return server, func() []delivery {
			mu.Lock()
			defer mu.Unlock()
			out := make([]delivery, len(records))
			for i := range records {
				out[i] = records[i]
				out[i].parts = append([]string(nil), records[i].parts...)
			}
			return out
		}
	}
	// restoreTree puts a stripped .git tree back to directories 0o755 and
	// files 0o644 (u+rwX). The recursive strip leaves .git itself unreadable,
	// so each directory's mode is restored before its entries are read and the
	// repair holds no matter how far the strip got; the temp dir cannot be
	// cleaned up while its .git is still unreadable.
	// filepath.Walk reads each directory before calling its callback, so a
	// stripped directory is never repaired by the callback; restore chmods
	// before reading and recurses by hand.
	var restoreTree func(dir string)
	restoreTree = func(dir string) {
		_ = os.Chmod(dir, 0o755)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			switch {
			case entry.Type()&os.ModeSymlink != 0:
			case entry.IsDir():
				restoreTree(path)
			default:
				_ = os.Chmod(path, 0o644)
			}
		}
	}
	// runScenario plays the failed-world-read scenario: the script writes the
	// outcome declaration to the scratch dir (outside the workspace) first,
	// then strips the .git tree, then exits 0. An armed run points the capture
	// at the part-recording intake; a disarmed one leaves capture
	// unconfigured, the baseline for the handoff comparison.
	runScenario := func(t *testing.T, armed bool) (int, string, func() []delivery) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
chmod -R a-rwx "$COURIER_WORKSPACE/.git"
exit 0
`)
		var snapshot func() []delivery
		if armed {
			intake, snap := newPartsIntake(t)
			snapshot = snap
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			armEvidence(t, intake.URL)
		} else {
			setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
			t.Setenv("COURIER_REF", "7")
			t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
			t.Setenv("GITHUB_TOKEN", "github-api-token")
			t.Setenv("COURIER_EVIDENCE_URL", "")
			t.Setenv("COURIER_EVIDENCE_TOKEN", "")
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		// Restore explicitly before the assertions and the temp dir cleanup:
		// defers run last, and a stripped .git would otherwise survive to them.
		gitDir := filepath.Join(root, "workspace", ".git")
		restoreTree(gitDir)
		t.Cleanup(func() { restoreTree(gitDir) })
		return code, output.String(), snapshot
	}

	code, armedOut, snapshot := runScenario(t, true)
	if code != exitFailed {
		t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(armedOut, `"phase":"Failed"`) {
		t.Fatalf("armed run should end failed: %q", armedOut)
	}

	t.Run("capture degrades to the manifest alone", func(t *testing.T) {
		records := snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want 1 for a failed world read", len(records))
		}
		rec := records[0]
		if len(rec.parts) != 1 || rec.parts[0] != "manifest" {
			t.Fatalf("delivery parts = %v, want the manifest alone (no archive part)", rec.parts)
		}
		if rec.manifest.Trigger != "terminal" {
			t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
		}
		events := parseEvents(t, bytes.NewBufferString(armedOut))
		if n := countEvents(t, events, "evidence.capture"); n != 1 {
			t.Fatalf("evidence.capture events = %d, want 1", n)
		}
		detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
		if detail["trigger"] != "terminal" {
			t.Fatalf("event trigger = %v, want terminal", detail["trigger"])
		}
		if detail["outcome"] != "degraded" {
			t.Fatalf("event outcome = %v, want degraded", detail["outcome"])
		}
	})

	t.Run("unconfigured leaves the ending unchanged", func(t *testing.T) {
		disarmedCode, disarmedOut, _ := runScenario(t, false)
		if disarmedCode != exitFailed {
			t.Fatalf("disarmed run exit code = %d, want %d", disarmedCode, exitFailed)
		}
		if !strings.Contains(disarmedOut, `"phase":"Failed"`) {
			t.Fatalf("disarmed run should end failed: %q", disarmedOut)
		}
		want := terminationLineJSON(t, armedOut)
		got := terminationLineJSON(t, disarmedOut)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("terminal handoff changed with capture unconfigured:\n armed=%v\n disarmed=%v", want, got)
		}
	})
}

// TestEvidenceDeadlineStopsAttempts proves the operation deadline bounds the
// delivery loop: with evidenceOperationDeadline shrunk to a second and an
// intake that sleeps past it before answering, the first POST attempt is
// aborted by the deadline and the loop's pre-attempt context check stops a
// second attempt — the intake saw exactly one request start, the run records
// a "lost" outcome, and the terminal handoff is identical to the same run
// with capture unconfigured, so a stalled intake never changes the ending.
func TestEvidenceDeadlineStopsAttempts(t *testing.T) {
	prev := evidenceOperationDeadline
	evidenceOperationDeadline = time.Second
	t.Cleanup(func() { evidenceOperationDeadline = prev })

	// runDirty plays the TestEvidenceDirtyTerminalCaptures scenario — the
	// fake leaves an uncommitted file and declares changes, so the run ends
	// Failed with a dirty worktree. An empty URL leaves capture unconfigured,
	// the baseline for the handoff comparison.
	runDirty := func(t *testing.T, evidenceURL string) (int, string) {
		t.Helper()
		root := t.TempDir()
		remote := remoteWithExistingBranch(t, root)
		fakeOpenCode := filepath.Join(root, "opencode")
		writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
		setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
		t.Setenv("COURIER_REF", "7")
		t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
		t.Setenv("GITHUB_TOKEN", "github-api-token")
		if evidenceURL == "" {
			t.Setenv("COURIER_EVIDENCE_URL", "")
			t.Setenv("COURIER_EVIDENCE_TOKEN", "")
		} else {
			armEvidence(t, evidenceURL)
		}
		var output bytes.Buffer
		var errorsOut bytes.Buffer
		code := run(context.Background(), &output, &errorsOut)
		return code, output.String()
	}

	// The intake records each request start, then sleeps past the shrunk
	// deadline before answering the first one, so the client aborts the
	// in-flight attempt and the delivery loop's pre-attempt context check
	// must stop the second one.
	var mu sync.Mutex
	var starts int
	intake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		mu.Lock()
		first := starts == 0
		starts++
		mu.Unlock()
		if first {
			time.Sleep(5 * time.Second)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(intake.Close)

	code, armedOut := runDirty(t, intake.URL)
	if code != exitFailed {
		t.Fatalf("armed run exit code = %d, want %d", code, exitFailed)
	}
	if !strings.Contains(armedOut, `"phase":"Failed"`) {
		t.Fatalf("armed run should end failed: %q", armedOut)
	}
	mu.Lock()
	startsCount := starts
	mu.Unlock()
	if startsCount != 1 {
		t.Fatalf("intake saw %d request starts, want 1 (the deadline must stop the second attempt)", startsCount)
	}
	events := parseEvents(t, bytes.NewBufferString(armedOut))
	if n := countEvents(t, events, "evidence.capture"); n != 1 {
		t.Fatalf("evidence.capture events = %d, want 1", n)
	}
	detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
	if detail["outcome"] != "lost" {
		t.Fatalf("event outcome = %v, want lost", detail["outcome"])
	}
	baselineCode, baselineOut := runDirty(t, "")
	if baselineCode != exitFailed {
		t.Fatalf("baseline exit code = %d, want %d", baselineCode, exitFailed)
	}
	want := terminationLineJSON(t, armedOut)
	got := terminationLineJSON(t, baselineOut)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("terminal handoff changed with a stalled intake:\n armed=%v\n baseline=%v", want, got)
	}
}

// TestEvidenceSIGTERMKillsProcessGroupAndCaptures proves the pod signal
// paths end to end against a real process, the way the kernel delivers them.
// Both scenarios share one harness: a run whose model child is a shell that
// spawns a long-lived grandchild (sleep) in its own process group and blocks
// on it.
//
// sigterm — the kubelet's SIGTERM cancels the run's NotifyContext, the
// child's Cancel SIGKILLs the whole process group (-pid) so the grandchild
// dies with it, and the run ends Failed (1) with a single
// cancellation-triggered evidence delivery that preserves the uncommitted
// work and a Failed termination file.
//
// sigkill — a SIGKILL to the executor's main process leaves the run no
// cancellation path at all, the second half of issue #198's signal
// done-condition: its own process group (the model child and the grandchild)
// survives the kill. That is the documented loss — the capture is lost with
// the process (the intake receives no request), the termination file is never
// written, and the process state is a death by SIGKILL. The test kills the
// surviving grandchild as cleanup.
//
// The scenarios are subprocess-based rather than in-process because the
// signal must reach a real process: an in-process run() cannot receive a
// SIGTERM or SIGKILL the way the executor binary does. The signals are sent
// with the shell kill builtin rather than syscall.Kill; it delivers the
// identical POSIX signal.
//
// The model child uses a direct-child `sleep 300 &` plus `wait` (not a
// subshell): a subshell would orphan the sleep, let `wait` return, and let
// the run finish before the signal, defeating the point.
func TestEvidenceSIGTERMKillsProcessGroupAndCaptures(t *testing.T) {
	t.Run("sigterm", func(t *testing.T) {
		res := signalKillScenario(t, "TERM", 60*time.Second)

		if res.processState == nil {
			t.Fatalf("no process state after the executor exited")
		}
		if code := res.processState.ExitCode(); code != 1 {
			t.Fatalf("executor exit code = %d, want 1 (Failed); stdout=%q stderr=%q", code, res.out, res.errOut)
		}

		// The intake received exactly one delivery: the bearer token is the one
		// we configured, the trigger is the cancellation path, and the archive
		// preserves the uncommitted file.
		records := res.snapshot()
		if len(records) != 1 {
			t.Fatalf("intake received %d deliveries, want exactly 1; stdout=%q stderr=%q", len(records), res.out, res.errOut)
		}
		rec := records[0]
		if rec.authorization != "Bearer ev-token-1" {
			t.Fatalf("authorization = %q, want Bearer ev-token-1", rec.authorization)
		}
		if rec.manifest.Trigger != "cancellation" {
			t.Fatalf("manifest trigger = %q, want cancellation", rec.manifest.Trigger)
		}
		if _, ok := rec.archiveFiles["partial.txt"]; !ok {
			t.Fatalf("archive members = %v, want partial.txt", rec.archiveFiles)
		}

		// The group SIGKILL must have reaped the grandchild: probe it with
		// signal 0 for up to ~15s; if it survives, that is a failure.
		deadline := time.Now().Add(15 * time.Second)
		for processRunning(t, res.grandchildPid) {
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if processRunning(t, res.grandchildPid) {
			bestEffortSignal("KILL", res.grandchildPid)
			t.Errorf("process-group kill failed: grandchild %d survived the SIGTERM cascade", res.grandchildPid)
		}

		// The cancellation path ends Failed in the termination file.
		term := mustRead(t, res.termination)
		if !strings.Contains(string(term), `"phase":"Failed"`) {
			t.Fatalf("termination file %q does not contain %q", term, `"phase":"Failed"`)
		}
	})

	t.Run("sigkill", func(t *testing.T) {
		res := signalKillScenario(t, "KILL", 10*time.Second)

		if res.processState == nil {
			t.Fatalf("no process state after the executor exited")
		}
		// The kernel delivered the signal, not the run: the process state is
		// a death by SIGKILL, with no exit code of its own.
		if res.processState.Success() || res.processState.ExitCode() != -1 || !strings.Contains(res.processState.String(), "killed") {
			t.Fatalf("executor process state = %q (exit code %d), want a death by SIGKILL; stdout=%q stderr=%q",
				res.processState.String(), res.processState.ExitCode(), res.out, res.errOut)
		}

		// The run never ran its cancellation path, so the capture is lost
		// with the process: the intake received no request at all.
		if records := res.snapshot(); len(records) != 0 {
			t.Fatalf("intake received %d deliveries, want 0 (the capture is lost with the process); stdout=%q stderr=%q", len(records), res.out, res.errOut)
		}

		// The run never reached its terminal handoff: the termination file is
		// unwritten (or empty if the handoff began but never completed).
		term, err := os.ReadFile(res.termination)
		if err == nil && len(term) != 0 {
			t.Fatalf("termination file holds %d bytes, want absent or empty: %q", len(term), term)
		}

		// The model child runs in its own process group, which a SIGKILL to
		// the main process cannot reach: the grandchild is expected alive
		// right after the kill. That survival is the documented loss; the
		// test kills it as cleanup and re-kills any survivor at the end.
		if !processRunning(t, res.grandchildPid) {
			t.Fatalf("grandchild %d is not alive right after the SIGKILL; want it alive (the documented loss)", res.grandchildPid)
		}
		bestEffortSignal("KILL", res.grandchildPid)
		t.Cleanup(func() { bestEffortSignal("KILL", res.grandchildPid) })
	})
}

// signalKillResult carries one real-process signal scenario's outcome for
// the per-signal assertions.
type signalKillResult struct {
	out           string
	errOut        string
	processState  *os.ProcessState
	snapshot      func() []intakeRecord
	grandchildPid int
	termination   string
}

// signalKillScenario runs one real-process signal scenario: it builds the
// executor into the scratch temp dir, stages a run whose model child spawns a
// long-lived grandchild (sleep) in its own process group and blocks on it,
// starts the executor as a subprocess, waits for the grandchild pid file and
// the dirty work to prove the run reached the model child and is blocked,
// sends the named signal to the executor's main process, and waits at most
// waitLimit for it to exit. Anything that outlives the scenario is killed in
// a cleanup.
func signalKillScenario(t *testing.T, sig string, waitLimit time.Duration) signalKillResult {
	t.Helper()
	root := t.TempDir()

	// 1. Build the executor once, into the scratch temp dir (never into the
	//    repo). Skip when the toolchain is unavailable or the build fails.
	execBin := filepath.Join(root, "executor")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", execBin, "./cmd/courier-executor")
	_, thisFile, _, _ := runtime.Caller(0)
	build.Dir = filepath.Join(filepath.Dir(thisFile), "..", "..")
	build.Env = withGoBin(os.Environ())
	var buildOut bytes.Buffer
	build.Stdout = &buildOut
	build.Stderr = &buildOut
	if err := build.Run(); err != nil {
		t.Skipf("cannot build the executor for a subprocess test, skipping: %v (%s)", err, buildOut.String())
	}

	// 2. Scenario layout, all under the scratch temp dir.
	remote := remoteWithExistingBranch(t, root)
	workspace := filepath.Join(root, "workspace")
	scratch := filepath.Join(root, "scratch")
	termination := filepath.Join(root, "termination")
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered\n' > partial.txt
mkdir -p "$COURIER_SCRATCH_DIR"
sleep 300 &
echo "$!" > "$COURIER_SCRATCH_DIR/grandchild.pid"
wait
`)

	// The live intake the subprocess POSTs to: records the delivery count, the
	// Authorization header, the decoded manifest, and the archive members.
	intake, snapshot := newEvidenceIntake(t)

	// 3. The subprocess environment: the test process environment (PATH,
	//    GIT_CONFIG_GLOBAL, ...) minus any live COURIER_/GITHUB settings, plus
	//    exactly the values setResolveIssueEnv writes, the evidence endpoint,
	//    and a per-incarnation token.
	env := subprocessEnv(os.Environ(), map[string]string{
		"COURIER_REPO_URL":         remote,
		"COURIER_WORKSPACE":        workspace,
		"COURIER_SCRATCH_DIR":      scratch,
		"COURIER_BASE":             "main",
		"COURIER_BRANCH":           "courier/resolve-issue/acme-widgets/7",
		"COURIER_GOAL":             "Open a PR to address issue #7.",
		"COURIER_MODEL":            "any-model/name",
		"COURIER_MODE":             "resolve-issue",
		"COURIER_REPO":             "acme/widgets",
		"COURIER_GITHUB_API_BASE":  fakeGitHubAPI(t).URL,
		"COURIER_OPENCODE_BINARY":  fakeOpenCode,
		"COURIER_TERMINATION_FILE": termination,
		"COURIER_REF":              "7",
		"COURIER_RUN_NAME":         "coderrun-it-7",
		"GITHUB_TOKEN":             "github-api-token",
		"COURIER_EVIDENCE_URL":     intake.URL,
		"COURIER_EVIDENCE_TOKEN":   "ev-token-1",
	})

	// 4. Start the executor as a real subprocess and drain its streams into
	//    bounded buffers. Do not Wait yet.
	var out, errOut bytes.Buffer
	cmd := exec.Command(execBin)
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("start executor subprocess: %v", err)
	}

	var grandchildPid int
	t.Cleanup(func() {
		// Reap anything that outlives the test; killing an already-dead
		// process is a harmless no-op.
		_ = cmd.Process.Kill()
		if grandchildPid > 0 {
			bestEffortSignal("KILL", grandchildPid)
		}
	})

	// 5. Poll up to ~20s for the grandchild to appear and the dirty work to be
	//    written, proving the run reached the model-child launch and is blocked.
	partialPath := filepath.Join(workspace, "partial.txt")
	gcPidPath := filepath.Join(scratch, "grandchild.pid")
	end := time.Now().Add(20 * time.Second)
	for {
		_, pidErr := os.Stat(gcPidPath)
		_, partErr := os.Stat(partialPath)
		if pidErr == nil && partErr == nil {
			break
		}
		if time.Now().After(end) {
			_ = cmd.Process.Kill()
			t.Fatalf("did not observe the grandchild pid and dirty work within 20s; stdout=%q stderr=%q", out.String(), errOut.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	rawPid := mustRead(t, gcPidPath)
	if _, err := fmt.Sscanf(string(rawPid), "%d", &grandchildPid); err != nil || grandchildPid <= 0 {
		t.Fatalf("grandchild.pid %q is not a positive pid: %v", rawPid, err)
	}

	// 6. Send the named signal to the executor's main process, the way the
	//    kubelet (TERM) or the kernel (KILL) would. Any group kill that
	//    follows is the executor's own doing (what the sigterm scenario
	//    tests).
	if err := sendSignal(t, sig, cmd.Process.Pid); err != nil {
		t.Fatalf("send %s to the executor: %v", sig, err)
	}

	// 7. Wait for the subprocess to finish, with the scenario's cap.
	done := make(chan struct{}, 1)
	go func() {
		_ = cmd.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(waitLimit):
		_ = cmd.Process.Kill()
		t.Fatalf("executor did not exit within %s of %s; stdout=%q stderr=%q", waitLimit, sig, out.String(), errOut.String())
	}
	return signalKillResult{
		out:           out.String(),
		errOut:        errOut.String(),
		processState:  cmd.ProcessState,
		snapshot:      snapshot,
		grandchildPid: grandchildPid,
		termination:   termination,
	}
}

// withGoBin returns env with the scratch Go toolchain's bin directory placed
// at the front of PATH so `go` resolves for the build step even when the test
// process was started without it.
func withGoBin(env []string) []string {
	const bin = "/var/tmp/courier-scratch/go/bin"
	out := make([]string, 0, len(env)+1)
	path := ""
	for _, kv := range env {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if name == "PATH" {
			path = value
			continue
		}
		out = append(out, kv)
	}
	out = append(out, "PATH="+bin+string(os.PathListSeparator)+path)
	return out
}

// subprocessEnv returns the environment for the executor subprocess: the
// caller's environment with any live COURIER_* and GITHUB_TOKEN settings
// removed, and the provided key/value pairs appended. The test's own run
// configuration (mirroring setResolveIssueEnv) is the only source of the
// subprocess's COURIER_* values, so a live pod's settings cannot leak in.
func subprocessEnv(base []string, set map[string]string) []string {
	out := make([]string, 0, len(base)+len(set))
	for _, kv := range base {
		name, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if strings.HasPrefix(name, "COURIER_") || name == "GITHUB_TOKEN" {
			continue
		}
		out = append(out, kv)
	}
	for name, value := range set {
		out = append(out, name+"="+value)
	}
	return out
}

// sendSignal sends a named POSIX signal (e.g. "TERM", "KILL") to pid via the
// shell kill builtin and returns an error if it cannot be delivered. The test
// process is the executor's parent and shares the grandchild's owner, so
// it reaches the same targets syscall.Kill would.
func sendSignal(t *testing.T, sig string, pid int) error {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("kill -%s %d", sig, pid))
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kill -%s %d: %v (%s)", sig, pid, err, buf.String())
	}
	return nil
}

// bestEffortSignal sends a named signal to pid, ignoring a delivery error. It
// is used only for cleanup, where the target may already be gone.
func bestEffortSignal(sig string, pid int) {
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("kill -%s %d", sig, pid))
	_ = cmd.Run()
}

// processRunning reports whether pid names a genuinely running process: its
// /proc/<pid>/stat entry exists and its state is not a zombie. A zombie is dead
// (killed) even though its pid lingers until it is reaped, and a signal-0
// probe cannot tell a zombie from a live process.
func processRunning(t *testing.T, pid int) bool {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	s := string(b)
	idx := len(s) - 1
	for idx > 0 && s[idx] != ')' {
		idx--
	}
	// The comm sits in the first parentheses; the last ")" in the line closes it.
	// After it the fields are: state ppid pgrp ...
	f := strings.Fields(s[idx+2:])
	return len(f) > 0 && f[0] != "Z"
}

// TestEvidenceCrashCapturePrecedesResume proves the crash path's capture
// ordering (issue #198's done-condition 4): a run whose first turn crashes
// dirty — the model child reports a session id, leaves an uncommitted file,
// and exits 1 — resumes once, and the resumed turn declares changes against
// the same dirty workspace, which classify resolves as a Failed terminal.
// The crash-triggered capture is synchronous: it completes before the
// resume decision, so in the run's stdout event order the evidence.capture
// (crash) event line appears before the second executor.start
// (resumed:true) — pinning capture-before-resume, and the same-bearer pair
// of deliveries pins same-incarnation duplicate replacement. The resume
// backoff is zero: the order, not the timing, is what is pinned.
func TestEvidenceCrashCapturePrecedesResume(t *testing.T) {
	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	// The scratch marker tells the fake which turn it is on: turn 1 (no
	// marker) creates the marker, reports the session id, leaves the dirty
	// work, and crashes; turn 2 (marker present) leaves the work dirty
	// still and declares changes, which classify resolves as a Failed
	// terminal because the workspace is still dirty.
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
if [ -e "$COURIER_SCRATCH_DIR/crash-marker" ]; then
  mkdir -p "$COURIER_SCRATCH_DIR"
  printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
  exit 0
fi
mkdir -p "$COURIER_SCRATCH_DIR"
touch "$COURIER_SCRATCH_DIR/crash-marker"
printf '{"sessionID":"s1"}\n'
printf 'dirty\n' > partial.txt
exit 1
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	// Zero backoff for speed: the order, not the timing, is what is pinned.
	t.Setenv("COURIER_RESUME_BACKOFF_SECONDS", "0")
	// Debug so the executor.start and executor.continuation payloads (the
	// resumed flag and the crash kind) are present for the order assertion.
	t.Setenv("COURIER_LOG_LEVEL", "debug")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	if code := run(context.Background(), &output, &errorsOut); code != exitFailed {
		t.Fatalf("run exit code = %d, want %d; stderr=%q stdout=%q", code, exitFailed, errorsOut.String(), output.String())
	}
	if !strings.Contains(output.String(), `"phase":"Failed"`) {
		t.Fatalf("run should end failed: %q", output.String())
	}

	// Exactly two deliveries, both authenticated with the same bearer: the
	// first is the crash capture, the second the resumed turn's terminal
	// capture.
	records := snapshot()
	if len(records) != 2 {
		t.Fatalf("intake received %d deliveries, want 2 (crash, then terminal); stdout=%q", len(records), output.String())
	}
	for i, rec := range records {
		if rec.authorization != "Bearer "+testEvidenceToken {
			t.Fatalf("delivery %d authorization = %q, want Bearer <testEvidenceToken>", i, rec.authorization)
		}
	}
	if records[0].manifest.Trigger != "crash" {
		t.Fatalf("first manifest trigger = %q, want crash", records[0].manifest.Trigger)
	}
	if records[1].manifest.Trigger != "terminal" {
		t.Fatalf("second manifest trigger = %q, want terminal", records[1].manifest.Trigger)
	}

	events := parseEvents(t, &output)
	continuationIdx := -1
	var captureIdxs, startIdxs []int
	for i, ev := range events {
		switch ev["event"] {
		case "executor.continuation":
			if detail, ok := ev["detail"].(map[string]any); ok && detail["kind"] == "crash" && continuationIdx < 0 {
				continuationIdx = i
			}
		case "executor.start":
			startIdxs = append(startIdxs, i)
		case "evidence.capture":
			captureIdxs = append(captureIdxs, i)
		}
	}
	if continuationIdx < 0 {
		t.Fatalf("no executor.continuation event of kind crash; stdout=%q", output.String())
	}
	if len(startIdxs) != 2 {
		t.Fatalf("executor.start events = %d, want 2 (initial, then resumed); stdout=%q", len(startIdxs), output.String())
	}
	if len(captureIdxs) != 2 {
		t.Fatalf("evidence.capture events = %d, want 2 (crash, then terminal); stdout=%q", len(captureIdxs), output.String())
	}
	resumedStart := startIdxs[1]
	resumedDetail, _ := events[resumedStart]["detail"].(map[string]any)
	if resumedDetail["resumed"] != true {
		t.Fatalf("second executor.start resumed = %v, want true", events[resumedStart]["detail"])
	}
	// Inspection and capture happen before the resume decision completes: the
	// crash continuation, then its capture, then the resumed turn.
	if !(continuationIdx < captureIdxs[0] && captureIdxs[0] < resumedStart) {
		t.Fatalf("event order continuation=%d, first capture=%d, resumed start=%d; want continuation < capture < resumed start", continuationIdx, captureIdxs[0], resumedStart)
	}
	for i, idx := range captureIdxs {
		detail := eventDetail(t, events[idx])
		want := "crash"
		if i == 1 {
			want = "terminal"
		}
		if detail["trigger"] != want {
			t.Fatalf("evidence.capture %d trigger = %v, want %s", i, detail["trigger"], want)
		}
		if detail["outcome"] != "delivered" {
			t.Fatalf("evidence.capture %d outcome = %v, want delivered", i, detail["outcome"])
		}
	}
}

// TestEvidenceDeadlineExpiredCaptureDeliversManifestOnly pins DESIGN.md's
// degradation guarantee at the operation deadline: when the 20-second capture
// bound itself expires before the snapshot lands, the manifest alone must
// still reach the intake inside a fresh short window, recorded as a degraded
// outcome. The operation deadline is shrunk to a millisecond so the capture's
// first git call always loses the race; the terminal ending is unchanged.
func TestEvidenceDeadlineExpiredCaptureDeliversManifestOnly(t *testing.T) {
	prev := evidenceOperationDeadline
	evidenceOperationDeadline = time.Millisecond
	t.Cleanup(func() { evidenceOperationDeadline = prev })

	root := t.TempDir()
	remote := remoteWithExistingBranch(t, root)
	fakeOpenCode := filepath.Join(root, "opencode")
	writeExecutable(t, fakeOpenCode, `#!/bin/sh
case "$1" in mcp) exit 0;; esac
printf 'uncovered work\n' > evidence-target.txt
mkdir -p "$COURIER_SCRATCH_DIR"
printf '{"outcome":"changes"}' > "$COURIER_SCRATCH_DIR/outcome.json"
`)
	intake, snapshot := newEvidenceIntake(t)
	setResolveIssueEnv(t, root, remote, fakeGitHubAPI(t).URL, fakeOpenCode)
	t.Setenv("COURIER_REF", "7")
	t.Setenv("COURIER_RUN_NAME", "coderrun-it-7")
	t.Setenv("GITHUB_TOKEN", "github-api-token")
	armEvidence(t, intake.URL)

	var output bytes.Buffer
	var errorsOut bytes.Buffer
	code := run(context.Background(), &output, &errorsOut)
	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d", code, exitFailed)
	}

	records := snapshot()
	if len(records) != 1 {
		t.Fatalf("intake received %d deliveries, want the manifest-only degraded bundle", len(records))
	}
	rec := records[0]
	if rec.manifest.Trigger != "terminal" {
		t.Fatalf("manifest trigger = %q, want terminal", rec.manifest.Trigger)
	}
	if len(rec.archiveFiles) != 0 {
		t.Fatalf("degraded delivery carried archive members %v, want the manifest alone", rec.archiveFiles)
	}
	events := parseEvents(t, bytes.NewBufferString(output.String()))
	detail := eventDetail(t, findEvent(t, events, "evidence.capture"))
	if detail["outcome"] != "degraded" {
		t.Fatalf("event outcome = %v, want degraded", detail["outcome"])
	}
}
