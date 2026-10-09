package controller

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/evidence"
	"github.com/misospace/courier/internal/executor"
)

// --- shared test helpers ----------------------------------------------------

// testKey is a fixed evidence key shared by the request-minting and the intake
// under test so the HMAC verifies.
var testKey = []byte("test-evidence-key-0123456789")

// testNonce is a fixed 16-byte nonce, so the persisted secret name is
// deterministic across a test.
func testNonce() []byte {
	nonce := make([]byte, 16)
	for i := range nonce {
		nonce[i] = byte('a' + i)
	}
	return nonce
}

// newIntakeRun builds a CoderRun in the given phase with a stable UID.
func newIntakeRun(name, namespace, uid string, phase courierv1alpha1.Phase) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
		},
		Spec:   courierv1alpha1.CoderRunSpec{Mode: courierv1alpha1.ModeResolveIssue, Repo: "acme/widgets", Ref: 1, Lane: "local"},
		Status: courierv1alpha1.CoderRunStatus{Phase: phase},
	}
}

// newIntake builds an EvidenceIntake backed by a fake client holding the run
// and any extra objects. The intake's APIReader is the same fake client, so
// "uncached" reads resolve from the store.
func newIntake(t *testing.T, key []byte, run *courierv1alpha1.CoderRun, pod executor.PodConfig, extra ...client.Object) (*EvidenceIntake, client.Client) {
	t.Helper()
	scheme := launcherScheme(t)
	objs := append([]client.Object{run}, extra...)
	runtimes := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		runtimes = append(runtimes, o)
	}
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(runtimes...).
		Build()
	return &EvidenceIntake{Client: fc, APIReader: fc, Scheme: scheme, Key: key, Pod: pod}, fc
}

// sampleIntakeManifest builds a minimal, internally consistent manifest for the
// run with one stored, benign file.
func sampleIntakeManifest(run *courierv1alpha1.CoderRun, content string) *evidence.Manifest {
	return &evidence.Manifest{
		SchemaVersion: evidence.SchemaVersion,
		Run:           evidence.RunIdentity{Name: run.Name, Namespace: run.Namespace, RunUID: string(run.UID), PodUID: "pod-1"},
		Workspace:     evidence.WorkspaceIdentity{BaseRepo: "acme/widgets", Branch: "courier/x", StartSHA: "00000000", HeadSHA: "11111111"},
		CapturedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Trigger:       evidence.TriggerTerminal,
		Entries: []evidence.Entry{
			{Path: "src/a.go", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "src/a.go", Bytes: int64(len(content))},
		},
		Totals: evidence.Totals{Files: 1, Stored: 1, StoredBytes: int64(len(content))},
	}
}

// intakeRequest builds a valid multipart intake POST (manifest + optional
// archive) signed with key against run's live UID, returning the request and
// the bearer token it carried.
func intakeRequest(t *testing.T, key []byte, run *courierv1alpha1.CoderRun, m *evidence.Manifest, members map[string][]byte, nonce []byte) (*http.Request, string) {
	t.Helper()
	token := executor.EvidenceToken(key, run.Namespace, run.Name, string(run.UID), nonce)
	manifestBytes, err := m.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	archiveBytes, err := buildArchive(members)
	if err != nil {
		t.Fatalf("build archive: %v", err)
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mp, err := mw.CreateFormFile("manifest", "manifest.json")
	if err != nil {
		t.Fatalf("create manifest part: %v", err)
	}
	if _, err := mp.Write(manifestBytes); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if len(archiveBytes) > 0 {
		ap, err := mw.CreateFormFile("archive", "bundle.tar.gz")
		if err != nil {
			t.Fatalf("create archive part: %v", err)
		}
		if _, err := ap.Write(archiveBytes); err != nil {
			t.Fatalf("write archive: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	return req, token
}

// doIntake runs the intake handler on the request and returns the recorder.
func doIntake(i *EvidenceIntake, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	i.Handler().ServeHTTP(rec, req)
	return rec
}

// getSecret reads the run's evidence secret by name.
func getSecret(t *testing.T, i *EvidenceIntake, name string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := i.APIReader.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, secret); err != nil {
		t.Fatalf("get secret %s: %v", name, err)
	}
	return secret
}

// --- tests ------------------------------------------------------------------

func TestEvidenceIntakeRejectsNonPOST(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	req := httptest.NewRequest(http.MethodGet, "/intake", nil)
	req.Header.Set("Authorization", "Bearer "+base64.StdEncoding.EncodeToString(testNonce()))
	if rec := doIntake(i, req); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET code = %d, want 405; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsMissingToken(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	req := httptest.NewRequest(http.MethodPost, "/intake", nil)
	if rec := doIntake(i, req); rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsMalformedToken(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	// A structurally invalid token: not the 48-byte nonce+MAC shape.
	req := httptest.NewRequest(http.MethodPost, "/intake", strings.NewReader("x"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
	req.Header.Set("Authorization", "Bearer "+base64.StdEncoding.EncodeToString([]byte("too-short")))
	if rec := doIntake(i, req); rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsWrongContentType(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), testNonce())

	req := httptest.NewRequest(http.MethodPost, "/intake", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("code = %d, want 415; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsOversizedBody(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), testNonce())

	body := make([]byte, maxRequestBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/intake", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsMissingManifestPart(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	nonce := testNonce()
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), nonce)

	// Only an archive part; no manifest.
	archiveBytes, _ := buildArchive(map[string][]byte{"src/a.go": []byte("x")})
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	p, _ := mw.CreateFormFile("archive", "bundle.tar.gz")
	_, _ = p.Write(archiveBytes)
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsBadManifestJSON(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	nonce := testNonce()
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), nonce)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	p, _ := mw.CreateFormFile("manifest", "manifest.json")
	_, _ = p.Write([]byte(`{not json`))
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsSchemaVersionMismatch(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	m := sampleIntakeManifest(run, "benign")
	m.SchemaVersion = evidence.SchemaVersion + 1

	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsUnknownRun(t *testing.T) {
	t.Parallel()
	// The manifest names a run that does not exist in the store.
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	// A manifest for a different (absent) run, signed against itself.
	absent := newIntakeRun("absent-run", "default", "uid-absent", courierv1alpha1.PhaseFailed)
	m := sampleIntakeManifest(absent, "benign")
	req, _ := intakeRequest(t, testKey, absent, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsHMACMismatch(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	// The intake holds a different key than the one that minted the token.
	i, _ := newIntake(t, []byte("intake-key-2222222222222"), run, executor.PodConfig{})

	m := sampleIntakeManifest(run, "benign")
	req, _ := intakeRequest(t, []byte("mint-key-1111111111111"), run, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsManifestUIDMismatch(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	// Token is valid (signed against the live UID), but the manifest claims a
	// different run UID, so the identity check fails after the HMAC passes.
	m := sampleIntakeManifest(run, "benign")
	m.Run.RunUID = "wrong-uid"
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsPhaseNotAdmitted(t *testing.T) {
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseAwaitingReview,
		courierv1alpha1.PhaseDone,
		"",
	} {
		t.Run(string(phase), func(t *testing.T) {
			run := newIntakeRun("run-1", "default", "uid-1", phase)
			i, _ := newIntake(t, testKey, run, executor.PodConfig{})
			m := sampleIntakeManifest(run, "benign")
			req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
			if rec := doIntake(i, req); rec.Code != http.StatusForbidden {
				t.Fatalf("phase %q: code = %d, want 403; body = %s", phase, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestEvidenceIntakeAcceptsAdmittedPhases(t *testing.T) {
	for _, phase := range []courierv1alpha1.Phase{
		courierv1alpha1.PhasePending,
		courierv1alpha1.PhaseClaimed,
		courierv1alpha1.PhaseRunning,
		courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseNeedsHuman,
	} {
		t.Run(string(phase), func(t *testing.T) {
			run := newIntakeRun("run-1", "default", "uid-1", phase)
			i, _ := newIntake(t, testKey, run, executor.PodConfig{})
			m := sampleIntakeManifest(run, "benign content")
			req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign content")}, testNonce())
			if rec := doIntake(i, req); rec.Code != http.StatusOK {
				t.Fatalf("phase %q: code = %d, want 200; body = %s", phase, rec.Code, rec.Body.String())
			}
		})
	}
}

// tarWithSlipMember builds an archive whose sole member has a parent-directory
// segment, the tar-slip shape.
func tarWithSlipMember(name string, content []byte) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg, Format: tar.FormatPAX})
	_, _ = tw.Write(content)
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

func TestEvidenceIntakeRejectsTarSlip(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	nonce := testNonce()
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), nonce)

	m := sampleIntakeManifest(run, "x")
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mp, _ := mw.CreateFormFile("manifest", "manifest.json")
	manifestBytes, _ := m.MarshalCanonical()
	_, _ = mp.Write(manifestBytes)
	ap, _ := mw.CreateFormFile("archive", "bundle.tar.gz")
	_, _ = ap.Write(tarWithSlipMember("../etc/passwd", []byte("root:x:0:0")))
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsNonRegularMember(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	nonce := testNonce()
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), nonce)

	m := sampleIntakeManifest(run, "x")
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mp, _ := mw.CreateFormFile("manifest", "manifest.json")
	manifestBytes, _ := m.MarshalCanonical()
	_, _ = mp.Write(manifestBytes)
	ap, _ := mw.CreateFormFile("archive", "bundle.tar.gz")
	// A symlink member.
	var tbuf bytes.Buffer
	gz := gzip.NewWriter(&tbuf)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "target", Format: tar.FormatPAX})
	_ = tw.Close()
	_ = gz.Close()
	_, _ = ap.Write(tbuf.Bytes())
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsMemberOverCap(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	// A member larger than the per-file cap.
	m := sampleIntakeManifest(run, "x")
	m.Entries = []evidence.Entry{{Path: "big", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "big"}}
	m.Totals = evidence.Totals{Files: 1, Stored: 1, StoredBytes: int64(evidence.MaxFileBytes + 1)}
	big := make([]byte, evidence.MaxFileBytes+1)
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"big": big}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsArchivePartOverCap(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	nonce := testNonce()
	token := executor.EvidenceToken(testKey, run.Namespace, run.Name, string(run.UID), nonce)

	m := sampleIntakeManifest(run, "x")
	// A raw archive part larger than the total-content cap (not even valid
	// gzip); it is bounded before parsing.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mp, _ := mw.CreateFormFile("manifest", "manifest.json")
	manifestBytes, _ := m.MarshalCanonical()
	_, _ = mp.Write(manifestBytes)
	ap, _ := mw.CreateFormFile("archive", "bundle.tar.gz")
	_, _ = ap.Write(make([]byte, evidence.MaxTotalBytes+1))
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	if rec := doIntake(i, req); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsMissingAdmittedContent(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	// The manifest references admitted content that is not in the archive.
	m := sampleIntakeManifest(run, "benign")
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeRejectsInconsistentTotals(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})

	// Totals claim more stored files than exist.
	m := sampleIntakeManifest(run, "benign")
	m.Totals = evidence.Totals{Files: 1, Stored: 5, StoredBytes: int64(len("benign"))}
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

// TestEvidenceIntakePersistsBundle is the happy path: a valid bundle is
// persisted as a run-owned secret with the expected name, label, owner ref,
// and data keys.
func TestEvidenceIntakePersistsBundle(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	content := "benign content, no secrets"
	m := sampleIntakeManifest(run, content)
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte(content)}, testNonce())

	if rec := doIntake(i, req); rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	wantName := evidenceSecretName("run-1", testNonce())
	secret := getSecret(t, i, wantName)

	if got := secret.Labels[EvidenceLabelKey]; got != evidenceLabelValue("run-1") {
		t.Errorf("label = %q, want %q", got, evidenceLabelValue("run-1"))
	}
	if len(secret.OwnerReferences) != 1 {
		t.Fatalf("owner refs = %d, want 1", len(secret.OwnerReferences))
	}
	or := secret.OwnerReferences[0]
	if or.Name != "run-1" || or.Kind != "CoderRun" || or.UID != types.UID("uid-1") {
		t.Errorf("owner ref = %+v, want run-1/CoderRun/uid-1", or)
	}
	if or.APIVersion != courierv1alpha1.GroupVersion.String() {
		t.Errorf("owner ref apiVersion = %q, want %q", or.APIVersion, courierv1alpha1.GroupVersion.String())
	}
	if !*or.BlockOwnerDeletion || *or.Controller {
		t.Errorf("owner ref controller flags wrong: %+v", or)
	}
	if _, ok := secret.Data["manifest.json"]; !ok {
		t.Error("missing manifest.json data key")
	}
	if _, ok := secret.Data["bundle.tar.gz"]; !ok {
		t.Error("missing bundle.tar.gz data key")
	}
}

// TestEvidenceIntakeIdempotentPersist posts the same bundle twice and checks the
// second post updates in place rather than erroring.
func TestEvidenceIntakeIdempotentPersist(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	content := "benign content, no secrets"
	m := sampleIntakeManifest(run, content)

	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte(content)}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusOK {
		t.Fatalf("first post code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	req2, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte(content)}, testNonce())
	if rec := doIntake(i, req2); rec.Code != http.StatusOK {
		t.Fatalf("second post code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	secret := getSecret(t, i, evidenceSecretName("run-1", testNonce()))
	if len(secret.Data) != 2 {
		t.Fatalf("data keys = %d, want 2", len(secret.Data))
	}
}

// TestEvidenceIntakeRescanWithholdsCredential simulates an executor that missed
// a credential in a stored file: the intake's re-scan withholds the file,
// drops it from the archive, and re-derives the totals.
func TestEvidenceIntakeRescanWithholdsCredential(t *testing.T) {
	t.Parallel()
	const cred = "supersecrettoken123"
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	envSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "exec-env", Namespace: "default"},
		Data:       map[string][]byte{"MY_TOKEN": []byte(cred)},
	}
	pod := executor.PodConfig{EnvironmentSecret: "exec-env"}
	i, _ := newIntake(t, testKey, run, pod, envSecret)

	content := "hello " + cred + " world"
	m := &evidence.Manifest{
		SchemaVersion: evidence.SchemaVersion,
		Run:           evidence.RunIdentity{Name: run.Name, Namespace: run.Namespace, RunUID: string(run.UID), PodUID: "pod-1"},
		Workspace:     evidence.WorkspaceIdentity{BaseRepo: "acme/widgets", Branch: "courier/x", StartSHA: "00000000", HeadSHA: "11111111"},
		CapturedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Trigger:       evidence.TriggerTerminal,
		Entries: []evidence.Entry{
			{Path: "src/a.go", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "src/a.go", Bytes: int64(len(content))},
		},
		Totals: evidence.Totals{Files: 1, Stored: 1, StoredBytes: int64(len(content))},
	}
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte(content)}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	secret := getSecret(t, i, evidenceSecretName("run-1", testNonce()))
	pm := &evidence.Manifest{}
	if err := json.Unmarshal(secret.Data["manifest.json"], pm); err != nil {
		t.Fatalf("unmarshal persisted manifest: %v", err)
	}
	if pm.Entries[0].Disposition != evidence.DispositionWithheld {
		t.Errorf("disposition = %q, want withheld", pm.Entries[0].Disposition)
	}
	if pm.Totals.Withheld != 1 || pm.Totals.Stored != 0 || pm.Totals.StoredBytes != 0 {
		t.Errorf("totals = %+v, want withheld 1, stored 0, storedBytes 0", pm.Totals)
	}
	// The persisted archive must be empty (the matching member was dropped).
	gz, err := gzip.NewReader(bytes.NewReader(secret.Data["bundle.tar.gz"]))
	if err != nil {
		t.Fatalf("read persisted archive: %v", err)
	}
	tr := tar.NewReader(gz)
	if _, err := tr.Next(); err != io.EOF {
		t.Errorf("persisted archive should be empty, got member: %v", err)
	}
}

// TestEvidenceIntakeRescanRedactsMetadata checks that a path containing a
// credential is redacted in the persisted manifest while the file is still
// stored.
func TestEvidenceIntakeRescanRedactsMetadata(t *testing.T) {
	t.Parallel()
	const cred = "mygittoken12345"
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	gitSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "git-cred", Namespace: "default"},
		// Both keys the pod builder injects from this secret: the token under
		// the named key and the git username under its default key.
		Data: map[string][]byte{
			"username": []byte("octocat"),
			"token":    []byte(cred),
		},
	}
	pod := executor.PodConfig{GitCredentialSecret: "git-cred", GitTokenKey: "token"}
	i, _ := newIntake(t, testKey, run, pod, gitSecret)

	content := "benign content"
	// A stored file whose path contains the credential.
	m := &evidence.Manifest{
		SchemaVersion: evidence.SchemaVersion,
		Run:           evidence.RunIdentity{Name: run.Name, Namespace: run.Namespace, RunUID: string(run.UID), PodUID: "pod-1"},
		Workspace:     evidence.WorkspaceIdentity{BaseRepo: "acme/widgets", Branch: "courier/x", StartSHA: "00000000", HeadSHA: "11111111"},
		CapturedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Trigger:       evidence.TriggerTerminal,
		Entries: []evidence.Entry{
			{Path: "notes/" + cred + ".md", Class: evidence.ClassUntracked, Disposition: evidence.DispositionStored, StoredPath: "notes/leaked.md", Bytes: int64(len(content))},
		},
		Totals: evidence.Totals{Files: 1, Stored: 1, StoredBytes: int64(len(content))},
	}
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"notes/leaked.md": []byte(content)}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	secret := getSecret(t, i, evidenceSecretName("run-1", testNonce()))
	pm := &evidence.Manifest{}
	if err := json.Unmarshal(secret.Data["manifest.json"], pm); err != nil {
		t.Fatalf("unmarshal persisted manifest: %v", err)
	}
	if pm.Entries[0].Disposition != evidence.DispositionStored {
		t.Errorf("disposition = %q, want stored", pm.Entries[0].Disposition)
	}
	if strings.Contains(pm.Entries[0].Path, cred) {
		t.Errorf("path %q still contains the credential", pm.Entries[0].Path)
	}
	if !strings.Contains(pm.Entries[0].Path, "[REDACTED]") {
		t.Errorf("path %q is not redacted", pm.Entries[0].Path)
	}
}

// TestEvidenceIntakeRescanMissingCredentialSecret checks the intake fails
// closed (500) when it cannot read a credential secret.
func TestEvidenceIntakeRescanMissingCredentialSecret(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	pod := executor.PodConfig{EnvironmentSecret: "nonexistent"}
	i, _ := newIntake(t, testKey, run, pod)

	m := sampleIntakeManifest(run, "benign")
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign")}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500; body = %s", rec.Code, rec.Body.String())
	}
}

func TestEvidenceIntakeListAndDelete(t *testing.T) {
	t.Parallel()
	run := newIntakeRun("run-1", "default", "uid-1", courierv1alpha1.PhaseFailed)
	i, _ := newIntake(t, testKey, run, executor.PodConfig{})
	content := "benign content"
	m := sampleIntakeManifest(run, content)
	req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte(content)}, testNonce())
	if rec := doIntake(i, req); rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	ctx := context.Background()
	secrets, err := i.ListEvidenceForRun(ctx, run)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(secrets) != 1 {
		t.Fatalf("listed %d secrets, want 1", len(secrets))
	}

	if err := i.DeleteEvidenceForRun(ctx, run); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if secrets, _ = i.ListEvidenceForRun(ctx, run); len(secrets) != 0 {
		t.Fatalf("listed %d secrets after delete, want 0", len(secrets))
	}
}

func TestEvidenceIntakeNeedLeaderElection(t *testing.T) {
	t.Parallel()
	i := &EvidenceIntake{}
	if i.NeedLeaderElection() {
		t.Error("NeedLeaderElection = true, want false (the intake must run on every replica)")
	}
}

func TestEvidenceIntakeStartDisabledWithEmptyBind(t *testing.T) {
	t.Parallel()
	i := &EvidenceIntake{Bind: ""}
	if err := i.Start(context.Background()); err != nil {
		t.Fatalf("Start with empty bind = %v, want nil (disabled)", err)
	}
}

// TestEvidenceSecretName checks the naming: a short run name stays verbatim,
// and a long one is truncated+hash-sufficed within the 253-char cap while
// staying unique per incarnation (nonce).
func TestEvidenceSecretName(t *testing.T) {
	t.Parallel()
	nonce := testNonce()

	short := evidenceSecretName("run-1", nonce)
	if !strings.HasPrefix(short, "courier-evidence-run-1-") {
		t.Errorf("short name = %q, want prefix courier-evidence-run-1-", short)
	}
	if len(short) > 253 {
		t.Errorf("short name length = %d, want <= 253", len(short))
	}

	long := strings.Repeat("r", 300)
	got := evidenceSecretName(long, nonce)
	if len(got) > 253 {
		t.Errorf("long name length = %d, want <= 253", len(got))
	}
	// Different nonces (incarnations) must give different names even for the
	// same (very long) run.
	other := make([]byte, 16)
	for i := range other {
		other[i] = byte('z' + i)
	}
	if got == evidenceSecretName(long, other) {
		t.Error("two incarnations of the same long run got the same secret name")
	}
}

func TestEvidenceLabelValue(t *testing.T) {
	t.Parallel()
	if got := evidenceLabelValue("run-1"); got != "run-1" {
		t.Errorf("short label = %q, want run-1", got)
	}
	long := strings.Repeat("r", 100)
	got := evidenceLabelValue(long)
	if len(got) > 63 {
		t.Errorf("long label length = %d, want <= 63", len(got))
	}
	if !strings.HasSuffix(got, got[len(got)-9:]) {
		t.Error("long label should be hash-suffixed")
	}
	// Sanitization: lowercased, with the label-legal set (alphanumerics,
	// '-', '_', '.') kept verbatim and everything else hyphenated. An
	// underscore is legal and must not be mangled away.
	if got := evidenceLabelValue("My_Run.2"); got != "my_run.2" {
		t.Errorf("sanitized label = %q, want my_run.2", got)
	}
	// A run name made purely of legal label characters passes through
	// unchanged: the sanitizer must never mangle a legal label value.
	if got := evidenceLabelValue("a.b-c"); got != "a.b-c" {
		t.Errorf("label = %q, want a.b-c unchanged", got)
	}
}

// TestTotalsConsistent drives the intake's totals cross-check directly: an
// uncollapsed manifest must match its totals exactly, a budget-collapsed one
// is only upper-bounded, and the synthetic budget entry must agree with
// totals.omittedByBudget.
func TestTotalsConsistent(t *testing.T) {
	t.Parallel()

	// (a) Hand-built consistent manifest: every disposition count, the
	// file/commit class split, and a zero omittedByBudget match the visible
	// entries exactly.
	consistent := &evidence.Manifest{
		Entries: []evidence.Entry{
			{Path: "src/a.go", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "src/a.go", Bytes: 5},
			{Path: "src/b.go", Class: evidence.ClassAdded, Disposition: evidence.DispositionStored, StoredPath: "src/b.go", Bytes: 3},
			{Path: "src/c.txt", Class: evidence.ClassUntracked, Disposition: evidence.DispositionWithheld},
			{Path: "link", Class: evidence.ClassUntracked, Disposition: evidence.DispositionSymlink, LinkTarget: "src/a.go"},
			{Path: "gone.txt", Class: evidence.ClassDeleted, Disposition: evidence.DispositionDeleted},
			{Path: "commits/0001-abc1234.patch", Class: evidence.ClassCommit, Disposition: evidence.DispositionStored, CommitSHA: "abc1234", StoredPath: "commits/0001-abc1234.patch", Bytes: 10},
		},
		Totals: evidence.Totals{
			Files:       5,
			Commits:     1,
			Stored:      3,
			StoredBytes: 18,
			Withheld:    1,
			Symlinks:    1,
			Deleted:     1,
		},
	}
	if !totalsConsistent(consistent) {
		t.Errorf("consistent manifest rejected: %+v", consistent.Totals)
	}

	// (b) Inflated Stored: the totals claim more stored entries than the
	// (uncollapsed) entry list shows.
	inflated := *consistent
	inflated.Totals.Stored = 5
	if totalsConsistent(&inflated) {
		t.Errorf("inflated stored accepted: %+v", inflated.Totals)
	}

	// (c) Budget-collapsed manifest: a few visible entries plus the single
	// synthetic budget entry, with omittedByBudget reporting the collapsed
	// count. Upper bounds hold.
	collapsed := &evidence.Manifest{
		Entries: []evidence.Entry{
			{Path: "artifacts/0001.dat", Class: evidence.ClassUntracked, Disposition: evidence.DispositionStored, StoredPath: "artifacts/0001.dat", Bytes: 1},
			{Path: "artifacts/0002.dat", Class: evidence.ClassUntracked, Disposition: evidence.DispositionStored, StoredPath: "artifacts/0002.dat", Bytes: 1},
			{Path: "artifacts/0003.dat", Class: evidence.ClassUntracked, Disposition: evidence.DispositionStored, StoredPath: "artifacts/0003.dat", Bytes: 1},
			{Disposition: evidence.DispositionOmittedBudget},
		},
		Totals: evidence.Totals{Files: 10, Stored: 10, StoredBytes: 10, OmittedByBudget: 7},
	}
	if !totalsConsistent(collapsed) {
		t.Errorf("budget-collapsed manifest rejected: %+v", collapsed.Totals)
	}

	// (d) The synthetic budget entry is present but the totals claim no
	// entries were collapsed by budget: the two disagree.
	budgetNoOmitted := *collapsed
	budgetNoOmitted.Totals.OmittedByBudget = 0
	if totalsConsistent(&budgetNoOmitted) {
		t.Errorf("budget entry with zero omittedByBudget accepted: %+v", budgetNoOmitted.Totals)
	}

	// (e) The totals claim a budget collapse but the entry list carries no
	// synthetic budget entry: the two disagree.
	noBudgetEntry := &evidence.Manifest{
		Entries: []evidence.Entry{
			{Path: "src/a.go", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "src/a.go", Bytes: 5},
		},
		Totals: evidence.Totals{Files: 1, Stored: 1, StoredBytes: 5, OmittedByBudget: 3},
	}
	if totalsConsistent(noBudgetEntry) {
		t.Errorf("omittedByBudget without a budget entry accepted: %+v", noBudgetEntry.Totals)
	}
}

// TestTotalsConsistentGenuineCaptureRoundTrip runs a real capture against a
// scratch git worktree — once small (uncollapsed) and once large enough to
// force the budget collapse — and requires the intake's totals cross-check to
// accept both manifests: the round-trip proof that genuine evidence.Capture
// output keeps passing.
func TestTotalsConsistentGenuineCaptureRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	// Isolate git from the workstation config, mirroring the evidence
	// package's TestMain.
	home := t.TempDir()
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(home, "gitconfig"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_AUTHOR_NAME", "Courier Test")
	t.Setenv("GIT_AUTHOR_EMAIL", "courier-test@example.invalid")
	t.Setenv("GIT_COMMITTER_NAME", "Courier Test")
	t.Setenv("GIT_COMMITTER_EMAIL", "courier-test@example.invalid")

	run := func(t *testing.T, files int) {
		t.Helper()
		dir := t.TempDir()
		gitRoundTrip(t, dir, "init", "-q")
		gitRoundTrip(t, dir, "config", "user.name", "Courier Test")
		gitRoundTrip(t, dir, "config", "user.email", "courier-test@example.invalid")
		writeTestFile(t, filepath.Join(dir, "base.txt"), "base\n")
		gitRoundTrip(t, dir, "add", "--all")
		gitRoundTrip(t, dir, "commit", "-q", "-m", "base: initial")
		for i := range files {
			name := fmt.Sprintf("file-%02d.txt", i)
			if files > 50 {
				name = fmt.Sprintf("artifacts/%04d-%s.dat", i, strings.Repeat("d", 190))
			}
			writeTestFile(t, filepath.Join(dir, name), "x")
		}
		bundle, err := evidence.Capture(context.Background(), evidence.Options{
			Directory: dir,
			Trigger:   evidence.TriggerTerminal,
		})
		if err != nil {
			t.Fatalf("capture: %v", err)
		}
		if !totalsConsistent(&bundle.Manifest) {
			t.Errorf("genuine %d-file capture manifest rejected: totals %+v, %d entries", files, bundle.Manifest.Totals, len(bundle.Manifest.Entries))
		}
	}

	t.Run("uncollapsed", func(t *testing.T) {
		run(t, 5)
	})
	t.Run("budget-collapsed", func(t *testing.T) {
		run(t, 700)
	})
}

func gitRoundTrip(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
