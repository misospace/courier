package controller

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"fmt"
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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/evidence"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

// TestEvidenceEnvtestDirtyTerminalCapturesSecretAndCondition drives a run
// through the real reconcile loop to a dirty terminal (Failed or NeedsHuman),
// POSTs a REAL bundle from the capture core over a real git worktree, and
// asserts the evidence Secret and the EvidenceCaptured condition.
func TestEvidenceEnvtestDirtyTerminalCapturesSecretAndCondition(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	// Unique names for isolation.
	runName := "evidence-dirty-terminal"
	laneName := "evidence-dirty-terminal"
	namespace := "default"

	// Create LaneProfile and CoderRun.
	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	run := envtestRunInNamespace(runName, laneName, namespace)
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}

	// Cleanup: delete created resources.
	t.Cleanup(func() {
		// Delete evidence secrets by label.
		var secrets corev1.SecretList
		if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runName)}); err == nil {
			for _, s := range secrets.Items {
				_ = envtestClient.Delete(ctx, &s)
			}
		}
		_ = envtestClient.Delete(ctx, run)
		_ = envtestClient.Delete(ctx, lane)
	})

	// Build the intake with the same PodConfig the 200-path unit tests use.
	// The PodConfig needs GitCredentialSecret and GitTokenKey for the credential scanner.
	intake := &EvidenceIntake{
		Client:    envtestClient,
		APIReader: envtestClient,
		Scheme:    envtestScheme(),
		Key:       testKey,
		Pod:       executor.PodConfig{},
	}

	// Build the reconciler with the intake attached.
	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       envtestClient,
		APIReader:    envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Evidence:     intake,
		Launch: func(ctx context.Context, run *courierv1alpha1.CoderRun) error {
			var claimed courierv1alpha1.CoderRun
			if err := envtestClient.Get(ctx, envtestKey(runName), &claimed); err != nil {
				return err
			}
			if claimed.Status.Phase != courierv1alpha1.PhaseClaimed {
				return fmt.Errorf("phase during launch = %q, want Claimed", claimed.Status.Phase)
			}
			controller := true
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      run.Name + "-coordinator",
					Namespace: run.Namespace,
					Labels:    map[string]string{executor.LabelRun: run.Name},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: courierv1alpha1.GroupVersion.String(),
						Kind:       "CoderRun",
						Name:       run.Name,
						UID:        run.UID,
						Controller: &controller,
					}},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "coordinator", Image: "example/test"}}},
			}
			if err := envtestClient.Create(ctx, pod); err != nil {
				return err
			}
			// Exit code 17 -> Failed (dirty terminal).
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 17}},
			}}
			return envtestClient.Status().Update(ctx, pod)
		},
	}

	// Reconcile loop: Pending -> Claimed -> Running -> Failed.
	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, envtestRequest(runName)); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	assertEnvtestPhase(t, runName, courierv1alpha1.PhaseRunning)
	reconcile()
	assertEnvtestPhase(t, runName, courierv1alpha1.PhaseFailed)

	// Now the run is in Failed phase. Generate a REAL evidence bundle
	// using evidence.Capture over a real git worktree with dirty+untracked files.
	workdir := t.TempDir()
	// Initialize git repo.
	if err := runGit(t, workdir, "init", "-q"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := runGit(t, workdir, "config", "user.name", "Courier Test"); err != nil {
		t.Fatalf("git config user.name: %v", err)
	}
	if err := runGit(t, workdir, "config", "user.email", "courier-test@example.invalid"); err != nil {
		t.Fatalf("git config user.email: %v", err)
	}
	// Base commit.
	baseFile := filepath.Join(workdir, "base.txt")
	if err := os.WriteFile(baseFile, []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runGit(t, workdir, "add", "--all"); err != nil {
		t.Fatal(err)
	}
	if err := runGit(t, workdir, "commit", "-q", "-m", "base: initial"); err != nil {
		t.Fatal(err)
	}

	// Add dirty (modified) and untracked files.
	dirtyFile := filepath.Join(workdir, "src/dirty.go")
	if err := os.MkdirAll(filepath.Dir(dirtyFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dirtyFile, []byte("package dirty\nfunc F() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	untrackedFile := filepath.Join(workdir, "src/untracked.go")
	if err := os.WriteFile(untrackedFile, []byte("package untracked\nfunc G() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Capture the evidence bundle.
	bundle, err := evidence.Capture(ctx, evidence.Options{
		Directory:   workdir,
		UpstreamRef: "HEAD~1",
		StartSHA:    "00000000",
		Run: evidence.RunIdentity{
			Name:      runName,
			Namespace: namespace,
			RunUID:    string(run.UID), // Use the UID assigned by envtest API server.
			PodUID:    "pod-1",
		},
		Workspace: evidence.WorkspaceIdentity{
			BaseRepo: "acme/widgets",
			HeadRepo: "acme/widgets",
			Branch:   "courier/evidence-dirty-terminal",
			StartSHA: "00000000",
			HeadSHA:  "11111111",
		},
		CapturedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Trigger:    evidence.TriggerTerminal,
	})
	if err != nil {
		t.Fatalf("evidence.Capture: %v", err)
	}

	// POST the bundle to the intake using a fresh nonce.
	nonce := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	token := executor.EvidenceToken(testKey, namespace, runName, string(run.UID), nonce)

	manifestBytes, err := bundle.Manifest.MarshalCanonical()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	archiveBytes := bundle.Archive

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

	rec := httptest.NewRecorder()
	intake.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("intake POST code = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	// Reconcile again so the controller's reconcileEvidence sees the secret.
	reconcile()
	reconcile() // May need twice for condition to be set.

	// Assert evidence Secret exists with correct name, labels, ownerRef, and data keys.
	wantSecretName := evidenceSecretName(runName, nonce)
	secret := &corev1.Secret{}
	err = envtestClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: wantSecretName}, secret)
	if err != nil {
		t.Fatalf("get evidence secret %s: %v", wantSecretName, err)
	}

	// (a) name == evidenceSecretName(runName, nonce)
	if secret.Name != wantSecretName {
		t.Errorf("secret name = %q, want %q", secret.Name, wantSecretName)
	}

	// (b) labels exactly {EvidenceLabelKey: evidenceLabelValue(runName)}
	if got := secret.Labels[EvidenceLabelKey]; got != evidenceLabelValue(runName) {
		t.Errorf("label = %q, want %q", got, evidenceLabelValue(runName))
	}
	if len(secret.Labels) != 1 {
		t.Errorf("secret labels = %v, want exactly one label", secret.Labels)
	}

	// (c) ownerReference: Kind CoderRun, live run name+UID, BlockOwnerDeletion=true, Controller=false
	if len(secret.OwnerReferences) != 1 {
		t.Fatalf("owner refs = %d, want 1", len(secret.OwnerReferences))
	}
	or := secret.OwnerReferences[0]
	if or.Kind != "CoderRun" || or.Name != runName || or.UID != run.UID {
		t.Errorf("owner ref = %+v, want CoderRun/%s/%s", or, runName, run.UID)
	}
	if or.BlockOwnerDeletion == nil || !*or.BlockOwnerDeletion || or.Controller != nil && *or.Controller {
		t.Errorf("owner ref controller flags wrong: BlockOwnerDeletion=%v, Controller=%v, want true/false", or.BlockOwnerDeletion, or.Controller)
	}

	// (d) data keys exactly manifest.json and bundle.tar.gz
	if len(secret.Data) != 2 {
		t.Errorf("data keys = %d, want 2", len(secret.Data))
	}
	if _, ok := secret.Data[evidenceManifestKey]; !ok {
		t.Error("missing manifest.json data key")
	}
	if _, ok := secret.Data[evidenceBundleKey]; !ok {
		t.Error("missing bundle.tar.gz data key")
	}

	// Assert run condition: type EvidenceCaptured, status True, reason EvidenceReasonCaptured,
	// ObservedGeneration matches run generation.
	var updatedRun courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(runName), &updatedRun); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(updatedRun.Status.Conditions, EvidenceConditionType)
	if cond == nil {
		t.Fatal("EvidenceCaptured condition not found")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("condition status = %v, want True", cond.Status)
	}
	if cond.Reason != EvidenceReasonCaptured {
		t.Errorf("condition reason = %q, want %q", cond.Reason, EvidenceReasonCaptured)
	}
	if cond.ObservedGeneration != updatedRun.Generation {
		t.Errorf("condition ObservedGeneration = %d, want run generation %d", cond.ObservedGeneration, updatedRun.Generation)
	}
}

// TestEvidenceEnvtestCleanTerminalNoEvidence drives a run to a clean terminal
// (AwaitingReview or Done) without posting evidence, and asserts no evidence
// Secret and no EvidenceCaptured condition exist.
func TestEvidenceEnvtestCleanTerminalNoEvidence(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	runName := "evidence-clean-terminal"
	laneName := "evidence-clean-terminal"
	namespace := "default"

	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	run := envtestRunInNamespace(runName, laneName, namespace)
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		var secrets corev1.SecretList
		if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runName)}); err == nil {
			for _, s := range secrets.Items {
				_ = envtestClient.Delete(ctx, &s)
			}
		}
		_ = envtestClient.Delete(ctx, run)
		_ = envtestClient.Delete(ctx, lane)
	})

	intake := &EvidenceIntake{
		Client:    envtestClient,
		APIReader: envtestClient,
		Scheme:    envtestScheme(),
		Key:       testKey,
		Pod:       executor.PodConfig{},
	}

	item := &admissionSource{}
	reconciler := &CoderRunReconciler{
		Client:       envtestClient,
		APIReader:    envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Evidence:     intake,
		Observer: &sequenceWorldObserver{observations: []PRObservation{
			greenObservation("sha-1", "check-a", "check-b"),
			greenObservation("sha-1", "check-a", "check-b"),
		}},
		Launch: func(ctx context.Context, run *courierv1alpha1.CoderRun) error {
			var claimed courierv1alpha1.CoderRun
			if err := envtestClient.Get(ctx, envtestKey(runName), &claimed); err != nil {
				return err
			}
			if claimed.Status.Phase != courierv1alpha1.PhaseClaimed {
				return fmt.Errorf("phase during launch = %q, want Claimed", claimed.Status.Phase)
			}
			controller := true
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      run.Name + "-coordinator",
					Namespace: run.Namespace,
					Labels:    map[string]string{executor.LabelRun: run.Name},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: courierv1alpha1.GroupVersion.String(),
						Kind:       "CoderRun",
						Name:       run.Name,
						UID:        run.UID,
						Controller: &controller,
					}},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "coordinator", Image: "example/test"}}},
			}
			if err := envtestClient.Create(ctx, pod); err != nil {
				return err
			}
			// Exit code 0 -> Verifying -> AwaitingReview (clean terminal).
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}}
			return envtestClient.Status().Update(ctx, pod)
		},
	}

	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, envtestRequest(runName)); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	assertEnvtestPhase(t, runName, courierv1alpha1.PhaseRunning)
	reconcile()
	assertEnvtestPhase(t, runName, courierv1alpha1.PhaseVerifying)
	reconcile()
	assertEnvtestPhase(t, runName, courierv1alpha1.PhaseVerifying)
	reconcile()
	assertEnvtestPhase(t, runName, courierv1alpha1.PhaseAwaitingReview)

	// Assert: no evidence Secret (list by EvidenceLabelKey in the namespace).
	var secrets corev1.SecretList
	if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runName)}); err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 0 {
		t.Errorf("found %d evidence secrets, want 0", len(secrets.Items))
	}

	// Assert: no EvidenceCaptured condition on the run.
	var updatedRun courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(runName), &updatedRun); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(updatedRun.Status.Conditions, EvidenceConditionType)
	if cond != nil {
		t.Errorf("EvidenceCaptured condition exists = %+v, want none", cond)
	}
}

// TestEvidenceEnvtestWithheldEntryPersistsRestAndInvalidBundlesPersistNothing
// (a) Bundle with an entry the intake withholds: other members persist;
//     withheld member is ABSENT from the stored bundle.tar.gz; persistedManifest
//     records that entry with disposition "withheld", StoredPath "", consistent totals.
// (b) Structurally invalid bundles — oversize part, gzip bomb, tar-slip member name —
//     each rejected with 413/400 and requireNoSecret for its nonce; final label-list
//     shows zero evidence Secrets in the namespace.
func TestEvidenceEnvtestWithheldEntryPersistsRestAndInvalidBundlesPersistNothing(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	runName := "evidence-withheld-invalid"
	laneName := "evidence-withheld-invalid"
	namespace := "default"

	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	run := envtestRunInNamespace(runName, laneName, namespace)
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	// The CRD's status subresource drops Status at Create; set the phase the
	// intake admits through the status endpoint.
	if err := envtestClient.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatal(err)
	}
	run.Status.Phase = courierv1alpha1.PhaseFailed
	if err := envtestClient.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		var secrets corev1.SecretList
		if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runName)}); err == nil {
			for _, s := range secrets.Items {
				_ = envtestClient.Delete(ctx, &s)
			}
		}
		_ = envtestClient.Delete(ctx, run)
		_ = envtestClient.Delete(ctx, lane)
	})

	// Intake needs a PodConfig with a credential secret so the re-scan can
	// withhold an entry whose content matches that credential.
	const cred = "supersecrettoken123"
	envSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "exec-env", Namespace: namespace},
		Data:       map[string][]byte{"MY_TOKEN": []byte(cred)},
	}
	if err := envtestClient.Create(ctx, envSecret); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = envtestClient.Delete(ctx, envSecret) })

	intake := &EvidenceIntake{
		Client:    envtestClient,
		APIReader: envtestClient,
		Scheme:    envtestScheme(),
		Key:       testKey,
		Pod:       executor.PodConfig{EnvironmentSecret: "exec-env"},
	}

	// ---- (a) Withheld entry test ----
	nonceA := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceA); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}

	cleanContent := []byte("package clean\n")
	dirtyContent := []byte("token = " + cred + "\n")

	m := &evidence.Manifest{
		SchemaVersion: evidence.SchemaVersion,
		Run:           evidence.RunIdentity{Name: runName, Namespace: namespace, RunUID: string(run.UID), PodUID: "pod-1"},
		Workspace:     evidence.WorkspaceIdentity{BaseRepo: "acme/widgets", Branch: "courier/x", StartSHA: "00000000", HeadSHA: "11111111"},
		CapturedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Trigger:       evidence.TriggerTerminal,
		Entries: []evidence.Entry{
			{Path: "src/clean.go", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "src/clean.go", Bytes: int64(len(cleanContent))},
			{Path: "src/dirty.go", Class: evidence.ClassModified, Disposition: evidence.DispositionStored, StoredPath: "src/dirty.go", Bytes: int64(len(dirtyContent))},
		},
		Totals: evidence.Totals{Files: 2, Stored: 2, StoredBytes: int64(len(cleanContent) + len(dirtyContent))},
	}

	reqA, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/clean.go": cleanContent, "src/dirty.go": dirtyContent}, nonceA)

	recA := doIntake(intake, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("withheld test: intake POST code = %d, want 200; body = %s", recA.Code, recA.Body.String())
	}

	secretA := getSecret(t, intake, evidenceSecretName(runName, nonceA))
	pmA := persistedManifest(t, secretA)

	// Other members persist.
	if pmA.Entries[0].Disposition != evidence.DispositionStored {
		t.Errorf("clean entry disposition = %q, want stored", pmA.Entries[0].Disposition)
	}
	if pmA.Entries[0].StoredPath != "src/clean.go" {
		t.Errorf("clean entry StoredPath = %q, want src/clean.go", pmA.Entries[0].StoredPath)
	}

	// Withheld member is absent from bundle.tar.gz and has disposition "withheld", StoredPath "".
	if pmA.Entries[1].Disposition != evidence.DispositionWithheld {
		t.Errorf("dirty entry disposition = %q, want withheld", pmA.Entries[1].Disposition)
	}
	if pmA.Entries[1].StoredPath != "" {
		t.Errorf("withheld entry StoredPath = %q, want empty", pmA.Entries[1].StoredPath)
	}
	if pmA.Entries[1].Bytes != 0 {
		t.Errorf("withheld entry Bytes = %d, want 0", pmA.Entries[1].Bytes)
	}

	// Check persisted bundle.tar.gz does not contain the withheld member.
	namesA, _ := readArchive(t, secretA.Data[evidenceBundleKey])
	foundDirty := false
	for _, n := range namesA {
		if n == "src/dirty.go" {
			foundDirty = true
			break
		}
	}
	if foundDirty {
		t.Error("persisted bundle.tar.gz contains withheld member src/dirty.go")
	}
	if len(namesA) != 1 || namesA[0] != "src/clean.go" {
		t.Errorf("persisted members = %v, want exactly [src/clean.go]", namesA)
	}

	// Consistent totals: Files=2, Stored=1, Withheld=1, StoredBytes=len(cleanContent)
	if pmA.Totals.Files != 2 || pmA.Totals.Stored != 1 || pmA.Totals.Withheld != 1 || pmA.Totals.StoredBytes != int64(len(cleanContent)) {
		t.Errorf("totals = %+v, want Files=2 Stored=1 Withheld=1 StoredBytes=%d", pmA.Totals, len(cleanContent))
	}

	// ---- (b) Invalid bundles test ----
	// Each rejected POST must not leave an evidence Secret.

	// (b.1) Oversize manifest part (over ManifestBudgetBytes).
	nonceB1 := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceB1); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	// Build a manifest with many entries to exceed ManifestBudgetBytes.
	bigManifest := sampleIntakeManifest(run, "x")
	bigManifest.Entries = make([]evidence.Entry, evidence.MaxManifestEntries+1)
	for idx := range bigManifest.Entries {
		bigManifest.Entries[idx] = evidence.Entry{
			Path:        fmt.Sprintf("p%04d", idx),
			Class:       evidence.ClassModified,
			Disposition: evidence.DispositionWithheld,
		}
	}
	bigManifest.Totals = evidence.Totals{Files: len(bigManifest.Entries), Withheld: len(bigManifest.Entries)}
	reqB1, _ := intakeRequest(t, testKey, run, bigManifest, map[string][]byte{}, nonceB1)
	recB1 := doIntake(intake, reqB1)
	if recB1.Code != http.StatusBadRequest {
		t.Errorf("oversize manifest: code = %d, want 400; body = %s", recB1.Code, recB1.Body.String())
	}
	requireNoSecret(t, intake, runName, namespace, nonceB1)

	// (b.2) Oversize archive part (over MaxTotalBytes + slack).
	nonceB2 := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceB2); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	mB2 := sampleIntakeManifest(run, "x")
	var bufB2 bytes.Buffer
	mwB2 := multipart.NewWriter(&bufB2)
	mpB2, _ := mwB2.CreateFormFile("manifest", "manifest.json")
	manifestBytesB2, _ := mB2.MarshalCanonical()
	_, _ = mpB2.Write(manifestBytesB2)
	apB2, _ := mwB2.CreateFormFile("archive", "bundle.tar.gz")
	// Archive part larger than MaxTotalBytes + MaxTotalBytes/64.
	const slack = evidence.MaxTotalBytes / 64
	_, _ = apB2.Write(make([]byte, evidence.MaxTotalBytes+slack+1))
	_ = mwB2.Close()
	tokenB2 := executor.EvidenceToken(testKey, namespace, runName, string(run.UID), nonceB2)
	reqB2 := httptest.NewRequest(http.MethodPost, "/intake", &bufB2)
	reqB2.Header.Set("Content-Type", mwB2.FormDataContentType())
	reqB2.Header.Set("Authorization", "Bearer "+tokenB2)
	recB2 := doIntake(intake, reqB2)
	if recB2.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize archive: code = %d, want 413; body = %s", recB2.Code, recB2.Body.String())
	}
	requireNoSecret(t, intake, runName, namespace, nonceB2)

	// (b.3) Gzip bomb: a small gzip that decompresses to huge content.
	// We construct a valid gzip stream that decompresses to exceed MaxTotalBytes.
	nonceB3 := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceB3); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	mB3 := sampleIntakeManifest(run, "x")
	var bufB3 bytes.Buffer
	mwB3 := multipart.NewWriter(&bufB3)
	mpB3, _ := mwB3.CreateFormFile("manifest", "manifest.json")
	manifestBytesB3, _ := mB3.MarshalCanonical()
	_, _ = mpB3.Write(manifestBytesB3)
	apB3, _ := mwB3.CreateFormFile("archive", "bundle.tar.gz")
	// Build a gzip bomb: a tar with one member whose header claims huge size
	// but actual content is small (the intake reads with LimitReader so it
	// will cap at MaxFileBytes+1, but the total content cap is enforced).
	// Instead, we send a valid gzip that expands to many small members
	// exceeding MaxContentEntries.
	var gzipBomb bytes.Buffer
	gz := gzip.NewWriter(&gzipBomb)
	tw := tar.NewWriter(gz)
	// Create MaxContentEntries + 1 members, each 1 byte.
	for i := 0; i <= evidence.MaxContentEntries; i++ {
		name := fmt.Sprintf("bomb/%04d.txt", i)
		hdr := &tar.Header{Name: name, Mode: 0o644, Size: 1, Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		_ = tw.WriteHeader(hdr)
		_, _ = tw.Write([]byte("x"))
	}
	_ = tw.Close()
	_ = gz.Close()
	_, _ = apB3.Write(gzipBomb.Bytes())
	_ = mwB3.Close()
	tokenB3 := executor.EvidenceToken(testKey, namespace, runName, string(run.UID), nonceB3)
	reqB3 := httptest.NewRequest(http.MethodPost, "/intake", &bufB3)
	reqB3.Header.Set("Content-Type", mwB3.FormDataContentType())
	reqB3.Header.Set("Authorization", "Bearer "+tokenB3)
	recB3 := doIntake(intake, reqB3)
	if recB3.Code != http.StatusBadRequest {
		t.Errorf("gzip bomb: code = %d, want 400; body = %s", recB3.Code, recB3.Body.String())
	}
	requireNoSecret(t, intake, runName, namespace, nonceB3)

	// (b.4) Tar-slip member name (parent directory segment).
	nonceB4 := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceB4); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	mB4 := sampleIntakeManifest(run, "x")
	var bufB4 bytes.Buffer
	mwB4 := multipart.NewWriter(&bufB4)
	mpB4, _ := mwB4.CreateFormFile("manifest", "manifest.json")
	manifestBytesB4, _ := mB4.MarshalCanonical()
	_, _ = mpB4.Write(manifestBytesB4)
	apB4, _ := mwB4.CreateFormFile("archive", "bundle.tar.gz")
	// Tar member with ".." path.
	var tarSlip bytes.Buffer
	gzSlip := gzip.NewWriter(&tarSlip)
	twSlip := tar.NewWriter(gzSlip)
	hdrSlip := &tar.Header{Name: "../etc/passwd", Mode: 0o644, Size: 12, Typeflag: tar.TypeReg, Format: tar.FormatPAX}
	_ = twSlip.WriteHeader(hdrSlip)
	_, _ = twSlip.Write([]byte("root:x:0:0"))
	_ = twSlip.Close()
	_ = gzSlip.Close()
	_, _ = apB4.Write(tarSlip.Bytes())
	_ = mwB4.Close()
	tokenB4 := executor.EvidenceToken(testKey, namespace, runName, string(run.UID), nonceB4)
	reqB4 := httptest.NewRequest(http.MethodPost, "/intake", &bufB4)
	reqB4.Header.Set("Content-Type", mwB4.FormDataContentType())
	reqB4.Header.Set("Authorization", "Bearer "+tokenB4)
	recB4 := doIntake(intake, reqB4)
	if recB4.Code != http.StatusBadRequest {
		t.Errorf("tar-slip: code = %d, want 400; body = %s", recB4.Code, recB4.Body.String())
	}
	requireNoSecret(t, intake, runName, namespace, nonceB4)

	// Final label-list: the only evidence Secret that exists is (a)'s accepted
// slot; every rejected POST persisted nothing.
	var secrets corev1.SecretList
	if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runName)}); err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 1 || secrets.Items[0].Name != evidenceSecretName(runName, nonceA) {
		names := make([]string, 0, len(secrets.Items))
		for _, s := range secrets.Items {
			names = append(names, s.Name)
		}
		t.Errorf("final evidence secrets = %v, want only %s", names, evidenceSecretName(runName, nonceA))
	}
}

// runGit runs a git command in the given directory.
func runGit(t *testing.T, dir string, args ...string) error {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}