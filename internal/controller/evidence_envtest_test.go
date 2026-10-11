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
//
//	withheld member is ABSENT from the stored bundle.tar.gz; persistedManifest
//	records that entry with disposition "withheld", StoredPath "", consistent totals.
//
// (b) Structurally invalid bundles — oversize part, gzip bomb, tar-slip member name —
//
//	each rejected with 413/400 and requireNoSecret for its nonce; final label-list
//	shows zero evidence Secrets in the namespace.
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

// TestEvidenceEnvtestGraceCapturePhaseAcceptance verifies the intake's phase
// allow-list: NeedsHuman admits (grace capture for crashloop ceiling),
// Verifying and Done reject with 403 and persist nothing.
func TestEvidenceEnvtestGraceCapturePhaseAcceptance(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	runNames := []string{
		"evidence-grace-needshuman",
		"evidence-grace-verifying",
		"evidence-grace-done",
	}
	phases := []courierv1alpha1.Phase{
		courierv1alpha1.PhaseNeedsHuman,
		courierv1alpha1.PhaseVerifying,
		courierv1alpha1.PhaseDone,
	}
	laneName := "evidence-grace-phases"
	namespace := "default"

	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}

	var runs []*courierv1alpha1.CoderRun
	for _, name := range runNames {
		run := envtestRunInNamespace(name, laneName, namespace)
		if err := envtestClient.Create(ctx, run); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, run)
	}

	t.Cleanup(func() {
		for _, run := range runs {
			var secrets corev1.SecretList
			if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(run.Name)}); err == nil {
				for _, s := range secrets.Items {
					_ = envtestClient.Delete(ctx, &s)
				}
			}
			_ = envtestClient.Delete(ctx, run)
		}
		_ = envtestClient.Delete(ctx, lane)
	})

	// Pin each run to its target phase via the status subresource.
	for i, run := range runs {
		if err := envtestClient.Get(ctx, envtestKey(run.Name), run); err != nil {
			t.Fatal(err)
		}
		run.Status.Phase = phases[i]
		if err := envtestClient.Status().Update(ctx, run); err != nil {
			t.Fatal(err)
		}
	}

	// Build an intake with no credential secrets (benign bundle).
	intake := &EvidenceIntake{
		Client:    envtestClient,
		APIReader: envtestClient,
		Scheme:    envtestScheme(),
		Key:       testKey,
		Pod:       executor.PodConfig{},
	}

	// POST a valid bundle to each run with a distinct nonce.
	for i, run := range runs {
		nonce := make([]byte, executor.EvidenceNonceBytes)
		if _, err := rand.Read(nonce); err != nil {
			t.Fatalf("generate nonce for %s: %v", run.Name, err)
		}

		m := sampleIntakeManifest(run, "benign content")
		req, _ := intakeRequest(t, testKey, run, m, map[string][]byte{"src/a.go": []byte("benign content")}, nonce)
		rec := doIntake(intake, req)

		wantCode := http.StatusOK
		if phases[i] != courierv1alpha1.PhaseNeedsHuman {
			wantCode = http.StatusForbidden
		}

		if rec.Code != wantCode {
			t.Errorf("%s (phase %s): intake POST code = %d, want %d; body = %s", run.Name, phases[i], rec.Code, wantCode, rec.Body.String())
		}

		if wantCode == http.StatusOK {
			// NeedsHuman: secret must exist.
			wantName := evidenceSecretName(run.Name, nonce)
			secret := &corev1.Secret{}
			if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: wantName}, secret); err != nil {
				t.Errorf("%s: evidence secret %s not found: %v", run.Name, wantName, err)
			}
		} else {
			// Verifying/Done: no secret must exist.
			requireNoSecret(t, intake, run.Name, namespace, nonce)
		}
	}
}

// TestEvidenceEnvtestOperatorDeletesAtLandedKeepsAtNeedsHuman verifies the
// reconcile loop's evidence lifecycle: evidence is deleted when a run lands
// (AwaitingReview/Done) and retained at NeedsHuman. It also checks that a run
// in Verifying with pre-seeded evidence keeps it while Verifying, and still
// keeps it after transitioning to NeedsHuman. The ownerRef contract is asserted
// to confirm production GC cascade behavior.
func TestEvidenceEnvtestOperatorDeletesAtLandedKeepsAtNeedsHuman(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	// Run A: NeedsHuman -> POST -> reconcile -> kept + condition.
	// Then phase -> Done -> reconcile -> deleted (idempotent).
	runAName := "evidence-op-needshuman-to-done"
	laneName := "evidence-op-lifecycle"
	namespace := "default"

	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	runA := envtestRunInNamespace(runAName, laneName, namespace)
	if err := envtestClient.Create(ctx, runA); err != nil {
		t.Fatal(err)
	}

	// Pin to NeedsHuman.
	if err := envtestClient.Get(ctx, envtestKey(runAName), runA); err != nil {
		t.Fatal(err)
	}
	runA.Status.Phase = courierv1alpha1.PhaseNeedsHuman
	if err := envtestClient.Status().Update(ctx, runA); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		var secrets corev1.SecretList
		if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runAName)}); err == nil {
			for _, s := range secrets.Items {
				_ = envtestClient.Delete(ctx, &s)
			}
		}
		_ = envtestClient.Delete(ctx, runA)
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
		Launch: func(ctx context.Context, run *courierv1alpha1.CoderRun) error {
			var claimed courierv1alpha1.CoderRun
			if err := envtestClient.Get(ctx, envtestKey(runAName), &claimed); err != nil {
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
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  "coordinator",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}}
			return envtestClient.Status().Update(ctx, pod)
		},
	}

	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, envtestRequest(runAName)); err != nil {
			t.Fatal(err)
		}
	}

	// POST evidence to run A while it is NeedsHuman.
	nonceA := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceA); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	m := sampleIntakeManifest(runA, "benign content")
	reqA, _ := intakeRequest(t, testKey, runA, m, map[string][]byte{"src/a.go": []byte("benign content")}, nonceA)
	recA := doIntake(intake, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("run A intake POST code = %d, want 200; body = %s", recA.Code, recA.Body.String())
	}

	// Reconcile: evidence should be kept and condition set.
	reconcile()
	var updatedRunA courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(runAName), &updatedRunA); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(updatedRunA.Status.Conditions, EvidenceConditionType)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != EvidenceReasonCaptured {
		t.Errorf("run A condition = %+v, want True/%s", cond, EvidenceReasonCaptured)
	}

	// Verify secret exists with correct ownerRef (BlockOwnerDeletion=true, Controller=false).
	// This ownerRef is what makes production GC cascade when the run is deleted.
	secretA := getSecret(t, intake, evidenceSecretName(runAName, nonceA))
	if len(secretA.OwnerReferences) != 1 {
		t.Fatalf("run A secret owner refs = %d, want 1", len(secretA.OwnerReferences))
	}
	or := secretA.OwnerReferences[0]
	if or.Kind != "CoderRun" || or.Name != runAName || or.UID != runA.UID {
		t.Errorf("run A owner ref = %+v, want CoderRun/%s/%s", or, runAName, runA.UID)
	}
	if or.BlockOwnerDeletion == nil || !*or.BlockOwnerDeletion || or.Controller != nil && *or.Controller {
		t.Errorf("run A owner ref flags: BlockOwnerDeletion=%v, Controller=%v, want true/false", or.BlockOwnerDeletion, or.Controller)
	}

	// Now move run A to Done via Status().Update and reconcile.
	if err := envtestClient.Get(ctx, envtestKey(runAName), &updatedRunA); err != nil {
		t.Fatal(err)
	}
	updatedRunA.Status.Phase = courierv1alpha1.PhaseDone
	if err := envtestClient.Status().Update(ctx, &updatedRunA); err != nil {
		t.Fatal(err)
	}
	reconcile()

	// Evidence should be deleted (label-list empty).
	var secretsA corev1.SecretList
	if err := envtestClient.List(ctx, &secretsA, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runAName)}); err != nil {
		t.Fatal(err)
	}
	if len(secretsA.Items) != 0 {
		t.Errorf("run A after Done: found %d evidence secrets, want 0", len(secretsA.Items))
	}

	// Second reconcile is idempotent (no error, still empty).
	reconcile()
	if err := envtestClient.List(ctx, &secretsA, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runAName)}); err != nil {
		t.Fatal(err)
	}
	if len(secretsA.Items) != 0 {
		t.Errorf("run A after second reconcile: found %d evidence secrets, want 0", len(secretsA.Items))
	}

	// Run B: Verifying with pre-seeded evidence -> reconcile (kept) ->
	// phase -> NeedsHuman -> reconcile (still kept).
	runBName := "evidence-op-verifying-to-needshuman"
	runB := envtestRunInNamespace(runBName, laneName, namespace)
	if err := envtestClient.Create(ctx, runB); err != nil {
		t.Fatal(err)
	}
	// Pin to Verifying.
	if err := envtestClient.Get(ctx, envtestKey(runBName), runB); err != nil {
		t.Fatal(err)
	}
	runB.Status.Phase = courierv1alpha1.PhaseVerifying
	if err := envtestClient.Status().Update(ctx, runB); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		var secrets corev1.SecretList
		if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runBName)}); err == nil {
			for _, s := range secrets.Items {
				_ = envtestClient.Delete(ctx, &s)
			}
		}
		_ = envtestClient.Delete(ctx, runB)
	})

	// Pre-seed an evidence secret for run B (like coderun_evidence_test.go's testEvidenceSecret
	// but via envtestClient). The secret must have the evidence label and an ownerRef.
	nonceB := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceB); err != nil {
		t.Fatalf("generate nonce B: %v", err)
	}
	secretB := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      evidenceSecretName(runBName, nonceB),
			Namespace: namespace,
			Labels:    map[string]string{EvidenceLabelKey: evidenceLabelValue(runBName)},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         courierv1alpha1.GroupVersion.String(),
				Kind:               "CoderRun",
				Name:               runBName,
				UID:                runB.UID,
				BlockOwnerDeletion: boolPtr(true),
				Controller:         boolPtr(false),
			}},
		},
		Data: map[string][]byte{
			evidenceManifestKey: []byte(`{"schema_version":1,"run":{"name":"` + runBName + `","namespace":"` + namespace + `","runUID":"` + string(runB.UID) + `","podUID":"pod-1"},"workspace":{"baseRepo":"acme/widgets","headRepo":"acme/widgets","branch":"courier/x","startSHA":"00000000","headSHA":"11111111"},"captured_at":"2026-01-01T00:00:00Z","trigger":"terminal","entries":[],"totals":{"files":0,"stored":0,"stored_bytes":0}}`),
			evidenceBundleKey:   []byte{},
		},
	}
	if err := envtestClient.Create(ctx, secretB); err != nil {
		t.Fatalf("create pre-seeded secret for run B: %v", err)
	}

	// Build a reconciler for run B (no Launch needed; we only test evidence lifecycle).
	reconcilerB := &CoderRunReconciler{
		Client:       envtestClient,
		APIReader:    envtestClient,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": &admissionSource{}}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Evidence:     intake,
	}

	reconcileB := func() {
		t.Helper()
		if _, err := reconcilerB.Reconcile(ctx, envtestRequest(runBName)); err != nil {
			t.Fatal(err)
		}
	}

	// First reconcile while Verifying: evidence should be kept (no condition set in Verifying).
	reconcileB()
	var runBUpdated courierv1alpha1.CoderRun
	if err := envtestClient.Get(ctx, envtestKey(runBName), &runBUpdated); err != nil {
		t.Fatal(err)
	}
	condB := apimeta.FindStatusCondition(runBUpdated.Status.Conditions, EvidenceConditionType)
	if condB != nil {
		t.Errorf("run B (Verifying) condition = %+v, want none (reconcileEvidence is no-op in Verifying)", condB)
	}
	// Secret should still exist.
	secretCheck := &corev1.Secret{}
	if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretB.Name}, secretCheck); err != nil {
		t.Errorf("run B (Verifying) secret deleted: %v", err)
	}

	// Transition to NeedsHuman and reconcile again: evidence should still be kept, condition set.
	if err := envtestClient.Get(ctx, envtestKey(runBName), &runBUpdated); err != nil {
		t.Fatal(err)
	}
	runBUpdated.Status.Phase = courierv1alpha1.PhaseNeedsHuman
	if err := envtestClient.Status().Update(ctx, &runBUpdated); err != nil {
		t.Fatal(err)
	}
	reconcileB()
	if err := envtestClient.Get(ctx, envtestKey(runBName), &runBUpdated); err != nil {
		t.Fatal(err)
	}
	condB2 := apimeta.FindStatusCondition(runBUpdated.Status.Conditions, EvidenceConditionType)
	if condB2 == nil || condB2.Status != metav1.ConditionTrue || condB2.Reason != EvidenceReasonCaptured {
		t.Errorf("run B (NeedsHuman) condition = %+v, want True/%s", condB2, EvidenceReasonCaptured)
	}
	secretCheck2 := &corev1.Secret{}
	if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretB.Name}, secretCheck2); err != nil {
		t.Errorf("run B (NeedsHuman) secret deleted: %v", err)
	}
}

// TestEvidenceEnvtestStaleEvidenceFencing verifies the intake fences against
// stale tokens: (a) a recreated same-name run has a new UID and rejects the old
// token; (b) a cross-run token replay is rejected by the HMAC binding.
func TestEvidenceEnvtestStaleEvidenceFencing(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	namespace := "default"
	laneName := "evidence-stale-fencing"

	lane := envtestLaneInNamespace(laneName, namespace)
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}

	// ---- (a) Old UID rejection ----
	runXName := "evidence-stale-run-x"
	runX := envtestRunInNamespace(runXName, laneName, namespace)
	if err := envtestClient.Create(ctx, runX); err != nil {
		t.Fatal(err)
	}
	// Pin to Failed.
	if err := envtestClient.Get(ctx, envtestKey(runXName), runX); err != nil {
		t.Fatal(err)
	}
	runX.Status.Phase = courierv1alpha1.PhaseFailed
	if err := envtestClient.Status().Update(ctx, runX); err != nil {
		t.Fatal(err)
	}
	oldUID := runX.UID

	t.Cleanup(func() {
		var secrets corev1.SecretList
		if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runXName)}); err == nil {
			for _, s := range secrets.Items {
				_ = envtestClient.Delete(ctx, &s)
			}
		}
		_ = envtestClient.Delete(ctx, runX)
	})

	intake := &EvidenceIntake{
		Client:    envtestClient,
		APIReader: envtestClient,
		Scheme:    envtestScheme(),
		Key:       testKey,
		Pod:       executor.PodConfig{},
	}

	// POST with nonce n1 -> 200.
	nonce1 := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonce1); err != nil {
		t.Fatalf("generate nonce1: %v", err)
	}
	mX := sampleIntakeManifest(runX, "content for run X")
	req1, _ := intakeRequest(t, testKey, runX, mX, map[string][]byte{"src/a.go": []byte("content for run X")}, nonce1)
	rec1 := doIntake(intake, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("run X first POST: code = %d, want 200; body = %s", rec1.Code, rec1.Body.String())
	}

	// Verify secret exists.
	secretName1 := evidenceSecretName(runXName, nonce1)
	secret1 := &corev1.Secret{}
	if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName1}, secret1); err != nil {
		t.Fatalf("run X secret %s not found: %v", secretName1, err)
	}

	// Delete the run.
	if err := envtestClient.Delete(ctx, runX); err != nil {
		t.Fatalf("delete run X: %v", err)
	}

	// Recreate SAME name/namespace.
	runXNew := envtestRunInNamespace(runXName, laneName, namespace)
	if err := envtestClient.Create(ctx, runXNew); err != nil {
		t.Fatal(err)
	}
	// Verify new UID differs.
	if runXNew.UID == oldUID {
		t.Fatalf("recreated run UID = %s, want different from old %s", runXNew.UID, oldUID)
	}

	// Pin new run to Failed so intake admits.
	if err := envtestClient.Get(ctx, envtestKey(runXName), runXNew); err != nil {
		t.Fatal(err)
	}
	runXNew.Status.Phase = courierv1alpha1.PhaseFailed
	if err := envtestClient.Status().Update(ctx, runXNew); err != nil {
		t.Fatal(err)
	}

	// Replay the OLD token (bound to old UID) with a FRESH nonce n2 -> 401.
	nonce2 := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonce2); err != nil {
		t.Fatalf("generate nonce2: %v", err)
	}
	// The old token was minted with oldUID. We construct it manually since the
	// helper would use the new run's UID.
	oldToken := executor.EvidenceToken(testKey, namespace, runXName, string(oldUID), nonce2)
	mXNew := sampleIntakeManifest(runXNew, "content for run X new")
	manifestBytes, _ := mXNew.MarshalCanonical()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mp, _ := mw.CreateFormFile("manifest", "manifest.json")
	_, _ = mp.Write(manifestBytes)
	_ = mw.Close()
	req2 := httptest.NewRequest(http.MethodPost, "/intake", &buf)
	req2.Header.Set("Content-Type", mw.FormDataContentType())
	req2.Header.Set("Authorization", "Bearer "+oldToken)
	rec2 := doIntake(intake, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Errorf("replay old token to new run: code = %d, want 401; body = %s", rec2.Code, rec2.Body.String())
	}
	requireNoSecret(t, intake, runXName, namespace, nonce2)

	// ---- (b) Cross-run token replay ----
	// Run Y (target), pin Failed.
	runYName := "evidence-stale-run-y"
	runY := envtestRunInNamespace(runYName, laneName, namespace)
	if err := envtestClient.Create(ctx, runY); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Get(ctx, envtestKey(runYName), runY); err != nil {
		t.Fatal(err)
	}
	runY.Status.Phase = courierv1alpha1.PhaseFailed
	if err := envtestClient.Status().Update(ctx, runY); err != nil {
		t.Fatal(err)
	}

	// Run X (third run, any phase) - create and pin Failed for token minting.
	runX2Name := "evidence-stale-run-x2"
	runX2 := envtestRunInNamespace(runX2Name, laneName, namespace)
	if err := envtestClient.Create(ctx, runX2); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Get(ctx, envtestKey(runX2Name), runX2); err != nil {
		t.Fatal(err)
	}
	runX2.Status.Phase = courierv1alpha1.PhaseFailed
	if err := envtestClient.Status().Update(ctx, runX2); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		for _, name := range []string{runYName, runX2Name} {
			var secrets corev1.SecretList
			if err := envtestClient.List(ctx, &secrets, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(name)}); err == nil {
				for _, s := range secrets.Items {
					_ = envtestClient.Delete(ctx, &s)
				}
			}
			_ = envtestClient.Delete(ctx, &courierv1alpha1.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
		}
	})

	// Mint a token for run X2, but POST a manifest locating run Y.
	nonceY := make([]byte, executor.EvidenceNonceBytes)
	if _, err := rand.Read(nonceY); err != nil {
		t.Fatalf("generate nonceY: %v", err)
	}
	crossToken := executor.EvidenceToken(testKey, namespace, runX2Name, string(runX2.UID), nonceY)
	mY := sampleIntakeManifest(runY, "content for run Y")
	manifestBytesY, _ := mY.MarshalCanonical()
	var bufY bytes.Buffer
	mwY := multipart.NewWriter(&bufY)
	mpY, _ := mwY.CreateFormFile("manifest", "manifest.json")
	_, _ = mpY.Write(manifestBytesY)
	_ = mwY.Close()
	reqY := httptest.NewRequest(http.MethodPost, "/intake", &bufY)
	reqY.Header.Set("Content-Type", mwY.FormDataContentType())
	reqY.Header.Set("Authorization", "Bearer "+crossToken)
	recY := doIntake(intake, reqY)
	if recY.Code != http.StatusUnauthorized {
		t.Errorf("cross-run token replay: code = %d, want 401; body = %s", recY.Code, recY.Body.String())
	}
	// Y's slot (by nonceY) must not exist, and Y's label-list must be empty.
	requireNoSecret(t, intake, runYName, namespace, nonceY)
	var secretsY corev1.SecretList
	if err := envtestClient.List(ctx, &secretsY, client.InNamespace(namespace), client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(runYName)}); err != nil {
		t.Fatal(err)
	}
	if len(secretsY.Items) != 0 {
		t.Errorf("run Y label-list after cross-run replay: found %d secrets, want 0", len(secretsY.Items))
	}

	_ = envtestClient.Delete(ctx, lane)
}
