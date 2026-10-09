// Package controller wires Courier's reconcilers into the manager. In addition to
// the run coordinator, it hosts the evidence intake: the HTTP listener that
// receives a failed run's final workspace snapshot, re-verifies it against the
// same credential set the pod builder injects, rebuilds the bounded bundle, and
// persists it as an owned Secret.
//
// The intake is the most untrusted surface in the system. The POST comes from a
// coordinator pod that has already failed and may be compromised; its manifest
// and tar are attacker-controllable. The intake therefore trusts only: the
// run's live identity (read uncached from the API server), the HMAC of the
// one-time token against that live identity, a fixed allow-list of admissible
// capture phases, the structural shape of the payload, and the credential
// set it re-scans against. Everything else is validated or dropped.
package controller

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/evidence"
	"github.com/misospace/courier/internal/executor"
)

// EvidenceLabelKey selects a run's evidence Secret. The label value is the
// run's name (sanitized, hash-suffixed if long), so a single label selector
// lists exactly one run's evidence.
const EvidenceLabelKey = "courier.misospace.dev/evidence"

// EvidenceConditionType and EvidenceReasonCaptured are the condition the
// reconcile loop derives from the persisted Secret's presence. The intake
// itself never writes a CoderRun condition — that is the loop's job, from the
// world (the Secret), not from memory.
const (
	EvidenceConditionType  = "EvidenceCaptured"
	EvidenceReasonCaptured = "Captured"
)

// The persisted Secret carries the bundle under these two data keys.
const (
	evidenceManifestKey = "manifest.json"
	evidenceBundleKey   = "bundle.tar.gz"
)

// maxRequestBodyBytes bounds the raw multipart body: the manifest budget plus
// the admitted-content cap, the largest a legitimate bundle can be. Bodies
// above it are rejected before any parse, so a huge upload cannot exhaust
// memory.
const maxRequestBodyBytes = int64(evidence.ManifestBudgetBytes + evidence.MaxTotalBytes)

// Sentinels for intake request-shape errors. The handler maps each to a status
// code; their messages are safe to surface (they name no secrets or tokens).
var (
	errIntakeMultipart = errors.New("evidence intake requires a well-formed multipart body with a manifest part")
	errPartTooLarge    = errors.New("evidence intake request part exceeds its size cap")
)

// EvidenceIntake receives a run's final evidence bundle, re-verifies and
// rebuilds it, and persists it as a Secret owned by the run. It is a Runnable
// (it serves an HTTP listener) and a LeaderElectionRunnable.
type EvidenceIntake struct {
	// Client writes the persisted Secret. APIReader performs uncached reads of
	// the run (the world is the source of truth for a persist that is hard to
	// take back) and of the credential Secret set.
	Client    client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme

	// Key is the evidence HMAC key, loaded from the named Secret in main.
	Key []byte

	// Pod is the coordinator pod's resolved configuration. The intake derives
	// the same credential set the pod builder injects (CredentialEnvRefs plus
	// the EnvironmentSecret) so its re-scan matches what the pod saw.
	Pod executor.PodConfig

	// Bind is the listener address (e.g. ":8082"). Empty disables the intake:
	// the runnable returns without binding, so a deployment that does not
	// configure the intake runs no listener at all.
	Bind string
}

// NeedLeaderElection reports that the intake must run on every replica. The
// intake is a network listener, not a reconciler: it has no per-resource
// bookkeeping to serialize, and a leader-election-gated instance would fail to
// bind its port, leaving a hole the other replicas' listeners (all running)
// would have covered.
func (i *EvidenceIntake) NeedLeaderElection() bool { return false }

// Start binds the listener and serves the intake until the context is done,
// then shuts down gracefully. A nil or empty Bind means the intake is disabled
// (returns immediately without binding).
func (i *EvidenceIntake) Start(ctx context.Context) error {
	if i.Bind == "" {
		return nil
	}
	ln, err := net.Listen("tcp", i.Bind)
	if err != nil {
		return fmt.Errorf("evidence intake: listening on %s: %w", i.Bind, err)
	}
	srv := &http.Server{
		// No request wall-clock timeout: a large-but-valid bundle may take time
		// to re-scan, and the body cap already bounds memory, not time.
		ReadHeaderTimeout: 10 * time.Second,
		Handler:           i.Handler(),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
		return nil
	case err := <-errCh:
		_ = ln.Close()
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

// Handler returns the intake's HTTP handler. It is the single entry point for
// every POST; all validation, re-scan, rebuild, and persistence happen inside
// it. It never writes a CoderRun status or condition.
func (i *EvidenceIntake) Handler() http.Handler {
	return http.HandlerFunc(i.handle)
}

// handle runs the intake pipeline in a fixed order. Each stage either proceeds
// to the next or rejects with a mapped status code; no stage can be skipped
// once reached. The cheap structural checks run before the expensive ones, and
// the cryptographic identity check runs only after the manifest is parsed
// (because the HMAC is bound to the run's live identity, which the manifest
// supplies).
//
// Pipeline:
//  1. method            POST only                     (405)
//  2. bearer token      present, structurally valid   (401)
//  3. content-type      multipart/form-data           (415)
//  4. body size         under the cap                 (413)
//  5. manifest part     present, parseable, schema    (400)
//  6. live run read     exists (uncached)             (401/500)
//  7. HMAC verify       against the run's live UID    (401)
//  8. manifest identity RunUID matches the run        (400)
//  9. phase allow-list  capture is admissible         (403)
//
// 10. archive           structural, cross-referenced  (400)
// 11. totals            internally consistent         (400)
// 12. re-scan           credential set, rebuild       (500 if unreadable)
// 13. persist           idempotent create-or-update   (500 on failure)
//
// The handler deliberately holds no request wall-clock timeout: a bundle within
// the size cap that takes long to re-scan is still valid, and the body cap
// bounds memory, not time.
func (i *EvidenceIntake) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeIntakeError(w, http.StatusMethodNotAllowed, "evidence intake accepts POST only")
		return
	}

	// Stage 2. Structural token check: anonymous or malformed requests are
	// rejected before the body is read. The cryptographic check runs later,
	// after the manifest supplies the run's identity.
	token, ok := bearerToken(r)
	if !ok {
		writeIntakeError(w, http.StatusUnauthorized, "evidence intake requires a bearer token")
		return
	}
	nonce, presentedMAC, ok := splitToken(token)
	if !ok {
		writeIntakeError(w, http.StatusUnauthorized, "evidence intake token is malformed")
		return
	}

	// Stage 3. The intake only consumes multipart/form-data; a JSON POST or a
	// raw body is a client bug, not a valid bundle.
	if mediaType(r) != "multipart/form-data" {
		writeIntakeError(w, http.StatusUnsupportedMediaType, "evidence intake requires multipart/form-data")
		return
	}

	// Stage 4. Bound the body before parsing so an oversized upload cannot
	// exhaust memory.
	if r.ContentLength > maxRequestBodyBytes {
		writeIntakeError(w, http.StatusRequestEntityTooLarge, "evidence intake request exceeds the size cap")
		return
	}

	// Stage 5. Read the manifest and archive parts.
	manifestBytes, archiveBytes, err := readIntakeRequest(r)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errPartTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		writeIntakeError(w, code, err.Error())
		return
	}
	manifest, err := decodeManifest(manifestBytes)
	if err != nil {
		writeIntakeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Stage 6. Live read (uncached) of the run. A recreated same-name run has a
	// new UID, so an old token fails the recompute; a deleted run fails the
	// read. The world is the source of truth for a persist that is hard to take
	// back.
	run := &courierv1alpha1.CoderRun{}
	if err := i.APIReader.Get(r.Context(), client.ObjectKey{Namespace: manifest.Run.Namespace, Name: manifest.Run.Name}, run); err != nil {
		if apierrors.IsNotFound(err) {
			writeIntakeError(w, http.StatusUnauthorized, "evidence intake cannot resolve the run")
			return
		}
		writeIntakeError(w, http.StatusInternalServerError, "evidence intake could not read the run")
		return
	}

	// Stage 7. Cryptographic check: recompute the HMAC against the run's LIVE
	// UID and compare to the MAC the token carried.
	mac := hmac.New(sha256.New, i.Key)
	mac.Write([]byte(manifest.Run.Namespace + "/" + manifest.Run.Name + "/" + string(run.UID) + "/" + hex.EncodeToString(nonce)))
	if !hmac.Equal(presentedMAC, mac.Sum(nil)) {
		writeIntakeError(w, http.StatusUnauthorized, "evidence intake token does not verify")
		return
	}

	// Stage 8. The manifest's claimed run identity must match the live run.
	if manifest.Run.RunUID != string(run.UID) {
		writeIntakeError(w, http.StatusBadRequest, "evidence intake manifest identity does not match the run")
		return
	}

	// Stage 9. The capture is only admissible from a phase the allow-list
	// covers. Verifying/AwaitingReview/Done are not admissible, and neither is
	// an unknown or empty phase.
	if !evidencePhaseAdmitted(run.Status.Phase) {
		writeIntakeError(w, http.StatusForbidden, "evidence intake does not accept captures in this phase")
		return
	}

	// Stage 10. The archive must be structurally sound and cross-referenced
	// against the manifest's admitted content.
	members, err := validateArchive(manifest, archiveBytes)
	if err != nil {
		writeIntakeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Stage 11. The manifest's totals must be internally consistent with its
	// (possibly collapsed) entry list.
	if !totalsConsistent(manifest) {
		writeIntakeError(w, http.StatusBadRequest, "evidence intake manifest totals disagree with its entries")
		return
	}

	// Stage 12. The intake re-scans the whole archive against the same
	// credential set the pod builder injects, and rebuilds the bundle rather
	// than persisting the POST verbatim: the executor's scan is untrusted.
	persistManifest, persistArchive, err := i.rescanAndRebuild(r.Context(), run, manifest, members)
	if err != nil {
		writeIntakeError(w, http.StatusInternalServerError, "evidence intake could not verify the credential set")
		return
	}

	// Stage 13. Persist idempotently as a Secret owned by the run.
	if err := i.persist(r.Context(), run, nonce, persistManifest, persistArchive); err != nil {
		writeIntakeError(w, http.StatusInternalServerError, "evidence intake could not persist the bundle")
		return
	}

	writeIntakeOK(w)
}

// persist writes (or rewrites) the run's evidence Secret, idempotently. The
// secret name is stable per run+incarnation, so a retry or a second POST from
// the same incarnation updates in place. An owner reference makes the run's
// deletion delete the secret too. A failed update leaves the previous secret
// in place.
func (i *EvidenceIntake) persist(ctx context.Context, run *courierv1alpha1.CoderRun, nonce, manifestBytes, archive []byte) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      evidenceSecretName(run.Name, nonce),
			Namespace: run.Namespace,
			Labels:    map[string]string{EvidenceLabelKey: evidenceLabelValue(run.Name)},
		},
	}
	secret.OwnerReferences = []metav1.OwnerReference{i.ownerRef(run)}
	secret.Data = map[string][]byte{
		evidenceManifestKey: manifestBytes,
		evidenceBundleKey:   archive,
	}

	if err := i.Client.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("evidence intake: creating secret %s: %w", secret.Name, err)
		}
		// Already exists: retry as an update of the stored object.
		existing := &corev1.Secret{}
		if err := i.APIReader.Get(ctx, client.ObjectKey{Namespace: secret.Namespace, Name: secret.Name}, existing); err != nil {
			return fmt.Errorf("evidence intake: re-reading secret %s: %w", secret.Name, err)
		}
		existing.Data = secret.Data
		existing.Labels = secret.Labels
		existing.OwnerReferences = secret.OwnerReferences
		if err := i.Client.Update(ctx, existing); err != nil {
			return fmt.Errorf("evidence intake: updating secret %s: %w", secret.Name, err)
		}
	}
	return nil
}

// ListEvidenceForRun lists the run's evidence Secrets, selected by label. The
// reconcile loop calls this to derive the evidence-captured condition from the
// world rather than from memory.
func (i *EvidenceIntake) ListEvidenceForRun(ctx context.Context, run *courierv1alpha1.CoderRun) ([]*corev1.Secret, error) {
	list := &corev1.SecretList{}
	if err := i.APIReader.List(ctx, list,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{EvidenceLabelKey: evidenceLabelValue(run.Name)}); err != nil {
		return nil, err
	}
	out := make([]*corev1.Secret, 0, len(list.Items))
	for idx := range list.Items {
		out = append(out, &list.Items[idx])
	}
	return out, nil
}

// DeleteEvidenceForRun deletes every secret the run owns (selected by the
// evidence label). The reconcile loop calls this when a run is deleted or when
// it needs to clear stale evidence.
func (i *EvidenceIntake) DeleteEvidenceForRun(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	secrets, err := i.ListEvidenceForRun(ctx, run)
	if err != nil {
		return err
	}
	for _, s := range secrets {
		if err := i.Client.Delete(ctx, s); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ownerRef builds the run's owner reference for the evidence secret. It is a
// plain owner (not a controller), so a run can own several evidence secrets
// across incarnations without controller-reference conflicts.
func (i *EvidenceIntake) ownerRef(run *courierv1alpha1.CoderRun) metav1.OwnerReference {
	gv := courierv1alpha1.GroupVersion.String()
	kind := "CoderRun"
	if i.Scheme != nil {
		if gvk, err := apiutil.GVKForObject(&courierv1alpha1.CoderRun{}, i.Scheme); err == nil {
			gv = gvk.GroupVersion().String()
			kind = gvk.Kind
		}
	}
	return metav1.OwnerReference{
		APIVersion:         gv,
		Kind:               kind,
		Name:               run.Name,
		UID:                run.UID,
		BlockOwnerDeletion: boolPtr(true),
		Controller:         boolPtr(false),
	}
}

// evidencePhaseAdmitted reports whether a capture is admissible from the given
// phase. The allow-list is the intake's second gate (after authentication): a
// bundle is only accepted from a phase where evidence is meaningful — before
// the run has transitioned to an outcome. Verifying, AwaitingReview, Done, and
// any unknown/empty phase are not admissible.
func evidencePhaseAdmitted(phase courierv1alpha1.Phase) bool {
	switch phase {
	case courierv1alpha1.PhasePending,
		courierv1alpha1.PhaseClaimed,
		courierv1alpha1.PhaseRunning,
		courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseNeedsHuman:
		return true
	default:
		return false
	}
}

// credentialScanner assembles the re-scan credential set: the same per-key
// references the pod builder injects (CredentialEnvRefs), plus the
// EnvironmentSecret's keys (envFrom, where the executor could not tell key
// from value). Each is read uncached. A missing or unreadable credential
// secret is a hard error: the intake cannot prove the bundle is clean, so it
// fails closed. The scanner never logs credential values.
func (i *EvidenceIntake) credentialScanner(ctx context.Context, run *courierv1alpha1.CoderRun) (*evidence.Scanner, error) {
	scanner := evidence.NewScanner()

	// Per-key references: only the named key of each secret is a credential.
	for _, ref := range executor.CredentialEnvRefs(i.Pod) {
		value, err := readSecretKey(ctx, i.APIReader, run.Namespace, ref.Secret, ref.Key)
		if err != nil {
			return nil, fmt.Errorf("evidence intake: reading credential secret %s: %w", ref.Secret, err)
		}
		scanner.RegisterCredentials(map[string]string{ref.Name: value})
	}

	// Environment secret: envFrom, so every key is a candidate.
	if i.Pod.EnvironmentSecret != "" {
		data, err := readSecret(ctx, i.APIReader, run.Namespace, i.Pod.EnvironmentSecret)
		if err != nil {
			return nil, fmt.Errorf("evidence intake: reading environment secret %s: %w", i.Pod.EnvironmentSecret, err)
		}
		values := make(map[string]string, len(data))
		for key, val := range data {
			values[key] = string(val)
		}
		scanner.RegisterCredentials(values)
	}
	return scanner, nil
}

// readSecret reads a whole Secret (all keys).
func readSecret(ctx context.Context, r client.Reader, namespace, name string) (map[string][]byte, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret); err != nil {
		return nil, err
	}
	return secret.Data, nil
}

// readSecretKey reads a single key from a Secret.
func readSecretKey(ctx context.Context, r client.Reader, namespace, name, key string) (string, error) {
	data, err := readSecret(ctx, r, namespace, name)
	if err != nil {
		return "", err
	}
	value, ok := data[key]
	if !ok {
		return "", fmt.Errorf("key %q missing from secret %s", key, name)
	}
	return string(value), nil
}

// rescanAndRebuild re-scans every admitted member against the credential
// scanner, reclassifies matching stored entries to withheld, redacts metadata
// (paths, symlink targets) that contain a credential, drops matching members
// from the archive, re-derives the authoritative totals, and rebuilds the
// canonical manifest and archive. The manifest and archive are returned in the
// form the intake will persist — never the POST as received.
//
// The archive can contain members with no visible manifest entry (a budget
// collapse drops entries but not their admitted content), so the re-scan walks
// the whole archive, not the visible list: the guarantee is that no matching
// content is persisted, regardless of how the manifest describes it.
func (i *EvidenceIntake) rescanAndRebuild(ctx context.Context, run *courierv1alpha1.CoderRun, manifest *evidence.Manifest, members map[string][]byte) ([]byte, []byte, error) {
	scanner, err := i.credentialScanner(ctx, run)
	if err != nil {
		return nil, nil, err
	}

	// Redact metadata across the entry list and reclassify visible stored
	// entries whose admitted content matches.
	for idx := range manifest.Entries {
		e := &manifest.Entries[idx]
		e.Path = scanner.RedactMetadata(e.Path)
		if e.LinkTarget != "" {
			e.LinkTarget = scanner.RedactMetadata(e.LinkTarget)
		}
		if e.Disposition == evidence.DispositionStored {
			if scanner.Matched(string(members[e.StoredPath])) {
				e.Disposition = evidence.DispositionWithheld
				e.StoredPath = ""
			}
		}
	}

	// Drop every matching member; the archive holds only admitted (stored)
	// content, so each dropped member is one stored file now withheld.
	kept := make(map[string][]byte, len(members))
	var dropped int
	var droppedBytes int64
	for name, content := range members {
		if scanner.Matched(string(content)) {
			dropped++
			droppedBytes += int64(len(content))
			continue
		}
		kept[name] = content
	}

	// Re-derive the authoritative totals. The dropped files move from stored
	// to withheld; the full-discovery counts (files, commits, omittedByBudget)
	// are untouched, so "a consumer can trust the totals without uncollapsing"
	// still holds.
	t := manifest.Totals
	t.Stored = subNonNeg(t.Stored, dropped)
	t.Withheld += dropped
	t.StoredBytes = subNonNeg64(t.StoredBytes, droppedBytes)
	manifest.Totals = t

	manifestBytes, err := manifest.MarshalCanonical()
	if err != nil {
		return nil, nil, fmt.Errorf("evidence intake: re-marshaling manifest: %w", err)
	}
	// Re-scan only shrinks the manifest (a stored entry loses its stored
	// path when reclassified withheld); the input was already within budget,
	// so this is defensive. If it somehow grew, fail closed rather than
	// persist an oversized manifest.
	if len(manifestBytes) > evidence.ManifestBudgetBytes {
		return nil, nil, errors.New("evidence intake: recomputed manifest exceeds the manifest budget")
	}
	archive, err := buildArchive(kept)
	if err != nil {
		return nil, nil, fmt.Errorf("evidence intake: rebuilding archive: %w", err)
	}
	return manifestBytes, archive, nil
}

// buildArchive builds a gzip-compressed PAX tar from the given members in a
// deterministic (sorted-by-name) order. It is the mirror of the capture's tar
// builder, so a rebuilt bundle is byte-compatible with the format a consumer
// expects.
func buildArchive(members map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(members[name])),
			Typeflag: tar.TypeReg,
			Format:   tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, err
		}
		if _, err := tw.Write(members[name]); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		_ = gz.Close()
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// validateArchive parses and structurally validates the archive, and
// cross-references it against the manifest's admitted content. It rejects:
//   - a body that is not a valid gzip/tar stream;
//   - a member whose name is not a safe relative path (absolute, or
//     containing a parent-directory segment) — a tar-slip guard;
//   - a member that is not a regular file (symlinks, dirs, devices);
//   - a member over the per-file cap;
//   - an archive whose total content exceeds the total cap;
//   - a manifest that references admitted content absent from the archive
//     (every visible stored entry must have its member present).
//
// It does NOT reject archive members that lack a visible manifest entry: a
// budget collapse can leave admitted content with no visible entry, and the
// intake re-scans the whole archive regardless.
func validateArchive(manifest *evidence.Manifest, data []byte) (map[string][]byte, error) {
	members, err := parseArchive(data)
	if err != nil {
		return nil, err
	}
	for _, e := range manifest.Entries {
		if e.Disposition != evidence.DispositionStored {
			continue
		}
		if e.StoredPath == "" {
			return nil, errors.New("evidence intake: a stored entry has no admitted path")
		}
		if _, ok := members[e.StoredPath]; !ok {
			return nil, fmt.Errorf("evidence intake: manifest references admitted content %q absent from the archive", safeDisplay(e.StoredPath))
		}
	}
	return members, nil
}

// parseArchive decompresses the gzip stream and walks the tar, applying the
// structural guards and the size caps. It returns the map of member name to
// content.
func parseArchive(data []byte) (map[string][]byte, error) {
	members := make(map[string][]byte)
	if len(data) == 0 {
		return members, nil
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("evidence intake: archive is not a valid gzip stream")
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var total int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.New("evidence intake: archive is not a valid tar stream")
		}
		if err := safeArchiveName(hdr.Name); err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("evidence intake: archive member %q is not a regular file", safeDisplay(hdr.Name))
		}
		if hdr.Size > evidence.MaxFileBytes {
			return nil, fmt.Errorf("evidence intake: archive member %q exceeds the per-file cap", safeDisplay(hdr.Name))
		}
		content, err := io.ReadAll(io.LimitReader(tr, evidence.MaxFileBytes+1))
		if err != nil {
			return nil, fmt.Errorf("evidence intake: reading archive member %q", safeDisplay(hdr.Name))
		}
		if int64(len(content)) > evidence.MaxFileBytes {
			return nil, fmt.Errorf("evidence intake: archive member %q exceeds the per-file cap", safeDisplay(hdr.Name))
		}
		total += int64(len(content))
		if total > evidence.MaxTotalBytes {
			return nil, errors.New("evidence intake: archive exceeds the total content cap")
		}
		members[hdr.Name] = content
	}
	return members, nil
}

// safeArchiveName rejects member names that could escape the workspace on
// extraction: absolute paths, or paths containing a parent-directory segment.
// A legitimate capture builds clean relative paths, so any such member is a
// red flag.
func safeArchiveName(name string) error {
	if name == "" {
		return errors.New("evidence intake: archive member has an empty name")
	}
	if path.IsAbs(name) {
		return fmt.Errorf("evidence intake: archive member %q is an absolute path", safeDisplay(name))
	}
	cleaned := path.Clean(name)
	if cleaned != name || strings.HasPrefix(cleaned, "..") {
		return fmt.Errorf("evidence intake: archive member %q is not a safe relative path", safeDisplay(name))
	}
	return nil
}

// safeDisplay returns a short, log-safe representation of a path for error
// messages. It truncates long paths so an oversized member name cannot flood
// the response.
func safeDisplay(name string) string {
	if len(name) > 64 {
		return name[:64] + "…"
	}
	return name
}

// knownDispositions is the full set of per-entry dispositions a capture
// classifies into, as mapped by totalForDisposition.
var knownDispositions = []string{
	evidence.DispositionStored,
	evidence.DispositionWithheld,
	evidence.DispositionSymlink,
	evidence.DispositionDeleted,
	evidence.DispositionOmittedBinary,
	evidence.DispositionOmittedOverLimit,
}

// totalsConsistent checks the manifest's totals against its (possibly
// collapsed) entry list, under the budget-overflow rule: the entry list can
// be budget-collapsed while the totals stay authoritative (full discovery).
//
// No synthetic omitted-budget entry: the list is the full discovery and must
// match the totals exactly. For every disposition the visible count equals
// the matching totals field (stored, withheld, symlinks, deleted,
// omitted-binary, omitted-over-limit), visible file-class entries equal
// totals.files, visible commit-class entries equal totals.commits, and
// totals.omittedByBudget is zero.
//
// Exactly one synthetic omitted-budget entry: the collapse hid entries of an
// unknown disposition mix, so the visible counts are only upper-bounded —
// each visible disposition count must not exceed its total (the synthetic
// entry is not counted toward any of them), and visible files+commits must
// not exceed totals.files+totals.commits. Then totals.omittedByBudget must be
// at least one.
//
// A manifest that fails this is internally inconsistent and is rejected.
func totalsConsistent(m *evidence.Manifest) bool {
	var counts map[string]int
	files, commits, budget := 0, 0, 0
	for _, e := range m.Entries {
		if e.Disposition == evidence.DispositionOmittedBudget {
			budget++
			continue
		}
		counts = addCount(counts, e.Disposition)
		switch {
		case isFileClass(e.Class):
			files++
		case e.Class == evidence.ClassCommit:
			commits++
		}
	}
	if budget > 1 {
		return false
	}
	// The synthetic entry and the totals must agree on whether a collapse
	// happened: one budget entry means at least one entry was collapsed, and
	// a positive omittedByBudget must be represented by a budget entry.
	if budget == 1 && m.Totals.OmittedByBudget < 1 {
		return false
	}
	if budget == 0 && m.Totals.OmittedByBudget != 0 {
		return false
	}
	if budget == 0 {
		// No collapse: the visible list is the full discovery, so every
		// count matches its total exactly, and no unknown disposition may
		// appear.
		for _, d := range knownDispositions {
			if counts[d] != totalForDisposition(d, m.Totals) {
				return false
			}
		}
		for d, c := range counts {
			if c > 0 && totalForDisposition(d, m.Totals) != c {
				return false
			}
		}
		return files == m.Totals.Files && commits == m.Totals.Commits
	}
	// Collapsed: the visible entries are a prefix of the discovery, so only
	// upper bounds can hold.
	for disposition, count := range counts {
		if count > totalForDisposition(disposition, m.Totals) {
			return false
		}
	}
	return files+commits <= m.Totals.Files+m.Totals.Commits
}

// totalForDisposition maps a disposition to its authoritative total field.
func totalForDisposition(disposition string, t evidence.Totals) int {
	switch disposition {
	case evidence.DispositionStored:
		return t.Stored
	case evidence.DispositionWithheld:
		return t.Withheld
	case evidence.DispositionSymlink:
		return t.Symlinks
	case evidence.DispositionDeleted:
		return t.Deleted
	case evidence.DispositionOmittedBinary:
		return t.OmittedBinary
	case evidence.DispositionOmittedOverLimit:
		return t.OmittedOverLimit
	default:
		return 0
	}
}

// isFileClass reports whether a class is a workspace-file class (as opposed to
// the commit class). Files are the modified, added, deleted, renamed, copied,
// and untracked entries; the manifest's totals.files counts all of them.
func isFileClass(class string) bool {
	switch class {
	case evidence.ClassModified, evidence.ClassAdded, evidence.ClassDeleted,
		evidence.ClassRenamed, evidence.ClassCopied, evidence.ClassUntracked:
		return true
	default:
		return false
	}
}

// readIntakeRequest reads the multipart body into its two parts. The manifest
// part is required and bounded by the manifest budget; the archive part is
// optional and bounded by the total-content cap. A part larger than its cap is
// a 413.
func readIntakeRequest(r *http.Request) (manifest, archive []byte, err error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, nil, errIntakeMultipart
	}
	seenManifest := false
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, errIntakeMultipart
		}
		switch part.FormName() {
		case "manifest":
			manifest, err = readPart(part, evidence.ManifestBudgetBytes)
			if err != nil {
				return nil, nil, err
			}
			seenManifest = true
		case "archive":
			archive, err = readPart(part, evidence.MaxTotalBytes)
			if err != nil {
				return nil, nil, err
			}
		default:
			// Unknown parts are ignored; the bundle is exactly manifest+archive.
			_, _ = io.Copy(io.Discard, part)
		}
		_ = part.Close()
	}
	if !seenManifest {
		return nil, nil, errIntakeMultipart
	}
	return manifest, archive, nil
}

// readPart reads a multipart part up to limit bytes, erroring (413) when the
// part is larger than the limit.
func readPart(part io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(part, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errPartTooLarge
	}
	return data, nil
}

// bearerToken extracts the bearer token from the Authorization header.
func bearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || strings.ToLower(scheme) != "bearer" || token == "" {
		return "", false
	}
	return token, true
}

// mediaType returns the request's media type without the parameters, lowercased.
func mediaType(r *http.Request) string {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return ""
	}
	return strings.ToLower(media)
}

// splitToken decodes the base64 token into its nonce and MAC components. The
// nonce is the leading 16 bytes and the MAC the trailing 32 (an HMAC-SHA256
// output). A malformed token is rejected before the cryptographic check.
func splitToken(token string) (nonce, mac []byte, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, nil, false
	}
	if len(raw) != executor.EvidenceNonceBytes+sha256.Size {
		return nil, nil, false
	}
	return raw[:executor.EvidenceNonceBytes], raw[executor.EvidenceNonceBytes:], true
}

// decodeManifest parses and validates the manifest's schema version.
func decodeManifest(data []byte) (*evidence.Manifest, error) {
	var m evidence.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, errors.New("evidence intake: manifest is not valid JSON")
	}
	if m.SchemaVersion != evidence.SchemaVersion {
		return nil, fmt.Errorf("evidence intake: unsupported manifest schema version %d", m.SchemaVersion)
	}
	return &m, nil
}

// evidenceSecretName builds the run's evidence secret name: a stable prefix,
// the run's name, and a hex slice of the incarnation nonce. It fits within the
// 253-character Kubernetes object-name cap by truncating and hash-sufficing the
// run's name (exactly like the pod name), so long run names stay short and
// unique per incarnation.
func evidenceSecretName(runName string, nonce []byte) string {
	const prefix = "courier-evidence-"
	const hashLen = 8
	// A slice of the nonce, hex-encoded: 16 characters from up to 8 bytes.
	nonceHex := hex.EncodeToString(nonce[:min(len(nonce), 8)])
	base := strings.Trim(strings.ToLower(runName), "-")
	// The long form is prefix + base[:K] + "-" + hash + "-" + nonceHex. Solve
	// for the largest K that keeps the whole name <= 253.
	K := 253 - len(prefix) - 1 - hashLen - 1 - len(nonceHex)
	if len(base) > K {
		digest := sha256.Sum256([]byte(runName))
		hash := hex.EncodeToString(digest[:])[:hashLen]
		base = strings.TrimRight(base[:K], "-") + "-" + hash
	}
	return prefix + base + "-" + nonceHex
}

// evidenceLabelValue builds the evidence label value: the run's name, sanitized
// to a DNS-label-safe form. It is truncated and hash-sufficed when long so two
// runs sharing a prefix do not collide in the label, which keeps the label
// selector a correct run selector.
func evidenceLabelValue(runName string) string {
	sanitized := sanitizeLabelValue(runName)
	const max = 63
	if len(sanitized) <= max {
		return sanitized
	}
	digest := sha256.Sum256([]byte(runName))
	hash := hex.EncodeToString(digest[:])[:8]
	cut := max - len(hash) - 1
	return strings.TrimRight(sanitized[:cut], "-_.") + "-" + hash
}

// sanitizeLabelValue lowercases a value and replaces any character outside the
// allowed DNS-label set with a hyphen, trimming the ends. It returns "unknown"
// for an empty result.
func sanitizeLabelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, char := range value {
		switch {
		case (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.':
			b.WriteRune(char)
		default:
			b.WriteByte('-')
		}
	}
	value = strings.Trim(b.String(), "-_.")
	if value == "" {
		return "unknown"
	}
	return value
}

// writeIntakeError writes a JSON error response with the given status code.
// The message is safe to surface: it names no secrets or tokens.
func writeIntakeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// writeIntakeOK writes the 200 response for a successfully persisted bundle.
func writeIntakeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "stored"})
}

// addCount increments a disposition's count in a map.
func addCount(m map[string]int, disposition string) map[string]int {
	if m == nil {
		m = make(map[string]int)
	}
	m[disposition]++
	return m
}

// subNonNeg returns a-b, clamped at zero.
func subNonNeg(a, b int) int {
	if a-b < 0 {
		return 0
	}
	return a - b
}

// subNonNeg64 returns a-b, clamped at zero.
func subNonNeg64(a, b int64) int64 {
	if a-b < 0 {
		return 0
	}
	return a - b
}

// boolPtr returns a pointer to the given bool.
func boolPtr(b bool) *bool { return &b }
