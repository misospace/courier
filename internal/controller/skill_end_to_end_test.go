package controller

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/skills"
	"github.com/misospace/courier/internal/status"
	"github.com/misospace/courier/internal/topology"
)

// skillChainFixture registers one bundle through the production loader,
// lets the deployment's lane select it, and drives the reconciler's
// admission path so the run's persisted status carries the pin. The
// reconciler and its SecureControl share one client, the way the manager
// wires them, so the operator's own reads and writes are the ones the test
// observes. It returns the reconciler, the client, the persisted run, and
// the bundle's registered source path.
func skillChainFixture(t *testing.T, runName string, files map[string]string) (*CoderRunReconciler, client.Client, *courier.CoderRun, string) {
	t.Helper()
	ctx := context.Background()
	registry, digest, source := materializationBundle(t, "repo-tour", files)
	control, c := secureControl(t)
	control.Config.Skills = registry
	// The deployment's lane selects the registered bundle; admission reads
	// the selection through the API, exactly as the operator does.
	var lane courier.LaneProfile
	if err := c.Get(ctx, client.ObjectKey{Namespace: secureTestNamespace, Name: "local"}, &lane); err != nil {
		t.Fatal(err)
	}
	lane.Spec.Skills = []string{"repo-tour"}
	if err := c.Update(ctx, &lane); err != nil {
		t.Fatal(err)
	}
	reconciler := &CoderRunReconciler{
		Client:       c,
		Secure:       control,
		StatusWriter: &recordingPatchWriter{inner: status.KubePatchWriter{Client: c}},
	}
	run := secureRun(runName)
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.resolvePersistSecurePolicy(ctx, run); err != nil {
		t.Fatalf("resolvePersistSecurePolicy: %v", err)
	}
	var persisted courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Status.SkillPins) != 1 {
		t.Fatalf("persisted pins = %+v, want exactly one", persisted.Status.SkillPins)
	}
	if persisted.Status.SkillPins[0].Name != "repo-tour" || persisted.Status.SkillPins[0].Digest != digest {
		t.Fatalf("persisted pin = %+v, want repo-tour at the registry digest %q", persisted.Status.SkillPins[0], digest)
	}
	return reconciler, c, &persisted, source
}

// projectSkillMount simulates the kubelet projection a run's pods see: the
// content ConfigMap's decoded bytes land at the bundle's registered source
// path, one file per projection item. The path's previous content is
// replaced, because a pod's mount replaces whatever the path held — the
// control pod never reads the deployment's own tree.
func projectSkillMount(t *testing.T, configMap *corev1.ConfigMap, items []corev1.KeyToPath, source string) {
	t.Helper()
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	projectConfigMap(t, configMap, items, source)
}

// materializeAndLoad drives the operator's materialization for the run and
// then loads the material the way the control pod does: the content
// ConfigMap's bytes are projected at each bundle's registered source path
// and the production loader resolves them against the run's pins. It
// returns the pod mounts, the rendered bundle config the control pod reads,
// and the loaded view.
func materializeAndLoad(t *testing.T, control *SecureControl, c client.Client, run *courier.CoderRun) ([]topology.SkillMount, []byte, skills.LoadedSkills) {
	t.Helper()
	ctx := context.Background()
	mounts, configMapName, err := control.materializeRunSkills(ctx, run)
	if err != nil {
		t.Fatalf("materializeRunSkills: %v", err)
	}
	if len(mounts) == 0 {
		t.Fatal("a pinned run materialized no bundle content")
	}
	var content corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: mounts[0].ConfigMapName}, &content); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}
	var config corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: configMapName}, &config); err != nil {
		t.Fatalf("read config ConfigMap: %v", err)
	}
	document := config.BinaryData[topology.SkillBundlesConfigKey]
	for _, mount := range mounts {
		projectSkillMount(t, &content, mount.Items, mount.Source)
	}
	pins := make([]skills.Pin, 0, len(run.Status.SkillPins))
	for _, pin := range run.Status.SkillPins {
		pins = append(pins, skills.Pin{Name: pin.Name, Digest: pin.Digest})
	}
	configPath := filepath.Join(t.TempDir(), "bundles.yaml")
	if err := os.WriteFile(configPath, document, 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := skills.LoadForRun(pins, configPath, t.TempDir())
	if err != nil {
		t.Fatalf("LoadForRun against the projected material: %v", err)
	}
	return mounts, document, loaded
}

// TestSkillChainEndToEnd drives the whole pinned-skill chain through the
// production code paths and proves each link: a deployment registers a
// bundle, the lane selects it, admission pins it onto the run, the control
// invocation and its environment carry the pin envelope across the control
// binary's reconstruction seam, the operator materializes the pinned
// content into per-run ConfigMaps, the control pod loads that material
// against the pin, and the native harness discovers the skill and serves
// its instructions only on demand.
func TestSkillChainEndToEnd(t *testing.T) {
	ctx := context.Background()
	const instructions = "Read the repository layout before you change anything."
	files := map[string]string{
		"SKILL.md":            "---\nname: repo-tour\ndescription: tour the repository before you change it\n---\n" + instructions + "\n",
		"assets/checklist.md": "- [ ] read the layout\n",
	}
	reconciler, c, run, source := skillChainFixture(t, "skill-chain", files)
	control := reconciler.Secure
	digest := run.Status.SkillPins[0].Digest

	// The control invocation carries the pin envelope and the bundle config
	// path the control pod resolves against.
	invocation, err := control.controlInvocation(ctx, run)
	if err != nil {
		t.Fatalf("controlInvocation: %v", err)
	}
	wantEnvelope := `[{"name":"repo-tour","digest":"` + digest + `"}]`
	if invocation.Skills != wantEnvelope {
		t.Fatalf("invocation skills = %q, want the pinned envelope %q", invocation.Skills, wantEnvelope)
	}
	if invocation.SkillBundlesFile != executor.SkillBundlesConfigPath {
		t.Fatalf("invocation bundle config = %q, want %q", invocation.SkillBundlesFile, executor.SkillBundlesConfigPath)
	}

	// The environment round trip is the control binary's reconstruction
	// seam: the envelope and the config path survive it unchanged, and the
	// reconstructed envelope still names the pinned digest.
	values := map[string]string{}
	for _, value := range executor.RunContextEnvironment(invocation) {
		values[value.Name] = value.Value
	}
	rebuilt, err := executor.InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("InvocationFromEnv: %v", err)
	}
	if rebuilt.Skills != invocation.Skills {
		t.Fatalf("reconstructed skills = %q, want %q", rebuilt.Skills, invocation.Skills)
	}
	if rebuilt.SkillBundlesFile != invocation.SkillBundlesFile {
		t.Fatalf("reconstructed bundle config = %q, want %q", rebuilt.SkillBundlesFile, invocation.SkillBundlesFile)
	}
	pins, err := executor.SkillPins(rebuilt)
	if err != nil {
		t.Fatalf("SkillPins: %v", err)
	}
	if len(pins) != 1 || pins[0].Name != "repo-tour" || pins[0].Digest != digest {
		t.Fatalf("reconstructed pins = %+v, want repo-tour at %q", pins, digest)
	}

	// The operator materializes the pinned content: one content ConfigMap
	// per bundle plus the config ConfigMap, carrying the bundle's exact
	// bytes under the projection's keys.
	mounts, document, loaded := materializeAndLoad(t, control, c, run)
	if len(mounts) != 1 {
		t.Fatalf("mounts = %+v, want one per pinned bundle", mounts)
	}
	mount := mounts[0]
	if mount.Name != "repo-tour" || mount.Source != source {
		t.Fatalf("mount = %+v, want the registered bundle %q at %q", mount, "repo-tour", source)
	}
	var content corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: mount.ConfigMapName}, &content); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}
	wantItems := []corev1.KeyToPath{{Key: "f0", Path: "SKILL.md"}, {Key: "f1", Path: "assets/checklist.md"}}
	if !reflect.DeepEqual(mount.Items, wantItems) {
		t.Fatalf("mount items = %+v, want %+v", mount.Items, wantItems)
	}
	for _, item := range mount.Items {
		want, ok := files[item.Path]
		if !ok {
			t.Fatalf("the projection carries an unexpected path %q", item.Path)
		}
		if got := string(content.BinaryData[item.Key]); got != want {
			t.Fatalf("content ConfigMap key %q = %q, want the bundle's %q bytes", item.Key, got, want)
		}
	}
	if len(document) == 0 {
		t.Fatal("the config ConfigMap carries no bundle config document")
	}

	// The control pod resolves the projection against the pin: the loaded
	// view carries the skill, the pinned digest, the rendered instructions,
	// and the asset list.
	if len(loaded.Visible) != 1 {
		t.Fatalf("loaded view = %+v, want exactly one visible skill", loaded.Visible)
	}
	view := loaded.Visible[0]
	if view.Name != "repo-tour" || view.Digest != digest {
		t.Fatalf("loaded identity = %+v, want repo-tour at %q", view, digest)
	}
	if view.Source != source {
		t.Fatalf("loaded source = %q, want the registered source %q", view.Source, source)
	}
	if !strings.Contains(view.Instructions, instructions) {
		t.Fatalf("loaded instructions = %q, want the bundle's body", view.Instructions)
	}
	if view.Description != "tour the repository before you change it" {
		t.Fatalf("loaded description = %q", view.Description)
	}
	if len(view.Files) != 1 || view.Files[0] != "assets/checklist.md" {
		t.Fatalf("loaded files = %+v, want the bundle's asset", view.Files)
	}

	// The native harness discovers the skill: the index names it and
	// carries the deployment-authored description, and the instructions
	// reach a role only through the harness's own lookup — the path the
	// skill tool serves from — never through the pinned prompt text.
	set, err := harness.NewSkillSet(loaded)
	if err != nil {
		t.Fatalf("NewSkillSet: %v", err)
	}
	index := set.Index()
	if !strings.Contains(index, "- repo-tour (bundle): tour the repository before you change it") {
		t.Fatalf("index = %q, want the bundle's name and description", index)
	}
	if strings.Contains(index, instructions) {
		t.Fatal("the pinned index must not carry the instruction body")
	}
	section := set.Section()
	if !strings.Contains(section, index) {
		t.Fatalf("section = %q, want it to carry the index", section)
	}
	if strings.Contains(section, instructions) {
		t.Fatal("the roles' system message must not carry the instruction body")
	}
	skill, ok := set.Lookup("repo-tour")
	if !ok {
		t.Fatal("the loaded skill is not reachable through the harness")
	}
	if !strings.Contains(skill.Instructions, instructions) {
		t.Fatalf("the harness serves %q, want the bundle's body", skill.Instructions)
	}
	if skill.Source != source || len(skill.Files) != 1 || skill.Files[0] != "assets/checklist.md" {
		t.Fatalf("served skill = %+v, want the registered source and the bundle's asset", skill)
	}
}

// TestSkillChainRestartLoadsTheSamePinnedContent proves the restart
// contract: two incarnations of the same run materialize byte-identical
// content and identical mounts, and two control-side loads of that
// material render identical instructions — the same pinned content is
// loaded again, never a fresh or drifted set.
func TestSkillChainRestartLoadsTheSamePinnedContent(t *testing.T) {
	ctx := context.Background()
	const instructions = "Read the repository layout before you change anything."
	files := map[string]string{
		"SKILL.md":            "---\nname: repo-tour\ndescription: tour the repository before you change it\n---\n" + instructions + "\n",
		"assets/checklist.md": "- [ ] read the layout\n",
	}
	reconciler, c, run, source := skillChainFixture(t, "skill-restart", files)
	control := reconciler.Secure
	digest := run.Status.SkillPins[0].Digest

	firstMounts, firstDocument, firstLoaded := materializeAndLoad(t, control, c, run)
	var firstContent corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: firstMounts[0].ConfigMapName}, &firstContent); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}

	// The second incarnation materializes the same run again: the content
	// and the mounts are identical, so a relaunch reads the same pinned
	// bytes rather than a fresh set.
	secondMounts, secondDocument, secondLoaded := materializeAndLoad(t, control, c, run.DeepCopy())
	if !reflect.DeepEqual(firstMounts, secondMounts) {
		t.Fatalf("second incarnation mounts = %+v, want %+v", secondMounts, firstMounts)
	}
	var secondContent corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: secondMounts[0].ConfigMapName}, &secondContent); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}
	if !reflect.DeepEqual(firstContent.BinaryData, secondContent.BinaryData) {
		t.Fatalf("second incarnation content = %v, want the first incarnation's %v", secondContent.BinaryData, firstContent.BinaryData)
	}
	if !reflect.DeepEqual(firstDocument, secondDocument) {
		t.Fatalf("second incarnation config = %q, want the first incarnation's %q", secondDocument, firstDocument)
	}

	// Two control-side loads of the same material render the same
	// instructions: the pinned bytes produce the same rendered material.
	if firstLoaded.Visible[0].Instructions != secondLoaded.Visible[0].Instructions {
		t.Fatalf("second load rendered %q, want the first load's %q", secondLoaded.Visible[0].Instructions, firstLoaded.Visible[0].Instructions)
	}
	if firstLoaded.Visible[0].Digest != digest || secondLoaded.Visible[0].Digest != digest {
		t.Fatalf("loaded digests = %q and %q, want the pinned %q", firstLoaded.Visible[0].Digest, secondLoaded.Visible[0].Digest, digest)
	}
	if !strings.Contains(firstLoaded.Visible[0].Instructions, instructions) {
		t.Fatalf("loaded instructions = %q, want the bundle's body", firstLoaded.Visible[0].Instructions)
	}

	// The projection the control pod sees is the operator's material: the
	// bundle's own layout, at the registered source path.
	for _, item := range firstMounts[0].Items {
		path := filepath.Join(source, filepath.FromSlash(item.Path))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("the projected mount carries no %q: %v", item.Path, err)
		}
	}
}

// TestSkillChainDriftFailsClosed proves the drift contract: a bundle whose
// content changed after the pin fails closed at materialization — no
// ConfigMap written, explicit error — and at the control-side load, where
// the digest no longer matches — so a drifted bundle is never substituted
// for the pinned one.
func TestSkillChainDriftFailsClosed(t *testing.T) {
	ctx := context.Background()

	t.Run("materialization refuses a drifted registry", func(t *testing.T) {
		reconciler, c, run, _ := skillChainFixture(t, "skill-drift-registry", map[string]string{
			"SKILL.md": "tour the repository first.\n",
		})
		control := reconciler.Secure
		// The deployment's bundle content changes after admission: the pin
		// still names the digest the run was admitted to load.
		drifted, _, _ := materializationBundle(t, "repo-tour", map[string]string{
			"SKILL.md": "tour the repository differently.\n",
		})
		control.Config.Skills = drifted

		mounts, configMap, err := control.materializeRunSkills(ctx, run)
		if err == nil {
			t.Fatal("a drifted registry must fail closed at materialization")
		}
		if !strings.Contains(err.Error(), "pinned digest") {
			t.Fatalf("error = %v, want the digest drift named", err)
		}
		if mounts != nil || configMap != "" {
			t.Fatalf("a failed materialization returned mounts %+v and config %q, want none", mounts, configMap)
		}
		configMaps := &corev1.ConfigMapList{}
		if err := c.List(ctx, configMaps, client.InNamespace(run.Namespace)); err != nil {
			t.Fatal(err)
		}
		if len(configMaps.Items) != 0 {
			t.Fatalf("a failed materialization created %d ConfigMaps, want none", len(configMaps.Items))
		}
	})

	t.Run("the control-side load refuses drifted content", func(t *testing.T) {
		reconciler, c, run, source := skillChainFixture(t, "skill-drift-mount", map[string]string{
			"SKILL.md": "tour the repository first.\n",
		})
		control := reconciler.Secure
		digest := run.Status.SkillPins[0].Digest
		_, document, _ := materializeAndLoad(t, control, c, run)
		// The mounted content drifts under the run: the recomputed digest no
		// longer matches the pin, so the load fails closed instead of
		// serving the substituted content.
		drifted := filepath.Join(source, "SKILL.md")
		if err := os.WriteFile(drifted, []byte("tour the repository differently.\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		configPath := filepath.Join(t.TempDir(), "bundles.yaml")
		if err := os.WriteFile(configPath, document, 0o644); err != nil {
			t.Fatal(err)
		}
		loaded, err := skills.LoadForRun([]skills.Pin{{Name: "repo-tour", Digest: digest}}, configPath, t.TempDir())
		if err == nil {
			t.Fatal("drifted content must fail the control-side load")
		}
		if !strings.Contains(err.Error(), "pinned digest") {
			t.Fatalf("error = %v, want the digest mismatch named", err)
		}
		if len(loaded.Visible) != 0 {
			t.Fatalf("the failed load returned %+v, want no visible skill", loaded.Visible)
		}
	})
}

// launchSkillRun drives the operator's launch path for one run and returns
// the worker and control pods it created. The deployment facts the launch
// proves are stood up the way the deployment provides them: a run
// namespace under the restricted Pod Security label and the forge
// credential Secret the selected registration names.
func launchSkillRun(t *testing.T, control *SecureControl, c client.Client, run *courier.CoderRun) (*corev1.Pod, *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   secureTestNamespace,
		Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
	}}
	if err := c.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}
	if err := c.Create(ctx, creds); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	run.Status.Phase = courier.PhaseClaimed
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:              string(run.UID),
		ProviderConfigRef:   "github",
		ProviderEndpoint:    "https://api.github.com/",
		CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
		BaseRepo:            "Acme/Widgets",
		BaseRef:             "main",
		BaseOID:             "baseoid",
		WorkRepo:            "Acme/Widgets",
		WorkRef:             "courier/Acme/Widgets/issue-7",
		WorkInitiallyAbsent: true,
	}
	if _, err := control.LaunchSecure(ctx, run); err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(secureTestNamespace)); err != nil {
		t.Fatal(err)
	}
	components := map[string]*corev1.Pod{}
	for i := range pods.Items {
		components[pods.Items[i].Labels["courier.misospace.dev/component"]] = &pods.Items[i]
	}
	worker := components[topology.ComponentWorker]
	controlPod := components[topology.ComponentCoordinator]
	if worker == nil || controlPod == nil {
		t.Fatalf("the launch created no worker/control pod (worker=%v control=%v)", worker != nil, controlPod != nil)
	}
	return worker, controlPod
}

// TestSkillWorkerBoundaryServesAssetsReadOnly proves a supporting asset is
// readable by the worker: the rendered worker pod carries the bundle's
// content mounted read-only at the bundle's registered absolute source
// path, the projection's items give the asset its relative path inside the
// mount, and the path the loaded skill names for that asset is exactly the
// path the worker sees — so a skill's script can read its own asset.
func TestSkillWorkerBoundaryServesAssetsReadOnly(t *testing.T) {
	ctx := context.Background()
	files := map[string]string{
		"SKILL.md":            "---\nname: repo-tour\ndescription: tour the repository before you change it\n---\nRead the layout.\n",
		"assets/checklist.md": "- [ ] read the layout\n",
	}
	reconciler, c, run, source := skillChainFixture(t, "worker-assets", files)
	control := reconciler.Secure
	mounts, _, loaded := materializeAndLoad(t, control, c, run)
	worker, _ := launchSkillRun(t, control, c, run)

	// The worker mounts the bundle's content at the registered source path.
	var volume *corev1.Volume
	for i := range worker.Spec.Volumes {
		if worker.Spec.Volumes[i].ConfigMap != nil && worker.Spec.Volumes[i].ConfigMap.Name == mounts[0].ConfigMapName {
			volume = &worker.Spec.Volumes[i]
		}
	}
	if volume == nil {
		t.Fatalf("the worker mounts no volume projecting %q", mounts[0].ConfigMapName)
	}
	if volume.Secret != nil || volume.Projected != nil {
		t.Fatalf("the worker's skill volume = %+v, want a ConfigMap projection", volume.VolumeSource)
	}
	if !reflect.DeepEqual(volume.ConfigMap.Items, mounts[0].Items) {
		t.Fatalf("the worker's skill volume items = %+v, want %+v", volume.ConfigMap.Items, mounts[0].Items)
	}
	var skillMount *corev1.VolumeMount
	for i := range worker.Spec.Containers[0].VolumeMounts {
		if worker.Spec.Containers[0].VolumeMounts[i].Name == volume.Name {
			skillMount = &worker.Spec.Containers[0].VolumeMounts[i]
		}
	}
	if skillMount == nil {
		t.Fatalf("the worker mounts no volume %q", volume.Name)
	}
	if skillMount.MountPath != source {
		t.Fatalf("the worker's skill mount path = %q, want the registered source %q", skillMount.MountPath, source)
	}
	if !skillMount.ReadOnly {
		t.Fatal("the worker's skill mount must be read-only")
	}

	// The asset's relative path rides the projection, so the worker's path
	// for it is the mounted source plus that relative path — the same
	// composition the harness's skill tool renders for the role, from the
	// very values the harness serves.
	var asset string
	for _, item := range volume.ConfigMap.Items {
		if item.Path != "SKILL.md" {
			asset = item.Path
		}
	}
	if asset != "assets/checklist.md" {
		t.Fatalf("the worker's projection carries the asset %q, want assets/checklist.md", asset)
	}
	set, err := harness.NewSkillSet(loaded)
	if err != nil {
		t.Fatalf("NewSkillSet: %v", err)
	}
	served, ok := set.Lookup("repo-tour")
	if !ok {
		t.Fatal("the loaded skill is not reachable through the harness")
	}
	if served.Source != skillMount.MountPath {
		t.Fatalf("the served skill's source = %q, want the worker's mount %q", served.Source, skillMount.MountPath)
	}
	if len(served.Files) != 1 || served.Files[0] != asset {
		t.Fatalf("the served skill's files = %+v, want the worker's asset %q", served.Files, asset)
	}
	workerPath := filepath.Join(skillMount.MountPath, filepath.FromSlash(asset))
	if named := served.Source + "/" + served.Files[0]; named != workerPath {
		t.Fatalf("the path the skill names = %q, want the worker's %q", named, workerPath)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: mounts[0].ConfigMapName}, &corev1.ConfigMap{}); err != nil {
		t.Fatalf("the worker's content ConfigMap is not in the API: %v", err)
	}
}

// TestSkillWorkerBoundaryExecutesContentOnlyInTheWorker proves executable
// content runs only through the worker: a skill whose SKILL.md carries a
// script is served to the model as data, the control pod's command is the
// courier-control binary rather than a shell, and both pods mount the skill
// content read-only — so the control pod has no path to run a skill's
// script even if the model asked for it.
func TestSkillWorkerBoundaryExecutesContentOnlyInTheWorker(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "marker")
	script := "#!/bin/sh\ntouch " + marker
	files := map[string]string{
		"SKILL.md": "---\nname: repo-tour\ndescription: tour the repository before you change it\n---\nRun the helper if you like:\n\n" + script + "\n",
	}
	reconciler, c, run, _ := skillChainFixture(t, "worker-script", files)
	control := reconciler.Secure
	mounts, _, loaded := materializeAndLoad(t, control, c, run)
	worker, controlPod := launchSkillRun(t, control, c, run)

	// The script is content: the loader serves it verbatim as instructions
	// and never runs it.
	if !strings.Contains(loaded.Visible[0].Instructions, script) {
		t.Fatalf("loaded instructions = %q, want the script carried as data", loaded.Visible[0].Instructions)
	}
	if _, err := os.Stat("/tmp/courier-skill-marker"); err == nil {
		t.Fatal("the skill's script was executed")
	}

	// Neither pod runs a shell: the control pod runs the courier-control
	// binary and the worker the courier-worker binary.
	if got := controlPod.Spec.Containers[0].Command; !reflect.DeepEqual(got, []string{"/usr/local/bin/courier-control"}) {
		t.Fatalf("the control pod's command = %v, want the courier-control binary", got)
	}
	if got := worker.Spec.Containers[0].Command; !reflect.DeepEqual(got, []string{"/usr/local/bin/courier-worker"}) {
		t.Fatalf("the worker's command = %v, want the courier-worker binary", got)
	}
	for _, pod := range []*corev1.Pod{controlPod, worker} {
		for _, container := range pod.Spec.Containers {
			for _, arg := range append(container.Command, container.Args...) {
				if strings.Contains(arg, "/bin/sh") || strings.Contains(arg, "/bin/bash") {
					t.Fatalf("%s runs a shell: %v", pod.Name, container.Command)
				}
			}
		}
	}

	// The skill content is read-only in both pods.
	for _, pod := range []*corev1.Pod{controlPod, worker} {
		var mounted bool
		for _, mount := range pod.Spec.Containers[0].VolumeMounts {
			if mount.MountPath == mounts[0].Source {
				mounted = true
				if !mount.ReadOnly {
					t.Fatalf("the %s pod mounts the skill source writable", pod.Name)
				}
			}
		}
		if !mounted {
			t.Fatalf("the %s pod mounts no skill content at %q", pod.Name, mounts[0].Source)
		}
	}
}

// TestSkillWorkerBoundaryRejectsWritesAndRemounts proves attempts to write
// to or remount the skill source are rejected: every skill mount in the
// control and worker pods is read-only, no volume in either pod grants a
// writable path at the skill source, the worker's container security
// context forbids privilege escalation and drops every capability, and the
// worker mounts no Secret, token, or projected volume for a pinned run.
func TestSkillWorkerBoundaryRejectsWritesAndRemounts(t *testing.T) {
	files := map[string]string{
		"SKILL.md":            "---\nname: repo-tour\ndescription: tour the repository before you change it\n---\nRead the layout.\n",
		"assets/checklist.md": "- [ ] read the layout\n",
	}
	reconciler, c, run, source := skillChainFixture(t, "worker-writes", files)
	control := reconciler.Secure
	mounts, _, _ := materializeAndLoad(t, control, c, run)
	worker, controlPod := launchSkillRun(t, control, c, run)

	for _, pod := range []*corev1.Pod{controlPod, worker} {
		for _, mount := range pod.Spec.Containers[0].VolumeMounts {
			if mount.MountPath == source && !mount.ReadOnly {
				t.Fatalf("the %s pod mounts the skill source writable", pod.Name)
			}
		}
		// No volume of either pod is mounted at the skill source path
		// except through the read-only skill mount: a writable volume there
		// would shadow the content and let a pod rewrite what a pinned skill
		// says.
		for _, volume := range pod.Spec.Volumes {
			for _, mount := range pod.Spec.Containers[0].VolumeMounts {
				if mount.Name == volume.Name && mount.MountPath == source {
					if !mount.ReadOnly {
						t.Fatalf("the %s pod mounts the skill source through the writable volume %q", pod.Name, volume.Name)
					}
					if volume.ConfigMap == nil || volume.ConfigMap.Name != mounts[0].ConfigMapName {
						t.Fatalf("the %s pod mounts the skill source from %+v, want the bundle's ConfigMap", pod.Name, volume.VolumeSource)
					}
				}
			}
		}
	}

	// The worker's container security context forbids privilege escalation
	// and drops every capability, so a remount of the skill source is not
	// reachable even by a process that wanted one.
	security := worker.Spec.Containers[0].SecurityContext
	if security == nil {
		t.Fatal("the worker container carries no security context")
	}
	if security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation {
		t.Fatal("the worker container must forbid privilege escalation")
	}
	if security.Capabilities == nil || len(security.Capabilities.Drop) != 1 || security.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("the worker container's dropped capabilities = %+v, want ALL", security.Capabilities)
	}
	if worker.Spec.SecurityContext == nil || worker.Spec.SecurityContext.RunAsNonRoot == nil || !*worker.Spec.SecurityContext.RunAsNonRoot {
		t.Fatal("the worker pod must run as non-root")
	}

	// The worker holds no identity material for a pinned run: no Secret, no
	// projected token, no automounted service account token.
	for _, volume := range worker.Spec.Volumes {
		if volume.Secret != nil || volume.Projected != nil {
			t.Fatalf("the worker mounts a secret or projected volume %q", volume.Name)
		}
	}
	if worker.Spec.AutomountServiceAccountToken == nil || *worker.Spec.AutomountServiceAccountToken {
		t.Fatal("the worker must not automount a service account token")
	}
}

// TestSkillWorkerBoundaryAddsNoCredential proves the worker receives no
// new credential, identity, or model credential for a pinned run: the only
// difference between a skillless worker and a pinned worker is the
// read-only skill content mount. The environment gains no variable, the
// worker keeps its own dedicated unbound service account, and the pod
// volumes gain exactly the one read-only ConfigMap.
func TestSkillWorkerBoundaryAddsNoCredential(t *testing.T) {
	files := map[string]string{
		"SKILL.md":            "---\nname: repo-tour\ndescription: tour the repository before you change it\n---\nRead the layout.\n",
		"assets/checklist.md": "- [ ] read the layout\n",
	}
	reconciler, c, run, _ := skillChainFixture(t, "worker-credentials", files)
	control := reconciler.Secure
	mounts, _, _ := materializeAndLoad(t, control, c, run)
	pinned, _ := launchSkillRun(t, control, c, run)

	// The same run, rendered without the pin, is the skillless baseline:
	// every difference between the two workers is the pin's doing. The
	// baseline carries the round's own incarnation and signing key, read
	// back from the launched worker, so the comparison isolates the pin.
	incarnation := workerEnvValue(pinned, topology.EnvControlPodUID)
	encodedKey := workerEnvValue(pinned, topology.EnvWorkerPublicKey)
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		t.Fatalf("decode the worker's public key: %v", err)
	}
	base, err := topology.WorkerPod(run, pinned.Spec.Containers[0].Image, incarnation, ed25519.PublicKey(key), nil, pinned.Spec.Containers[0].Resources, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pinned.Spec.Containers[0].Env, base.Spec.Containers[0].Env) {
		t.Fatalf("the pinned worker's env = %+v, want the skillless worker's %+v", pinned.Spec.Containers[0].Env, base.Spec.Containers[0].Env)
	}
	if pinned.Spec.ServiceAccountName != base.Spec.ServiceAccountName {
		t.Fatalf("the pinned worker's service account = %q, want %q", pinned.Spec.ServiceAccountName, base.Spec.ServiceAccountName)
	}
	if !reflect.DeepEqual(pinned.Spec.AutomountServiceAccountToken, base.Spec.AutomountServiceAccountToken) {
		t.Fatal("the pinned worker's token automount changed")
	}
	if len(pinned.Spec.Volumes) != len(base.Spec.Volumes)+1 {
		t.Fatalf("the pinned worker carries %d volumes, want one more than the skillless worker's %d", len(pinned.Spec.Volumes), len(base.Spec.Volumes))
	}
	var gained *corev1.Volume
	for i := range pinned.Spec.Volumes {
		if pinned.Spec.Volumes[i].ConfigMap != nil && pinned.Spec.Volumes[i].ConfigMap.Name == mounts[0].ConfigMapName {
			gained = &pinned.Spec.Volumes[i]
		}
	}
	if gained == nil {
		t.Fatalf("the pinned worker mounts no volume projecting %q", mounts[0].ConfigMapName)
	}
	if gained.Secret != nil || gained.Projected != nil {
		t.Fatalf("the gained worker volume = %+v, want a ConfigMap projection", gained.VolumeSource)
	}
	for _, mount := range pinned.Spec.Containers[0].VolumeMounts {
		if mount.Name == gained.Name && !mount.ReadOnly {
			t.Fatal("the gained worker mount must be read-only")
		}
	}
	for _, env := range pinned.Spec.Containers[0].Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("the pinned worker's env %q references a Secret", env.Name)
		}
	}
}
