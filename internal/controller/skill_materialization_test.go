package controller

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/skills"
	"github.com/misospace/courier/internal/topology"
)

// materializationBundle registers one bundle through the production loader
// and returns the registry, the bundle's content digest, and the source path
// the deployment registered. The digest is the loader's own content identity,
// so the materialization's bytes can be compared against it.
func materializationBundle(t *testing.T, name string, files map[string]string) (*skills.Registry, string, string) {
	t.Helper()
	content := make(map[string][]byte, len(files))
	for path, body := range files {
		content[path] = []byte(body)
	}
	return materializationBundleBytes(t, name, content)
}

// materializationBundleBytes is materializationBundle over raw bytes: a
// bundle file is arbitrary content, so the materialization must carry bytes
// that are not valid UTF-8 exactly as they are.
func materializationBundleBytes(t *testing.T, name string, files map[string][]byte) (*skills.Registry, string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, name)
	for path, body := range files {
		full := filepath.Join(source, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	config := fmt.Sprintf("bundles:\n  - name: %s\n    source: %s\n", name, source)
	path := filepath.Join(root, "bundles.yaml")
	if err := os.WriteFile(path, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	registry, err := skills.Load(path)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	bundle, ok := registry.Lookup(name)
	if !ok {
		t.Fatalf("bundle %q did not register", name)
	}
	return registry, bundle.Digest, source
}

// projectConfigMap writes a materialized ConfigMap's content into a
// directory the way kubelet projects it: every key lands at the relative path
// its key-to-path item names. The projection is what the control pod reads,
// so the test resolves the bundle from it rather than from the registry's own
// tree.
func projectConfigMap(t *testing.T, configMap *corev1.ConfigMap, items []corev1.KeyToPath, root string) {
	t.Helper()
	for _, item := range items {
		path := filepath.Join(root, filepath.FromSlash(item.Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		content, ok := configMap.BinaryData[item.Key]
		if !ok {
			t.Fatalf("config map %s carries no binary key %q", configMap.Name, item.Key)
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestMaterializeRunSkillsCreatesContentAndConfigConfigMaps pins the whole
// materialization: the content ConfigMap carries the bundle's exact bytes
// under the encoded data keys and maps them to the bundle's own relative
// paths, and the config ConfigMap's bytes parse back through the loader and
// resolve the materialized bundle to the registry's digest — the property
// that lets the run's pinned digest verify inside the control pod.
func TestMaterializeRunSkillsCreatesContentAndConfigConfigMaps(t *testing.T) {
	files := map[string]string{
		"SKILL.md":            "tour the repository first.",
		"assets/checklist.md": "check the checklist.",
	}
	registry, digest, source := materializationBundle(t, "repo-tour", files)
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("materialize")
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}

	mounts, configMap, err := control.materializeRunSkills(context.Background(), run)
	if err != nil {
		t.Fatalf("materializeRunSkills: %v", err)
	}
	if configMap != topology.SkillBundlesConfigMapName(run.Name) {
		t.Fatalf("config ConfigMap name = %q, want %q", configMap, topology.SkillBundlesConfigMapName(run.Name))
	}
	if len(mounts) != 1 {
		t.Fatalf("mounts = %+v, want one per pinned bundle", mounts)
	}
	mount := mounts[0]
	if mount.Name != "repo-tour" || mount.Source != source {
		t.Fatalf("mount = %+v, want the registered bundle %q at %q", mount, "repo-tour", source)
	}
	wantItems := []corev1.KeyToPath{
		{Key: "f0", Path: "SKILL.md"},
		{Key: "f1", Path: "assets/checklist.md"},
	}
	if !reflect.DeepEqual(mount.Items, wantItems) {
		t.Fatalf("mount items = %+v, want %+v", mount.Items, wantItems)
	}
	var content corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: mount.ConfigMapName}, &content); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}
	for _, item := range wantItems {
		if got := string(content.BinaryData[item.Key]); got != files[item.Path] {
			t.Fatalf("content ConfigMap key %q = %q, want the bundle's %q bytes", item.Key, got, files[item.Path])
		}
	}
	var config corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: configMap}, &config); err != nil {
		t.Fatalf("read config ConfigMap: %v", err)
	}
	document := config.BinaryData[topology.SkillBundlesConfigKey]
	if len(document) == 0 {
		t.Fatal("config ConfigMap carries no bundle config document")
	}
	if _, err := skills.Parse(document); err != nil {
		t.Fatalf("the rendered config does not parse back: %v", err)
	}
	// The projection the pods mount carries only what the ConfigMap
	// carries, so the bundle the control pod resolves comes from the
	// operator's bytes.
	projection := t.TempDir()
	projectConfigMap(t, &content, mount.Items, projection)
	configPath := filepath.Join(t.TempDir(), "bundles.yaml")
	if err := os.WriteFile(configPath, document, 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := skills.LoadForRun([]skills.Pin{{Name: "repo-tour", Digest: digest}}, configPath, t.TempDir())
	if err != nil {
		t.Fatalf("the projected bundle does not verify against the pinned digest: %v", err)
	}
	if len(loaded.Visible) != 1 || loaded.Visible[0].Digest != digest {
		t.Fatalf("loaded view = %+v, want the pinned bundle at digest %q", loaded.Visible, digest)
	}
}

// TestMaterializeRunSkillsCarriesExactBytes pins the byte-exactness of the
// materialization: a bundle file that is not valid UTF-8 reaches the
// ConfigMap's binary data unchanged, so the mounted content is the bundle
// the pin named rather than a lossy rendering of it.
func TestMaterializeRunSkillsCarriesExactBytes(t *testing.T) {
	blob := []byte{0xff, 0xfe, 0x00, 0x80, 0x01, 'a'}
	registry, digest, _ := materializationBundleBytes(t, "repo-tour", map[string][]byte{
		"SKILL.md":        []byte("tour the repository first."),
		"assets/blob.bin": blob,
	})
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("exact-bytes")
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}

	mounts, _, err := control.materializeRunSkills(context.Background(), run)
	if err != nil {
		t.Fatalf("materializeRunSkills: %v", err)
	}
	var content corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: mounts[0].ConfigMapName}, &content); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}
	if len(content.Data) != 0 {
		t.Fatalf("the content ConfigMap carries string data %v, want the bytes in binary data only", content.Data)
	}
	var carried []byte
	for _, item := range mounts[0].Items {
		if item.Path == "assets/blob.bin" {
			carried = content.BinaryData[item.Key]
		}
	}
	if !bytes.Equal(carried, blob) {
		t.Fatalf("the materialized blob = %v, want the bundle's exact bytes %v", carried, blob)
	}
}

// TestMaterializeRunSkillsRoundTripsBytesThroughTheAPIServer pins the round
// trip against a real API server: a ConfigMap's binary data is base64
// encoded on the wire and decoded on read, so the bundle's exact bytes —
// including a file that is not valid UTF-8 — must come back unchanged.
func TestMaterializeRunSkillsRoundTripsBytesThroughTheAPIServer(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	blob := []byte{0xff, 0xfe, 0x00, 0x80, 0x01, 'a'}
	registry, digest, _ := materializationBundleBytes(t, "repo-tour", map[string][]byte{
		"SKILL.md":        []byte("tour the repository first."),
		"assets/blob.bin": blob,
	})
	run := &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "skill-materialization-envtest", Namespace: "default", UID: types.UID("skill-materialization-envtest-uid")},
		Spec: courier.CoderRunSpec{
			Mode:       courier.ModeResolveIssue,
			Source:     "manual",
			Repo:       "acme/widgets",
			Ref:        14,
			Lane:       "local",
			WorkItemID: "item-1",
		},
	}
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}
	control := &SecureControl{
		Client:    envtestClient,
		APIReader: envtestClient,
		Config:    SecureConfig{Skills: registry},
	}
	mounts, _, err := control.materializeRunSkills(ctx, run)
	if err != nil {
		t.Fatalf("materializeRunSkills: %v", err)
	}
	var content corev1.ConfigMap
	if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: mounts[0].ConfigMapName}, &content); err != nil {
		t.Fatalf("read content ConfigMap: %v", err)
	}
	var carried []byte
	for _, item := range mounts[0].Items {
		if item.Path == "assets/blob.bin" {
			carried = content.BinaryData[item.Key]
		}
	}
	if !bytes.Equal(carried, blob) {
		t.Fatalf("the API server returned the blob %v, want the bundle's exact bytes %v", carried, blob)
	}
}

// TestMaterializeRunSkillsConvergesAnExistingConfigMap pins the relaunch
// contract: materializing over a ConfigMap that is already there converges
// it onto the current content instead of failing, so a relaunch never wedges
// on the ConfigMap its own previous round created.
func TestMaterializeRunSkillsConvergesAnExistingConfigMap(t *testing.T) {
	files := map[string]string{"SKILL.md": "tour the repository first."}
	registry, digest, _ := materializationBundle(t, "repo-tour", files)
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("relaunch")
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}

	mounts, configMap, err := control.materializeRunSkills(context.Background(), run)
	if err != nil {
		t.Fatalf("first materialization: %v", err)
	}
	var content corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: mounts[0].ConfigMapName}, &content); err != nil {
		t.Fatal(err)
	}
	// Drift the ConfigMap the way a stale or foreign writer would.
	content.BinaryData = map[string][]byte{"f0": []byte("something else")}
	if err := c.Update(context.Background(), &content); err != nil {
		t.Fatal(err)
	}
	if _, _, err := control.materializeRunSkills(context.Background(), run); err != nil {
		t.Fatalf("relaunch over an existing ConfigMap: %v", err)
	}
	var converged corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: mounts[0].ConfigMapName}, &converged); err != nil {
		t.Fatal(err)
	}
	if got := string(converged.BinaryData["f0"]); got != files["SKILL.md"] {
		t.Fatalf("content ConfigMap key f0 = %q, want the bundle's bytes %q", got, files["SKILL.md"])
	}
	var config corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: configMap}, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.BinaryData[topology.SkillBundlesConfigKey]) == 0 {
		t.Fatal("the config ConfigMap carries no bundle config document")
	}
}

// TestMaterializeRunSkillsFailsClosedOnOversizedBundle pins the ConfigMap
// size bound: a bundle whose decoded content exceeds what a ConfigMap can
// hold fails closed, naming the bundle and its size, and creates nothing.
func TestMaterializeRunSkillsFailsClosedOnOversizedBundle(t *testing.T) {
	registry, digest, _ := materializationBundle(t, "repo-tour", map[string]string{
		"SKILL.md": strings.Repeat("a", maxSkillConfigMapBytes+1),
	})
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("oversized")
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}

	mounts, configMap, err := control.materializeRunSkills(context.Background(), run)
	if err == nil {
		t.Fatal("an oversized bundle must fail closed")
	}
	if !strings.Contains(err.Error(), "repo-tour") || !strings.Contains(err.Error(), fmt.Sprint(maxSkillConfigMapBytes+1)) {
		t.Fatalf("error = %v, want the bundle name and its byte count", err)
	}
	if mounts != nil || configMap != "" {
		t.Fatalf("a failed materialization returned mounts %+v and config %q, want none", mounts, configMap)
	}
	configMaps := &corev1.ConfigMapList{}
	if err := c.List(context.Background(), configMaps, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(configMaps.Items) != 0 {
		t.Fatalf("a failed materialization created %d ConfigMaps, want none", len(configMaps.Items))
	}
}

// TestMaterializeRunSkillsFailsClosedOnUnknownPin pins the registry boundary:
// a pin the deployment no longer registers cannot be materialized, so the
// launch fails instead of bringing up a pod without the pinned content.
func TestMaterializeRunSkillsFailsClosedOnUnknownPin(t *testing.T) {
	registry, _, _ := materializationBundle(t, "repo-tour", map[string]string{"SKILL.md": "tour the repository first."})
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("unknown-pin")
	run.Status.SkillPins = []courier.SkillPin{{Name: "ghost-skill", Digest: "sha256:" + strings.Repeat("a", 64)}}

	mounts, configMap, err := control.materializeRunSkills(context.Background(), run)
	if err == nil {
		t.Fatal("a pin absent from the registry must fail closed")
	}
	if !strings.Contains(err.Error(), "ghost-skill") {
		t.Fatalf("error = %v, want the offending skill name", err)
	}
	if mounts != nil || configMap != "" {
		t.Fatalf("a failed materialization returned mounts %+v and config %q, want none", mounts, configMap)
	}
	configMaps := &corev1.ConfigMapList{}
	if err := c.List(context.Background(), configMaps, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(configMaps.Items) != 0 {
		t.Fatalf("a failed materialization created %d ConfigMaps, want none", len(configMaps.Items))
	}
}

// TestMaterializeRunSkillsLeavesSkilllessRunsAlone pins the absence
// contract: a run that pins no skill materializes nothing and asks for no
// mount, so a skillless run never carries an empty bundle projection.
func TestMaterializeRunSkillsLeavesSkilllessRunsAlone(t *testing.T) {
	registry, _, _ := materializationBundle(t, "repo-tour", map[string]string{"SKILL.md": "tour the repository first."})
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("skillless")

	mounts, configMap, err := control.materializeRunSkills(context.Background(), run)
	if err != nil {
		t.Fatalf("materializeRunSkills: %v", err)
	}
	if len(mounts) != 0 {
		t.Fatalf("a skillless run got mounts %+v, want none", mounts)
	}
	if configMap != "" {
		t.Fatalf("a skillless run got config ConfigMap %q, want none", configMap)
	}
	configMaps := &corev1.ConfigMapList{}
	if err := c.List(context.Background(), configMaps, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(configMaps.Items) != 0 {
		t.Fatalf("a skillless run created %d ConfigMaps, want none", len(configMaps.Items))
	}
}

// TestRevokeDeletesMaterializedSkillConfigMaps pins the teardown: the run's
// materialized skill material is per-run content and goes away with the run,
// the config ConfigMap and every pinned bundle's content ConfigMap alike.
func TestRevokeDeletesMaterializedSkillConfigMaps(t *testing.T) {
	registry, digest, _ := materializationBundle(t, "repo-tour", map[string]string{"SKILL.md": "tour the repository first."})
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("teardown")
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}

	mounts, configMap, err := control.materializeRunSkills(context.Background(), run)
	if err != nil {
		t.Fatalf("materializeRunSkills: %v", err)
	}
	names := []string{configMap, mounts[0].ConfigMapName}
	for _, name := range names {
		var object corev1.ConfigMap
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: name}, &object); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
	}
	done, err := control.Revoke(context.Background(), run)
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if !done {
		t.Fatal("Revoke did not complete")
	}
	for _, name := range names {
		var object corev1.ConfigMap
		err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: name}, &object)
		if err == nil {
			t.Fatalf("the teardown left the skill ConfigMap %q behind", name)
		}
	}
}

// TestLaunchSecureMaterializesPinnedSkillBundles pins the launch path's
// wiring: the operator materializes the run's pinned bundles into per-run
// ConfigMaps and mounts them into the control and worker pods, so the pods
// read the bundle content where the deployment registered it.
func TestLaunchSecureMaterializesPinnedSkillBundles(t *testing.T) {
	registry, digest, source := materializationBundle(t, "repo-tour", map[string]string{
		"SKILL.md":            "tour the repository first.",
		"assets/checklist.md": "check the checklist.",
	})
	control, c := secureControl(t)
	control.Config.Skills = registry
	run := secureRun("launch-skills")
	run.Status.Phase = courier.PhaseClaimed
	run.Status.SkillPins = []courier.SkillPin{{Name: "repo-tour", Digest: digest}}
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
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: secureTestNamespace,
			Labels: map[string]string{
				"pod-security.kubernetes.io/enforce": "restricted",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := control.LaunchSecure(context.Background(), run); err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}

	configMapName := topology.SkillContentConfigMapName(run.Name, "repo-tour")
	var content corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: configMapName}, &content); err != nil {
		t.Fatalf("the launch materialized no content ConfigMap: %v", err)
	}
	var config corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: topology.SkillBundlesConfigMapName(run.Name)}, &config); err != nil {
		t.Fatalf("the launch materialized no config ConfigMap: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(secureTestNamespace)); err != nil {
		t.Fatal(err)
	}
	components := map[string]*corev1.Pod{}
	for i := range pods.Items {
		components[pods.Items[i].Labels["courier.misospace.dev/component"]] = &pods.Items[i]
	}
	for _, component := range []string{topology.ComponentCoordinator, topology.ComponentWorker} {
		pod := components[component]
		if pod == nil {
			t.Fatalf("the launch created no %s pod", component)
		}
		var mounted bool
		for _, volume := range pod.Spec.Volumes {
			if volume.ConfigMap != nil && volume.ConfigMap.Name == configMapName {
				mounted = true
				if !reflect.DeepEqual(volume.ConfigMap.Items, []corev1.KeyToPath{
					{Key: "f0", Path: "SKILL.md"},
					{Key: "f1", Path: "assets/checklist.md"},
				}) {
					t.Fatalf("%s pod skill volume items = %+v", component, volume.ConfigMap.Items)
				}
			}
		}
		if !mounted {
			t.Fatalf("the %s pod mounts no content ConfigMap %q", component, configMapName)
		}
		var skillMount *corev1.VolumeMount
		for i := range pod.Spec.Containers[0].VolumeMounts {
			if pod.Spec.Containers[0].VolumeMounts[i].MountPath == source {
				skillMount = &pod.Spec.Containers[0].VolumeMounts[i]
			}
		}
		if skillMount == nil {
			t.Fatalf("the %s pod mounts the bundle at no registered source path %q", component, source)
		}
		if !skillMount.ReadOnly {
			t.Fatalf("the %s pod mounts the bundle writable", component)
		}
	}
	// The worker mounts the bundle content and nothing that identifies it.
	worker := components[topology.ComponentWorker]
	for _, volume := range worker.Spec.Volumes {
		if volume.Secret != nil || volume.Projected != nil {
			t.Fatalf("the worker pod mounts a secret or projected volume %q", volume.Name)
		}
	}
	// The control pod also mounts the config it resolves its pins against.
	controlPod := components[topology.ComponentCoordinator]
	var configMounted bool
	for _, volume := range controlPod.Spec.Volumes {
		if volume.ConfigMap != nil && volume.ConfigMap.Name == topology.SkillBundlesConfigMapName(run.Name) {
			configMounted = true
		}
	}
	if !configMounted {
		t.Fatal("the control pod mounts no bundle config ConfigMap")
	}
}
