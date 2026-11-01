package controller

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/skills"
	"github.com/misospace/courier/internal/topology"
)

// maxSkillConfigMapBytes is the largest decoded payload one materialized
// bundle ConfigMap may carry: a Kubernetes ConfigMap holds at most 1 MiB of
// decoded data and the API server refuses a larger one. The registry's own
// 8 MiB bound still applies at registration; this is the materialization
// bound, and a bundle above it fails closed instead of launching pods whose
// mount would never carry the pinned content.
const maxSkillConfigMapBytes = 1 << 20

// skillMaterial is one run's resolved skill material: the content each
// pinned bundle carries, the pod mounts that content renders, and the bundle
// config the control pod resolves its pins against. Resolution is pure — it
// reads only the deployment registry and the run's persisted pins — so the
// dry-run proof can render the very artifact the launch path creates, and a
// failure to resolve is known before anything is created.
//
// The material travels in ConfigMaps, not Secrets: skill bundles are
// deployment-approved prompt and asset content the run deliberately exposes
// to its pods, never credential material, and a ConfigMap volume is neither
// a Secret nor a projected volume — the worker's no-identity-material
// posture is untouched by a pinned bundle.
type skillMaterial struct {
	// configMaps are the per-bundle content ConfigMaps, one per pinned
	// bundle.
	configMaps []*corev1.ConfigMap
	// mounts are the pod mounts those ConfigMaps render: one read-only
	// projection per bundle, at the bundle's registered source path.
	mounts []topology.SkillMount
	// configMap is the per-run ConfigMap carrying the rendered bundle
	// config. It is empty for a skillless run, which needs no config.
	configMap string
	// document is the rendered bundle config the config ConfigMap carries.
	document []byte
}

// materializeRunSkills materializes the run's pinned skill bundles into
// per-run ConfigMaps: one content ConfigMap per bundle, plus the config
// ConfigMap the control pod resolves its pins against. It returns the pod
// mounts and the config ConfigMap's name, both empty for a skillless run.
//
// The materialization happens before the run's pods are created: both the
// control and the worker mount the content, so the ConfigMaps must exist
// before either pod starts. Every pin is resolved before anything is
// created, so a pin that cannot be materialized fails the whole call with no
// ConfigMap written — and the launch path never brings up a pod without the
// full pinned content.
func (s *SecureControl) materializeRunSkills(ctx context.Context, run *courier.CoderRun) ([]topology.SkillMount, string, error) {
	material, err := s.resolveRunSkills(run)
	if err != nil {
		return nil, "", err
	}
	if len(material.configMaps) == 0 {
		return nil, "", nil
	}
	// The content ConfigMaps come first: the config names bundles whose
	// content the pods mount, so a config ConfigMap that outlives a failed
	// content ConfigMap would point the control pod at bundles it cannot
	// read.
	for _, configMap := range material.configMaps {
		if err := s.ensureSkillConfigMap(ctx, configMap); err != nil {
			return nil, "", err
		}
	}
	config := topology.SkillBundlesConfigMap(run, material.document)
	if err := s.ensureSkillConfigMap(ctx, config); err != nil {
		return nil, "", err
	}
	return material.mounts, material.configMap, nil
}

// resolveRunSkills resolves the run's pinned skill bundles against the
// deployment registry into the material the launch path creates. The pins
// are the run's persisted identities: every pin must still resolve to a
// registered bundle carrying the pinned digest, so the materialized content
// is exactly what the run was admitted to load. A dropped name or a drifted
// digest fails closed rather than substituting newer content.
//
// The bundle's files carry the exact bytes the registry digested, and the
// projection's key-to-path items give every file its own relative path
// inside the mount, so the mounted content is the bundle the pin named. The
// data keys are encoded (f0, f1, ...) because a ConfigMap's key names must
// match [-._a-zA-Z0-9]+ while a bundle's file paths may not.
func (s *SecureControl) resolveRunSkills(run *courier.CoderRun) (*skillMaterial, error) {
	if len(run.Status.SkillPins) == 0 {
		return &skillMaterial{}, nil
	}
	pins := make([]skills.Pin, 0, len(run.Status.SkillPins))
	for _, pin := range run.Status.SkillPins {
		pins = append(pins, skills.Pin{Name: pin.Name, Digest: pin.Digest})
	}
	if err := skills.Verify(s.Config.Skills, pins); err != nil {
		return nil, fmt.Errorf("secure topology: materialize run skills: %w", err)
	}
	sorted := make([]skills.Pin, len(pins))
	copy(sorted, pins)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	material := &skillMaterial{}
	bundles := make([]skills.MaterializedBundle, 0, len(sorted))
	for _, pin := range sorted {
		bundle, ok := s.Config.Skills.Lookup(pin.Name)
		if !ok {
			return nil, fmt.Errorf("secure topology: materialize run skills: skill %q is not registered in the deployment skill registry", pin.Name)
		}
		files := bundle.MaterializedFiles()
		if len(files) == 0 {
			return nil, fmt.Errorf("secure topology: materialize run skills: skill %q carries no files", pin.Name)
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
		var total int64
		for _, file := range files {
			total += int64(len(file.Content))
		}
		if total > maxSkillConfigMapBytes {
			return nil, fmt.Errorf("secure topology: materialize run skills: skill %q carries %d bytes, above the %d byte ConfigMap limit", pin.Name, total, maxSkillConfigMapBytes)
		}
		var (
			data  = make(map[string][]byte, len(files))
			items = make([]corev1.KeyToPath, 0, len(files))
			paths = make([]string, 0, len(files))
		)
		for i, file := range files {
			key := fmt.Sprintf("f%d", i)
			data[key] = file.Content
			items = append(items, corev1.KeyToPath{Key: key, Path: file.Path})
			paths = append(paths, file.Path)
		}
		configMapName := topology.SkillContentConfigMapName(run.Name, pin.Name)
		if configMapName == topology.SkillBundlesConfigMapName(run.Name) {
			return nil, fmt.Errorf("secure topology: materialize run skills: skill %q resolves to the bundle config ConfigMap name %q; a skill name may not collide with the config ConfigMap", pin.Name, configMapName)
		}
		material.configMaps = append(material.configMaps, topology.SkillContentConfigMap(run, pin.Name, data))
		material.mounts = append(material.mounts, topology.SkillMount{
			Name:          pin.Name,
			ConfigMapName: configMapName,
			Source:        bundle.Source,
			Items:         items,
		})
		bundles = append(bundles, skills.MaterializedBundle{
			Name:   pin.Name,
			Source: bundle.Source,
			Files:  paths,
		})
	}
	// The registered source path is the mount path in the run's pods, so it
	// has to be mountable: two bundles may not share one, and no bundle may
	// sit at a path the operator's own pods own. The check runs here, before
	// any ConfigMap is written, so a colliding registration fails the run
	// closed with nothing created.
	if err := topology.ValidateSkillMounts(material.mounts); err != nil {
		return nil, fmt.Errorf("secure topology: materialize run skills: %w", err)
	}
	document, err := skills.RenderRunConfig(bundles)
	if err != nil {
		return nil, fmt.Errorf("secure topology: materialize run skills: %w", err)
	}
	if len(document) > maxSkillConfigMapBytes {
		return nil, fmt.Errorf("secure topology: materialize run skills: the rendered bundle config is %d bytes, above the %d byte ConfigMap limit", len(document), maxSkillConfigMapBytes)
	}
	material.document = document
	material.configMap = topology.SkillBundlesConfigMapName(run.Name)
	return material, nil
}

// ensureSkillConfigMap creates the run's skill ConfigMap, or converges an
// existing one onto the current content. The operator's grant covers
// create, delete, get, and list on ConfigMaps but not update, so the
// convergence replaces the object wholesale — the same delete-then-create
// shape the signing key rotation uses — and only when the content actually
// differs: identical content needs no write, so a relaunch over an
// unchanged bundle touches nothing and never fails on the ConfigMap that is
// already there.
//
// The read goes through the uncached API reader: the chart grants the
// operator no watch on ConfigMaps, so a cached read would start an informer
// the grant cannot sustain.
func (s *SecureControl) ensureSkillConfigMap(ctx context.Context, configMap *corev1.ConfigMap) error {
	existing := &corev1.ConfigMap{}
	err := s.reader().Get(ctx, client.ObjectKeyFromObject(configMap), existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := s.Client.Create(ctx, configMap); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("secure topology: create skill ConfigMap %s: %w", configMap.Name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("secure topology: read skill ConfigMap %s: %w", configMap.Name, err)
	}
	if configMapContentEqual(existing, configMap) {
		return nil
	}
	if err := s.Client.Delete(ctx, existing); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("secure topology: replace skill ConfigMap %s: %w", configMap.Name, err)
	}
	if err := s.Client.Create(ctx, configMap); err != nil {
		return fmt.Errorf("secure topology: recreate skill ConfigMap %s: %w", configMap.Name, err)
	}
	return nil
}

// configMapContent flattens a ConfigMap's content into one key-to-bytes
// view: the string form and the binary form together, because the API
// server keeps them apart while a reader sees one file per key.
func configMapContent(configMap *corev1.ConfigMap) map[string][]byte {
	content := make(map[string][]byte, len(configMap.Data)+len(configMap.BinaryData))
	for key, value := range configMap.Data {
		content[key] = []byte(value)
	}
	for key, value := range configMap.BinaryData {
		content[key] = value
	}
	return content
}

// configMapContentEqual reports whether two ConfigMaps carry the same
// content. The comparison is by value, not by pointer, so a ConfigMap read
// back from the API server compares equal to the one the operator rendered.
func configMapContentEqual(a, b *corev1.ConfigMap) bool {
	left, right := configMapContent(a), configMapContent(b)
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		other, ok := right[key]
		if !ok || !bytes.Equal(value, other) {
			return false
		}
	}
	return true
}
