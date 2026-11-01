package topology

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/protocol"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// TestValidateSkillSourcesRejectsPodOwnedPaths pins the mount-path invariant
// the operator's own volumes depend on: a bundle may not be mounted at a path
// the control or the worker pod already owns, and neither at an ancestor of
// one. Both would shadow the operator's volume, so both fail closed before a
// pod is rendered.
func TestValidateSkillSourcesRejectsPodOwnedPaths(t *testing.T) {
	for _, reserved := range ReservedSkillMountPaths() {
		cases := map[string]string{
			"the path itself": reserved,
			"an ancestor":     filepath.Dir(reserved),
			"a descendant":    reserved + "/nested",
		}
		for kind, source := range cases {
			err := ValidateSkillSources([]SkillSource{{Name: "repo-tour", Source: source}})
			if err == nil {
				t.Fatalf("source %q (%s) must be rejected: it overlaps the pod-owned path %q", source, kind, reserved)
			}
			if !strings.Contains(err.Error(), source) {
				t.Fatalf("error = %v, want it to name the rejected source %q", err, source)
			}
		}
	}
}

// TestValidateSkillSourcesRejectsOverlappingBundles pins the uniqueness rule:
// two bundles at one path would render two volumes at one mount path, and a
// bundle nested inside another would shadow it.
func TestValidateSkillSourcesRejectsOverlappingBundles(t *testing.T) {
	cases := map[string][]SkillSource{
		"the same path": {
			{Name: "repo-tour", Source: "/opt/courier/skills/repo-tour"},
			{Name: "repo-tour-copy", Source: "/opt/courier/skills/repo-tour"},
		},
		"a nested path": {
			{Name: "repo-tour", Source: "/opt/courier/skills"},
			{Name: "repo-tour-copy", Source: "/opt/courier/skills/repo-tour"},
		},
	}
	for kind, sources := range cases {
		err := ValidateSkillSources(sources)
		if err == nil {
			t.Fatalf("two bundles sharing %s must be rejected", kind)
		}
		if !strings.Contains(err.Error(), sources[0].Name) || !strings.Contains(err.Error(), sources[1].Name) {
			t.Fatalf("error = %v, want it to name both bundles", err)
		}
	}
}

// TestValidateSkillSourcesRejectsMalformedSources pins the shape rule: the
// source is a mount point, so it must be an absolute, clean path. A relative
// path cannot be mounted, and a path carrying "." or ".." elements names a
// location other than the one the deployment registered.
func TestValidateSkillSourcesRejectsMalformedSources(t *testing.T) {
	cases := map[string]string{
		"an empty source":      "",
		"a relative source":    "opt/courier/skills/repo-tour",
		"a NUL byte":           "/opt/courier/skills/\x00repo-tour",
		"a dot element":        "/opt/./courier/skills",
		"a parent element":     "/opt/courier/skills/../other",
		"a trailing separator": "/opt/courier/skills/",
		"a doubled separator":  "/opt//courier/skills",
	}
	for kind, source := range cases {
		err := ValidateSkillSources([]SkillSource{{Name: "repo-tour", Source: source}})
		if err == nil {
			t.Fatalf("a source with %s must be rejected", kind)
		}
	}
}

// TestValidateSkillSourcesAcceptsDistinctMountPaths pins the accepting case:
// distinct, well-formed paths outside the operator's own mounts are the
// ordinary registration.
func TestValidateSkillSourcesAcceptsDistinctMountPaths(t *testing.T) {
	err := ValidateSkillSources([]SkillSource{
		{Name: "repo-tour", Source: "/opt/courier/skills/repo-tour"},
		{Name: "simple-english", Source: "/opt/courier/skills/simple-english"},
	})
	if err != nil {
		t.Fatalf("distinct mount paths must be accepted: %v", err)
	}
}

// TestControlPodRejectsCollidingSkillMount pins the render boundary: the
// control pod is never rendered with a bundle whose source path collides with
// a path the pod itself owns.
func TestControlPodRejectsCollidingSkillMount(t *testing.T) {
	run := testRun("control-skill-collision")
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
		Skills:                []SkillMount{skillMountFixture("repo-tour", "/workspace")},
	})
	if err == nil {
		t.Fatal("a bundle mounted at the control pod's own workspace path must be rejected")
	}
	if pod != nil {
		t.Fatal("a rejected control pod must not be rendered")
	}
	if !strings.Contains(err.Error(), "/workspace") {
		t.Fatalf("error = %v, want it to name the colliding path", err)
	}
}

// TestWorkerPodRejectsCollidingSkillMount pins the same boundary on the
// worker: the untrusted pod is never rendered with a bundle whose source path
// collides with a path the worker itself owns.
func TestWorkerPodRejectsCollidingSkillMount(t *testing.T) {
	run := testRun("worker-skill-collision")
	pub, _, _ := protocol.GenerateKey()
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil, courier.DefaultPodResources(), []SkillMount{skillMountFixture("repo-tour", "/courier-toolchain")})
	if err == nil {
		t.Fatal("a bundle mounted at the worker's own toolchain path must be rejected")
	}
	if pod != nil {
		t.Fatal("a rejected worker pod must not be rendered")
	}
	if !strings.Contains(err.Error(), "/courier-toolchain") {
		t.Fatalf("error = %v, want it to name the colliding path", err)
	}
}
