package executor

import (
	"errors"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The trusted skill envelope (#658) is part of the run-context environment
// contract: present exactly when the run pins skills, reconstructed by
// InvocationFromEnv so the native control pod sees the identical
// operator-authored value the run's persisted status carries.
func TestSkillsEnvContract(t *testing.T) {
	pinned := Invocation{
		RunName: "run-1", Namespace: "runs", Mode: courierv1alpha1.ModeResolveIssue,
		Repo: "acme/widgets", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
		Skills:           `[{"name":"repo-tour","digest":"sha256:` + strings.Repeat("b", 64) + `"}]`,
		SkillBundlesFile: SkillBundlesConfigPath,
	}
	values := map[string]string{}
	for _, env := range RunContextEnvironment(pinned) {
		values[env.Name] = env.Value
	}
	if got := values[EnvSkills]; got != pinned.Skills {
		t.Fatalf("COURIER_SKILLS_JSON = %q, want the operator-authored envelope", got)
	}
	if got := values[EnvSkillBundlesFile]; got != SkillBundlesConfigPath {
		t.Fatalf("COURIER_SKILL_BUNDLES_FILE = %q, want the mounted config path", got)
	}
	decoded, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("InvocationFromEnv: %v", err)
	}
	if decoded.Skills != pinned.Skills {
		t.Fatalf("round trip Skills = %q, want %q", decoded.Skills, pinned.Skills)
	}
	if decoded.SkillBundlesFile != SkillBundlesConfigPath {
		t.Fatalf("round trip SkillBundlesFile = %q, want %q", decoded.SkillBundlesFile, SkillBundlesConfigPath)
	}
	pins, err := SkillPins(decoded)
	if err != nil {
		t.Fatalf("SkillPins: %v", err)
	}
	if len(pins) != 1 || pins[0].Name != "repo-tour" || pins[0].Digest != "sha256:"+strings.Repeat("b", 64) {
		t.Fatalf("SkillPins = %#v, want the decoded pin", pins)
	}

	// A skillless run emits nothing: absence is meaningful.
	values = map[string]string{}
	for _, env := range RunContextEnvironment(Invocation{
		RunName: "run-1", Namespace: "runs", Mode: courierv1alpha1.ModeResolveIssue,
		Repo: "acme/widgets", Ref: 1, Goal: "g", Model: "m", Workspace: "/w",
	}) {
		values[env.Name] = env.Value
	}
	if _, ok := values[EnvSkills]; ok {
		t.Fatal("a skillless invocation must not emit COURIER_SKILLS_JSON")
	}
	if _, ok := values[EnvSkillBundlesFile]; ok {
		t.Fatal("a skillless invocation must not emit COURIER_SKILL_BUNDLES_FILE")
	}
	skillless, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("InvocationFromEnv: %v", err)
	}
	if skillless.SkillBundlesFile != "" {
		t.Fatalf("skillless SkillBundlesFile = %q, want empty", skillless.SkillBundlesFile)
	}
	if pins, err := SkillPins(skillless); err != nil || pins != nil {
		t.Fatalf("SkillPins(skillless) = %#v, %v; want nil, nil", pins, err)
	}
}

// TestInvocationFromEnvRejectsMalformedSkills pins the fail-closed
// reconstruction: a malformed envelope — unparsable JSON, a pin naming no
// skill, or a digest that is not a sha256 content identity — is a
// configuration failure the control pod must surface, never a session that
// silently runs skillless while claiming pins.
func TestInvocationFromEnvRejectsMalformedSkills(t *testing.T) {
	base := map[string]string{
		"COURIER_RUN_NAME": "r", "COURIER_RUN_NAMESPACE": "n", "COURIER_MODE": "resolve-issue",
		"COURIER_REF": "1", "COURIER_GOAL": "g", "COURIER_MODEL": "m", "COURIER_WORKSPACE": "/w",
	}
	for name, tc := range map[string]struct {
		envelope string
	}{
		"unparsable":   {envelope: "{not skill pins"},
		"empty name":   {envelope: `[{"name":"","digest":"sha256:` + strings.Repeat("a", 64) + `"}]`},
		"bad digest":   {envelope: `[{"name":"repo-tour","digest":"sha256:zz"}]`},
		"short digest": {envelope: `[{"name":"repo-tour","digest":"sha256:abc"}]`},
	} {
		values := map[string]string{}
		for k, v := range base {
			values[k] = v
		}
		values[EnvSkills] = tc.envelope
		_, err := InvocationFromEnv(func(key string) string { return values[key] })
		if !errors.Is(err, ErrInvalidSkills) {
			t.Fatalf("%s: error = %v, want ErrInvalidSkills", name, err)
		}
	}

	// The skillless environment keeps its shape: no envelope, no error.
	legacy, err := InvocationFromEnv(func(name string) string { return base[name] })
	if err != nil {
		t.Fatalf("InvocationFromEnv without skills: %v", err)
	}
	if legacy.Skills != "" {
		t.Fatalf("skillless env reconstructed Skills %q, want empty", legacy.Skills)
	}
}

// TestSkillPinsRejectsHandAssembledEnvelope pins the helper's fail-closed
// contract: an invocation assembled outside the environment boundary must
// not drift from the envelope validation the reconstruction applies.
func TestSkillPinsRejectsHandAssembledEnvelope(t *testing.T) {
	for name, envelope := range map[string]string{
		"unparsable":   "{not skill pins",
		"empty name":   `[{"name":"","digest":"sha256:` + strings.Repeat("a", 64) + `"}]`,
		"bad digest":   `[{"name":"repo-tour","digest":"sha256:zz"}]`,
		"short digest": `[{"name":"repo-tour","digest":"sha256:abc"}]`,
	} {
		_, err := SkillPins(Invocation{Skills: envelope})
		if !errors.Is(err, ErrInvalidSkills) {
			t.Fatalf("%s: error = %v, want ErrInvalidSkills", name, err)
		}
	}
}
