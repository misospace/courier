package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseValidDocument(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    runtimeImage: ghcr.io/misospace/coordinator:1.0.0
    concurrency: 3
    roles:
      coordinator: claude-sonnet-4-5
      coder: gpt-5
      reviewer: o3
    framing: |
      One A100 GPU, no load tool.
      Prefer a single parallel worker and batch reviews.
  - name: cloud
    roles:
      coordinator: claude-opus-4-5
`)
	profiles, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 {
		t.Fatalf("parsed %d profiles, want 2", len(profiles))
	}
	first := profiles[0]
	if first.Name != "local" {
		t.Fatalf("name = %q, want local", first.Name)
	}
	if first.RuntimeImage != "ghcr.io/misospace/coordinator:1.0.0" {
		t.Fatalf("runtimeImage = %q, want the configured image", first.RuntimeImage)
	}
	if first.Concurrency != 3 {
		t.Fatalf("concurrency = %d, want 3", first.Concurrency)
	}
	if len(first.Roles) != 3 || first.Roles["coordinator"] != "claude-sonnet-4-5" || first.Roles["coder"] != "gpt-5" || first.Roles["reviewer"] != "o3" {
		t.Fatalf("roles = %#v, want all three roles", first.Roles)
	}
	wantFraming := "One A100 GPU, no load tool.\nPrefer a single parallel worker and batch reviews.\n"
	if first.Framing != wantFraming {
		t.Fatalf("framing = %q, want %q", first.Framing, wantFraming)
	}
	second := profiles[1]
	if second.Name != "cloud" {
		t.Fatalf("second name = %q, want cloud", second.Name)
	}
	if second.Concurrency != 1 {
		t.Fatalf("second concurrency = %d, want default 1", second.Concurrency)
	}
}

func TestParseRequiresName(t *testing.T) {
	data := []byte(`profiles:
  - roles:
      coder: gpt-5
`)
	_, err := Parse(data)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("error = %v, want missing name rejected", err)
	}
}

func TestParseRejectsDuplicateNames(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles:
      coder: gpt-5
  - name: local
    roles:
      coder: o3
`)
	_, err := Parse(data)
	if err == nil || !strings.Contains(err.Error(), "duplicate") || !strings.Contains(err.Error(), "local") {
		t.Fatalf("error = %v, want duplicate name local rejected", err)
	}
}

func TestParseDefaultsOmittedConcurrency(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles:
      coder: gpt-5
`)
	profiles, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if profiles[0].Concurrency != 1 {
		t.Fatalf("concurrency = %d, want 1", profiles[0].Concurrency)
	}
}

func TestParseRejectsNegativeConcurrency(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    concurrency: -2
    roles:
      coder: gpt-5
`)
	_, err := Parse(data)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("error = %v, want negative concurrency rejected", err)
	}
}

func TestParseRequiresRoles(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles: {}
`)
	_, err := Parse(data)
	if err == nil || !strings.Contains(err.Error(), "roles") {
		t.Fatalf("error = %v, want empty roles rejected", err)
	}
}

func TestParseRejectsEmptyRoleEntries(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles:
      "": gpt-5
`)
	if _, err := Parse(data); err == nil {
		t.Fatal("empty role name was accepted")
	}

	data = []byte(`profiles:
  - name: local
    roles:
      coder: "  "
`)
	if _, err := Parse(data); err == nil {
		t.Fatal("empty model value was accepted")
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles:
      coder: gpt-5
    surprise: true
`)
	_, err := Parse(data)
	if err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatalf("error = %v, want unknown field surprise rejected", err)
	}
}

func TestParseRejectsInvalidNameShapes(t *testing.T) {
	// DNS-1123 label violations: the error must name the requirement.
	dnsCases := []string{
		`profiles:
  - name: "Local/x"
    roles:
      coder: gpt-5
`,
		`profiles:
  - name: my_lane
    roles:
      coder: gpt-5
`,
		`profiles:
  - name: lane.beta
    roles:
      coder: gpt-5
`,
	}
	for _, data := range dnsCases {
		_, err := Parse([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "DNS-1123") {
			t.Fatalf("error = %v, want a DNS-1123 label requirement", err)
		}
		if strings.Contains(err.Error(), "or '.'") {
			t.Fatalf("error = %v, want no guidance that dots are valid", err)
		}
	}
	// Empty-after-trim: caught by the name-is-required check.
	_, err := Parse([]byte(`profiles:
  - name: "   "
    roles:
      coder: gpt-5
`))
	if err == nil {
		t.Fatal("empty-after-trim name was accepted, want it rejected")
	}
}

func TestParseStoresTrimmedRoleEntries(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles:
      " coordinator ": " gpt-5 "
`)
	profiles, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles[0].Roles) != 1 {
		t.Fatalf("roles = %#v, want one entry", profiles[0].Roles)
	}
	if _, ok := profiles[0].Roles["coordinator"]; !ok {
		t.Fatalf("roles = %#v, want the trimmed key coordinator", profiles[0].Roles)
	}
	if profiles[0].Roles["coordinator"] != "gpt-5" {
		t.Fatalf("role coordinator = %q, want the trimmed value gpt-5", profiles[0].Roles["coordinator"])
	}
}

func TestParseRejectsDuplicateRolesAfterTrim(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    roles:
      "coordinator ": gpt-5
      "coordinator": o3
`)
	_, err := Parse(data)
	if err == nil || !strings.Contains(err.Error(), "duplicate role") {
		t.Fatalf("error = %v, want duplicate role after trim rejected", err)
	}
}

func TestParseCoercesZeroConcurrencyToOne(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    concurrency: 0
    roles:
      coder: gpt-5
`)
	profiles, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if profiles[0].Concurrency != 1 {
		t.Fatalf("concurrency = %d, want 1 (explicit zero coerced)", profiles[0].Concurrency)
	}
}

func TestParseStoresTrimmedRuntimeImage(t *testing.T) {
	data := []byte(`profiles:
  - name: local
    runtimeImage: "  ghcr.io/misospace/coordinator:1.0.0  "
    roles:
      coder: gpt-5
`)
	profiles, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if profiles[0].RuntimeImage != "ghcr.io/misospace/coordinator:1.0.0" {
		t.Fatalf("runtimeImage = %q, want it trimmed", profiles[0].RuntimeImage)
	}
}

func TestFileProviderRereadsOnEveryCall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.yaml")
	initial := []byte(`profiles:
  - name: local
    roles:
      coder: gpt-5
`)
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatal(err)
	}
	provider := &FileProvider{Path: path}
	profiles, err := provider.Profiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Name != "local" {
		t.Fatalf("profiles = %#v, want one local profile", profiles)
	}

	rotated := []byte(`profiles:
  - name: local
    concurrency: 4
    roles:
      coder: claude-sonnet-4-5
`)
	if err := os.WriteFile(path, rotated, 0o644); err != nil {
		t.Fatal(err)
	}
	profiles, err = provider.Profiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Concurrency != 4 || profiles[0].Roles["coder"] != "claude-sonnet-4-5" {
		t.Fatalf("after rotation profiles = %#v, want the new file content", profiles)
	}
}

func TestFileProviderMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.yaml")
	provider := &FileProvider{Path: path}
	_, err := provider.Profiles(context.Background())
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("error = %v, want it to mention %s", err, path)
	}
}

func TestLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.yaml")
	data := []byte(`profiles:
  - name: local
    concurrency: 2
    roles:
      coder: gpt-5
`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	profiles, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Name != "local" || profiles[0].Concurrency != 2 {
		t.Fatalf("profiles = %#v", profiles)
	}
}
