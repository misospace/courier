package forge

import (
	"strings"
	"testing"
)

func validRegistryJSON() string {
	return `{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["misospace/*"]}]}`
}

func TestLoadAcceptsValidRegistry(t *testing.T) {
	registry, err := Load([]byte(validRegistryJSON()), "github")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	registration, err := registry.Select("misospace/courier")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if registration.Name != "github" || registration.Type != "github" {
		t.Fatalf("selected registration = %+v", registration)
	}
	ref, ok := registration.CredentialRef(PurposeForgeAPI)
	if !ok || ref.SecretName != "forge-creds" || ref.Key != "token" {
		t.Fatalf("forge-api credential ref = %+v ok=%v", ref, ok)
	}
	gitRef, ok := registration.CredentialRef(PurposeGit)
	if !ok || gitRef.SecretName != "forge-creds" {
		t.Fatalf("git purpose must default to forge-api, got %+v ok=%v", gitRef, ok)
	}
}

func TestLoadRejectsStructuralProblems(t *testing.T) {
	cases := map[string]struct {
		data    string
		message string
	}{
		"duplicate names": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"]},{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "duplicate provider name",
		},
		"unknown type": {
			data:    `{"providers":[{"name":"a","type":"gitlab","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "unknown provider type",
		},
		"missing endpoint": {
			data:    `{"providers":[{"name":"a","type":"github","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "endpoint",
		},
		"plaintext endpoint": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"http://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "must use https",
		},
		"endpoint with userinfo": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://user:pass@api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "userinfo",
		},
		"missing forge-api credential": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"git":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "forge-api",
		},
		"incomplete credential ref": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s"}},"serves":["*/*"]}]}`,
			message: "secretName and key",
		},
		"unknown credential purpose": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"},"admin":{"secretName":"s","key":"k"}},"serves":["*/*"]}]}`,
			message: "unknown credential purpose",
		},
		"no serves patterns": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":[]}]}`,
			message: "at least one serves pattern",
		},
		"malformed serves pattern": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["misospace"]}]}`,
			message: "owner/name",
		},
		"empty registry": {
			data:    `{"providers":[]}`,
			message: "no providers registered",
		},
		"unknown field": {
			data:    `{"providers":[{"name":"a","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*/*"],"admin":true}]}`,
			message: "unknown field",
		},
		"trailing data": {
			data:    validRegistryJSON() + `{"providers":[]}`,
			message: "trailing data",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load([]byte(tc.data), "github")
			if err == nil {
				t.Fatal("Load unexpectedly succeeded")
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.message)
			}
		})
	}
}

func TestSelectPatternMatching(t *testing.T) {
	t.Run("case insensitive", func(t *testing.T) {
		registry, err := Load([]byte(`{"providers":[{"name":"misospace","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["MisoSpace/*"]}]}`), "github")
		if err != nil {
			t.Fatal(err)
		}
		registration, err := registry.Select("MISOSPACE/courier")
		if err != nil || registration.Name != "misospace" {
			t.Fatalf("Select = %v, %v", registration, err)
		}
	})

	t.Run("ambiguous fails closed", func(t *testing.T) {
		registry, err := Load([]byte(`{"providers":[
			{"name":"misospace","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["MisoSpace/*"]},
			{"name":"everything","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["*"]}
		]}`), "github")
		if err != nil {
			t.Fatal(err)
		}
		_, err = registry.Select("misospace/x")
		if err == nil || !strings.Contains(err.Error(), "ambiguously") {
			t.Fatalf("ambiguous selection error = %v", err)
		}
		if !strings.Contains(err.Error(), "everything") || !strings.Contains(err.Error(), "misospace") {
			t.Fatalf("ambiguity diagnostic must name both registrations: %v", err)
		}
	})

	t.Run("no match fails closed", func(t *testing.T) {
		specific, err := Load([]byte(`{"providers":[{"name":"misospace","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["misospace/*"]}]}`), "github")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := specific.Select("other/project"); err == nil || !strings.Contains(err.Error(), "no provider registration") {
			t.Fatalf("no-match error = %v", err)
		}
	})

	t.Run("malformed repo never matches", func(t *testing.T) {
		specific, err := Load([]byte(`{"providers":[{"name":"misospace","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"s","key":"k"}},"serves":["misospace/*"]}]}`), "github")
		if err != nil {
			t.Fatal(err)
		}
		for _, repo := range []string{"", "noslash", "too/many/slashes", "a/"} {
			if _, err := specific.Select(repo); err == nil {
				t.Fatalf("repo %q unexpectedly matched", repo)
			}
		}
	})
}

func TestGitEndpointTemplate(t *testing.T) {
	cases := map[string]struct {
		typ, endpoint, gitEndpoint, want string
	}{
		"github.com api base":  {typ: "github", endpoint: "https://api.github.com/", want: "https://github.com/%s.git"},
		"enterprise api base":  {typ: "github", endpoint: "https://ghes.example.test/api/v3/", want: "https://ghes.example.test/%s.git"},
		"explicit override":    {typ: "github", endpoint: "https://api.github.com/", gitEndpoint: "https://ssh.github.com/%s.git", want: "https://ssh.github.com/%s.git"},
		"unknown type derived": {typ: "gitlab", endpoint: "https://gitlab.example.test/api/v4/", want: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := &Registration{Name: "x", Type: tc.typ, Endpoint: tc.endpoint, GitEndpoint: tc.gitEndpoint}
			if got := r.GitEndpointTemplate(); got != tc.want {
				t.Fatalf("GitEndpointTemplate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProjectionDigestBindsEveryField(t *testing.T) {
	base := Registration{
		Name:        "github",
		Type:        "github",
		Endpoint:    "https://api.github.com/",
		Credentials: map[string]CredentialRef{PurposeForgeAPI: {SecretName: "creds", Key: "token"}},
		Serves:      []string{"*"},
	}
	digest := base.Projection().Digest
	if len(digest) != 64 {
		t.Fatalf("digest = %q", digest)
	}
	if base.Projection().Digest != digest {
		t.Fatal("digest must be stable across calls")
	}

	mutations := map[string]func(*Registration){
		"name":        func(r *Registration) { r.Name = "other" },
		"type":        func(r *Registration) { r.Type = "github-enterprise" },
		"endpoint":    func(r *Registration) { r.Endpoint = "https://ghes.example.test/" },
		"gitEndpoint": func(r *Registration) { r.GitEndpoint = "https://git.example.test/%s.git" },
		"secretName": func(r *Registration) {
			r.Credentials = map[string]CredentialRef{PurposeForgeAPI: {SecretName: "other", Key: "token"}}
		},
		"key": func(r *Registration) {
			r.Credentials = map[string]CredentialRef{PurposeForgeAPI: {SecretName: "creds", Key: "other"}}
		},
		"purpose": func(r *Registration) {
			r.Credentials = map[string]CredentialRef{PurposeGit: {SecretName: "creds", Key: "token"}}
		},
	}
	for name, mutate := range mutations {
		t.Run("digest changes on "+name, func(t *testing.T) {
			mutated := base
			mutate(&mutated)
			if mutated.Projection().Digest == digest {
				t.Fatalf("digest did not change when %s changed", name)
			}
		})
	}
}
