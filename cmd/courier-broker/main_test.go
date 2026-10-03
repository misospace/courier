package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/topology"
)

func TestRunFailsClosedWithoutOperatorWiring(t *testing.T) {
	if err := runArgs(nil); err == nil || !strings.Contains(err.Error(), "requires address") {
		t.Fatalf("run() error = %v, want missing configuration", err)
	}
}

func TestVerifyProjectionRejectsMismatch(t *testing.T) {
	registration := &forge.Registration{
		Name:     "github",
		Type:     "github",
		Endpoint: "https://api.github.com/",
		Credentials: map[string]forge.CredentialRef{
			forge.PurposeForgeAPI: {SecretName: "src", Key: "token"},
		},
		Serves: []string{"*"},
	}
	projection := registration.Projection()
	document := &topology.BrokerPolicyDocument{
		PublicationPolicy: &courierv1alpha1.PublicationPolicy{
			ProviderConfigRef:   projection.Name,
			ProviderEndpoint:    projection.Endpoint,
			CredentialRefDigest: projection.Digest,
		},
		Provider: projection,
	}
	env := &brokerEnv{
		namespace: "runs", podName: "r-broker",
		controlSA: "r-control", controlSAUID: "sa-uid",
		providerName: "github", providerType: "github",
		providerEndpt: projection.Endpoint, providerGitTpl: projection.GitTemplate,
		forgeAPIToken: "tok", gitToken: "tok", apiTokenFile: "/tmp/token",
	}
	if err := verifyProjection(env, document); err != nil {
		t.Fatalf("consistent projection must verify: %v", err)
	}

	tampered := env
	tampered.providerEndpt = "https://evil.example.test/"
	if err := verifyProjection(tampered, document); err == nil {
		t.Fatal("a swapped endpoint must refuse service")
	}
	tampered = env
	tampered.providerName = "other"
	if err := verifyProjection(tampered, document); err == nil {
		t.Fatal("a swapped provider name must refuse service")
	}

	// A digest that does not recompute from the projection is rejected.
	badDocument := *document
	badDocument.PublicationPolicy.CredentialRefDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := verifyProjection(env, &badDocument); err == nil {
		t.Fatal("digest mismatch must refuse service")
	}
}

func TestLoadPolicyDocumentIsStrict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	valid := `{"publicationPolicy":{"runUID":"u","providerConfigRef":"github","baseRepo":"a/b","baseRef":"main","baseOID":"x","workRepo":"a/b","workRef":"w"},"provider":{"name":"github","type":"github","endpoint":"https://api.github.com/","gitEndpointTemplate":"https://github.com/%s.git","gitUsername":"","credentials":{"forge-api":{"secretName":"s","key":"k"}},"digest":"d"}}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	document, err := loadPolicyDocument(path)
	if err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if document.Provider.Name != "github" {
		t.Fatalf("provider = %+v", document.Provider)
	}
	if err := os.WriteFile(path, []byte(`{"publicationPolicy":{},"provider":{},"extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPolicyDocument(path); err == nil {
		t.Fatal("unknown fields must be rejected")
	}
}

func TestPinnedResolverOnlyServesPolicyRepositories(t *testing.T) {
	resolver := newPinnedResolver(broker.Policy{BaseRepo: "a/b", WorkRepo: "c/d"},
		forge.Projection{GitTemplate: "https://github.com/%s.git"})
	if endpoint, err := resolver.Endpoint(context.Background(), "a/b"); err != nil || endpoint != "https://github.com/a/b.git" {
		t.Fatalf("base endpoint = %q, %v", endpoint, err)
	}
	if _, err := resolver.Endpoint(context.Background(), "other/repo"); err == nil {
		t.Fatal("a repository outside the policy must have no endpoint")
	}
}

func TestPolicyMatchesDetectsDrift(t *testing.T) {
	persisted := &courierv1alpha1.PublicationPolicy{
		BaseRepo: "a/b", BaseRef: "main", BaseOID: "x",
		WorkRepo: "a/b", WorkRef: "w", WorkInitiallyAbsent: true,
	}
	resolved := broker.Policy{
		BaseRepo: "a/b", BaseRef: "main", BaseOID: "x",
		WorkRepo: "a/b", WorkRef: "w", WorkInitiallyAbsent: true,
		Mode: broker.ModeResolveIssue, Provider: "github", RunUID: "u",
	}
	if err := policyMatches(resolved, persisted); err != nil {
		t.Fatalf("equal policies must match: %v", err)
	}
	resolved.WorkRef = "moved"
	if err := policyMatches(resolved, persisted); err == nil {
		t.Fatal("drifted work ref must be refused")
	}
}
