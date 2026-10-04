package forge

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
)

// Credential purposes a registration may reference. The git purpose defaults
// to the forge-api reference when omitted, so a deployment that uses one
// credential for both surfaces declares it once.
const (
	PurposeForgeAPI = "forge-api"
	PurposeGit      = "git"
)

// CredentialRef references a Secret by name and key. It is a reference only:
// the value is resolved exclusively inside the broker pod, never by the
// operator, and never appears in logs or diagnostics.
type CredentialRef struct {
	SecretName string `json:"secretName"`
	Key        string `json:"key"`
}

// Registration is one administrator-configured forge provider in the
// deployment-level registry (HARNESS.md §4). It is deliberately not a
// Kubernetes object: a namespaced registration that broader subjects could
// create would let them shadow a provider with their own endpoint and
// credential reference. Capabilities are not configurable here; they come
// from the implementation the Type selects.
type Registration struct {
	// Name is the registry key. The exact string persists as
	// PublicationPolicy.ProviderConfigRef and the broker requires byte-exact
	// equality with its own projection.
	Name string `json:"name"`
	// Type selects the provider implementation. The first is "github".
	Type string `json:"type"`
	// Endpoint is the canonical API base. No request ever names an endpoint;
	// this registration is also the sole source of the git push endpoint.
	Endpoint string `json:"endpoint"`
	// GitEndpoint optionally overrides the derived git push endpoint as an
	// URL template containing one %s placeholder for the canonical
	// owner/name. Empty means the implementation derives it from Endpoint.
	// +optional
	GitEndpoint string `json:"gitEndpoint,omitempty"`
	// GitUsername is the plain (non-secret) username for token-based HTTPS
	// git pushes. Empty defaults to "x-access-token".
	// +optional
	GitUsername string `json:"gitUsername,omitempty"`
	// Credentials maps purpose → secret reference. PurposeGit falls back to
	// the PurposeForgeAPI reference when omitted.
	Credentials map[string]CredentialRef `json:"credentials"`
	// Serves holds case-insensitive owner/name patterns (for example
	// "misospace/*") selecting which runs this registration resolves.
	// Patterns are selection configuration, never a destination policy.
	Serves []string `json:"serves"`
}

// Registry is the loaded, structurally validated provider registry.
type Registry struct {
	registrations []Registration
}

type registryFile struct {
	Providers []Registration `json:"providers"`
}

// Load parses and validates the registry file. Structural problems fail
// outright at operator startup; semantic problems such as an unreachable
// endpoint or a missing Secret are admission-time failures, not load-time
// ones.
func Load(data []byte, validTypes ...string) (*Registry, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var file registryFile
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("forge registry: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("forge registry: trailing data after providers object")
	}
	types := make(map[string]struct{}, len(validTypes))
	for _, t := range validTypes {
		types[t] = struct{}{}
	}
	seen := make(map[string]struct{}, len(file.Providers))
	out := &Registry{registrations: make([]Registration, 0, len(file.Providers))}
	for i := range file.Providers {
		registration := file.Providers[i]
		if err := validateRegistration(&registration, types); err != nil {
			return nil, fmt.Errorf("forge registry: provider %d: %w", i, err)
		}
		if _, dup := seen[registration.Name]; dup {
			return nil, fmt.Errorf("forge registry: duplicate provider name %q", registration.Name)
		}
		seen[registration.Name] = struct{}{}
		out.registrations = append(out.registrations, registration)
	}
	if len(out.registrations) == 0 {
		return nil, errors.New("forge registry: no providers registered")
	}
	return out, nil
}

func validateRegistration(r *Registration, validTypes map[string]struct{}) error {
	if strings.TrimSpace(r.Name) != r.Name || r.Name == "" {
		return errors.New("name must be non-empty with no surrounding whitespace")
	}
	if strings.ContainsAny(r.Name, "\x00\r\n") {
		return errors.New("name must not contain control characters")
	}
	if _, ok := validTypes[r.Type]; !ok {
		return fmt.Errorf("unknown provider type %q", r.Type)
	}
	if err := validateEndpoint(r.Endpoint); err != nil {
		return err
	}
	if r.GitEndpoint != "" {
		if strings.Count(r.GitEndpoint, "%s") != 1 {
			return errors.New("gitEndpoint template must contain exactly one %s placeholder")
		}
	}
	if strings.TrimSpace(r.GitUsername) != r.GitUsername || strings.ContainsAny(r.GitUsername, "\x00\r\n") {
		return errors.New("gitUsername must be plain text with no surrounding whitespace")
	}
	if len(r.Serves) == 0 {
		return errors.New("at least one serves pattern is required")
	}
	for _, pattern := range r.Serves {
		if err := validateServesPattern(pattern); err != nil {
			return err
		}
	}
	if _, ok := r.Credentials[PurposeForgeAPI]; !ok {
		return fmt.Errorf("credentials must define the %q purpose", PurposeForgeAPI)
	}
	for purpose, ref := range r.Credentials {
		if purpose != PurposeForgeAPI && purpose != PurposeGit {
			return fmt.Errorf("unknown credential purpose %q", purpose)
		}
		if ref.SecretName == "" || ref.Key == "" {
			return fmt.Errorf("credential purpose %q requires secretName and key", purpose)
		}
	}
	return nil
}

func validateEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("endpoint %q is not an absolute URL without userinfo, query, or fragment", raw)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("endpoint %q must use https", raw)
	}
	return nil
}

// validateServesPattern accepts exactly owner/name where either half may be
// the wildcard *, or the bare * that matches any repository. Case is
// insignificant and normalized away at match time.
func validateServesPattern(pattern string) error {
	if pattern == "*" {
		return nil
	}
	parts := strings.Split(pattern, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("serves pattern %q must be owner/name", pattern)
	}
	return nil
}

// Registrations returns the loaded registrations in file order, for
// deployment-level validation such as the legacy-taint check.
func (r *Registry) Registrations() []Registration {
	if r == nil {
		return nil
	}
	return r.registrations
}

// Select returns the unique registration whose serves patterns match the
// run's immutable spec.repo. Zero matches or more than one is a fail-closed
// selection error; the caller turns it into an actionable NeedsHuman. The
// spec repo string is never widened or rewritten by selection.
func (r *Registry) Select(repo string) (*Registration, error) {
	if r == nil {
		return nil, errors.New("forge registry is not loaded")
	}
	var matches []*Registration
	for i := range r.registrations {
		if registrationMatches(&r.registrations[i], repo) {
			matches = append(matches, &r.registrations[i])
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return nil, fmt.Errorf("no provider registration serves repository %q", repo)
	default:
		names := make([]string, 0, len(matches))
		for _, match := range matches {
			names = append(names, match.Name)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("repository %q is served ambiguously by provider registrations %s", repo, strings.Join(names, ", "))
	}
}

func registrationMatches(r *Registration, repo string) bool {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, pattern := range r.Serves {
		if pattern == "*" {
			return true
		}
		want := strings.Split(pattern, "/")
		if len(want) != 2 {
			continue
		}
		if patternHalf(want[0], parts[0]) && patternHalf(want[1], parts[1]) {
			return true
		}
	}
	return false
}

func patternHalf(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	return strings.EqualFold(pattern, value)
}

// CredentialRef resolves the reference for one purpose. PurposeGit falls
// back to the PurposeForgeAPI reference when omitted.
func (r *Registration) CredentialRef(purpose string) (CredentialRef, bool) {
	if r == nil {
		return CredentialRef{}, false
	}
	if ref, ok := r.Credentials[purpose]; ok {
		return ref, true
	}
	if purpose == PurposeGit {
		ref, ok := r.Credentials[PurposeForgeAPI]
		return ref, ok
	}
	return CredentialRef{}, false
}

// GitEndpointTemplate returns the push endpoint template for canonical
// owner/name: the explicit override when configured, otherwise the
// implementation default derived from the registration's endpoint. The
// registration is the sole source of the git push endpoint; request- or
// model-supplied URLs are never endpoints.
func (r *Registration) GitEndpointTemplate() string {
	if r == nil {
		return ""
	}
	if r.GitEndpoint != "" {
		return r.GitEndpoint
	}
	return derivedGitEndpoint(r.Type, r.Endpoint)
}

// derivedGitEndpoint maps a canonical API base to the git host of the same
// provider: github.com's API base api.github.com maps to
// https://github.com/%s.git, and a GitHub Enterprise base
// https://<host>/api/v3 maps to https://<host>/%s.git.
func derivedGitEndpoint(typ, endpoint string) string {
	if typ != "github" {
		return ""
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return ""
	}
	host := u.Host
	switch host {
	case "api.github.com":
		host = "github.com"
	default:
		host = u.Hostname()
		if host == "" {
			return ""
		}
	}
	return "https://" + host + "/%s.git"
}

// Projection is the single registration rendered into one run's broker pod:
// exactly one provider, frozen at pod creation. The whole registry never
// enters a broker pod.
type Projection struct {
	Name        string                   `json:"name"`
	Type        string                   `json:"type"`
	Endpoint    string                   `json:"endpoint"`
	GitTemplate string                   `json:"gitEndpointTemplate"`
	GitUsername string                   `json:"gitUsername"`
	Credentials map[string]CredentialRef `json:"credentials"`
	// Digest is the SHA-256 over the canonical projection bytes. The
	// persisted PublicationPolicy records it so the broker can verify its
	// projection against the policy it binds and refuse service on mismatch.
	Digest string `json:"digest"`
}

// Projection returns the broker projection of this registration with its
// digest. It never contains a resolved credential value — references only.
func (r *Registration) Projection() Projection {
	p := Projection{
		Name:        r.Name,
		Type:        r.Type,
		Endpoint:    r.Endpoint,
		GitTemplate: r.GitEndpointTemplate(),
		GitUsername: r.GitUsername,
		Credentials: make(map[string]CredentialRef, len(r.Credentials)),
	}
	for purpose, ref := range r.Credentials {
		p.Credentials[purpose] = ref
	}
	p.Digest = ProjectionDigest(p)
	return p
}

// ProjectionDigest computes the stable SHA-256 identity of a projection:
// every field except the digest itself, in a fixed order. Selection and
// enforcement are provably the same record because both sides compare this
// digest.
func ProjectionDigest(p Projection) string {
	purposes := make([]string, 0, len(p.Credentials))
	for purpose := range p.Credentials {
		purposes = append(purposes, purpose)
	}
	sort.Strings(purposes)
	var b strings.Builder
	b.WriteString(p.Name)
	b.WriteByte(0)
	b.WriteString(p.Type)
	b.WriteByte(0)
	b.WriteString(p.Endpoint)
	b.WriteByte(0)
	b.WriteString(p.GitTemplate)
	b.WriteByte(0)
	b.WriteString(p.GitUsername)
	for _, purpose := range purposes {
		ref := p.Credentials[purpose]
		b.WriteByte(0)
		b.WriteString(purpose)
		b.WriteByte(0)
		b.WriteString(ref.SecretName)
		b.WriteByte(0)
		b.WriteString(ref.Key)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
