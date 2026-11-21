package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/misospace/courier/internal/topology"
)

// SecretInstallationTokenSource reads the run's InstallationTokenDocument from
// the per-run credentials Secret through a live API reader and supplies, per
// named repository, the current token minted for that repository until just
// before its forge-reported expiry. The operator mints one token per pinned
// repository with only that repository's permissions, so a request against
// one repository can never carry a token scoped to another: a repository
// with no entry in the document is a refusal. The tokens live only in process
// memory: they never appear in logs, error strings, or pod environment. Any
// unreadable, malformed, stale-versioned, mismatched, or expired document is
// a refusal — App-mode brokers have no static credential to fall back to.
type SecretInstallationTokenSource struct {
	reader           ctrlclient.Reader
	namespace        string
	secretName       string
	documentKey      string
	expectedOwner    string
	expectedEndpoint string
	now              func() time.Time

	mu      sync.Mutex
	entries map[string]cachedInstallationToken
}

type cachedInstallationToken struct {
	token       string
	expiresAt   time.Time
	usableUntil time.Time
}

// NewSecretInstallationTokenSource validates expectations once (expectedOwner
// and expectedEndpoint non-empty, reader non-nil) and returns a source bound
// to the named Secret and document key. now defaults to time.Now.
func NewSecretInstallationTokenSource(reader ctrlclient.Reader, namespace, secretName, documentKey, expectedOwner, expectedEndpoint string, now func() time.Time) (*SecretInstallationTokenSource, error) {
	if reader == nil || namespace == "" || secretName == "" || documentKey == "" || expectedOwner == "" || expectedEndpoint == "" {
		return nil, errors.New("broker installation token source: reader and document expectations are required")
	}
	if now == nil {
		now = time.Now
	}
	return &SecretInstallationTokenSource{
		reader: reader, namespace: namespace, secretName: secretName,
		documentKey: documentKey, expectedOwner: expectedOwner,
		expectedEndpoint: expectedEndpoint, now: now,
		entries: make(map[string]cachedInstallationToken),
	}, nil
}

// InstallationToken returns the cached token for the named repository while it
// remains usable, and otherwise re-reads the Secret, parses the document, and
// validates its binding. A repository with no entry in the document is a
// refusal, never a fall-back to another entry.
func (s *SecretInstallationTokenSource) InstallationToken(ctx context.Context, repository string) (string, time.Time, error) {
	if repository == "" {
		return "", time.Time{}, errors.New("broker installation token document is unusable")
	}
	key := strings.ToLower(repository)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if cached, ok := s.entries[key]; ok && now.Before(cached.usableUntil) {
		return cached.token, cached.expiresAt, nil
	}

	secret := &corev1.Secret{}
	if err := s.reader.Get(ctx, ctrlclient.ObjectKey{Namespace: s.namespace, Name: s.secretName}, secret); err != nil {
		return "", time.Time{}, errors.New("broker installation token document is unavailable")
	}
	data, ok := secret.Data[s.documentKey]
	if !ok {
		return "", time.Time{}, errors.New("broker installation token document is unavailable")
	}
	var document topology.InstallationTokenDocument
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil || !documentStreamComplete(decoder) {
		return "", time.Time{}, errors.New("broker installation token document is unusable")
	}
	if document.Version != topology.InstallationTokenDocumentVersion ||
		!strings.EqualFold(document.Owner, s.expectedOwner) || !sameInstallationEndpoint(document.Endpoint, s.expectedEndpoint) {
		return "", time.Time{}, errors.New("broker installation token document is unusable")
	}
	entry, ok := document.Entry(repository)
	if !ok || entry.Token == "" || !entry.ExpiresAt.After(now.Add(2*time.Minute)) {
		return "", time.Time{}, errors.New("broker installation token document is unusable")
	}
	s.entries[key] = cachedInstallationToken{
		token: entry.Token, expiresAt: entry.ExpiresAt, usableUntil: entry.ExpiresAt.Add(-2 * time.Minute),
	}
	return entry.Token, entry.ExpiresAt, nil
}

// documentStreamComplete reports that the decoder consumed exactly one JSON
// value: More() alone misses a lone trailing bracket, so the next decode must
// fail with io.EOF rather than a parse error or another value.
func documentStreamComplete(decoder *json.Decoder) bool {
	if decoder.More() {
		return false
	}
	var extra json.RawMessage
	return errors.Is(decoder.Decode(&extra), io.EOF)
}

func sameInstallationEndpoint(actual, expected string) bool {
	actualURL, actualErr := url.Parse(actual)
	expectedURL, expectedErr := url.Parse(expected)
	if actualErr != nil || expectedErr != nil || actualURL.Scheme != "https" || expectedURL.Scheme != "https" ||
		actualURL.Host == "" || expectedURL.Host == "" || actualURL.User != nil || expectedURL.User != nil ||
		actualURL.RawQuery != "" || expectedURL.RawQuery != "" || actualURL.Fragment != "" || expectedURL.Fragment != "" ||
		actualURL.Opaque != "" || expectedURL.Opaque != "" {
		return false
	}
	actualURL.Host = strings.ToLower(strings.TrimSuffix(actualURL.Host, "/"))
	expectedURL.Host = strings.ToLower(strings.TrimSuffix(expectedURL.Host, "/"))
	actualURL.Path = strings.TrimSuffix(actualURL.Path, "/")
	expectedURL.Path = strings.TrimSuffix(expectedURL.Path, "/")
	return actualURL.Scheme == expectedURL.Scheme && actualURL.Host == expectedURL.Host && actualURL.Path == expectedURL.Path
}

// installationTokenSource supplies the installation token minted for one
// named repository, re-read on demand so operator rotation flows through
// without a broker restart.
type installationTokenSource interface {
	InstallationToken(ctx context.Context, repository string) (token string, expiresAt time.Time, err error)
}

// InstallationGitCredentials serves git fetch and push credentials from the
// same token source: username x-access-token, the token minted for exactly
// the repository being asked for, re-read from the source per call so
// operator rotation flows through without a broker restart. Credentials are
// served only for the repositories the provider was bound to at startup — the
// run's pinned base and work set; any other identity is refused before the
// token source is consulted, so a token scoped to one repository can never
// accompany a fetch or push asked for under a different name.
type InstallationGitCredentials struct {
	source       installationTokenSource
	repositories []string
}

func NewInstallationGitCredentials(source installationTokenSource, repositories []string) GitCredentialProvider {
	bound := make([]string, 0, len(repositories))
	for _, repository := range repositories {
		if repository != "" {
			bound = append(bound, repository)
		}
	}
	return InstallationGitCredentials{source: source, repositories: bound}
}

func (c InstallationGitCredentials) Credentials(ctx context.Context, repository string) (GitCredentials, error) {
	pinned := ""
	if c.source != nil && repository != "" {
		for _, bound := range c.repositories {
			if strings.EqualFold(repository, bound) {
				pinned = bound
				break
			}
		}
	}
	if pinned == "" {
		return GitCredentials{}, errors.New("broker installation git credentials are unavailable")
	}
	token, _, err := c.source.InstallationToken(ctx, pinned)
	if err != nil || token == "" {
		return GitCredentials{}, errors.New("broker installation git credentials are unavailable")
	}
	return GitCredentials{Username: topology.DefaultGitUsername, Token: token}, nil
}

var _ installationTokenSource = (*SecretInstallationTokenSource)(nil)
