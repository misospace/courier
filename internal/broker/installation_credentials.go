package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/misospace/courier/internal/topology"
)

// SecretInstallationTokenSource reads the run's InstallationTokenDocument from
// the per-run credentials Secret through a live API reader and supplies the
// current token until just before its forge-reported expiry. The token lives
// only in process memory: it never appears in logs, error strings, or pod
// environment. Any unreadable, malformed, stale-versioned, mismatched, or
// expired document is a refusal — App-mode brokers have no static credential
// to fall back to.
type SecretInstallationTokenSource struct {
	reader           ctrlclient.Reader
	namespace        string
	secretName       string
	documentKey      string
	expectedOwner    string
	expectedEndpoint string
	now              func() time.Time

	mu          sync.Mutex
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
	}, nil
}

// InstallationToken returns the cached token while it remains usable, and
// otherwise re-reads the Secret, parses the document, and validates its binding.
func (s *SecretInstallationTokenSource) InstallationToken(ctx context.Context) (string, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.token != "" && now.Before(s.usableUntil) {
		return s.token, s.expiresAt, nil
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
	if err := json.Unmarshal(data, &document); err != nil {
		return "", time.Time{}, errors.New("broker installation token document is unusable")
	}
	if document.Version != topology.InstallationTokenDocumentVersion || document.Token == "" ||
		!strings.EqualFold(document.Owner, s.expectedOwner) || !sameInstallationEndpoint(document.Endpoint, s.expectedEndpoint) ||
		!document.ExpiresAt.After(now.Add(2*time.Minute)) {
		return "", time.Time{}, errors.New("broker installation token document is unusable")
	}
	s.token = document.Token
	s.expiresAt = document.ExpiresAt
	s.usableUntil = document.ExpiresAt.Add(-2 * time.Minute)
	return s.token, s.expiresAt, nil
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

// InstallationGitCredentials serves git push credentials from the same token
// source: username x-access-token, token re-read from the source per call so
// operator rotation flows through without a broker restart.
type installationTokenSource interface {
	InstallationToken(ctx context.Context) (token string, expiresAt time.Time, err error)
}

type InstallationGitCredentials struct {
	source installationTokenSource
}

func NewInstallationGitCredentials(source installationTokenSource) GitCredentialProvider {
	return InstallationGitCredentials{source: source}
}

func (c InstallationGitCredentials) Credentials(ctx context.Context, _ string) (GitCredentials, error) {
	if c.source == nil {
		return GitCredentials{}, errors.New("broker installation git credentials are unavailable")
	}
	token, _, err := c.source.InstallationToken(ctx)
	if err != nil || token == "" {
		return GitCredentials{}, errors.New("broker installation git credentials are unavailable")
	}
	return GitCredentials{Username: topology.DefaultGitUsername, Token: token}, nil
}

var _ installationTokenSource = (*SecretInstallationTokenSource)(nil)
