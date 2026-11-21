package github

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// InstallationTokenSource supplies the current repository-scoped GitHub App
// installation token for broker forge reads and writes. Implementations
// refresh on expiry; a returned token is short-lived and must never be
// embedded in error strings or logs.
type InstallationTokenSource interface {
	InstallationToken(ctx context.Context) (token string, expiresAt time.Time, err error)
}

// BrokerInstallationAuth is the non-secret identity a broker installation
// client is bound to. The broker never holds the App private key; the token
// source supplies short-lived tokens minted by the trusted operator. When
// Repositories is non-empty, the client additionally refuses any repository
// outside the pinned set, so a same-owner repository the run's policy does
// not name is refused before a token is ever spent.
type BrokerInstallationAuth struct {
	Owner        string
	Repositories []string
	Endpoint     string
	HTTPClient   *http.Client
}

// NewBrokerInstallationClient returns a write-capable client scoped to one
// installation owner and API endpoint, with credentials supplied on demand.
func NewBrokerInstallationClient(auth BrokerInstallationAuth, source InstallationTokenSource) (*Client, error) {
	if strings.TrimSpace(auth.Owner) == "" {
		return nil, errors.New("GitHub App authentication requires an owner")
	}
	endpoint, err := url.Parse(strings.TrimSpace(auth.Endpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Opaque != "" {
		return nil, errors.New("GitHub App endpoint must be an absolute HTTPS URL without userinfo, query, or fragment")
	}
	if source == nil {
		return nil, errors.New("GitHub App installation token source is required")
	}
	pinned := make([]string, 0, len(auth.Repositories))
	for _, repository := range auth.Repositories {
		owner, _, ok := strings.Cut(repository, "/")
		if !ok || owner == "" || strings.TrimSpace(repository) == "" || !strings.EqualFold(owner, auth.Owner) {
			return nil, errors.New("GitHub App credential pinned repositories must be owner/name entries of the configured account")
		}
		pinned = append(pinned, repository)
	}
	transport := brokerInstallationDoer{endpoint: endpoint, owner: strings.TrimSpace(auth.Owner), repositories: pinned, source: source, client: auth.HTTPClient}
	if transport.client == nil {
		transport.client = &http.Client{Timeout: defaultRequestTimeout}
	}
	client, err := NewClient(endpoint.String(), "", transport)
	if err != nil {
		return nil, err
	}
	client.endpointBound = true
	client.appInstallation = true
	return client, nil
}

type brokerInstallationDoer struct {
	endpoint     *url.URL
	owner        string
	repositories []string
	source       InstallationTokenSource
	client       *http.Client
}

func (d brokerInstallationDoer) Do(req *http.Request) (*http.Response, error) {
	path := req.URL.EscapedPath()
	basePath := endpointPath(d.endpoint)
	if req.URL.Scheme != d.endpoint.Scheme || !strings.EqualFold(req.URL.Host, d.endpoint.Host) || (basePath != "" && path != basePath && !strings.HasPrefix(path, basePath+"/")) || hasDotPathSegment(path) {
		return nil, errors.New("GitHub App credential requires a request bound to its API endpoint")
	}
	owner, repo, scoped := repositoryFromPath(path, basePath)
	if !scoped || !strings.EqualFold(owner, d.owner) {
		return nil, errors.New("GitHub App credential requires a repository owned by its configured installation account")
	}
	if len(d.repositories) > 0 {
		pinned := false
		for _, repository := range d.repositories {
			if _, name, _ := strings.Cut(repository, "/"); strings.EqualFold(name, repo) {
				pinned = true
				break
			}
		}
		if !pinned {
			return nil, errors.New("GitHub App credential requires a repository pinned by this run's policy")
		}
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return nil, errors.New("GitHub App credential refused an unsupported request method")
	}
	token, _, err := d.source.InstallationToken(req.Context())
	if err != nil {
		return nil, err
	}
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+token)
	client := *d.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(clone)
	if err != nil || resp == nil {
		return nil, &transportError{err: errors.New("broker installation token request failed")}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, installationAPIError(resp)
	}
	return resp, nil
}
