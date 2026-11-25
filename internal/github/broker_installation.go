package github

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// InstallationTokenSource supplies the current GitHub App installation token
// minted for one named repository (full owner/name identity, compared
// case-insensitively) for broker forge reads and writes. Implementations
// refresh on expiry and refuse a repository outside the run's pinned set, so
// a request is never accompanied by a token scoped to a different repository.
// A returned token is short-lived and must never be embedded in error strings
// or logs.
type InstallationTokenSource interface {
	InstallationToken(ctx context.Context, repository string) (token string, expiresAt time.Time, err error)
}

// BrokerInstallationAuth is the non-secret identity a broker installation
// client is bound to. The broker never holds the App private key; the token
// source supplies short-lived tokens minted by the trusted operator for one
// repository at a time, and the client names the request's pinned repository
// when it asks. When Repositories is non-empty, the client additionally
// refuses any repository outside the pinned set, so a same-owner repository
// the run's policy does not name is refused before a token is ever spent.
type BrokerInstallationAuth struct {
	Owner        string
	Repositories []string
	Endpoint     string
	HTTPClient   *http.Client
	// ReadOnly refuses a write method before any token is spent: the transport
	// serves only GET and HEAD and the client's API layer only GET, so a
	// model-facing read path cannot reach a write-capable request even if its
	// route vocabulary regresses.
	ReadOnly bool
}

// NewBrokerInstallationClient returns a client scoped to one installation
// owner and API endpoint, with credentials supplied on demand: write-capable
// unless auth.ReadOnly restricts it to reads.
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
		owner, name, ok := strings.Cut(repository, "/")
		if !ok || owner == "" || name == "" || strings.Contains(name, "/") || !strings.EqualFold(owner, auth.Owner) {
			return nil, errors.New("GitHub App credential pinned repositories must be owner/name entries of the configured account")
		}
		pinned = append(pinned, repository)
	}
	transport := brokerInstallationDoer{endpoint: endpoint, owner: strings.TrimSpace(auth.Owner), repositories: pinned, source: source, client: auth.HTTPClient, readOnly: auth.ReadOnly}
	if transport.client == nil {
		transport.client = &http.Client{Timeout: defaultRequestTimeout}
	}
	client, err := NewClient(endpoint.String(), "", transport)
	if err != nil {
		return nil, err
	}
	client.endpointBound = true
	client.appInstallation = true
	client.readOnly = auth.ReadOnly
	return client, nil
}

type brokerInstallationDoer struct {
	endpoint     *url.URL
	owner        string
	repositories []string
	source       InstallationTokenSource
	client       *http.Client
	readOnly     bool
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
			if strings.EqualFold(repository, owner+"/"+repo) {
				pinned = true
				break
			}
		}
		if !pinned {
			return nil, errors.New("GitHub App credential requires a repository pinned by this run's policy")
		}
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead:
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		if d.readOnly {
			return nil, errors.New("GitHub App read-scoped credential refused a write request")
		}
	default:
		return nil, errors.New("GitHub App credential refused an unsupported request method")
	}
	// The source is asked by full owner/name identity so it can serve the
	// token minted for exactly this repository; the identity compared here is
	// already pinned to d.owner above.
	token, _, err := d.source.InstallationToken(req.Context(), owner+"/"+repo)
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
