package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type InstallationAuth struct {
	AppID            int64
	InstallationID   int64
	Owner            string
	PrivateKey       *rsa.PrivateKey
	PrivateKeySecret string
	Endpoint         string
	HTTPClient       *http.Client
	Now              func() time.Time
}

// InstallationTokenMinter exchanges the operator-held GitHub App private key for short-lived repository-scoped installation tokens. It stays inside the trusted operator process; brokers receive only minted tokens, never the key or any signing capability.
type InstallationTokenMinter struct{ credential *installationCredential }

// NewInstallationTokenMinter validates auth exactly like NewInstallationClient and returns a minter over the same installation identity.
func NewInstallationTokenMinter(auth InstallationAuth) (*InstallationTokenMinter, error) {
	if auth.AppID <= 0 || auth.InstallationID <= 0 || auth.PrivateKey == nil || strings.TrimSpace(auth.Owner) == "" {
		return nil, errors.New("GitHub App authentication requires positive IDs, an owner, and a private key")
	}
	base, err := url.Parse(strings.TrimSpace(auth.Endpoint))
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Opaque != "" {
		return nil, errors.New("GitHub App endpoint must be an absolute HTTPS URL without userinfo, query, or fragment")
	}
	httpClient := auth.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	if auth.Now == nil {
		auth.Now = time.Now
	}
	return &InstallationTokenMinter{credential: &installationCredential{
		appID: auth.AppID, installationID: auth.InstallationID, owner: strings.TrimSpace(auth.Owner), privateKey: auth.PrivateKey,
		endpoint: base, client: httpClient, now: auth.Now,
	}}, nil
}

// Identity reports the installation owner and API endpoint this minter signs
// for. The launcher compares them against a run's pinned owner and
// registration endpoint before minting, so a token is never minted for one
// identity and presented to another.
func (m *InstallationTokenMinter) Identity() (owner, endpoint string) {
	return m.credential.owner, m.credential.endpoint.String()
}

// Mint exchanges an app assertion for a token restricted to the requested repositories and permissions.
func (m *InstallationTokenMinter) Mint(ctx context.Context, repositories []string, permissions map[string]string) (string, time.Time, error) {
	c := m.credential
	if len(repositories) == 0 {
		return "", time.Time{}, errors.New("GitHub App token mint requires repositories")
	}
	for _, repository := range repositories {
		parts := strings.Split(repository, "/")
		if len(parts) != 2 || parts[0] == "" || !strings.EqualFold(parts[0], c.owner) || parts[1] == "" {
			return "", time.Time{}, errors.New("GitHub App token mint requires repositories owned by the configured account")
		}
	}
	if len(permissions) == 0 {
		return "", time.Time{}, errors.New("GitHub App token mint requires permissions")
	}
	for _, level := range permissions {
		if level != "read" && level != "write" {
			return "", time.Time{}, errors.New("GitHub App token mint requires read or write permissions")
		}
	}
	now := c.now()
	if err := c.verifyInstallation(ctx, now); err != nil {
		return "", time.Time{}, err
	}
	jwt, err := c.appJWT(now)
	if err != nil {
		return "", time.Time{}, errors.New("GitHub App authentication could not create an app assertion")
	}
	endpoint := *c.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/app/installations/" + strconv.FormatInt(c.installationID, 10) + "/access_tokens"
	endpoint.RawPath = ""
	body, _ := json.Marshal(struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{repositories, permissions})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, errors.New("GitHub App token mint request could not be created")
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+jwt)
	request.Header.Set("Content-Type", "application/json")
	resp, err := c.doAppRequest(request)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	var result struct {
		Token       string            `json:"token"`
		ExpiresAt   time.Time         `json:"expires_at"`
		Permissions map[string]string `json:"permissions"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil || strings.TrimSpace(result.Token) == "" || !result.ExpiresAt.After(now) || result.Permissions == nil {
		return "", time.Time{}, errors.New("GitHub App installation token mint returned an invalid response")
	}
	for permission, level := range result.Permissions {
		requested, known := permissions[permission]
		if !known {
			if level != "read" {
				return "", time.Time{}, errors.New("GitHub App installation token mint returned excessive permissions")
			}
			fmt.Fprintf(os.Stderr, "GitHub App installation token mint returned unrequested read permission %q\n", permission)
			continue
		}
		if requested != level {
			return "", time.Time{}, errors.New("GitHub App installation token mint returned insufficient permissions")
		}
	}
	for permission, level := range permissions {
		if result.Permissions[permission] != level {
			return "", time.Time{}, errors.New("GitHub App installation token mint returned insufficient permissions")
		}
	}
	return result.Token, result.ExpiresAt, nil
}

type installationCredential struct {
	mu             sync.Mutex
	appID          int64
	installationID int64
	owner          string
	privateKey     *rsa.PrivateKey
	endpoint       *url.URL
	client         *http.Client
	now            func() time.Time
	cache          map[string]installationToken
	inflight       map[string]*tokenRequest
	installation   installationResult
	installPending *tokenRequest
}

type installationResult struct {
	checked bool
	err     error
	until   time.Time
}

type tokenRequest struct {
	done      chan struct{}
	value     string
	expiresAt time.Time
	err       error
}

type installationToken struct {
	value     string
	expiresAt time.Time
	err       error
}

const negativeCacheDuration = 5 * time.Second

func NewInstallationClient(auth InstallationAuth) (*Client, error) {
	if auth.AppID <= 0 || auth.InstallationID <= 0 || auth.PrivateKey == nil || strings.TrimSpace(auth.Owner) == "" {
		return nil, errors.New("GitHub App authentication requires positive IDs, an owner, and a private key")
	}
	base, err := url.Parse(strings.TrimSpace(auth.Endpoint))
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Opaque != "" {
		return nil, errors.New("GitHub App endpoint must be an absolute HTTPS URL without userinfo, query, or fragment")
	}
	httpClient := auth.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	if auth.Now == nil {
		auth.Now = time.Now
	}
	credential := &installationCredential{
		appID: auth.AppID, installationID: auth.InstallationID, owner: strings.TrimSpace(auth.Owner), privateKey: auth.PrivateKey,
		endpoint: base, client: httpClient, now: auth.Now, cache: make(map[string]installationToken), inflight: make(map[string]*tokenRequest),
	}
	client, err := NewClient(base.String(), "", credential)
	if err != nil {
		return nil, err
	}
	client.readOnly = true
	client.endpointBound = true
	client.appInstallation = true
	return client, nil
}

func (c *installationCredential) Do(req *http.Request) (*http.Response, error) {
	basePath := endpointPath(c.endpoint)
	requestPath := req.URL.EscapedPath()
	if req.Method != http.MethodGet || req.URL.Scheme != c.endpoint.Scheme || !strings.EqualFold(req.URL.Host, c.endpoint.Host) || (basePath != "" && requestPath != basePath && !strings.HasPrefix(requestPath, basePath+"/")) || hasDotPathSegment(requestPath) {
		return nil, errors.New("GitHub App credential requires a read-only request bound to its API endpoint")
	}
	owner, repo, repoScoped := repositoryFromPath(req.URL.EscapedPath(), endpointPath(c.endpoint))
	if !repoScoped || !strings.EqualFold(owner, c.owner) {
		return nil, errors.New("GitHub App credential requires a repository owned by its configured installation account")
	}
	token, err := c.token(req.Context(), owner+"/"+repo)
	if err != nil {
		return nil, err
	}
	return c.appRequest(req, token)
}

func (c *installationCredential) appRequest(req *http.Request, token string) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+token)
	client := *c.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(clone)
	if err != nil || resp == nil {
		return nil, &transportError{err: errors.New("GitHub App API request failed")}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, installationAPIError(resp)
	}
	return resp, nil
}

func (c *installationCredential) token(ctx context.Context, repository string) (string, error) {
	now := c.now()
	c.mu.Lock()
	if cached, ok := c.cache[repository]; ok && now.Before(cached.expiresAt) {
		c.mu.Unlock()
		return cached.value, cached.err
	}
	if pending := c.inflight[repository]; pending != nil {
		c.mu.Unlock()
		select {
		case <-pending.done:
			return pending.value, pending.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	pending := &tokenRequest{done: make(chan struct{})}
	c.inflight[repository] = pending
	c.mu.Unlock()

	pending.value, pending.expiresAt, pending.err = c.refreshToken(ctx, repository, now)
	c.mu.Lock()
	if pending.err == nil {
		c.cache[repository] = installationToken{value: pending.value, expiresAt: pending.expiresAt.Add(-time.Minute)}
	} else {
		c.cache[repository] = installationToken{expiresAt: c.now().Add(negativeCacheDuration), err: pending.err}
	}
	delete(c.inflight, repository)
	close(pending.done)
	c.mu.Unlock()
	return pending.value, pending.err
}

func (c *installationCredential) refreshToken(ctx context.Context, repository string, now time.Time) (string, time.Time, error) {
	if err := c.verifyInstallation(ctx, now); err != nil {
		return "", time.Time{}, err
	}
	jwt, err := c.appJWT(now)
	if err != nil {
		return "", time.Time{}, errors.New("GitHub App authentication could not create an app assertion")
	}
	endpoint := *c.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/app/installations/" + strconv.FormatInt(c.installationID, 10) + "/access_tokens"

	endpoint.RawPath = ""
	parts := strings.SplitN(repository, "/", 2)
	// The requested set is the read surface the observation and admission
	// reads exercise. administration read is not optional: the secure
	// admission protection read walks the branch-rules and branch-protection
	// endpoints, which GitHub gates on the Administration repository
	// permission.
	requested := requestedReadPermissions()
	body, _ := json.Marshal(struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{[]string{parts[1]}, requested})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, errors.New("GitHub App authentication request could not be created")
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+jwt)
	request.Header.Set("Content-Type", "application/json")
	resp, err := c.doAppRequest(request)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	var result struct {
		Token       string            `json:"token"`
		ExpiresAt   time.Time         `json:"expires_at"`
		Permissions map[string]string `json:"permissions"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil || strings.TrimSpace(result.Token) == "" || !result.ExpiresAt.After(now) || result.Permissions == nil {
		return "", time.Time{}, errors.New("GitHub App installation token refresh returned an invalid response")
	}
	for permission, level := range result.Permissions {
		if level != "read" {
			return "", time.Time{}, errors.New("GitHub App installation token refresh returned excessive permissions")
		}
		if _, known := requested[permission]; !known {
			// A permission key outside the requested set is refused when it
			// carries write access; for read access it is tolerated rather
			// than rejected so GitHub introducing a new read-only key does
			// not break token refresh.
			fmt.Fprintf(os.Stderr, "GitHub App installation token refresh for %q returned unrequested read permission %q\n", repository, permission)
		}
	}
	for permission := range requested {
		if result.Permissions[permission] != "read" {
			return "", time.Time{}, errors.New("GitHub App installation token refresh returned insufficient read permissions")
		}
	}
	return result.Token, result.ExpiresAt, nil
}

// requestedReadPermissions is the permission set the token request asks for
// and the refresh response must carry. It builds a fresh map per call so no
// caller can mutate shared state.
func requestedReadPermissions() map[string]string {
	return map[string]string{
		"administration": "read", "checks": "read", "contents": "read",
		"issues": "read", "metadata": "read", "pull_requests": "read",
		"statuses": "read",
	}
}

func (c *installationCredential) verifyInstallation(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	if c.installation.checked && now.Before(c.installation.until) {
		err := c.installation.err
		c.mu.Unlock()
		return err
	}
	if pending := c.installPending; pending != nil {
		c.mu.Unlock()
		select {
		case <-pending.done:
			return pending.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	pending := &tokenRequest{done: make(chan struct{})}
	c.installPending = pending
	c.mu.Unlock()
	pending.err = c.lookupInstallation(ctx, now)
	c.mu.Lock()
	cacheDuration := 5 * time.Minute
	if pending.err != nil {
		cacheDuration = negativeCacheDuration
	}
	c.installation = installationResult{checked: true, err: pending.err, until: c.now().Add(cacheDuration)}
	c.installPending = nil
	close(pending.done)
	c.mu.Unlock()
	return pending.err
}

func (c *installationCredential) lookupInstallation(ctx context.Context, now time.Time) error {
	jwt, err := c.appJWT(now)
	if err != nil {
		return errors.New("GitHub App authentication could not create an app assertion")
	}
	endpoint := *c.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/app/installations/" + strconv.FormatInt(c.installationID, 10)
	endpoint.RawPath = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return errors.New("GitHub App installation lookup request could not be created")
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+jwt)
	resp, err := c.doAppRequest(request)
	if err != nil {
		return err
	}
	var result struct {
		Account struct {
			Login string `json:"login"`
		} `json:"account"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
	_ = resp.Body.Close()
	if decodeErr != nil || strings.TrimSpace(result.Account.Login) == "" || !strings.EqualFold(result.Account.Login, c.owner) {
		err = errors.New("GitHub App installation account does not match the configured repository owner")
	}
	return err
}

func (c *installationCredential) doAppRequest(request *http.Request) (*http.Response, error) {
	client := *c.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(request)
	if err != nil || resp == nil {
		return nil, &transportError{err: errors.New("GitHub App authentication request failed")}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		verdict := installationAPIError(resp)
		return nil, &installationExchangeError{status: verdict.StatusCode, transient: verdict.TransientAvailability()}
	}
	return resp, nil
}

type installationExchangeError struct {
	status    int
	transient bool
}

func (e *installationExchangeError) Error() string {
	return fmt.Sprintf("GitHub App authentication returned HTTP %d", e.status)
}

func (e *installationExchangeError) TransientAvailability() bool { return e.transient }

func installationAPIError(resp *http.Response) *APIError {
	defer resp.Body.Close()
	err := &APIError{StatusCode: resp.StatusCode}
	if resp.StatusCode == http.StatusForbidden {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		probe := &APIError{StatusCode: resp.StatusCode, Body: string(body)}
		if resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "" || probe.TransientAvailability() {
			err.Message = "rate limit reached"
		}
	}
	return err
}

func (c *installationCredential) appJWT(now time.Time) (string, error) {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]int64{"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": c.appID})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, c.privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func hasDotPathSegment(requestPath string) bool {
	for _, segment := range strings.Split(requestPath, "/") {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "." || decoded == ".." {
			return true
		}
	}
	return false
}

func repositoryFromPath(requestPath, basePath string) (string, string, bool) {
	if hasDotPathSegment(requestPath) {
		return "", "", false
	}
	basePath = strings.TrimRight(basePath, "/")
	prefix := "/repos/"
	if basePath != "" {
		prefix = basePath + prefix
	}
	if !strings.HasPrefix(requestPath, prefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(requestPath, prefix), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	owner, errOwner := url.PathUnescape(parts[0])
	repo, errRepo := url.PathUnescape(parts[1])
	if errOwner != nil || errRepo != nil || owner == "" || repo == "" {
		return "", "", false
	}
	return owner, repo, true
}

func endpointPath(endpoint *url.URL) string {
	return strings.TrimRight(endpoint.EscapedPath(), "/")
}
