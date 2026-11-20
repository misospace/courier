package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/forge"
)

func TestInstallationClientRefreshesRepositoryScopedReadToken(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	var exchanges int
	var observedAuth string
	var exchangeBody map[string]any
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/app/installations/42":
			if r.Method != http.MethodGet || r.Header.Get("Authorization") == "Bearer installation-token" {
				t.Errorf("unexpected installation lookup request: %#v", r)
			}
			_, _ = io.WriteString(w, `{"account":{"login":"acme"}}`)
		case "/api/v3/app/installations/42/access_tokens":
			exchanges++
			if r.Method != http.MethodPost || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				t.Errorf("token exchange = %s with auth %q", r.Method, r.Header.Get("Authorization"))
			}
			if err := json.NewDecoder(r.Body).Decode(&exchangeBody); err != nil {
				t.Errorf("decode exchange: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			expires := "2026-11-20T13:00:00Z"
			if exchanges > 1 {
				expires = "2026-11-20T14:00:00Z"
			}
			_, _ = io.WriteString(w, `{"token":"installation-token","expires_at":"`+expires+`","permissions":{"checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read"}}`)
		case "/api/v3/repos/acme/demo/issues/9":
			observedAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"number":9,"title":"issue"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewInstallationClient(InstallationAuth{AppID: 7, InstallationID: 42, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL + "/api/v3", HTTPClient: server.Client(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := client.GetIssue(context.Background(), "acme", "demo", 9); err != nil {
			t.Fatal(err)
		}
	}
	if exchanges != 1 || observedAuth != "Bearer installation-token" {
		t.Fatalf("exchanges = %d, request auth = %q", exchanges, observedAuth)
	}
	repositories, ok := exchangeBody["repositories"].([]any)
	if !ok || len(repositories) != 1 || repositories[0] != "demo" {
		t.Fatalf("repositories scope = %#v", exchangeBody["repositories"])
	}
	permissions := exchangeBody["permissions"].(map[string]any)
	for _, permission := range permissions {
		if permission != "read" {
			t.Fatalf("unexpected installation permission: %#v", permissions)
		}
	}
	now = now.Add(60 * time.Minute)
	if _, err := client.GetIssue(context.Background(), "acme", "demo", 9); err != nil {
		t.Fatal(err)
	}
	if exchanges != 2 {
		t.Fatalf("exchanges after expiry = %d, want 2", exchanges)
	}
}

func TestInstallationClientRefusesRedirectAndEndpointEscape(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var escapedAuth string
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/access_tokens") {
			_, _ = io.WriteString(w, `{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read"}}`)
			return
		}
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer server.Close()
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetIssue(context.Background(), "acme", "demo", 1); err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("redirect response error = %v", err)
	}
	if escapedAuth != "" {
		t.Fatalf("authorization escaped to redirect destination: %q", escapedAuth)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://other.invalid/repos/acme/demo/issues/1", nil)
	if _, err := client.httpClient.Do(req); err == nil || !strings.Contains(err.Error(), "bound") {
		t.Fatalf("unbound endpoint error = %v", err)
	}
}

func TestInstallationClientWithBaseURLOnlyAcceptsMatchingEndpoint(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "https://api.github.com/api/v3/"})
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"https://API.GITHUB.COM/api/v3", "https://api.github.com/api/v3/"} {
		got, err := client.WithBaseURL(endpoint)
		if err != nil || got != client {
			t.Fatalf("matching endpoint %q returned same client=%v, err=%v", endpoint, got == client, err)
		}
	}
	for _, endpoint := range []string{"https://api.github.com/other", "https://evil.example/api/v3", "http://api.github.com/api/v3"} {
		if _, err := client.WithBaseURL(endpoint); err == nil {
			t.Fatalf("mismatched endpoint %q accepted", endpoint)
		}
	}
}

func TestRepositoryFromPathRejectsDotSegments(t *testing.T) {
	for _, requestPath := range []string{
		"/repos/acme/../other/issues/1",
		"/repos/acme/%2e%2e/issues/1",
		"/repos/acme/%2E/demo/issues/1",
	} {
		if owner, repo, ok := repositoryFromPath(requestPath, ""); ok {
			t.Fatalf("dot-segment path %q accepted as %s/%s", requestPath, owner, repo)
		}
	}
}

func TestInstallationClientRefusesMutationMethods(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "https://api.github.com/"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreatePullRequest(context.Background(), "acme", "demo", CreatePullRequestRequest{}); err == nil {
		t.Fatal("App observer credential must refuse pull request creation")
	}
}

func TestInstallationClientSanitizesRefreshErrors(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"sensitive upstream detail"}`)
	}))
	defer server.Close()
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetIssue(context.Background(), "acme", "demo", 1)
	var authErr *installationExchangeError
	var apiErr *APIError
	if !errors.As(err, &authErr) || authErr.status != http.StatusForbidden || errors.As(err, &apiErr) || forge.IsTransientAvailability(err) || strings.Contains(err.Error(), "sensitive upstream detail") {
		t.Fatalf("refresh error = %v", err)
	}
}

func TestInstallationAPIErrorClassifiesRateLimitsWithoutRetainingDetails(t *testing.T) {
	for _, tc := range []struct {
		name, header, value, detail string
	}{
		{"primary", "X-RateLimit-Remaining", "0", "private detail"},
		{"retry", "Retry-After", "10", "private detail"},
		{"secondary", "", "", "secondary rate limit: private detail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.detail))}
			if tc.header != "" {
				resp.Header.Set(tc.header, tc.value)
			}
			err := installationAPIError(resp)
			if !forge.IsTransientAvailability(err) || err.Body != "" || strings.Contains(err.Error(), "private detail") {
				t.Fatalf("rate limit error = %v", err)
			}
		})
	}
}

func TestInstallationClientRejectsOwnerMismatchBeforeTokenExchange(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var requests int
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer server.Close()
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetIssue(context.Background(), "other", "demo", 1)
	if err == nil || !strings.Contains(err.Error(), "configured installation account") || requests != 0 {
		t.Fatalf("owner mismatch error=%v requests=%d", err, requests)
	}
}

func TestInstallationClientRejectsInstallationAccountMismatch(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var exchanges int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/access_tokens") {
			exchanges++
			return
		}
		_, _ = io.WriteString(w, `{"account":{"login":"someone-else"}}`)
	}))
	defer server.Close()
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetIssue(context.Background(), "acme", "demo", 1)
	if err == nil || !strings.Contains(err.Error(), "does not match") || strings.Contains(err.Error(), "someone-else") || exchanges != 0 {
		t.Fatalf("installation mismatch error=%v exchanges=%d", err, exchanges)
	}
}

func TestInstallationClientRefusesInsufficientPermission(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range []string{
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read"}}`,
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"write"}}`,
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read","deployments":"write"}}`,
	} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, response)
		}))
		client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client()})
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		_, err = client.GetIssue(context.Background(), "acme", "demo", 1)
		server.Close()
		if err == nil || strings.Contains(err.Error(), "secret-token") {
			t.Fatalf("invalid mint response accepted or leaked: %v", err)
		}
	}
}

func TestInstallationClientToleratesUnrequestedReadOnlyPermission(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app/installations/42":
			_, _ = io.WriteString(w, `{"account":{"login":"acme"}}`)
		case "/app/installations/42/access_tokens":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if _, requested := body["permissions"].(map[string]any)["administration"]; requested {
				t.Errorf("token request asked for administration permission: %#v", body["permissions"])
			}
			_, _ = io.WriteString(w, `{"token":"installation-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read","deployments":"read"}}`)
		case "/repos/acme/demo/issues/9":
			_, _ = io.WriteString(w, `{"number":9,"title":"issue"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewInstallationClient(InstallationAuth{AppID: 1, InstallationID: 42, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetIssue(context.Background(), "acme", "demo", 9); err != nil {
		t.Fatalf("unrequested read-only permission rejected refresh: %v", err)
	}
}
