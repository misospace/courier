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
	"os"
	"reflect"
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
			_, _ = io.WriteString(w, `{"token":"installation-token","expires_at":"`+expires+`","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read"}}`)
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
	for key, level := range map[string]any{"administration": "read", "checks": "read", "contents": "read", "issues": "read", "metadata": "read", "pull_requests": "read", "statuses": "read"} {
		if permissions[key] != level {
			t.Fatalf("requested permission %q = %#v, want %q", key, permissions[key], level)
		}
	}
	if len(permissions) != 7 {
		t.Fatalf("requested permission set = %#v, want exactly the 7 read permissions", permissions)
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
			_, _ = io.WriteString(w, `{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read"}}`)
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
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read"}}`,
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"write"}}`,
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read","deployments":"write"}}`,
		`{"token":"secret-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","deployments":"read"}}`,
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
			var body struct {
				Permissions map[string]string `json:"permissions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode exchange body: %v", err)
			}
			want := requestedReadPermissions()
			if len(body.Permissions) != len(want) {
				t.Errorf("requested permission set = %#v, want %#v", body.Permissions, want)
			}
			for key, level := range want {
				if body.Permissions[key] != level {
					t.Errorf("requested permission %q = %q, want %q", key, body.Permissions[key], level)
				}
			}
			_, _ = io.WriteString(w, `{"token":"installation-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"administration":"read","checks":"read","contents":"read","issues":"read","metadata":"read","pull_requests":"read","statuses":"read","deployments":"read"}}`)
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
	rescue := os.Stderr
	reader, writer, _ := os.Pipe()
	os.Stderr = writer
	_, err = client.GetIssue(context.Background(), "acme", "demo", 9)
	writer.Close()
	os.Stderr = rescue
	if err != nil {
		t.Fatalf("unrequested read-only permission rejected refresh: %v", err)
	}
	warning, _ := io.ReadAll(reader)
	if !strings.Contains(string(warning), `"deployments"`) {
		t.Fatalf("missing unrequested-permission warning: %q", warning)
	}
}

func TestNewInstallationTokenMinterValidation(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	valid := InstallationAuth{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "https://api.github.com"}
	cases := []InstallationAuth{
		{InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: valid.Endpoint},
		{AppID: 1, Owner: "acme", PrivateKey: privateKey, Endpoint: valid.Endpoint},
		{AppID: 1, InstallationID: 2, Owner: "acme", Endpoint: valid.Endpoint},
		{AppID: 1, InstallationID: 2, PrivateKey: privateKey, Endpoint: valid.Endpoint},
		{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "http://api.github.com"},
		{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "https://user:pass@api.github.com"},
		{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "https://api.github.com?query=1"},
		{AppID: 1, InstallationID: 2, Owner: "acme", PrivateKey: privateKey, Endpoint: "https://api.github.com#fragment"},
	}
	for i, auth := range cases {
		if _, err := NewInstallationTokenMinter(auth); err == nil {
			t.Errorf("invalid auth case %d accepted", i)
		}
	}
	if _, err := NewInstallationTokenMinter(valid); err != nil {
		t.Fatalf("valid auth rejected: %v", err)
	}
}

func TestInstallationTokenMinterMint(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	var verified, minted int
	var mintedNames []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/app/installations/42":
			verified++
			if r.Method != http.MethodGet || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				t.Errorf("installation verification request: %s %q", r.Method, r.Header.Get("Authorization"))
			}
			_, _ = io.WriteString(w, `{"account":{"login":"acme"}}`)
		case "/api/v3/app/installations/42/access_tokens":
			minted++
			if r.Method != http.MethodPost || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || r.Header.Get("Accept") != "application/vnd.github+json" {
				t.Errorf("mint request: %s headers=%v", r.Method, r.Header)
			}
			var body struct {
				Repositories []string          `json:"repositories"`
				Permissions  map[string]string `json:"permissions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode request: %v", err)
			}
			if body.Permissions["contents"] != "write" || body.Permissions["issues"] != "read" {
				t.Errorf("mint request body = %#v", body)
			}
			mintedNames = body.Repositories
			_, _ = io.WriteString(w, `{"token":"minted-token","expires_at":"2026-11-20T13:00:00Z","permissions":{"contents":"write","issues":"read"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	minter, err := NewInstallationTokenMinter(InstallationAuth{AppID: 7, InstallationID: 42, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL + "/api/v3", HTTPClient: server.Client(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	// GitHub's repositories field takes bare repository names, never the
	// owner/name identities the minter boundary validates.
	for _, tc := range []struct {
		identities, wantNames []string
	}{
		{[]string{"acme/demo"}, []string{"demo"}},
		{[]string{"acme/demo", "acme/base"}, []string{"demo", "base"}},
	} {
		token, expiry, err := minter.Mint(context.Background(), tc.identities, map[string]string{"contents": "write", "issues": "read"})
		if err != nil || token != "minted-token" || !expiry.Equal(now.Add(time.Hour)) {
			t.Fatalf("Mint(%v) = %q, %v, %v", tc.identities, token, expiry, err)
		}
		if !reflect.DeepEqual(mintedNames, tc.wantNames) {
			t.Fatalf("Mint(%v) wire repositories = %#v, want %#v", tc.identities, mintedNames, tc.wantNames)
		}
	}
	// The second mint reuses the installation verification cached by the
	// first, so only one lookup runs while both mints exchange a token.
	if verified != 1 || minted != 2 {
		t.Fatalf("verified=%d minted=%d, want verified=1 minted=2", verified, minted)
	}
}

func TestInstallationTokenMinterRefusals(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	requested := map[string]string{"contents": "write"}
	for _, tc := range []struct {
		name, repositories string
		permissions        map[string]string
		response           string
		wantError          bool
	}{
		{"empty repositories", "", requested, "", true},
		{"wrong owner", "other/demo", requested, "", true},
		{"empty name", "acme/", requested, "", true},
		{"empty permissions", "acme/demo", map[string]string{}, "", true},
		{"invalid permission", "acme/demo", map[string]string{"contents": "admin"}, "", true},
		{"missing token", "acme/demo", requested, `{"expires_at":"2026-11-20T13:00:00Z","permissions":{"contents":"write"}}`, true},
		{"expired", "acme/demo", requested, `{"token":"token","expires_at":"2026-11-20T11:00:00Z","permissions":{"contents":"write"}}`, true},
		{"write granted read", "acme/demo", requested, `{"token":"token","expires_at":"2026-11-20T13:00:00Z","permissions":{"contents":"read"}}`, true},
		{"read granted write", "acme/demo", map[string]string{"contents": "read"}, `{"token":"token","expires_at":"2026-11-20T13:00:00Z","permissions":{"contents":"write"}}`, true},
		{"missing requested permission", "acme/demo", requested, `{"token":"token","expires_at":"2026-11-20T13:00:00Z","permissions":{}}`, true},
		{"extra write", "acme/demo", requested, `{"token":"token","expires_at":"2026-11-20T13:00:00Z","permissions":{"contents":"write","deployments":"write"}}`, true},
		{"extra read", "acme/demo", requested, `{"token":"token","expires_at":"2026-11-20T13:00:00Z","permissions":{"contents":"write","deployments":"read"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mintCalls int
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/app/installations/42" {
					_, _ = io.WriteString(w, `{"account":{"login":"acme"}}`)
					return
				}
				if r.URL.Path == "/app/installations/42/access_tokens" {
					mintCalls++
					_, _ = io.WriteString(w, tc.response)
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			minter, err := NewInstallationTokenMinter(InstallationAuth{AppID: 1, InstallationID: 42, Owner: "acme", PrivateKey: privateKey, Endpoint: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			var repositories []string
			if tc.repositories != "" {
				repositories = []string{tc.repositories}
			}
			_, _, err = minter.Mint(context.Background(), repositories, tc.permissions)
			if (err != nil) != tc.wantError {
				t.Fatalf("Mint error = %v, wantError=%v", err, tc.wantError)
			}
			if (tc.repositories == "" || tc.repositories == "other/demo" || tc.repositories == "acme/" || len(tc.permissions) == 0 || tc.permissions["contents"] == "admin") && mintCalls != 0 {
				t.Fatalf("invalid request reached exchange %d times", mintCalls)
			}
		})
	}
}
