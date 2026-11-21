package github

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type testInstallationSource struct {
	calls int
	token string
	err   error
}

func (s *testInstallationSource) InstallationToken(context.Context) (string, time.Time, error) {
	s.calls++
	return s.token, time.Now().Add(time.Hour), s.err
}

func TestNewBrokerInstallationClientValidation(t *testing.T) {
	source := &testInstallationSource{token: "token"}
	for _, tc := range []struct {
		name string
		auth BrokerInstallationAuth
		src  InstallationTokenSource
	}{
		{"owner", BrokerInstallationAuth{Endpoint: "https://api.example"}, source},
		{"scheme", BrokerInstallationAuth{Owner: "acme", Endpoint: "http://api.example"}, source},
		{"userinfo", BrokerInstallationAuth{Owner: "acme", Endpoint: "https://user:pass@api.example"}, source},
		{"query", BrokerInstallationAuth{Owner: "acme", Endpoint: "https://api.example?x=1"}, source},
		{"fragment", BrokerInstallationAuth{Owner: "acme", Endpoint: "https://api.example#x"}, source},
		{"source", BrokerInstallationAuth{Owner: "acme", Endpoint: "https://api.example"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewBrokerInstallationClient(tc.auth, tc.src); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestBrokerInstallationClientRefusesBeforeTokenFetch(t *testing.T) {
	source := &testInstallationSource{token: "secret"}
	client, err := NewBrokerInstallationClient(BrokerInstallationAuth{Owner: "acme", Endpoint: "https://api.example/api/v3"}, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, method, url string }{
		{"host", "GET", "https://other.example/api/v3/repos/acme/demo/issues"},
		{"scheme", "GET", "http://api.example/api/v3/repos/acme/demo/issues"},
		{"prefix", "GET", "https://api.example/other/repos/acme/demo/issues"},
		{"app", "POST", "https://api.example/api/v3/app/installations/1/access_tokens"},
		{"user", "GET", "https://api.example/api/v3/user"},
		{"org", "GET", "https://api.example/api/v3/orgs/acme/repos"},
		{"owner", "GET", "https://api.example/api/v3/repos/other/demo/issues"},
		{"dots", "GET", "https://api.example/api/v3/repos/acme/demo/../../user"},
		{"method", "TRACE", "https://api.example/api/v3/repos/acme/demo/issues"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, tc.url, nil)
			_, err := client.httpClient.Do(req)
			if err == nil {
				t.Fatal("request accepted")
			}
			if source.calls != 0 {
				t.Fatalf("token fetched %d times", source.calls)
			}
		})
	}
}

func TestBrokerInstallationClientPassesMethodsAndFetchesCurrentToken(t *testing.T) {
	var calls int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got, want := r.Header.Values("Authorization"), []string{"Bearer token-" + string(rune('0'+calls))}; len(got) != 1 || got[0] != want[0] {
			t.Errorf("authorization = %v, want %v", got, want)
		}
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			if string(body) != "payload" {
				t.Errorf("body = %q", body)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	source := &changingSource{}
	client, err := NewBrokerInstallationClient(BrokerInstallationAuth{Owner: "acme", Endpoint: server.URL + "/api/v3", HTTPClient: server.Client()}, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		var body io.Reader
		if method == http.MethodPost {
			body = strings.NewReader("payload")
		}
		req, _ := http.NewRequest(method, server.URL+"/api/v3/repos/acme/demo/issues", body)
		if _, err := client.httpClient.Do(req); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 || source.calls != 3 {
		t.Fatalf("requests=%d token fetches=%d", calls, source.calls)
	}
	for _, endpoint := range []string{server.URL + "/api/v3"} {
		c, err := client.WithBaseURL(endpoint)
		if err != nil || c != client {
			t.Fatalf("matching endpoint returned %v, %v", c == client, err)
		}
	}
	if _, err := client.WithBaseURL("https://other.example"); err == nil || !strings.Contains(err.Error(), "mismatched API endpoint") {
		t.Fatalf("endpoint mismatch = %v", err)
	}

	rootServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/demo/issues" {
			t.Errorf("root endpoint path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer rootServer.Close()
	rootClient, err := NewBrokerInstallationClient(BrokerInstallationAuth{Owner: "acme", Endpoint: rootServer.URL, HTTPClient: rootServer.Client()}, &testInstallationSource{token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	rootRequest, _ := http.NewRequest(http.MethodGet, rootServer.URL+"/repos/acme/demo/issues", nil)
	if _, err := rootClient.httpClient.Do(rootRequest); err != nil {
		t.Fatalf("root endpoint request: %v", err)
	}
}

type changingSource struct{ calls int }

func (s *changingSource) InstallationToken(context.Context) (string, time.Time, error) {
	s.calls++
	return "token-" + string(rune('0'+s.calls)), time.Now().Add(time.Hour), nil
}

func TestBrokerInstallationClientErrors(t *testing.T) {
	sourceErr := errors.New("source failed safely")
	source := &testInstallationSource{err: sourceErr}
	client, err := NewBrokerInstallationClient(BrokerInstallationAuth{Owner: "acme", Endpoint: "https://api.example"}, source)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", "https://api.example/repos/acme/demo/issues", nil)
	if _, err := client.httpClient.Do(req); err != sourceErr {
		t.Fatalf("source error = %v", err)
	}

	transport := roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("secret-token transport detail") })
	transportClient, err := NewBrokerInstallationClient(BrokerInstallationAuth{Owner: "acme", Endpoint: "https://api.example", HTTPClient: &http.Client{Transport: transport}}, &testInstallationSource{token: "secret-token"})
	if err != nil {
		t.Fatal(err)
	}
	req, _ = http.NewRequest("GET", "https://api.example/repos/acme/demo/issues", nil)
	_, err = transportClient.httpClient.Do(req)
	var te *transportError
	if !errors.As(err, &te) || strings.Contains(err.Error(), "secret-token") || err.Error() != "broker installation token request failed" {
		t.Fatalf("transport error = %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestBrokerInstallationClientAPIErrorAndRedirect(t *testing.T) {
	var redirectCalls int
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirectCalls++ }))
	defer destination.Close()
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusInternalServerError, http.StatusFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if status == http.StatusForbidden {
					w.Header().Set("X-RateLimit-Remaining", "0")
				}
				if status == http.StatusFound {
					http.Redirect(w, r, destination.URL, status)
					return
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			client, err := NewBrokerInstallationClient(BrokerInstallationAuth{Owner: "acme", Endpoint: server.URL, HTTPClient: server.Client()}, &testInstallationSource{token: "token"})
			if err != nil {
				t.Fatal(err)
			}
			req, _ := http.NewRequest("GET", server.URL+"/repos/acme/demo/issues", nil)
			_, err = client.httpClient.Do(req)
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
				t.Fatalf("error = %#v", err)
			}
			if status == http.StatusForbidden && apiErr.Message != "rate limit reached" {
				t.Fatalf("rate limit message = %q", apiErr.Message)
			}
			if status == http.StatusFound && redirectCalls != 0 {
				t.Fatalf("redirect destination received %d requests", redirectCalls)
			}
		})
	}
}

// TestBrokerInstallationClientPinsRepositories proves the pinned-repository
// restriction is enforced at the client boundary, before any token is spent:
// a same-owner repository outside the pin set is refused.
func TestBrokerInstallationClientPinsRepositories(t *testing.T) {
	var served string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	source := &testInstallationSource{token: "secret"}
	client, err := NewBrokerInstallationClient(BrokerInstallationAuth{
		Owner: "acme", Repositories: []string{"acme/widgets"}, Endpoint: server.URL, HTTPClient: server.Client(),
	}, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		method      string
		path        string
		wantRefused bool
	}{
		{"pinned repo passes", http.MethodGet, "/repos/acme/widgets/pulls", false},
		{"unpinned same-owner repo refused", http.MethodGet, "/repos/acme/other/pulls", true},
		{"case-insensitive pin", http.MethodGet, "/repos/ACME/Widgets/issues", false},
		{"patch write passes", http.MethodPatch, "/repos/acme/widgets/pulls/1", false},
		{"put write passes", http.MethodPut, "/repos/acme/widgets/labels", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := source.calls
			request, err := http.NewRequest(tc.method, server.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.httpClient.Do(request)
			if tc.wantRefused {
				if err == nil {
					t.Fatal("unpinned repository accepted")
				}
				if source.calls != before {
					t.Fatalf("refused request consumed a token: %d -> %d", before, source.calls)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatalf("refusal carries the token: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("pinned repository refused: %v", err)
			}
			if served != tc.path {
				t.Fatalf("server path = %q, want %q", served, tc.path)
			}
		})
	}
}

// TestBrokerInstallationClientValidationRefusesForeignPin pins the constructor
// refusal for a pin set naming another account: the client must never accept
// a configuration whose entries it cannot serve.
func TestBrokerInstallationClientValidationRefusesForeignPin(t *testing.T) {
	if _, err := NewBrokerInstallationClient(BrokerInstallationAuth{
		Owner: "acme", Repositories: []string{"other/widgets"}, Endpoint: "https://api.example",
	}, &testInstallationSource{token: "t"}); err == nil {
		t.Fatal("foreign-owner pin set accepted")
	}
}

// TestBrokerInstallationClientOwnerOnlyWithoutPins keeps the owner-level
// contract for callers that pass no pin set: any repository of the pinned
// owner is accepted at the client, whose guard GitHub's token scoping backs.
func TestBrokerInstallationClientOwnerOnlyWithoutPins(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	source := &testInstallationSource{token: "secret"}
	client, err := NewBrokerInstallationClient(BrokerInstallationAuth{
		Owner: "acme", Endpoint: server.URL, HTTPClient: server.Client(),
	}, source)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodHead, server.URL+"/repos/acme/anything/pulls", nil)
	if _, err := client.httpClient.Do(request); err != nil {
		t.Fatalf("owner-only request refused: %v", err)
	}
}
