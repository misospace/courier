package harness

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fixedForgeToken struct{ value string }

func (t fixedForgeToken) BrokerToken() ([]byte, error) { return []byte(t.value), nil }

const brokerWriteDeadline = 30 * time.Second

// TestBrokerForgeCallTimeoutFitsInsideBrokerWriteDeadline pins the client
// timeout below the broker listener's write deadline: a call the broker can
// no longer answer whole must fail on the client clock as a clean retryable
// transport error, not wait out a truncated body and read it as malformed.
func TestBrokerForgeCallTimeoutFitsInsideBrokerWriteDeadline(t *testing.T) {
	if brokerForgeCallTimeout >= brokerWriteDeadline {
		t.Fatalf("forge call timeout %v must stay under the broker write deadline %v", brokerForgeCallTimeout, brokerWriteDeadline)
	}
}

func TestBrokerForgeClientCall(t *testing.T) {
	var body, authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ID":"1"}`)
	}))
	defer server.Close()
	client := &BrokerForgeClient{BaseURL: server.URL, TokenSource: fixedForgeToken{value: "rotating-token"}, HTTP: server.Client()}
	result, err := client.Call(context.Background(), "read-work-item", `{"repo":"org/repo","id":"1"}`)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer rotating-token" {
		t.Fatalf("authorization = %q", authorization)
	}
	if body != `{"operation":"read-work-item","arguments":{"repo":"org/repo","id":"1"}}` {
		t.Fatalf("request body = %s", body)
	}
	if result != `{"ID":"1"}` {
		t.Fatalf("result = %q", result)
	}
}

func TestBrokerForgeClientSafeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, "provider-secret-response")
	}))
	defer server.Close()
	client := &BrokerForgeClient{BaseURL: server.URL, TokenSource: fixedForgeToken{value: "token"}, HTTP: server.Client()}
	_, err := client.Call(context.Background(), "read-work-item", `{"repo":"org/repo","id":"1"}`)
	if err == nil || strings.Contains(err.Error(), "provider-secret-response") {
		t.Fatalf("error = %v, expected safe broker denial", err)
	}
	callErr, ok := err.(*BrokerCallError)
	if !ok || callErr.StatusCode != http.StatusUnprocessableEntity || callErr.Retryable {
		t.Fatalf("error = %#v, want definite 422 denial", err)
	}
}

func TestBrokerForgeClientRejectsUnsupportedOperation(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer server.Close()
	client := &BrokerForgeClient{BaseURL: server.URL, TokenSource: fixedForgeToken{value: "token"}, HTTP: server.Client()}
	for _, operation := range []string{"create-pull-request", "merge-pull-request", ""} {
		if _, err := client.Call(context.Background(), operation, `{"repo":"org/repo"}`); err == nil {
			t.Fatalf("operation %q was not refused locally", operation)
		}
	}
	if called {
		t.Fatal("unsupported operation reached the broker")
	}
}

func TestBrokerForgeClientBoundsArguments(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer server.Close()
	client := &BrokerForgeClient{BaseURL: server.URL, TokenSource: fixedForgeToken{value: "token"}, HTTP: server.Client()}
	if _, err := client.Call(context.Background(), "read-work-item", `{"repo":"org/repo","id":"`+strings.Repeat("1", maxBrokerForgeArgumentsBytes)+`"}`); err == nil {
		t.Fatal("oversized arguments were not refused locally")
	}
	if _, err := client.Call(context.Background(), "read-work-item", `not-json`); err == nil {
		t.Fatal("malformed arguments were not refused locally")
	}
	if called {
		t.Fatal("invalid arguments reached the broker")
	}
}

func TestBrokerForgeClientTransientUnavailableIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forge read is temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := &BrokerForgeClient{BaseURL: server.URL, TokenSource: fixedForgeToken{value: "token"}, HTTP: server.Client()}
	_, err := client.Call(context.Background(), "read-work-item", `{"repo":"org/repo","id":"1"}`)
	callErr, ok := err.(*BrokerCallError)
	if !ok || callErr.StatusCode != http.StatusServiceUnavailable || !callErr.Retryable {
		t.Fatalf("error = %#v, want retryable 503 classification", err)
	}
}
