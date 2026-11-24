package gitegress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type fakeProvider struct {
	calls  []string
	called int
	err    error
}

func (p *fakeProvider) Credentials(_ context.Context, identity string) (Credential, error) {
	p.called++
	p.calls = append(p.calls, identity)
	if p.err != nil {
		return Credential{}, p.err
	}
	return Credential{Username: "x-access-token", Token: "tok-" + identity}, nil
}

func forkConfig(provider CredentialProvider) Config {
	return Config{
		Remotes: []Remote{
			{URL: "https://github.com/o/r.git", Identity: "o/r"},
			{URL: "https://github.com/o/w.git", Identity: "o/w", Push: true},
		},
		WorkRef:     "refs/heads/courier/run",
		Credentials: provider,
	}
}

func TestNewRejectsMalformedSurface(t *testing.T) {
	provider := &fakeProvider{}
	for _, test := range []struct {
		name   string
		config Config
	}{
		{"no provider", Config{WorkRef: "refs/heads/b", Remotes: []Remote{{URL: "https://h/o/r.git", Identity: "o/r", Push: true}}}},
		{"no remotes", Config{WorkRef: "refs/heads/b", Credentials: provider}},
		{"relative ref", Config{WorkRef: "b", Credentials: provider, Remotes: []Remote{{URL: "https://h/o/r.git", Identity: "o/r", Push: true}}}},
		{"tag ref", Config{WorkRef: "refs/tags/v1", Credentials: provider, Remotes: []Remote{{URL: "https://h/o/r.git", Identity: "o/r", Push: true}}}},
		{"ref with space", Config{WorkRef: "refs/heads/a b", Credentials: provider, Remotes: []Remote{{URL: "https://h/o/r.git", Identity: "o/r", Push: true}}}},
		{"routable http", Config{WorkRef: "refs/heads/b", Credentials: provider, Remotes: []Remote{{URL: "http://example.com/o/r.git", Identity: "o/r", Push: true}}}},
		{"userinfo", Config{WorkRef: "refs/heads/b", Credentials: provider, Remotes: []Remote{{URL: "https://u:p@h/o/r.git", Identity: "o/r", Push: true}}}},
		{"identity without owner", Config{WorkRef: "refs/heads/b", Credentials: provider, Remotes: []Remote{{URL: "https://h/o/r.git", Identity: "r", Push: true}}}},
		{"path collision", Config{WorkRef: "refs/heads/b", Credentials: provider, Remotes: []Remote{
			{URL: "https://h/o/r.git", Identity: "o/r", Push: true},
			{URL: "https://h2/o/r", Identity: "o/other"},
		}}},
	} {
		if _, err := New(test.config); err == nil {
			t.Errorf("%s: New accepted a malformed surface", test.name)
		}
	}
	if _, err := New(Config{
		WorkRef:     "refs/heads/b",
		Credentials: provider,
		Remotes:     []Remote{{URL: "http://127.0.0.1:9/o/r.git", Identity: "o/r", Push: true}},
	}); err != nil {
		t.Errorf("loopback http remote rejected: %v", err)
	}
}

func TestResolve(t *testing.T) {
	guard, err := New(forkConfig(&fakeProvider{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		target string
		method string
		want   bool
	}{
		{"https://x/o/r.git/info/refs?service=git-upload-pack", "GET", true},
		{"https://x/o/r.git/info/refs?service=git-receive-pack", "GET", false},
		{"https://x/o/r/info/refs?service=git-upload-pack", "GET", true},
		{"https://x/o/r.git/info/refs", "GET", false},
		{"https://x/o/r.git/info/refs?service=git-upload-pack&service=git-receive-pack", "GET", false},
		{"https://x/o/r.git/info/refs?service=https", "GET", false},
		{"https://x/o/w.git/info/refs?service=git-receive-pack", "GET", true},
		{"https://x/o/r.git/git-upload-pack", "POST", true},
		{"https://x/o/r.git/git-receive-pack", "POST", false},
		{"https://x/o/w.git/git-receive-pack", "POST", true},
		{"https://x/o/w.git/git-receive-pack", "GET", false},
		{"https://x/o/w.git/git-upload-pack", "GET", false},
		{"https://x/o/other.git/git-upload-pack", "POST", false},
		{"https://x/o/r.git/objects/info/packs", "GET", false},
		{"https://x/o/r.git/repos", "GET", false},
		{"https://x/", "GET", false},
		{"https://x/o/w.git/../x/git-receive-pack", "POST", false},
		{"https://x/o/w.git/%2e%2e/git-upload-pack", "POST", false},
		{"https://x/o/w.git/./git-upload-pack", "POST", false},
		// A trailing-slash spelling of a pinned repository resolves because
		// the guard rebuilds the upstream URL from the pinned remote and
		// never forwards the client path.
		{"https://x/o/w.git//git-upload-pack", "POST", true},
		{"https://x/O/W.GIT/git-receive-pack", "POST", true},
	} {
		parsed, err := url.Parse(test.target)
		if err != nil {
			t.Fatal(err)
		}
		_, _, resolveErr := guard.resolve(parsed, test.method)
		if (resolveErr == nil) != test.want {
			t.Errorf("%s %s: resolved=%v want=%v (%v)", test.method, test.target, resolveErr == nil, test.want, resolveErr)
		}
	}
}

func TestCheckReceivePack(t *testing.T) {
	guard, err := New(forkConfig(&fakeProvider{}))
	if err != nil {
		t.Fatal(err)
	}
	const (
		oidA = "1111111111111111111111111111111111111111"
		oidB = "2222222222222222222222222222222222222222"
		zero = "0000000000000000000000000000000000000000"
	)
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"create approved", pkt(oidZero(40)+" "+oidA+" "+"refs/heads/courier/run") + flush, true},
		{"update approved", pkt(oidA+" "+oidB+" refs/heads/courier/run") + flush, true},
		{"approved with capabilities", pkt(oidA+" "+oidB+" refs/heads/courier/run\x00report-status side-band-64k") + flush, true},
		{"outside work ref", pkt(oidA+" "+oidB+" refs/heads/main") + flush, false},
		{"second ref outside", pkt(oidA+" "+oidB+" refs/heads/courier/run") + pkt(oidZero(40)+" "+oidA+" refs/heads/other") + flush, false},
		{"delete", pkt(oidA+" "+oidZero(40)+" refs/heads/courier/run") + flush, false},
		{"tag destination", pkt(oidZero(40)+" "+oidA+" refs/tags/v1") + flush, false},
		{"no updates", flush, false},
		{"malformed oid", pkt("zz "+oidB+" refs/heads/courier/run") + flush, false},
		{"garbage line", pkt("nonsense") + flush, false},
		{"v2 command section", pkt("command=push\x00agent=git/2.47") + flush, false},
		{"mixed lengths", pkt(oidA+" "+strings.Repeat("a", 64)+" refs/heads/courier/run") + flush, true},
	}
	for _, test := range tests {
		head, rest, err := guard.checkReceivePack(strings.NewReader(test.body))
		if (err == nil) != test.want {
			t.Errorf("%s: accepted=%v want=%v (%v)", test.name, err == nil, test.want, err)
			continue
		}
		if err != nil {
			continue
		}
		restBytes, _ := io.ReadAll(rest)
		if got := string(head) + string(restBytes); got != test.body {
			t.Errorf("%s: forwarding reconstruction drifted: %q != %q", test.name, got, test.body)
		}
	}
	// A body longer than the bound cannot grow the parse.
	var flood bytes.Buffer
	for i := 0; i < maxUpdateLines+2; i++ {
		flood.WriteString(pkt(oidA + " " + oidB + " refs/heads/courier/run"))
	}
	flood.WriteString(flush)
	if _, _, err := guard.checkReceivePack(&flood); err == nil {
		t.Error("push over the ref bound accepted")
	}
}

func TestServeHTTPServesOnlyApprovedSurface(t *testing.T) {
	type observed struct {
		method  string
		path    string
		query   string
		auth    string
		proto   string
		body    string
		content string
	}
	var requests []observed
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, observed{
			method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
			auth: r.Header.Get("Authorization"), proto: r.Header.Get("Git-Protocol"),
			body: string(body), content: r.Header.Get("Content-Type"),
		})
		switch {
		case r.URL.Path == "/backend/work.git/git-receive-pack":
			w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
			fmt.Fprint(w, "upstream-result")
		case r.URL.Path == "/backend/work.git/info/refs":
			w.Header().Set("Www-Authenticate", `Basic realm="real host"`)
			fmt.Fprint(w, "advertisement")
		case strings.HasSuffix(r.URL.Path, "/git-upload-pack"):
			fmt.Fprint(w, "fetch-stream")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	provider := &fakeProvider{}
	guard, err := New(Config{
		Remotes: []Remote{
			{URL: upstream.URL + "/backend/base.git", Identity: "o/base"},
			{URL: upstream.URL + "/backend/work.git", Identity: "o/work", Push: true},
		},
		WorkRef:     "refs/heads/courier/run",
		Credentials: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	const oidA = "1111111111111111111111111111111111111111"
	const oidB = "2222222222222222222222222222222222222222"

	// A push to the approved ref is forwarded with the run's credential and
	// a body preserved byte-for-byte, and the client's own Authorization
	// header never reaches upstream.
	recorder := httptest.NewRecorder()
	pushBody := pkt(oidA+" "+oidB+" refs/heads/courier/run") + flush + "PACKDATA"
	request := httptest.NewRequest("POST", "/backend/work.git/git-receive-pack", strings.NewReader(pushBody))
	request.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	request.Header.Set("Authorization", "Basic dXNlcjpzdGVhbGVudG9rZW4=")
	guard.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.String() != "upstream-result" {
		t.Fatalf("approved push: code=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if len(requests) != 1 {
		t.Fatalf("approved push: %d upstream requests", len(requests))
	}
	last := requests[0]
	if want := "Basic eC1hY2Nlc3MtdG9rZW46dG9rLW8vd29yaw=="; last.auth != want {
		t.Errorf("upstream saw auth %q want %q", last.auth, want)
	}
	if last.body != pushBody {
		t.Errorf("upstream body drifted: %q", last.body)
	}
	if last.proto != "version=1" {
		t.Errorf("upstream saw Git-Protocol %q", last.proto)
	}
	if last.content != "application/x-git-receive-pack-request" {
		t.Errorf("content type not forwarded: %q", last.content)
	}

	// A chunked client body (no Content-Length) is inspected and forwarded
	// byte-for-byte as well.
	recorder = httptest.NewRecorder()
	chunked := httptest.NewRequest("POST", "/backend/work.git/git-receive-pack", nil)
	chunked.Body = io.NopCloser(strings.NewReader(pushBody))
	chunked.ContentLength = -1
	guard.ServeHTTP(recorder, chunked)
	if recorder.Code != http.StatusOK {
		t.Fatalf("chunked approved push: code=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if got := requests[len(requests)-1]; got.body != pushBody {
		t.Errorf("chunked upstream body drifted: %q", got.body)
	}

	// A push outside the work ref is refused before the credential is
	// resolved and before any upstream request exists.
	before := len(requests)
	providerCalls := provider.called
	recorder = httptest.NewRecorder()
	guard.ServeHTTP(recorder, httptest.NewRequest("POST", "/backend/work.git/git-receive-pack",
		strings.NewReader(pkt(oidA+" "+oidB+" refs/heads/main")+flush+"PACKDATA")))
	if recorder.Code != http.StatusForbidden || len(requests) != before || provider.called != providerCalls {
		t.Fatalf("off-scope push: code=%d upstream=%d provider=%d", recorder.Code, len(requests)-before, provider.called-providerCalls)
	}

	// The base repository is read-only: neither the push nor its
	// advertisement resolves.
	for _, target := range []string{"/backend/base.git/git-receive-pack", "/backend/base.git/info/refs?service=git-receive-pack"} {
		recorder = httptest.NewRecorder()
		method := "POST"
		var body io.Reader = strings.NewReader(flush)
		if strings.Contains(target, "info/refs") {
			method, body = "GET", nil
		}
		guard.ServeHTTP(recorder, httptest.NewRequest(method, target, body))
		if recorder.Code != http.StatusForbidden || len(requests) != before {
			t.Fatalf("%s: code=%d upstream=%d", target, recorder.Code, len(requests)-before)
		}
	}

	// Fetch is served for both pinned repositories with the credential
	// injected and the upstream challenge withheld.
	recorder = httptest.NewRecorder()
	guard.ServeHTTP(recorder, httptest.NewRequest("GET", "/backend/work.git/info/refs?service=git-upload-pack", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "advertisement" {
		t.Fatalf("advertisement: code=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Www-Authenticate") != "" {
		t.Error("upstream auth challenge leaked to the client")
	}
	if got := requests[len(requests)-1].query; got != "service=git-upload-pack" {
		t.Errorf("advertisement query %q", got)
	}
	recorder = httptest.NewRecorder()
	guard.ServeHTTP(recorder, httptest.NewRequest("POST", "/backend/base.git/git-upload-pack", strings.NewReader("want")))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "fetch-stream" {
		t.Fatalf("base fetch: code=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if got := requests[len(requests)-1].auth; got != "Basic eC1hY2Nlc3MtdG9rZW46dG9rLW8vYmFzZQ==" {
		t.Errorf("base fetch auth %q", got)
	}

	// A credential failure serves nothing and reaches no upstream.
	before = len(requests)
	broken := &fakeProvider{err: errors.New("gone")}
	brokenGuard, err := New(Config{
		Remotes:     []Remote{{URL: upstream.URL + "/backend/work.git", Identity: "o/work", Push: true}},
		WorkRef:     "refs/heads/courier/run",
		Credentials: broken,
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	brokenGuard.ServeHTTP(recorder, httptest.NewRequest("POST", "/backend/work.git/git-upload-pack", strings.NewReader("want")))
	if recorder.Code != http.StatusBadGateway || len(requests) != before {
		t.Fatalf("credential failure: code=%d upstream=%d", recorder.Code, len(requests)-before)
	}

	// Anything outside the pinned surface is refused unseen by upstream.
	for _, target := range []string{"/backend/other.git/git-upload-pack", "/repos/work", "/"} {
		recorder = httptest.NewRecorder()
		brokenGuard.ServeHTTP(recorder, httptest.NewRequest("GET", target, nil))
		if recorder.Code != http.StatusForbidden || len(requests) != before {
			t.Fatalf("%s: code=%d upstream=%d", target, recorder.Code, len(requests)-before)
		}
	}
}

const flush = "0000"

func pkt(payload string) string {
	length := len(payload) + 4
	return fmt.Sprintf("%04x%s", length, payload)
}

func oidZero(n int) string { return strings.Repeat("0", n) }
