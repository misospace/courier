package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func serve(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func mustMapping(t *testing.T, backend string) Mapping {
	t.Helper()
	mapping, err := For(backend)
	if err != nil {
		t.Fatalf("For(%q): %v", backend, err)
	}
	return mapping
}

// failingTransport fails every request the way a dial failure does, without
// binding or releasing any port.
type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("dial tcp 127.0.0.1:1: connect: connection refused")
}

func TestScrapeNormalizesVLLMLoad(t *testing.T) {
	server := serve(t, http.StatusOK, vllmMetrics)
	scraper := NewScraper(server.Client(), mustMapping(t, BackendAuto))
	load := scraper.Scrape(context.Background(), server.URL)
	if !load.Available {
		t.Fatalf("load = %+v, want available", load)
	}
	if load.Running == nil || *load.Running != 1.0 {
		t.Fatalf("running = %v, want 1", load.Running)
	}
	if load.Waiting == nil || *load.Waiting != 3.0 {
		t.Fatalf("waiting = %v, want 3", load.Waiting)
	}
	if load.Backpressured == nil || !*load.Backpressured {
		t.Fatalf("backpressured = %v, want true: %+v", load.Backpressured, load)
	}
	if load.KVCacheUsage == nil || *load.KVCacheUsage != 0.72 {
		t.Fatalf("kvCacheUsage = %v, want 0.72", load.KVCacheUsage)
	}
}

func TestScrapeBackpressureSemantics(t *testing.T) {
	healthy := `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 2.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting 0.0
`
	tests := []struct {
		name      string
		metrics   string
		wantSet   bool
		wantValue bool
	}{
		{name: "running with an empty queue is healthy", metrics: healthy, wantSet: true, wantValue: false},
		{name: "a queue is backpressure", metrics: "# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting 1.0\n", wantSet: true, wantValue: true},
		{name: "unknown waiting leaves backpressure unset", metrics: "# TYPE vllm:gpu_cache_usage_perc gauge\nvllm:gpu_cache_usage_perc 0.5\n", wantSet: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := serve(t, http.StatusOK, tt.metrics)
			scraper := NewScraper(server.Client(), mustMapping(t, BackendAuto))
			load := scraper.Scrape(context.Background(), server.URL)
			if !load.Available {
				t.Fatalf("load = %+v, want available", load)
			}
			if (load.Backpressured != nil) != tt.wantSet {
				t.Fatalf("backpressured presence = %v, want set=%v (%+v)", load.Backpressured != nil, tt.wantSet, load)
			}
			if load.Backpressured != nil && *load.Backpressured != tt.wantValue {
				t.Fatalf("backpressured = %v, want %v", *load.Backpressured, tt.wantValue)
			}
		})
	}
}

// One tool call is one bounded scrape: no retries, no waiting past the
// client timeout.
func TestScrapeTimeoutIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	scraper := NewScraper(&http.Client{Timeout: 20 * time.Millisecond}, mustMapping(t, BackendAuto))

	start := time.Now()
	load := scraper.Scrape(context.Background(), server.URL)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("scrape returned after %s, want it bounded by the client timeout", elapsed)
	}
	if load.Available || !strings.Contains(load.Detail, "failed") {
		t.Fatalf("load = %+v, want a degradation detail", load)
	}
}

func TestScrapeDegradesGracefully(t *testing.T) {
	unknownBackend := `# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 4.2
`
	malformed := "this is {{{ not prometheus"

	tests := []struct {
		name       string
		url        func(t *testing.T) string
		client     func(t *testing.T) *http.Client
		wantDetail string
	}{
		{
			name:       "server error",
			url:        func(t *testing.T) string { return serve(t, http.StatusInternalServerError, "boom").URL },
			wantDetail: "500",
		},
		{
			name:       "unreachable endpoint",
			url:        func(*testing.T) string { return "http://127.0.0.1:1" },
			client:     func(*testing.T) *http.Client { return &http.Client{Transport: failingTransport{}} },
			wantDetail: "failed",
		},
		{
			name:       "malformed metrics",
			url:        func(t *testing.T) string { return serve(t, http.StatusOK, malformed).URL },
			wantDetail: "malformed",
		},
		{
			name:       "valid metrics from an unknown backend",
			url:        func(t *testing.T) string { return serve(t, http.StatusOK, unknownBackend).URL },
			wantDetail: "no recognized model-server metrics",
		},
		{
			name:       "empty metrics body",
			url:        func(t *testing.T) string { return serve(t, http.StatusOK, "").URL },
			wantDetail: "no recognized model-server metrics",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := tt.url(t)
			client := &http.Client{Timeout: 2 * time.Second}
			if tt.client != nil {
				client = tt.client(t)
			}
			scraper := NewScraper(client, mustMapping(t, BackendAuto))
			load := scraper.Scrape(context.Background(), url)
			if load.Available {
				t.Fatalf("load = %+v, want unavailable", load)
			}
			if !strings.Contains(load.Detail, tt.wantDetail) {
				t.Fatalf("detail = %q, want it to contain %q", load.Detail, tt.wantDetail)
			}
			if load.Backpressured != nil || load.Waiting != nil || load.Running != nil || load.KVCacheUsage != nil {
				t.Fatalf("degraded load must not carry fabricated numbers: %+v", load)
			}
		})
	}
}

// Credentials embedded in the configured URL must never leak into tool
// output: net/http echoes them in transport errors unless scrubbed, and the
// whole userinfo component is secret — a username can be a bearer token.
// Query parameters and fragments can carry credentials too (?token=…), so
// every failure path is exercised with a secret in each position.
func TestScrapeRedactsCredentialsInDiagnostics(t *testing.T) {
	host := "http://127.0.0.1:1"
	parsed, err := url.Parse(host)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		raw     string
		secrets []string
	}{
		{name: "password userinfo", raw: "http://alice:hunter2@" + parsed.Host + "/metrics", secrets: []string{"hunter2"}},
		{name: "username is the token", raw: "http://super-secret-token@" + parsed.Host + "/metrics", secrets: []string{"super-secret-token"}},
		{name: "query token", raw: host + "/metrics?token=query-secret-tok3n&region=us", secrets: []string{"query-secret-tok3n"}},
		{name: "fragment secret", raw: host + "/metrics#fragment-secret-secr3t", secrets: []string{"fragment-secret-secr3t"}},
		{
			// net/http rewrites u:pw@ to u:***@ before building url.Error, so
			// the whole-URL replacement misses and only the query/fragment
			// scrub can catch these.
			name:    "password masks the query and fragment",
			raw:     "http://alice:hunter2@" + parsed.Host + "/metrics?token=query-secret-tok3n&z=1#fragment-secret-secr3t",
			secrets: []string{"hunter2", "query-secret-tok3n", "fragment-secret-secr3t"},
		},
		{
			// Same masking plus a %-encoded fragment: net/url stores Fragment
			// decoded but renders EscapedFragment, so only the escaped form
			// appears in the message.
			name:    "password masks an escaped fragment",
			raw:     "http://alice:hunter2@" + parsed.Host + "/metrics#tok%3AenFRAG3",
			secrets: []string{"hunter2", "tok%3AenFRAG3", "tok:enFRAG3"},
		},
		{
			// url.URL.Fragment is stored decoded but rendered %-escaped; both
			// spellings must disappear.
			name:    "escaped fragment",
			raw:     host + "/metrics#tok%3AenESC3",
			secrets: []string{"tok%3AenESC3", "tok:enESC3"},
		},
		{
			// url.Error formats the URL through %q, escaping quotes and
			// backslashes; the escaped spelling must not survive either.
			name:    "quote-escaping in the error rendering",
			raw:     host + `/metrics?token=QUOTE"LEAK`,
			secrets: []string{`QUOTE"LEAK`, `QUOTE\"LEAK`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraper := NewScraper(&http.Client{Transport: failingTransport{}}, mustMapping(t, BackendAuto))
			load := scraper.Scrape(context.Background(), tt.raw)
			if load.Available {
				t.Fatalf("load = %+v, want unavailable", load)
			}
			for _, leaked := range append(tt.secrets, "alice", "@", "xxxxx") {
				if strings.Contains(load.Detail, leaked) {
					t.Fatalf("detail %q leaks %q", load.Detail, leaked)
				}
			}
			if !strings.Contains(load.Detail, parsed.Host+"/metrics") {
				t.Fatalf("detail %q should keep the host and path for debugging", load.Detail)
			}
		})
	}
}

// A query-authenticated endpoint must lose its secret on every failure path
// that names the URL — non-200 responses, malformed bodies, and transport
// errors — while the scrape itself still receives the configured query.
func TestScrapeRedactsQueryCredentialsOnFailurePaths(t *testing.T) {
	const secret = "url-secret-c3eb91a1"

	serve500 := serve(t, http.StatusInternalServerError, "boom")
	defer serve500.Close()
	malformed := serve(t, http.StatusOK, "this is {{{ not prometheus")
	defer malformed.Close()
	const transportURL = "http://127.0.0.1:1"

	tests := []struct {
		name    string
		raw     string
		refused bool
	}{
		{name: "non-200", raw: serve500.URL + "/metrics?token=" + secret + "&z=1#" + secret},
		{name: "malformed response", raw: malformed.URL + "/metrics?token=" + secret + "&z=1#" + secret},
		{name: "transport error", raw: transportURL + "/metrics?token=" + secret + "&z=1#" + secret, refused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{Timeout: 2 * time.Second}
			if tt.refused {
				client.Transport = failingTransport{}
			}
			scraper := NewScraper(client, mustMapping(t, BackendAuto))
			load := scraper.Scrape(context.Background(), tt.raw)
			if load.Available {
				t.Fatalf("load = %+v, want unavailable", load)
			}
			if strings.Contains(load.Detail, secret) {
				t.Fatalf("detail %q leaks the query secret", load.Detail)
			}
			if !strings.Contains(load.Detail, "/metrics") {
				t.Fatalf("detail %q should keep the path for debugging", load.Detail)
			}
		})
	}

	// The configured query reaches the wire unredacted: a scraping handler
	// must observe the token parameter.
	var gotQuery url.Values
	observed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_, _ = w.Write([]byte(`# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting 3.0
`))
	}))
	defer observed.Close()
	scraper := NewScraper(&http.Client{Timeout: 2 * time.Second}, mustMapping(t, BackendAuto))
	load := scraper.Scrape(context.Background(), observed.URL+"/metrics?token="+secret)
	if !load.Available {
		t.Fatalf("load = %+v, want available", load)
	}
	if gotQuery.Get("token") != secret {
		t.Fatalf("upstream saw token %q, want the configured secret", gotQuery.Get("token"))
	}
}

// A redirect chain can reflect the configured credential into its Location
// under a different path and parameter name; when the chain exhausts Go's
// redirect limit, the transport error must name the failure without echoing
// any URL the endpoint chose.
func TestScrapeRedactsRedirectReflectedCredentials(t *testing.T) {
	const secret = "redirect-secret-8d41b"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/new-path?session="+secret+"&test=1", http.StatusFound)
	}))
	defer srv.Close()

	scraper := NewScraper(&http.Client{Timeout: 2 * time.Second}, mustMapping(t, BackendAuto))
	load := scraper.Scrape(context.Background(), srv.URL+"/metrics?token="+secret)
	if load.Available {
		t.Fatalf("load = %+v, want unavailable", load)
	}
	if strings.Contains(load.Detail, secret) {
		t.Fatalf("detail %q leaks the reflected credential", load.Detail)
	}
	if !strings.Contains(load.Detail, srv.URL+"/metrics") {
		t.Fatalf("detail %q should keep the configured host and path for debugging", load.Detail)
	}
	if !strings.Contains(load.Detail, "redirect") {
		t.Fatalf("detail %q should name the failure category", load.Detail)
	}
}

// RedactedURL backs every diagnostic surface that prints the URL, including
// the startup log.
func TestRedactedURLStripsAllUserinfo(t *testing.T) {
	tests := []struct{ raw, want string }{
		{raw: "https://alice:hunter2@example/metrics", want: "https://example/metrics"},
		{raw: "https://super-secret-token@example/metrics", want: "https://example/metrics"},
		{raw: "http://vllm:8000/metrics", want: "http://vllm:8000/metrics"},
		{raw: "https://example/metrics?token=secret-tok3n&x=1#frag-s3cret", want: "https://example/metrics"},
		{raw: "https://alice:pw@example/metrics?token=secret-tok3n", want: "https://example/metrics"},
		{raw: "https://example/metrics?", want: "https://example/metrics"},
	}
	for _, tt := range tests {
		if got := RedactedURL(tt.raw); got != tt.want {
			t.Fatalf("RedactedURL(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

// Non-finite samples must never reach the JSON-facing Load — Go's JSON
// encoding rejects NaN and infinities, so letting one through would turn a
// valid scrape into a serialization failure.
func TestScrapeDropsNonFiniteGauges(t *testing.T) {
	nonFinite := `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 2.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting NaN
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc -Inf
`
	server := serve(t, http.StatusOK, nonFinite)
	scraper := NewScraper(server.Client(), mustMapping(t, BackendAuto))
	load := scraper.Scrape(context.Background(), server.URL)
	if !load.Available {
		t.Fatalf("load = %+v, want available with the usable running gauge", load)
	}
	if load.Running == nil || *load.Running != 2.0 {
		t.Fatalf("running = %v, want 2", load.Running)
	}
	if load.Waiting != nil || load.Backpressured != nil || load.KVCacheUsage != nil {
		t.Fatalf("non-finite gauges leaked into the load: %+v", load)
	}
	encoded, err := json.Marshal(load)
	if err != nil {
		t.Fatalf("marshal load: %v", err)
	}
	if strings.Contains(string(encoded), "NaN") || strings.Contains(string(encoded), "Inf") {
		t.Fatalf("encoded load %s carries a non-finite value", encoded)
	}

	allBad := `# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting +Inf
`
	server = serve(t, http.StatusOK, allBad)
	load = scraper.Scrape(context.Background(), server.URL)
	if load.Available || load.Detail == "" {
		t.Fatalf("load = %+v, want the normal unavailable result", load)
	}
}

func TestValidateMetricsURL(t *testing.T) {
	tests := []struct {
		raw     string
		wantErr bool
	}{
		{raw: "http://vllm:8000/metrics"},
		{raw: "https://vllm:8000/metrics"},
		{raw: "  https://vllm:8000/metrics  "},
		{raw: "ftp://vllm:8000/metrics", wantErr: true},
		{raw: "not a url", wantErr: true},
		{raw: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run("url="+tt.raw, func(t *testing.T) {
			if _, err := ValidateMetricsURL(tt.raw); (err != nil) != tt.wantErr {
				t.Fatalf("ValidateMetricsURL(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
		})
	}
}
