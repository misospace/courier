package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
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
		wantDetail string
	}{
		{
			name:       "server error",
			url:        func(t *testing.T) string { return serve(t, http.StatusInternalServerError, "boom").URL },
			wantDetail: "500",
		},
		{
			name: "unreachable endpoint",
			url: func(t *testing.T) string {
				refused := httptest.NewServer(http.NotFoundHandler())
				url := refused.URL
				refused.Close() // nothing listens there anymore
				return url
			},
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
			scraper := NewScraper(&http.Client{Timeout: 2 * time.Second}, mustMapping(t, BackendAuto))
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
// output: net/http echoes them in transport errors unless scrubbed.
func TestScrapeRedactsCredentialsInDiagnostics(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	url = strings.Replace(url, "http://", "http://alice:hunter2@", 1)

	scraper := NewScraper(&http.Client{Timeout: 2 * time.Second}, mustMapping(t, BackendAuto))
	load := scraper.Scrape(context.Background(), url)
	if load.Available {
		t.Fatalf("load = %+v, want unavailable", load)
	}
	if strings.Contains(load.Detail, "hunter2") {
		t.Fatalf("detail %q leaks the password", load.Detail)
	}
	if !strings.Contains(load.Detail, "xxxxx") {
		t.Fatalf("detail %q should carry a redacted URL", load.Detail)
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
