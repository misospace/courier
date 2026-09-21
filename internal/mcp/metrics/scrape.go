package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// maxScrapeBytes bounds how much of the response is read. A metrics endpoint
// is trusted infrastructure, but the tool must not buffer unboundedly even
// from a misbehaving one.
const maxScrapeBytes = 8 << 20

// Scraper performs one bounded scrape per call and normalizes it through a
// Mapping. One tool call is one scrape; there are deliberately no retries —
// the coordinator can call again on its own next turn.
type Scraper struct {
	client  *http.Client
	mapping Mapping
}

func NewScraper(client *http.Client, mapping Mapping) *Scraper {
	return &Scraper{client: client, mapping: mapping}
}

// Scrape returns the endpoint's load. Failure is a value, not an error:
// degraded load information is exactly what this tool exists to report.
func (s *Scraper) Scrape(ctx context.Context, metricsURL string) Load {
	families, err := s.scrape(ctx, metricsURL)
	if err != nil {
		return unavailable(err.Error())
	}
	signals, ok := s.mapping.Map(families)
	if !ok {
		return unavailable(fmt.Sprintf("%s exposed no recognized model-server metrics at %s", s.mapping.Name(), RedactedURL(metricsURL)))
	}
	return normalize(signals)
}

func (s *Scraper) scrape(ctx context.Context, metricsURL string) ([]*dto.MetricFamily, error) {
	parsed, err := ValidateMetricsURL(metricsURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, errors.New("configured metrics URL is invalid")
	}
	req.Header.Set("Accept", "text/plain; version=0.0.4, application/openmetrics-text; version=1.0.0, text/plain")
	resp, err := s.client.Do(req)
	if err != nil {
		// url.Error carries the URL's userinfo — net/http masks only the
		// password — and a username can itself be a secret. Never surface
		// it unredacted.
		return nil, fmt.Errorf("metrics scrape of %s failed: %s", redacted(parsed), redactError(parsed, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics scrape of %s failed: %s", redacted(parsed), resp.Status)
	}
	families, err := parsePrometheus(io.LimitReader(resp.Body, maxScrapeBytes))
	if err != nil {
		return nil, fmt.Errorf("metrics response from %s was malformed", redacted(parsed))
	}
	return families, nil
}

// ValidateMetricsURL checks that raw is a usable http(s) URL and returns its
// parsed form.
func ValidateMetricsURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return nil, errors.New("configured metrics URL is invalid")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("configured metrics URL must be http or https")
	}
	return parsed, nil
}

// RedactedURL renders raw safe for diagnostics: the entire userinfo
// component is removed, not just the password — a username can itself be a
// bearer token. Host and path are kept for debugging.
func RedactedURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "(invalid URL)"
	}
	return redacted(parsed)
}

func redacted(parsed *url.URL) string {
	sanitized := *parsed
	sanitized.User = nil
	return sanitized.String()
}

func redactError(parsed *url.URL, err error) string {
	message := strings.ReplaceAll(err.Error(), parsed.String(), redacted(parsed))
	// The URL can survive inside the error in other renderings: verbatim,
	// with net/http's own password masking (user:***@), or as bare userinfo.
	if parsed.User != nil {
		message = strings.ReplaceAll(message, parsed.User.String()+"@", "")
		message = strings.ReplaceAll(message, parsed.User.Username()+":***@", "")
		message = strings.ReplaceAll(message, parsed.User.Username()+"@", "")
	}
	return message
}

// parsePrometheus decodes the Prometheus text exposition format. The text
// format is the common denominator every supported backend serves; anything
// unparseable is an error the caller degrades on.
func parsePrometheus(r io.Reader) ([]*dto.MetricFamily, error) {
	decoder := expfmt.NewDecoder(r, expfmt.NewFormat(expfmt.TypeTextPlain))
	var families []*dto.MetricFamily
	for {
		family := &dto.MetricFamily{}
		switch err := decoder.Decode(family); {
		case errors.Is(err, io.EOF):
			return families, nil
		case err != nil:
			return nil, err
		}
		families = append(families, family)
	}
}
