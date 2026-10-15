package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
// bearer token — and the query and fragment are dropped, since query- and
// fragment-authenticated endpoints put credentials there (?token=…).
// Scheme, host, and path are kept for debugging. The scrape itself still
// receives the configured URL unredacted.
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
	sanitized.RawQuery = ""
	sanitized.ForceQuery = false
	sanitized.Fragment = ""
	return sanitized.String()
}

// urlFreeError strips *url.Error wrappers — which embed a URL — down to the
// underlying cause. If the error is not a url.Error, it is returned as-is.
func urlFreeError(err error) string {
	for {
		ue, ok := err.(*url.Error)
		if !ok || ue.Err == nil {
			return err.Error()
		}
		err = ue.Err
	}
}

// queryFragmentRenderings lists every spelling of the URL's query and
// fragment that can appear in an error message: the raw forms, the escaped
// fragment renderings net/url actually emits, and the %q-escaped forms
// url.Error produces. URLs with neither query nor fragment yield an empty
// slice.
func queryFragmentRenderings(parsed *url.URL) []string {
	var forms []string
	if parsed.RawQuery != "" {
		forms = append(forms, "?"+parsed.RawQuery)
	}
	for _, fragment := range []string{parsed.EscapedFragment(), parsed.Fragment, parsed.RawFragment} {
		if fragment != "" {
			forms = append(forms, "#"+fragment)
		}
	}
	var renderings []string
	for _, form := range forms {
		renderings = append(renderings, form)
		if escaped := strconv.Quote(form); len(escaped) > 2 {
			renderings = append(renderings, escaped[1:len(escaped)-1])
		}
	}
	return renderings
}

func redactError(parsed *url.URL, err error) string {
	// Start from the unwrapped cause: *url.Error embeds a request URL, and on
	// a redirect chain that URL can be a redirect target the endpoint chose —
	// one that may repeat a configured credential reflected into its
	// Location. The underlying cause (dial failures, redirect-loop stops,
	// context deadlines) names the operation and address but never a URL, so
	// it is safe to surface.
	message := urlFreeError(err)
	// Defense in depth: an inner error can still echo the configured URL, so
	// scrub every spelling of its credential-bearing parts from whatever
	// text remains. net/url stores Fragment decoded but renders it
	// %-escaped, and url.Error formats URLs through %q, which escapes quotes
	// and backslashes — match each spelling.
	message = strings.ReplaceAll(message, parsed.String(), redacted(parsed))
	for _, rendering := range queryFragmentRenderings(parsed) {
		message = strings.ReplaceAll(message, rendering, "")
	}
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
