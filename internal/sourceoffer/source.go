// Package sourceoffer advertises the corresponding source of a deployed Courier build.
package sourceoffer

import (
	"net/http"
	"net/url"
	"os"
	"strings"
)

// ReleaseURL is set by the official release build, after its source archive is published.
var ReleaseURL string

// URL returns an operator override or the built-in release URL, never a request-derived URL.
func URL() string {
	raw := ReleaseURL
	if override, ok := os.LookupEnv("COURIER_SOURCE_URL"); ok {
		raw = override
	}
	if strings.ContainsAny(raw, "<>\\\" \t\r\n") {
		return ""
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return ""
	}
	return u.String()
}

// Wrap advertises source to API clients without changing routing or authentication.
func Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if source := URL(); source != "" {
			w.Header().Add("Link", "<"+source+">; rel=\"source\"")
		}
		next.ServeHTTP(w, r)
	})
}

// Serve redirects to configured corresponding source, or reports an unconfigured build honestly.
func Serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if source := URL(); source != "" {
		http.Redirect(w, r, source, http.StatusSeeOther)
		return
	}
	http.Error(w, "Corresponding source URL is not configured; contact the deployment operator.", http.StatusServiceUnavailable)
}
