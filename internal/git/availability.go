package git

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// remoteUnavailableError marks a remote read whose git process ran to
// completion and failed while the remote's own network endpoint was not
// available to a direct probe: the endpoint could not be reached, the
// connection was lost before an answer, or the endpoint answered with a
// rate limit or a server error. The probe's observation — not a text match
// on git's stderr — is the evidence, so the error is transient availability
// evidence for forge.IsTransientAvailability: the world the read was meant
// to observe was never observed. An endpoint that answered with any other
// status never produces this error, because there git's non-zero exit is
// its own verdict about a world it reached.
type remoteUnavailableError struct {
	err *CommandError
}

func (e *remoteUnavailableError) Error() string { return e.err.Error() }

func (e *remoteUnavailableError) Unwrap() error { return e.err }

func (e *remoteUnavailableError) TransientAvailability() bool { return true }

// remoteProbeTimeout bounds one endpoint availability probe. It runs only
// on a failed remote read, so a short bound keeps the failure path from
// outliving the caller's patience without deciding anything about the
// caller's own context deadline.
const remoteProbeTimeout = 5 * time.Second

// remoteEndpointAvailable reports whether the remote's network endpoint was
// available to a direct probe. For http(s) remotes the probe is one
// unauthenticated request to the smart-HTTP endpoint git itself would
// read: any completed response proves the endpoint was reached and answered,
// while a request that could not complete, and a 429 or 5xx response, match
// the forge API's own transient-availability vocabulary. For ssh and git
// remotes the probe is a TCP dial of the endpoint. A remote with no
// probeable network endpoint — a local path, file://, an unknown scheme,
// or a URL that does not parse — reports available so the classification
// fails closed to the caller's pre-existing behavior instead of guessing.
func remoteEndpointAvailable(ctx context.Context, remoteURL string) bool {
	trimmed := strings.TrimSpace(remoteURL)
	if !strings.Contains(trimmed, "://") {
		if host := scpLikeHost(trimmed); host != "" {
			return dialAvailable(ctx, net.JoinHostPort(host, "22"))
		}
		return true
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return true
	}
	switch parsed.Scheme {
	case "http", "https":
		return httpEndpointAvailable(ctx, parsed)
	case "ssh", "git":
		port := parsed.Port()
		if port == "" {
			if parsed.Scheme == "git" {
				port = "9418"
			} else {
				port = "22"
			}
		}
		return dialAvailable(ctx, net.JoinHostPort(parsed.Hostname(), port))
	default:
		return true
	}
}

// httpEndpointAvailable probes the smart-HTTP ref advertisement endpoint
// and reports whether the endpoint was available to it: the request must
// complete with a status the endpoint's operator, not the network, chose.
func httpEndpointAvailable(ctx context.Context, parsed *url.URL) bool {
	endpoint := parsed.Scheme + "://" + parsed.Host + strings.TrimSuffix(parsed.Path, "/") + "/info/refs?service=git-upload-pack"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return true
	}
	resp, err := (&http.Client{Timeout: remoteProbeTimeout}).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
		return false
	}
	return true
}

// dialAvailable reports whether a TCP connection to address can complete.
func dialAvailable(ctx context.Context, address string) bool {
	dialer := &net.Dialer{Timeout: remoteProbeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scpLikeHost extracts the host from the scp-like remote form git accepts
// for ssh remotes without an explicit scheme — user@host:path — and returns
// the empty string when the URL does not have that shape. A host that still
// contains a colon (an unbracketed IPv6 literal) is not extracted: it has no
// unambiguous dial target, so the remote reports available and the
// classification fails closed to git's own verdict.
func scpLikeHost(remoteURL string) string {
	at := strings.Index(remoteURL, "@")
	colon := strings.Index(remoteURL, ":")
	if at <= 0 || colon < at+1 || strings.ContainsAny(remoteURL[:colon], "/ ") {
		return ""
	}
	host := remoteURL[at+1 : colon]
	if strings.Contains(host, ":") {
		return ""
	}
	return host
}
