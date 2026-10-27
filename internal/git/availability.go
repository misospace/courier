package git

import (
	"context"
	"fmt"
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
// remotes the probe is a minimal handshake over a TCP connection to the
// endpoint — the ssh server's own identification banner, and a probe request
// plus the daemon's answer for git:// — so an endpoint that answers proves
// it was reached, while one that closes or stalls before answering left the
// handshake unobserved. A remote with no
// probeable network endpoint — a local path, file://, an unknown scheme,
// or a URL that does not parse — reports available so the classification
// fails closed to the caller's pre-existing behavior instead of guessing.
func remoteEndpointAvailable(ctx context.Context, remoteURL string) bool {
	trimmed := strings.TrimSpace(remoteURL)
	if !strings.Contains(trimmed, "://") {
		if host := scpLikeHost(trimmed); host != "" {
			return sshEndpointAvailable(ctx, net.JoinHostPort(host, "22"))
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
	case "ssh":
		port := parsed.Port()
		if port == "" {
			port = "22"
		}
		return sshEndpointAvailable(ctx, net.JoinHostPort(parsed.Hostname(), port))
	case "git":
		port := parsed.Port()
		if port == "" {
			port = "9418"
		}
		return gitEndpointAvailable(ctx, net.JoinHostPort(parsed.Hostname(), port), parsed.Hostname())
	default:
		return true
	}
}

// httpEndpointAvailable probes the smart-HTTP ref advertisement endpoint
// and reports whether the endpoint was available to it: the request must
// complete with a status the endpoint's operator, not the network, chose.
// Redirects are not followed — the probe is exactly one request to the
// endpoint git itself would read, never a chain to another host — and a
// redirect answer proves the endpoint answered, so it is available.
func httpEndpointAvailable(ctx context.Context, parsed *url.URL) bool {
	endpoint := parsed.Scheme + "://" + parsed.Host + strings.TrimSuffix(parsed.Path, "/") + "/info/refs?service=git-upload-pack"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return true
	}
	resp, err := (&http.Client{
		Timeout: remoteProbeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}).Do(req)
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

// sshEndpointAvailable reports whether an ssh endpoint answers a minimal
// handshake: an ssh server speaks first (RFC 4253 sends its identification
// string before any client input), so after the dial one byte from the peer
// proves the endpoint was reached and answered, while a connection that is
// closed, reset, or left silent past the probe deadline before any byte
// proves only that the handshake went unobserved.
func sshEndpointAvailable(ctx context.Context, address string) bool {
	conn, err := (&net.Dialer{Timeout: remoteProbeTimeout}).DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	defer conn.Close()
	return endpointAnswered(ctx, conn)
}

// gitEndpointAvailable reports whether a git:// daemon answers a minimal
// handshake: the git protocol makes the client speak first, so the probe
// sends a pkt-line request for a path it cannot resolve and then reads; any
// byte back — a ref advertisement or an error line — proves the endpoint was
// reached and answered, while a connection lost or left silent before an
// answer proves only that the handshake went unobserved.
func gitEndpointAvailable(ctx context.Context, address, host string) bool {
	conn, err := (&net.Dialer{Timeout: remoteProbeTimeout}).DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(probeDeadline(ctx))
	_, err = conn.Write([]byte(pktLine("git-upload-pack /probe-invalid\x00host=" + host + "\x00")))
	if err != nil {
		return false
	}
	return endpointAnswered(ctx, conn)
}

// endpointAnswered reads once and reports whether the peer sent any byte
// before the connection was lost, the probe deadline passed, or the
// caller's context ended — the same cancellation the HTTP probe honors.
// The read keeps its deadline so the helper's goroutine, if the caller's
// context ends first, still finishes when the deadline does.
func endpointAnswered(ctx context.Context, conn net.Conn) bool {
	deadline := probeDeadline(ctx)
	_ = conn.SetReadDeadline(deadline)
	answered := make(chan bool, 1)
	go func() {
		var one [1]byte
		n, _ := conn.Read(one[:])
		answered <- n > 0
	}()
	select {
	case result := <-answered:
		return result
	case <-ctx.Done():
		return false
	}
}

// probeDeadline bounds the probe's own I/O: the earlier of the caller's
// deadline, if any, and remoteProbeTimeout from now.
func probeDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(remoteProbeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return deadline
}

// pktLine frames payload as one git pkt-line packet.
func pktLine(payload string) string {
	return fmt.Sprintf("%04x%s", len(payload)+4, payload)
}

// scpLikeHost extracts the host from the scp-like remote form git accepts
// for ssh remotes without an explicit scheme — user@host:path and the
// userless host:path form alike — and returns the empty string when the URL
// does not have that shape. Git treats a colon before any slash as the
// scp-like separator, so a prefix containing a slash is a local path, not a
// host; a space or backslash in the prefix is rejected too, since neither
// can appear in a dialable host. An unbracketed IPv6 literal whose first
// segment is empty (::1) has its separator before any host text, and a
// bracketed literal is rejected by its bracket: both fail closed to git's
// own verdict. A literal with a non-empty first segment (fe80::1) splits at
// the first colon exactly as git itself splits it, so the extracted host
// always matches the host git would dial.
func scpLikeHost(remoteURL string) string {
	colon := strings.Index(remoteURL, ":")
	if colon < 1 || strings.ContainsAny(remoteURL[:colon], "/\\ [") {
		return ""
	}
	host := remoteURL[:colon]
	if at := strings.Index(host, "@"); at >= 0 {
		if at == 0 {
			return ""
		}
		host = host[at+1:]
	}
	return host
}
