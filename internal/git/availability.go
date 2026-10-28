package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// remoteUnavailableError marks a remote read that left the world unobserved.
// It covers both shapes of that condition: the read was cut short before git
// answered — the git process was killed by its deadline, by its
// cancellation, or by a signal — and the read ran and failed while the
// remote's own network endpoint was not available to a direct probe: the
// endpoint could not be reached, the connection was lost before an answer,
// or the endpoint answered with a rate limit or a server error.
//
// The probe's observation — not a text match on git's stderr — is the
// evidence, so the error is transient availability evidence for
// forge.IsTransientAvailability: the world the read was meant to observe was
// never observed. An endpoint that answered with any other status never
// produces this error, because there git's non-zero exit is its own verdict
// about a world it reached; neither does a probe the caller's own
// cancellation or deadline cut short, because such a probe yields no evidence
// at all.
type remoteUnavailableError struct {
	err *CommandError
}

func (e *remoteUnavailableError) Error() string { return e.err.Error() }

func (e *remoteUnavailableError) Unwrap() error { return e.err }

func (e *remoteUnavailableError) TransientAvailability() bool { return true }

// remoteProbeTimeout bounds one endpoint availability probe. It runs only
// on a failed remote read, so a short bound keeps the failure path from
// outliving the caller's patience without deciding anything about the
// caller's own context deadline. The ssh path resolves the effective
// endpoint through ssh before the handshake probe, so that remote's failure
// path can use up to two such bounds in sequence, each still the probe's own.
const remoteProbeTimeout = 5 * time.Second

// remoteEndpointAvailable reports the probe's evidence about the remote's
// network endpoint.
//
// A false answer is the transient-availability evidence: the endpoint was
// unavailable to a direct probe — it could not be reached, it closed or
// reset before answering, it stalled past the probe's own bound, or it
// answered 429/5xx. A true answer means the probe found no evidence of
// unavailability: the endpoint answered, the remote has no probeable network
// endpoint at all (a local path, file://, an unknown scheme, or a URL that
// does not parse), the effective ssh endpoint of an ssh remote could not be
// resolved, or the caller's own cancellation or deadline cut the probe
// short before it could observe anything. No evidence leaves git's own verdict
// standing, so the classification fails closed to the caller's pre-existing
// behavior instead of guessing.
//
// For http(s) remotes the probe is one unauthenticated request to the
// smart-HTTP endpoint git itself would read: any completed response proves
// the endpoint was reached and answered, while a request that could not
// complete, and a 429 or 5xx response, match the forge API's own
// transient-availability vocabulary. For ssh and git remotes the probe is a
// minimal handshake over a TCP connection to the endpoint — the ssh server's
// own identification banner, and a probe request plus the daemon's answer
// for git:// — so an endpoint that answers proves it was reached, while one
// that closes or stalls before answering left the handshake unobserved.
// For ssh remotes the address dialed is not the literal URL host: git hands
// the destination to ssh — the command its environment and configuration
// select, the same one the probe resolves through (see sshProbeCommand) —
// which applies the user's ssh configuration — a HostName rewrite, a Port —
// before connecting, so the probe resolves the effective endpoint the same
// way (see sshEffectiveAddress) and dials that.
// An ssh remote whose effective endpoint cannot be resolved yields no
// evidence rather than a probe of an endpoint ssh would never dial.
func remoteEndpointAvailable(ctx context.Context, remoteURL string) bool {
	trimmed := strings.TrimSpace(remoteURL)
	if !strings.Contains(trimmed, "://") {
		if target := scpLikeTarget(trimmed); target != "" {
			return sshRemoteAvailable(ctx, target, "")
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
		target := parsed.Hostname()
		if user := parsed.User.Username(); user != "" {
			target = user + "@" + target
		}
		return sshRemoteAvailable(ctx, target, parsed.Port())
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

// httpEndpointAvailable probes the smart-HTTP ref advertisement endpoint and
// reports the probe's evidence about it.
//
// A false answer means the endpoint was unavailable: the request could not
// complete, or the endpoint answered with a rate limit or a server error.
// A true answer means the probe found no evidence of unavailability: the
// endpoint answered, the request could not even be built, or the caller's own
// context ended before the request could complete — a probe the caller's
// cancellation or deadline cut short observed nothing, so it must not be read
// as evidence that the endpoint was down.
//
// The request must complete with a status the endpoint's operator, not the
// network, chose. Redirects are not followed — the probe is exactly one
// request to the endpoint git itself would read, never a chain to another
// host — and a redirect answer proves the endpoint answered, so it is
// available.
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
		if callerContextEnded(ctx) {
			// The caller's context ended: the probe observed nothing,
			// so it yields no evidence about the endpoint.
			return true
		}
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
		return false
	}
	return true
}

// sshRemoteAvailable reports the probe's evidence about the ssh endpoint git
// itself would dial: the effective host and port ssh selects for the
// destination after applying the user's ssh configuration. target is the
// ssh destination git hands to ssh — host or user@host — and urlPort is an
// explicit port from the remote URL, empty when the URL names none.
//
// An effective endpoint that cannot be resolved yields no evidence: git's
// verdict stands. Dialing a literal URL host ssh would never connect to is
// not evidence about the remote — it is a probe of an endpoint chosen by
// this code alone.
func sshRemoteAvailable(ctx context.Context, target, urlPort string) bool {
	address, ok := sshEffectiveAddress(ctx, target, urlPort)
	if !ok {
		return true
	}
	return sshEndpointAvailable(ctx, address)
}

// sshConfigOutputLimit caps the ssh configuration output the probe reads.
// The effective configuration is a fixed set of keyword lines, so anything
// past the cap means the probe cannot trust the output.
const sshConfigOutputLimit = 1 << 16

// sshEffectiveAddress resolves the host:port git itself would dial for an
// ssh remote: the probe asks the same ssh command git would invoke (see
// sshProbeCommand), and `ssh -G` prints the destination's effective
// configuration after the user's ssh configuration — HostName rewrites,
// Port, and Match blocks — is applied, without connecting. An explicit URL
// port is passed with -p after the command's own options because git passes
// it to ssh after the same options: both processes hand the user's options
// the same precedence position, so where they overlap — a -p in
// GIT_SSH_COMMAND, say — ssh's own option precedence resolves the conflict
// identically for both, and the configuration's Port is overridden alike.
//
// The answer is false — no endpoint, so the caller yields no evidence —
// when the ssh command cannot start at all, exits non-zero on an unreadable
// or invalid configuration, reports an effective connection routed through
// a proxy or jump host the probe cannot mirror, produces output past the
// probe's cap or without a usable hostname and port, or the caller's
// context ends before ssh answers. The probe never guesses an endpoint:
// what the resolved ssh command reports is the only address it will dial.
func sshEffectiveAddress(ctx context.Context, target, urlPort string) (string, bool) {
	// A destination starting with "-" would reach ssh as an option rather
	// than a host, and a non-numeric URL port is not something git could
	// hand to ssh either: neither is a destination whose endpoint this
	// probe can resolve.
	if target == "" || strings.HasPrefix(target, "-") {
		return "", false
	}
	binary, preArgs, ok := sshProbeCommand(ctx)
	if !ok {
		return "", false
	}
	args := append([]string(nil), preArgs...)
	args = append(args, "-G")
	if urlPort != "" {
		port, err := strconv.Atoi(urlPort)
		if err != nil || port < 1 || port > 65535 {
			return "", false
		}
		args = append(args, "-p", strconv.Itoa(port))
	}
	args = append(args, target)
	ctx, cancel := context.WithTimeout(ctx, remoteProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout boundedBuffer
	stdout.limit = sshConfigOutputLimit
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil || stdout.overflow {
		return "", false
	}
	return sshEffectiveEndpoint(stdout.Bytes())
}

// sshProbeCommand resolves the ssh command the probe must invoke so it reads
// the configuration of the ssh process git itself would run: git honors
// GIT_SSH_COMMAND, core.sshCommand, GIT_SSH, and plain ssh, in that order of
// precedence, and a probe of a different ssh command could resolve an
// endpoint that process would never dial. The read runs outside any
// repository — the remote-publication read passes no directory — so
// core.sshCommand is read from git's system and global configuration alone,
// exactly the scopes that read would apply. A key present with a blank or
// whitespace-only value answers false rather than falling through: git takes
// that value as the command it runs, dials nothing with it, and an endpoint
// resolved through a command git never selected is evidence of the wrong
// target.
//
// A shell-command value — GIT_SSH_COMMAND or core.sshCommand, which git runs
// through the shell — is resolved as the shell resolves it for word
// splitting only: quoted segments group their characters into one token,
// adjacent quoted and unquoted parts concatenate, and the first token is the
// binary. A value that relies on any other shell feature — command
// substitution, expansion, control operators, a metacharacter or a command
// separator outside quotes — or that resolves to no first token at all,
// answers false, because the probe cannot faithfully emulate it and would
// rather yield no evidence than resolve an endpoint that command line would
// never dial; a value git itself could not run dials nothing, so no evidence
// is the truthful answer there too. The same doubt governs a first token
// starting with "-": neither can name an ssh binary whose configuration the
// probe could read. GIT_SSH is taken as one literal path — git exec's it
// directly, never through a shell, so no splitting happens and the value is
// not trimmed — and a value that is present but blank is not a path git
// could exec, so it answers false rather than falling through to plain ssh.
// The answer is false whenever the environment names a command the probe
// cannot mirror; the "ssh" fallback is the case where git itself would run
// plain ssh. An unreadable git configuration (git itself failing on the
// core.sshCommand lookup with anything but "key unset") is also no endpoint:
// the probe cannot rule out a command it could not read.
func sshProbeCommand(ctx context.Context) (string, []string, bool) {
	if value, ok := os.LookupEnv("GIT_SSH_COMMAND"); ok {
		return sshCommandLine(ctx, value)
	}
	value, set, err := gitConfigSSHCommand(ctx)
	if err != nil {
		return "", nil, false
	}
	if set {
		if value == "" {
			return "", nil, false
		}
		return sshCommandLine(ctx, value)
	}
	if path, ok := os.LookupEnv("GIT_SSH"); ok {
		if path == "" || strings.HasPrefix(path, "-") {
			return "", nil, false
		}
		return path, nil, true
	}
	return "ssh", nil, true
}

// sshCommandLine resolves one shell-command-line ssh selection (a
// GIT_SSH_COMMAND or core.sshCommand value) into the binary and leading
// arguments the probe must invoke. The answer is false for a line the shell
// would run as more than one command, as expansion, or as no command at all:
// the probe mirrors word splitting and nothing else.
func sshCommandLine(ctx context.Context, command string) (string, []string, bool) {
	tokens, ok := splitShellTokens(command)
	if !ok || len(tokens) == 0 || strings.HasPrefix(tokens[0], "-") {
		return "", nil, false
	}
	return tokens[0], append([]string(nil), tokens[1:]...), true
}

// gitConfigSSHCommand reads core.sshCommand from git's system and global
// configuration the way a remote read with no repository would see it. The
// boolean reports whether the key is present: git config --get exits 1 when
// the key is unset (false with a nil error) and exits 0 with an empty value
// when the key is present but blank (true with an empty string), and the
// caller must tell those apart because git runs a present blank value as the
// command itself rather than falling through. Any other failure is an error
// the caller fails closed on.
func gitConfigSSHCommand(ctx context.Context) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, remoteProbeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "config", "--get", "core.sshCommand").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(string(out)), true, nil
}

// splitShellTokens splits a shell command line into tokens the way a shell's
// word splitting does and no further: unquoted blanks (space and tab)
// separate tokens, single- and double-quoted segments group their characters
// — including whitespace — into the current token, and adjacent parts
// concatenate. A shell metacharacter outside quotes — a control operator, a
// command separator (a newline), an expansion — an unterminated quote, or an
// expansion metacharacter inside double quotes makes the line unemulatable,
// and the answer is false: the probe never executes a shell, so a line whose
// meaning depends on one must resolve nothing rather than something else.
// Inside single quotes every character is literal; inside double quotes a
// shell would still expand parameters and commands, so those metacharacters
// fail closed there too.
func splitShellTokens(command string) ([]string, bool) {
	var tokens []string
	var token strings.Builder
	quoted := byte(0)
	started := false
	for i := 0; i < len(command); i++ {
		c := command[i]
		switch {
		case quoted != 0:
			if c == quoted {
				quoted = 0
				continue
			}
			if quoted == '"' && strings.IndexByte("$`\\", c) >= 0 {
				return nil, false
			}
			token.WriteByte(c)
		case c == '\'' || c == '"':
			quoted = c
			started = true
		case c == ' ' || c == '\t':
			if started {
				tokens = append(tokens, token.String())
				token.Reset()
				started = false
			}
		case strings.IndexByte("$|&;<>()*?[]{}~\\#`\n\r", c) >= 0:
			return nil, false
		default:
			token.WriteByte(c)
			started = true
		}
	}
	if quoted != 0 {
		return nil, false
	}
	if started {
		tokens = append(tokens, token.String())
	}
	return tokens, true
}

// sshEffectiveEndpoint reads the hostname and port out of `ssh -G` output.
// The first value for each keyword wins, as ssh_config's first-obtained
// value does; a missing or unusable value means no effective endpoint. A
// configured proxy — a proxycommand or proxyjump line with a value — means
// no endpoint either: ssh's real connection is routed through the proxy or
// jump host, so a direct dial to the resolved host and port would observe a
// network path ssh never takes, and `ssh -G` prints those lines only when
// they are set: OpenSSH omits an unset string option from the dump and
// clears the documented keyword value "none" to unset while parsing, so a
// dumped "none" names no proxy rather than one — treated as unset here, it
// keeps the ordinary resolution rather than failing it closed.
func sshEffectiveEndpoint(out []byte) (string, bool) {
	host, port := "", ""
	for _, line := range strings.Split(string(out), "\n") {
		for _, proxy := range []string{"proxycommand", "proxyjump"} {
			if value, found := strings.CutPrefix(line, proxy+" "); found {
				if value = strings.TrimRight(value, " \t\r"); value != "" && !strings.EqualFold(value, "none") {
					return "", false
				}
			}
		}
		if host == "" {
			if value, found := strings.CutPrefix(line, "hostname "); found {
				host = strings.TrimRight(value, " \t\r")
			}
		}
		if port == "" {
			if value, found := strings.CutPrefix(line, "port "); found {
				port = strings.TrimRight(value, " \t\r")
			}
		}
	}
	if host == "" {
		return "", false
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", false
	}
	return net.JoinHostPort(host, strconv.Itoa(number)), true
}

// sshEndpointAvailable reports the probe's evidence about an ssh endpoint.
//
// A false answer means the endpoint was unavailable: the dial could not
// complete, or the connection closed, reset, or stayed silent past the
// probe's own bound before any byte arrived. A true answer means the probe
// found no evidence of unavailability: the server spoke its identification
// banner — an ssh server speaks first (RFC 4253 sends its identification
// string before any client input), so one byte from the peer proves the
// endpoint was reached and answered — or the caller's own context ended the
// handshake before it could observe anything.
func sshEndpointAvailable(ctx context.Context, address string) bool {
	conn, err := (&net.Dialer{Timeout: remoteProbeTimeout}).DialContext(ctx, "tcp", address)
	if err != nil {
		if callerContextEnded(ctx) {
			// The caller's context ended: the probe observed nothing.
			return true
		}
		return false
	}
	defer conn.Close()
	return endpointAnswered(ctx, conn)
}

// gitEndpointAvailable reports the probe's evidence about a git:// daemon.
//
// A false answer means the endpoint was unavailable: the dial could not
// complete, the probe request could not be written, or the daemon's answer
// never arrived. A true answer means the probe found no evidence of
// unavailability: any byte back — a ref advertisement or an error line —
// proves the endpoint was reached and answered, since the git protocol
// makes the client speak first; or the caller's own context ended the
// handshake before it could observe anything.
func gitEndpointAvailable(ctx context.Context, address, host string) bool {
	conn, err := (&net.Dialer{Timeout: remoteProbeTimeout}).DialContext(ctx, "tcp", address)
	if err != nil {
		if callerContextEnded(ctx) {
			// The caller's context ended: the probe observed nothing.
			return true
		}
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(probeDeadline(ctx))
	_, err = conn.Write([]byte(pktLine("git-upload-pack /probe-invalid\x00host=" + host + "\x00")))
	if err != nil {
		if callerContextEnded(ctx) {
			// The caller's context ended: the probe observed nothing.
			return true
		}
		return false
	}
	return endpointAnswered(ctx, conn)
}

// endpointAnswered reads once and reports the probe's evidence about the
// peer.
//
// A true answer means the peer answered — one byte arrived — or the wait was
// cut short by the caller's own context, which observed nothing and so
// yields no evidence of unavailability. A false answer means the endpoint was
// unavailable: the peer sent no byte and the probe's own bound ended the
// wait, whether the connection was lost, reset, or simply silent.
//
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
		if result {
			return true
		}
		// The peer sent nothing. Only the probe's own bound ending the
		// wait makes that evidence of unavailability; the caller's own
		// context ending it yields no evidence.
		return callerContextEnded(ctx)
	case <-ctx.Done():
		// The caller's cancellation ended the wait before the peer
		// answered: the probe observed nothing.
		return true
	}
}

// callerContextEnded reports whether the caller's own context ended before
// the probe could observe anything: it was canceled, or its deadline passed.
// The deadline instant decides the deadline half, so the answer does not race
// the context's cancellation goroutine — a context whose deadline has passed
// may not have observed it yet, and reading that as "the endpoint was
// unavailable" would flip a completed git verdict to transient.
func callerContextEnded(ctx context.Context) bool {
	return ctx.Err() != nil || callerDeadlineEnded(ctx)
}

// callerDeadlineEnded reports whether the caller's own deadline ended the
// probe's wait. The deadline instant decides it, so the answer does not race
// the context's cancellation goroutine: a deadline at or before now means
// the caller's deadline ended the wait, and the probe observed nothing.
// A caller with no deadline, or one whose deadline is still in the future,
// left the probe's own bound to end the wait.
func callerDeadlineEnded(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return ok && !deadline.After(time.Now())
}

// probeDeadline bounds the probe's own I/O: the earlier of the caller's
// deadline, if any, and remoteProbeTimeout from now. The bound is the
// probe's own, so a wait the caller's own deadline ends is reported as the
// caller cutting the probe short rather than as the endpoint being
// unavailable.
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

// scpLikeTarget extracts the ssh destination — the user@host or host part —
// from the scp-like remote form git accepts for ssh remotes without an
// explicit scheme: user@host:path and the userless host:path form alike. Git
// hands that part to ssh as the destination, and ssh matches its
// configuration against the destination, so the probe resolves the effective
// endpoint from exactly what ssh would see. The function returns the empty
// string when the URL does not have that shape. Git treats a colon before
// any slash as the scp-like separator, so a prefix containing a slash is a
// local path, not a host; a space or backslash in the prefix is rejected
// too, since neither can appear in a dialable host. An unbracketed IPv6
// literal whose first segment is empty (::1) has its separator before any
// host text, and a bracketed literal is rejected by its bracket: both fail
// closed to git's own verdict. A literal with a non-empty first segment
// (fe80::1) splits at the first colon exactly as git itself splits it, so
// the extracted destination always matches the destination git would hand
// to ssh.
func scpLikeTarget(remoteURL string) string {
	colon := strings.Index(remoteURL, ":")
	if colon < 1 || strings.ContainsAny(remoteURL[:colon], "/\\ [") {
		return ""
	}
	target := remoteURL[:colon]
	host := target
	if at := strings.Index(host, "@"); at >= 0 {
		if at == 0 {
			return ""
		}
		host = host[at+1:]
	}
	if host == "" {
		return ""
	}
	return target
}
