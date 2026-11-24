// Package gitegress enforces the run's approved git surface on the
// loopback egress guard of an App-mode coordinator pod (#902).
//
// The guard is the only process that can serve the run's installation-token
// credentials: it resolves them itself, injects them into the upstream
// request, and refuses every operation the run was not approved for before
// any credential leaves the guard. Git never tells an askpass helper which
// refspec a push carries, so a ref-and-operation binding is impossible at the
// prompt; it is possible here, where the push request itself is parsed.
//
// The approved surface is narrow by construction: fetch (upload-pack) for the
// run's two pinned repositories, and receive-pack push that updates only the
// run's approved work ref of the work repository. Everything else — other
// refs, ref deletion, the base repository as a push destination, non-git
// endpoints — is refused without an upstream call, so the credential is
// never spent on an unapproved operation.
package gitegress

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	serviceUploadPack  = "git-upload-pack"
	serviceReceivePack = "git-receive-pack"

	// endpointInfoRefs names the advertisement endpoint; its query service
	// names the service being advertised. The push endpoints are named by
	// their service directly.
	endpointInfoRefs = "info/refs"

	// maxUpdateLines bounds the ref-update section of one push. A real
	// publish updates one ref; the bound only has to exceed any plausible
	// batch so a hostile body cannot grow the guard's parse unbounded.
	maxUpdateLines = 32
	// maxLineLength bounds one pkt-line of the update section. Capabilities
	// travel on the first update line, so the bound carries them; nothing
	// legitimate in the section is longer.
	maxLineLength = 65536
)

// Credential is one git credential the guard may inject upstream.
type Credential struct {
	Username string
	Token    string
}

// CredentialProvider resolves the run's credential for one pinned
// repository identity. Implementations must refuse identities they do not
// serve.
type CredentialProvider interface {
	Credentials(ctx context.Context, identity string) (Credential, error)
}

// Remote binds one absolute forge remote URL to the repository identity it
// serves. Push marks the destination a receive-pack may target: the work
// repository only, or the single repository of a same-repository run.
type Remote struct {
	URL      string
	Identity string
	Push     bool
}

// Config is the guard's run-pinned surface.
type Config struct {
	// Remotes are the run's configured remotes, one per distinct repository.
	Remotes []Remote
	// WorkRef is the fully qualified refs/heads/… ref a push may update.
	WorkRef string
	// Credentials resolves the credential for a pinned identity. It is
	// consulted only after the request and (for pushes) the ref-update
	// section passed.
	Credentials CredentialProvider
}

type binding struct {
	path     string // normalized repository path, shared by both remote spellings
	identity string
	upstream string // the absolute forge remote URL
	push     bool
}

// Guard serves exactly the run's approved git surface and nothing else.
type Guard struct {
	bindings    []binding
	workRef     string
	credentials CredentialProvider
	upstream    *http.Client
}

// New validates the pinned surface and builds the guard. Every malformed
// input is a configuration refusal, not a request-time surprise.
func New(config Config) (*Guard, error) {
	if config.Credentials == nil {
		return nil, errors.New("gitegress: credential provider is required")
	}
	if err := validateWorkRef(config.WorkRef); err != nil {
		return nil, err
	}
	if len(config.Remotes) == 0 {
		return nil, errors.New("gitegress: at least one pinned remote is required")
	}
	guard := &Guard{
		workRef:     config.WorkRef,
		credentials: config.Credentials,
		// The guard never follows a redirect: the credential it injects may
		// travel only to the pinned upstream, and a redirect it passed back
		// is the client's problem (git reports it), not a token forward.
		upstream: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
	seen := map[string]string{}
	for _, remote := range config.Remotes {
		identity := strings.TrimSpace(remote.Identity)
		if !strings.Contains(identity, "/") || strings.ContainsAny(identity, " \t\r\n\x00") {
			return nil, fmt.Errorf("gitegress: remote identity %q is not an owner/name", remote.Identity)
		}
		parsed, err := url.Parse(strings.TrimSpace(remote.URL))
		if err != nil || parsed.Host == "" || parsed.User != nil ||
			parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
			return nil, fmt.Errorf("gitegress: remote URL %q is not an absolute URL without userinfo, query, or fragment", remote.URL)
		}
		// The production forge is always https. Plain http is admitted only
		// for a loopback-IP host so a deterministic test can stand a local
		// upstream behind the guard; a routable http host is refused.
		if parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname())) {
			return nil, fmt.Errorf("gitegress: remote URL %q must be https", remote.URL)
		}
		path := normalizeRepositoryPath(parsed.EscapedPath())
		if path == "" || path == "/" {
			return nil, fmt.Errorf("gitegress: remote URL %q carries no repository path", remote.URL)
		}
		if previous, ok := seen[path]; ok && !strings.EqualFold(previous, identity) {
			return nil, fmt.Errorf("gitegress: remote path %q is pinned to two identities", path)
		}
		seen[path] = identity
		guard.bindings = append(guard.bindings, binding{
			path: path, identity: identity, upstream: parsed.String(), push: remote.Push,
		})
	}
	return guard, nil
}

// resolve maps a request onto the pinned repository and git service it may
// serve, or refuses. Only the smart-HTTP service endpoints of a pinned
// repository resolve; a receive-pack resolves only for a push-allowed remote,
// and a receive-pack advertisement only when the push itself would resolve.
func (g *Guard) resolve(rawURL *url.URL, method string) (binding, string, error) {
	escaped := rawURL.EscapedPath()
	var rest, endpoint, wantService string
	switch {
	case strings.HasSuffix(escaped, "/"+endpointInfoRefs):
		rest = strings.TrimSuffix(escaped, "/"+endpointInfoRefs)
		endpoint = endpointInfoRefs
		if method != "GET" || len(rawURL.Query()["service"]) != 1 {
			return binding{}, "", errors.New("refused: info/refs requires exactly one service")
		}
		wantService = rawURL.Query().Get("service")
		if wantService != serviceUploadPack && wantService != serviceReceivePack {
			return binding{}, "", errors.New("refused: info/refs names an unknown service")
		}
	case strings.HasSuffix(escaped, "/"+serviceUploadPack):
		rest = strings.TrimSuffix(escaped, "/"+serviceUploadPack)
		endpoint = serviceUploadPack
		if method != "POST" {
			return binding{}, "", errors.New("refused: upload-pack requires POST")
		}
	case strings.HasSuffix(escaped, "/"+serviceReceivePack):
		rest = strings.TrimSuffix(escaped, "/"+serviceReceivePack)
		endpoint = serviceReceivePack
		if method != "POST" {
			return binding{}, "", errors.New("refused: receive-pack requires POST")
		}
	default:
		return binding{}, "", errors.New("refused: request is outside the pinned git surface")
	}
	path := normalizeRepositoryPath(rest)
	for _, candidate := range g.bindings {
		if candidate.path == path {
			// An advertisement for receive-pack previews a push the guard
			// would later refuse, so a read-only repository is refused here
			// too: git must see the destination as closed from the first
			// request, not discover it mid-push.
			if !candidate.push && (endpoint == serviceReceivePack ||
				(endpoint == endpointInfoRefs && wantService == serviceReceivePack)) {
				return binding{}, "", errors.New("refused: repository is read-only for this run")
			}
			return candidate, endpoint, nil
		}
	}
	return binding{}, "", errors.New("refused: repository is not pinned by this run")
}

// isLoopbackHost reports a loopback-IP host (not the name "localhost", which
// a resolver could point anywhere).
func isLoopbackHost(host string) bool {
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// normalizeRepositoryPath reduces a repository path to its comparison form:
// lowercase, no trailing slash, no trailing .git — the same form the
// credential binding used before this guard existed.
func normalizeRepositoryPath(raw string) string {
	path := strings.TrimRight(raw, "/")
	path = strings.TrimSuffix(strings.ToLower(path), ".git")
	return strings.TrimRight(path, "/")
}

// checkReceivePack reads the pkt-line ref-update section of a receive-pack
// request and refuses the push unless every update targets the run's approved
// work ref. It returns the exact bytes consumed so the caller can forward the
// request unchanged, with the unread remainder of the body behind them.
// Refusal is all-or-nothing: a section with one unapproved ref forwards
// nothing.
func (g *Guard) checkReceivePack(body io.Reader) ([]byte, *bufio.Reader, error) {
	buffered := bufio.NewReader(body)
	var consumed bytes.Buffer
	updates := 0
	for {
		raw, err := readPktLine(buffered, &consumed)
		if err != nil {
			return nil, nil, err
		}
		if raw == nil { // flush-pkt ends the ref-update section
			break
		}
		updates++
		if updates > maxUpdateLines {
			return nil, nil, errors.New("refused: push names too many refs")
		}
		if err := g.checkUpdateLine(raw); err != nil {
			return nil, nil, err
		}
	}
	if updates == 0 {
		return nil, nil, errors.New("refused: push names no ref")
	}
	return consumed.Bytes(), buffered, nil
}

// checkUpdateLine validates one raw ref-update line: <old> SP <new> SP <ref>
// with an optional NUL-delimited capability suffix. Both OIDs must be full
// lowercase-or-uppercase hex of one supported length; deleting a ref (a zero
// new OID) is never an approved operation; the ref must be exactly the run's
// work ref.
func (g *Guard) checkUpdateLine(line []byte) error {
	if i := bytes.IndexByte(line, 0); i >= 0 {
		line = line[:i]
	}
	line = bytes.TrimSuffix(line, []byte("\n"))
	fields := bytes.Split(line, []byte(" "))
	if len(fields) != 3 {
		return errors.New("refused: malformed ref update")
	}
	if !isOID(fields[0]) || !isOID(fields[1]) {
		return errors.New("refused: malformed object id in ref update")
	}
	if isZeroOID(fields[1]) {
		return errors.New("refused: deleting a ref is not an approved operation")
	}
	if string(fields[2]) != g.workRef {
		return fmt.Errorf("refused: ref update outside the approved work ref")
	}
	return nil
}

func isOID(value []byte) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, char := range value {
		switch {
		case char >= '0' && char <= '9', char >= 'a' && char <= 'f', char >= 'A' && char <= 'F':
		default:
			return false
		}
	}
	return true
}

func isZeroOID(value []byte) bool {
	for _, char := range value {
		if char != '0' {
			return false
		}
	}
	return true
}

// readPktLine reads one pkt-line, appends its exact bytes to consumed, and
// returns the payload without the length prefix, or nil for a flush-pkt. A
// delimiter packet has no meaning in the ref-update section, and oversized or
// short packets are refusals, so the bound holds before memory does.
func readPktLine(reader *bufio.Reader, consumed *bytes.Buffer) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, fmt.Errorf("refused: truncated pkt-line header: %w", err)
	}
	consumed.Write(header[:])
	length, err := parseHex4(header[:])
	if err != nil {
		return nil, err
	}
	switch {
	case length == 0: // flush-pkt
		return nil, nil
	case length == 1: // delimiter-pkt
		return nil, errors.New("refused: unexpected pkt-line delimiter")
	case length < 4:
		return nil, errors.New("refused: malformed pkt-line length")
	case length-4 > maxLineLength:
		return nil, errors.New("refused: oversized pkt-line")
	}
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("refused: truncated pkt-line: %w", err)
	}
	consumed.Write(payload)
	return payload, nil
}

func parseHex4(header []byte) (int, error) {
	length := 0
	for _, char := range header {
		length <<= 4
		switch {
		case char >= '0' && char <= '9':
			length |= int(char - '0')
		case char >= 'a' && char <= 'f':
			length |= int(char-'a') + 10
		case char >= 'A' && char <= 'F':
			length |= int(char-'A') + 10
		default:
			return 0, errors.New("refused: malformed pkt-line length")
		}
	}
	return length, nil
}

// validateWorkRef pins the approved destination ref to a fully qualified,
// single-branch ref that can travel in a ref update: refs/heads/… with no
// empty component and no character git or a URL would treat specially.
func validateWorkRef(ref string) error {
	const prefix = "refs/heads/"
	if !strings.HasPrefix(ref, prefix) || len(ref) == len(prefix) {
		return errors.New("gitegress: the approved work ref must be a fully qualified refs/heads/… ref")
	}
	if strings.ContainsAny(ref, " \t\r\n\x00~^:?*[\\") || strings.Contains(ref, "..") ||
		strings.HasSuffix(ref, ".") || strings.HasSuffix(ref, ".lock") || strings.Contains(ref, "//") {
		return errors.New("gitegress: the approved work ref is not a safe branch name")
	}
	for _, component := range strings.Split(strings.TrimPrefix(ref, prefix), "/") {
		if component == "" || strings.HasPrefix(component, ".") || strings.HasSuffix(component, ".lock") {
			return errors.New("gitegress: the approved work ref is not a safe branch name")
		}
	}
	return nil
}
