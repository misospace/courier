package broker

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	couriergit "github.com/misospace/courier/internal/git"
)

// GitCredentials are broker-owned credentials for one configured provider.
type GitCredentials struct {
	Username string
	Token    string
}

// GitCredentialProvider resolves typed credentials inside the broker. It cannot
// inject arbitrary child-process environment or Git configuration.
type GitCredentialProvider interface {
	Credentials(context.Context, string) (GitCredentials, error)
}

// GitEndpointResolver resolves a canonical policy repository through trusted
// provider configuration. Implementations must reject identities they do not
// know; they must not accept request-provided URLs.
type GitEndpointResolver interface {
	Endpoint(context.Context, string) (string, error)
}

// GitTransportConfig binds the run policy to a trusted provider resolver.
type GitTransportConfig struct {
	Policy      Policy
	Git         *couriergit.Broker
	Resolver    GitEndpointResolver
	Credentials GitCredentialProvider
}

type gitTransport struct {
	policy      Policy
	git         *couriergit.Broker
	endpoints   map[string]string
	credentials GitCredentialProvider
}

// NewGitPusher resolves and pins only the policy's base and work repositories.
// Local/file endpoints are allowed only when explicitly returned by the trusted
// resolver; arbitrary caller/model URLs are never accepted.
func NewGitPusher(ctx context.Context, config GitTransportConfig) (Pusher, error) {
	if config.Git == nil || config.Git.Directory() == "" || config.Resolver == nil {
		return nil, errors.New("broker git transport: git broker and trusted endpoint resolver are required")
	}
	if err := validatePolicy(config.Policy); err != nil {
		return nil, errors.New("broker git transport: valid publication policy is required")
	}
	identities := map[string]bool{config.Policy.BaseRepo: true, config.Policy.WorkRepo: true}
	pins := make(map[string]string, len(identities))
	for identity := range identities {
		endpoint, err := config.Resolver.Endpoint(ctx, identity)
		if err != nil {
			return nil, errors.New("broker git transport: trusted provider endpoint unavailable")
		}
		canonical, err := validatePinnedEndpoint(endpoint)
		if err != nil {
			return nil, err
		}
		pins[identity] = canonical
	}
	return &gitTransport{policy: config.Policy, git: config.Git, endpoints: pins, credentials: config.Credentials}, nil
}

func (t *gitTransport) IsAncestor(ctx context.Context, repo, ancestor, descendant string) (bool, error) {
	if _, ok := t.endpoint(repo); !ok {
		return false, errors.New("broker git transport: repository is not pinned")
	}
	// The publication engine supplies the freshly observed base/work tip and
	// compares it with provider state before invoking this local object check.
	// Git ancestry is content-addressed; it does not assert remote ownership.
	return t.git.IsAncestor(ctx, ancestor, descendant)
}

func (t *gitTransport) Push(ctx context.Context, repo, ref, oid string) error {
	endpoint, ok := t.endpoint(repo)
	if !ok {
		return errors.New("broker git transport: repository is not pinned")
	}
	if repo != t.policy.WorkRepo || ref != t.policy.WorkRef {
		return errors.New("broker git transport: destination does not match pinned work repository and ref")
	}
	if t.credentials == nil {
		return t.git.Push(ctx, endpoint, ref, oid)
	}
	credentials, err := t.credentials.Credentials(ctx, repo)
	if err != nil {
		return errors.New("broker git transport: credential provider failed")
	}
	return t.git.PushWithCredentials(ctx, endpoint, ref, oid, credentials.Username, credentials.Token)
}

func (t *gitTransport) endpoint(repo string) (string, bool) {
	endpoint, ok := t.endpoints[repo]
	if !ok || (repo != t.policy.BaseRepo && repo != t.policy.WorkRepo) {
		return "", false
	}
	return endpoint, true
}

func hasGitTransportControl(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func validatePinnedEndpoint(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.HasPrefix(raw, "-") || strings.ContainsAny(raw, "\r\n\x00") {
		return "", errors.New("broker git transport: invalid configured endpoint")
	}
	if filepath.IsAbs(raw) {
		resolved, err := resolveLocalPath(raw)
		if err != nil {
			return "", errors.New("broker git transport: local endpoint is unavailable or unsafe")
		}
		return resolved, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("broker git transport: invalid configured endpoint")
	}
	switch u.Scheme {
	case "https", "ssh":
		if u.Host == "" || strings.Contains(u.Host, "@") || u.Path == "" || hasGitTransportControl(u.String()) {
			return "", errors.New("broker git transport: invalid configured endpoint")
		}
		return raw, nil
	case "file":
		if (u.Host != "" && u.Host != "localhost") || !filepath.IsAbs(u.Path) {
			return "", errors.New("broker git transport: invalid configured local endpoint")
		}
		resolved, err := resolveLocalPath(u.Path)
		if err != nil {
			return "", errors.New("broker git transport: local endpoint is unavailable or unsafe")
		}
		return (&url.URL{Scheme: "file", Path: resolved}).String(), nil
	default:
		return "", errors.New("broker git transport: unsupported configured endpoint")
	}
}

func resolveLocalPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	canonical := filepath.Join(parent, filepath.Base(abs))
	item, err := os.Lstat(canonical)
	if err != nil || item.Mode()&os.ModeSymlink != 0 || !item.IsDir() {
		return "", errors.New("endpoint must be a non-symlink directory")
	}
	return canonical, nil
}
