package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

// Installation-mode askpass (#871): an App-enabled coordinator pod serves git
// credentials by re-reading the run's installation-token document through the
// Kubernetes API on every prompt, so operator rotation and expiry take effect
// immediately. The mode never consults COURIER_GIT_TOKEN: a prompt the pinned
// bindings do not explain is refused before any API call, and every refusal
// prints nothing on stdout so git receives no credential at all.

// installationAskpassMessage is the single diagnostic an installation-mode
// refusal may emit. It is fixed and non-secret by construction.
const installationAskpassMessage = "courier: installation git credential unavailable"

// normalizeGitRemoteURL parses an https remote URL and reduces it to the
// comparison form used to bind a git prompt to a configured remote: lowercase
// host, no default :443, no trailing slash, no trailing .git. Anything that is
// not an absolute https URL with a host is refused, and so is any userinfo
// except the single case git itself produces: once the helper has answered a
// username prompt, git embeds that exact token username in the following
// password prompt (https://x-access-token@host/repo.git). Foreign or
// password-bearing userinfo has no comparison form and is refused, so the
// path-bound identity match still pins every served credential.
func normalizeGitRemoteURL(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Opaque != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false
	}
	if parsed.User != nil {
		username, password := parsed.User.Username(), func() string { p, _ := parsed.User.Password(); return p }()
		if password != "" || !strings.EqualFold(username, topology.DefaultGitUsername) {
			return "", false
		}
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", false
	}
	if port := parsed.Port(); port != "" && port != "443" {
		host = net.JoinHostPort(host, port)
	}
	path := strings.TrimRight(parsed.EscapedPath(), "/")
	path = strings.TrimSuffix(strings.ToLower(path), ".git")
	path = strings.TrimRight(path, "/")
	if path == "" {
		return "", false
	}
	return "https://" + host + path, true
}

// installationRemotes binds the run's two configured remote URLs to the
// repository identities they may serve: the work remote serves the head repo
// (or the base repo in a same-repository run) and the base remote serves the
// base repo. Comparison happens only on normalized forms; the identities are
// compared case-insensitively downstream by the token source.
type installationRemotes struct {
	workURL      string
	baseURL      string
	workIdentity string
	baseIdentity string
}

func installationRemotesFrom(getenv func(string) string) (installationRemotes, bool) {
	base := strings.TrimSpace(getenv("COURIER_REPO"))
	if base == "" {
		return installationRemotes{}, false
	}
	work := strings.TrimSpace(getenv("COURIER_HEAD_REPO"))
	if work == "" {
		work = base
	}
	workURL, ok := normalizeGitRemoteURL(strings.TrimSpace(getenv("COURIER_REPO_URL")))
	if !ok {
		return installationRemotes{}, false
	}
	remotes := installationRemotes{workURL: workURL, baseURL: workURL, workIdentity: work, baseIdentity: base}
	if raw := strings.TrimSpace(getenv("COURIER_BASE_REPO_URL")); raw != "" {
		baseURL, ok := normalizeGitRemoteURL(raw)
		if !ok {
			return installationRemotes{}, false
		}
		remotes.baseURL = baseURL
	}
	return remotes, true
}

// identityFor maps a remote URL to the repository identity pinned for it, or
// refuses when the URL is neither configured remote.
func (r installationRemotes) identityFor(rawURL string) (string, bool) {
	normalized, ok := normalizeGitRemoteURL(rawURL)
	if !ok {
		return "", false
	}
	switch normalized {
	case r.workURL:
		return r.workIdentity, true
	case r.baseURL:
		return r.baseIdentity, true
	}
	return "", false
}

// parseAskpassPrompt extracts the single-quoted remote URL from a git askpass
// prompt argument such as Password for 'https://host/owner/repo.git': and
// reports whether the prompt asks for a username.
func parseAskpassPrompt(args []string) (remoteURL string, wantsUsername bool, ok bool) {
	joined := strings.Join(args, " ")
	open := strings.IndexByte(joined, '\'')
	if open < 0 {
		return "", false, false
	}
	// Git prompts are "Username for '<url>':" and "Password for '<url>':";
	// only the label before the quoted URL names the requested half.
	// Searching the whole prompt would answer a password prompt with the
	// username whenever the repository path itself contains "username".
	wantsUsername = strings.Contains(strings.ToLower(joined[:open]), "username")
	rest := joined[open+1:]
	end := strings.IndexByte(rest, '\'')
	if end <= 0 {
		return "", wantsUsername, false
	}
	return rest[:end], wantsUsername, true
}

// installationAskpassRequest is the pure decision of an installation-mode
// prompt: which pinned identity to serve, which half of the credential git
// asked for, and the Secret the runner must read.
type installationAskpassRequest struct {
	identity      string
	serveUsername bool
	namespace     string
	secretName    string
	owner         string
	endpoint      string
	tokenFile     string
	pinned        []string
}

// decideInstallationAskpass validates the App-mode configuration and binds the
// git prompt to exactly one pinned identity without touching any API. Any
// missing, malformed, or unbound input refuses closed.
func decideInstallationAskpass(getenv func(string) string, args []string) (installationAskpassRequest, bool) {
	if getenv(executor.EnvGitInstallation) != "1" {
		return installationAskpassRequest{}, false
	}
	request := installationAskpassRequest{
		owner:      strings.TrimSpace(getenv(executor.EnvInstallationOwner)),
		endpoint:   strings.TrimSpace(getenv(executor.EnvInstallationEndpoint)),
		namespace:  strings.TrimSpace(getenv("POD_NAMESPACE")),
		tokenFile:  strings.TrimSpace(getenv(executor.EnvAPITokenFile)),
		secretName: strings.TrimSpace(getenv(executor.EnvInstallationSecret)),
	}
	if request.owner == "" || request.endpoint == "" || request.namespace == "" || request.tokenFile == "" || request.secretName == "" {
		return installationAskpassRequest{}, false
	}
	remotes, ok := installationRemotesFrom(getenv)
	if !ok {
		return installationAskpassRequest{}, false
	}
	request.pinned = []string{remotes.baseIdentity, remotes.workIdentity}
	promptURL, wantsUsername, ok := parseAskpassPrompt(args)
	if !ok {
		return installationAskpassRequest{}, false
	}
	identity, ok := remotes.identityFor(promptURL)
	if !ok {
		return installationAskpassRequest{}, false
	}
	request.identity = identity
	request.serveUsername = wantsUsername
	return request, true
}

// installationAPIReader builds the uncached live reader for the credentials
// Secret read: the in-cluster API host with a projected token and CA bundle,
// as the broker and the observer already reach the API. Unlike the long-lived
// broker, the helper answers one prompt per process, so it carries a static
// REST mapping for the single kind it reads instead of paying (and failing on)
// API discovery per prompt.
func installationAPIReader(host, port, tokenFile, caFile string) (ctrlclient.Reader, error) {
	if host == "" || port == "" || tokenFile == "" {
		return nil, errors.New("executor: kubernetes API host, port, and projected token are required")
	}
	if caFile == "" {
		caFile = executor.DefaultAPICertPath
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Secret"), meta.RESTScopeNamespace)
	liveClient, err := ctrlclient.New(&rest.Config{
		Host:            "https://" + net.JoinHostPort(host, port),
		BearerTokenFile: tokenFile,
		TLSClientConfig: rest.TLSClientConfig{CAFile: caFile},
		Timeout:         10 * time.Second,
	}, ctrlclient.Options{Scheme: scheme.Scheme, Mapper: mapper})
	if err != nil {
		return nil, errors.New("executor: kubernetes API reader unavailable")
	}
	return liveClient, nil
}

// serveInstallationAskpass reads the document through the given reader and
// prints exactly the requested half of the served credential on success. Every
// failure prints nothing on stdout and one fixed non-secret line on stderr.
func serveInstallationAskpass(ctx context.Context, request installationAskpassRequest, reader ctrlclient.Reader, stdout, stderr io.Writer) int {
	source, err := broker.NewSecretInstallationTokenSource(reader, request.namespace, request.secretName,
		executor.InstallationTokenDocumentKey, request.owner, request.endpoint, nil)
	if err != nil {
		fmt.Fprintln(stderr, installationAskpassMessage)
		return exitFailed
	}
	credentials, err := broker.NewInstallationGitCredentials(source, request.pinned).Credentials(ctx, request.identity)
	if err != nil || credentials.Token == "" || credentials.Username == "" {
		fmt.Fprintln(stderr, installationAskpassMessage)
		return exitFailed
	}
	value := credentials.Token
	if request.serveUsername {
		value = credentials.Username
	}
	_, _ = io.WriteString(stdout, value)
	return exitSuccess
}

// runInstallationAskpass is the askpass entry for App-mode pods: decide, read,
// serve. It never falls back to static credentials.
func runInstallationAskpass(getenv func(string) string, args []string, stdout, stderr io.Writer) int {
	request, ok := decideInstallationAskpass(getenv, args)
	if !ok {
		fmt.Fprintln(stderr, installationAskpassMessage)
		return exitFailed
	}
	reader, err := installationAPIReader(getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT"), request.tokenFile, getenv(executor.EnvAPICertFile))
	if err != nil {
		fmt.Fprintln(stderr, installationAskpassMessage)
		return exitFailed
	}
	return serveInstallationAskpass(context.Background(), request, reader, stdout, stderr)
}
