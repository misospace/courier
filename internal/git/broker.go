package git

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Broker owns a private bare repository used to validate and publish worker
// bundles. Create it with NewBroker; its directory should be private to a run.
type Broker struct {
	directory string
}

// NewBroker creates a private bare repository beneath parent. The caller owns
// its lifetime and should remove the returned broker directory when the run ends.
func NewBroker(ctx context.Context, parent string) (*Broker, error) {
	if strings.TrimSpace(parent) == "" {
		return nil, errors.New("git broker: parent directory is required")
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, errors.New("git broker: cannot create parent directory")
	}
	dir, err := os.MkdirTemp(parent, "git-broker-")
	if err != nil {
		return nil, errors.New("git broker: cannot create private repository")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return nil, errors.New("git broker: cannot secure private repository")
	}
	if _, err := brokerGit(ctx, dir, "init", "--bare", "--quiet"); err != nil {
		_ = os.RemoveAll(dir)
		return nil, errors.New("git broker: cannot initialize private repository")
	}
	return &Broker{directory: dir}, nil
}

// Directory returns the private bare repository path, primarily for lifecycle
// management and tests. Do not expose it to model-controlled processes.
func (b *Broker) Directory() string {
	if b == nil {
		return ""
	}
	return b.directory
}

// ImportBundle imports the proposed commit from a git bundle and confirms it
// is a commit. expectedTip is the exact current remote commit; empty means the
// pinned branch was observed absent. For an existing branch, proposedOID must
// descend from expectedTip. OIDs may be SHA-1 or SHA-256 and are compared
// exactly after Git resolves them to full object IDs.
func (b *Broker) ImportBundle(ctx context.Context, bundlePath, proposedOID, expectedTip string) error {
	if b == nil || b.directory == "" {
		return errors.New("git broker: repository is unavailable")
	}
	bundlePath = filepath.Clean(strings.TrimSpace(bundlePath))
	if bundlePath == "." || !filepath.IsAbs(bundlePath) || hasControl(bundlePath) {
		return errors.New("git broker: bundle path must be an absolute local path")
	}
	if err := validOID(proposedOID); err != nil {
		return errors.New("git broker: invalid proposed commit OID")
	}
	if expectedTip != "" {
		if err := validOID(expectedTip); err != nil {
			return errors.New("git broker: invalid expected tip OID")
		}
	}
	scratch, err := os.MkdirTemp(filepath.Dir(b.directory), "git-bundle-check-")
	if err != nil {
		return errors.New("git broker: cannot create isolated bundle repository")
	}
	defer os.RemoveAll(scratch)
	if _, err := brokerGit(ctx, scratch, "init", "--bare", "--quiet"); err != nil {
		return errors.New("git broker: cannot initialize isolated bundle repository")
	}
	if _, err := brokerGit(ctx, scratch, "fetch", "--no-tags", "--", bundlePath, proposedOID); err != nil {
		return errors.New("git broker: bundle import failed")
	}
	proposed, err := resolveCommitIn(ctx, scratch, proposedOID)
	if err != nil {
		return errors.New("git broker: proposed OID is not in the bundle")
	}
	if expectedTip != "" {
		expected, err := resolveCommitIn(ctx, scratch, expectedTip)
		if err != nil {
			return errors.New("git broker: expected tip is unavailable in the bundle")
		}
		if _, err := brokerGit(ctx, scratch, "merge-base", "--is-ancestor", expected, proposed); err != nil {
			return errors.New("git broker: proposed commit is not a fast-forward of expected tip")
		}
	}
	if _, err := brokerGit(ctx, b.directory, "fetch", "--no-tags", "--", scratch, proposed); err != nil {
		return errors.New("git broker: cannot retain validated commit")
	}
	return nil
}

// Push publishes an imported commit to exactly the supplied pinned remote URL
// and branch ref. It uses a normal, non-force push; server-side fast-forward
// checks remain authoritative if the remote changes after observation.
func (b *Broker) Push(ctx context.Context, remoteURL, ref, proposedOID string) error {
	if b == nil || b.directory == "" {
		return errors.New("git broker: repository is unavailable")
	}
	if err := validateBrokerURL(remoteURL); err != nil {
		return err
	}
	if err := validateRef(ref, "branch"); err != nil {
		return errors.New("git broker: invalid destination ref")
	}
	if err := validOID(proposedOID); err != nil {
		return errors.New("git broker: invalid proposed commit OID")
	}
	commit, err := b.resolveCommit(ctx, proposedOID)
	if err != nil {
		return errors.New("git broker: proposed OID is not an imported commit")
	}
	refspec := commit + ":refs/heads/" + ref
	if _, err := brokerGit(ctx, b.directory, "-c", "protocol.ext.allow=never", "push", "--porcelain", "--", remoteURL, refspec); err != nil {
		return errors.New("git broker: push failed")
	}
	return nil
}

// Observe returns the live advertised OID for the exact branch ref. exists is
// false only when the remote does not advertise the ref.
func (b *Broker) Observe(ctx context.Context, remoteURL, ref string) (oid string, exists bool, err error) {
	if err := validateBrokerURL(remoteURL); err != nil {
		return "", false, err
	}
	if err := validateRef(ref, "branch"); err != nil {
		return "", false, errors.New("git broker: invalid observation ref")
	}
	out, runErr := brokerGit(ctx, "", "-c", "protocol.ext.allow=never", "ls-remote", "--heads", "--", remoteURL, "refs/heads/"+ref)
	if runErr != nil {
		return "", false, errors.New("git broker: remote observation failed")
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		return "", false, nil
	}
	fields := strings.Fields(line)
	if len(fields) != 2 || fields[1] != "refs/heads/"+ref || validOID(fields[0]) != nil {
		return "", false, errors.New("git broker: invalid remote observation")
	}
	return strings.ToLower(fields[0]), true, nil
}

func (b *Broker) resolveCommit(ctx context.Context, oid string) (string, error) {
	return resolveCommitIn(ctx, b.directory, oid)
}

func resolveCommitIn(ctx context.Context, directory, oid string) (string, error) {
	out, err := brokerGit(ctx, directory, "rev-parse", "--verify", oid+"^{commit}")
	if err != nil {
		return "", err
	}
	resolved := strings.TrimSpace(string(out))
	if validOID(resolved) != nil {
		return "", errors.New("invalid resolved OID")
	}
	return strings.ToLower(resolved), nil
}

func brokerGit(ctx context.Context, directory string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if directory != "" {
		cmd.Dir = directory
	}
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		// Never propagate Git's stderr: transports routinely echo credentialed URLs.
		return nil, errors.New("git command failed")
	}
	return stdout.Bytes(), nil
}

func validOID(oid string) error {
	if (len(oid) != 40 && len(oid) != 64) || strings.TrimSpace(oid) != oid {
		return errors.New("invalid object ID")
	}
	decoded, err := hex.DecodeString(oid)
	if err != nil || len(decoded)*2 != len(oid) {
		return errors.New("invalid object ID")
	}
	return nil
}

func hasControl(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func validateBrokerURL(raw string) error {
	if raw == "" || strings.TrimSpace(raw) != raw || hasControl(raw) || strings.HasPrefix(raw, "-") {
		return errors.New("git broker: invalid remote URL")
	}
	if filepath.IsAbs(raw) {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("git broker: invalid remote URL")
	}
	switch u.Scheme {
	case "https", "ssh":
		if u.Host == "" || strings.Contains(u.Host, "@") {
			return errors.New("git broker: invalid remote URL")
		}
	case "file":
		if u.Host != "" && u.Host != "localhost" || !filepath.IsAbs(u.Path) {
			return errors.New("git broker: invalid remote URL")
		}
	default:
		return errors.New("git broker: unsupported remote URL")
	}
	return nil
}
