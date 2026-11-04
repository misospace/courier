package github

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/misospace/courier/internal/controller"
)

// defaultWebBase is the GitHub.com web front. It is also the web base of any
// endpoint that resolves to GitHub's API host, with or without scheme or path:
// GitHub's API host has exactly one web front, so no GHES-style host rewrite
// can ever point a public-API link somewhere else.
const (
	defaultWebBase = "https://github.com"
	githubAPIHost  = "api.github.com"

	// maxWebLinkPathLen bounds a rendered repo-relative path. A longer path
	// is never truncated into a broken link; it simply renders as plain text.
	maxWebLinkPathLen = 1024
)

// revisionPattern accepts a commit SHA or short revision the web front
// resolves: hex, four to sixty-four characters.
var revisionPattern = regexp.MustCompile(`^[0-9a-fA-F]{4,64}$`)

// webBase resolves a forge API base to the forge's web front. An empty
// endpoint is the GitHub.com default. A GitHub API base — the host
// api.github.com, with or without scheme or path — resolves to the public
// web front. Any other HTTPS endpoint resolves to its own host with the API
// path stripped: an API base's host serves the web UI at its own root, so
// https://ghe.example.com/api/v3 renders as https://ghe.example.com.
// Anything that is not a recognizable secure endpoint — a plain-HTTP base,
// a relative path, an unknown scheme, a missing host, or a userinfo-bearing
// URL — resolves to no link rather than a guess, and nothing is ever
// cleaned or rewritten to force a match.
func webBase(endpoint string) string {
	e := strings.TrimSpace(endpoint)
	if e == "" {
		return defaultWebBase
	}
	if u, err := url.Parse(e); err == nil && u.Scheme != "" {
		if u.User != nil || u.Host == "" {
			return ""
		}
		switch strings.ToLower(u.Scheme) {
		case "http", "https":
		default:
			return ""
		}
		if strings.EqualFold(u.Hostname(), githubAPIHost) {
			return defaultWebBase
		}
		// A non-HTTPS endpoint is never rewritten up to a scheme the
		// deployment may not serve: an unrenderable endpoint yields no
		// link, the same reject-don't-rewrite rule as every other input.
		if !strings.EqualFold(u.Scheme, "https") {
			return ""
		}
		return "https://" + u.Host
	}
	// No scheme. The only bare host with a meaningful web front is GitHub's
	// own API host; any other bare or relative string is not an absolute
	// URL and yields no link.
	host := e
	if i := strings.IndexByte(e, ':'); i > 0 {
		host = e[:i]
	}
	if !strings.ContainsAny(host, "/\\") && strings.EqualFold(host, githubAPIHost) {
		return defaultWebBase
	}
	return ""
}

// validWebLinkRepository reports whether repo is a clean owner/name pair that
// may be placed verbatim in a forge web URL. Each part is limited to the
// characters GitHub itself allows in owner and repository names —
// alphanumerics and dot, dash, underscore — so no character needs escaping
// and no extra slash can smuggle a path segment. Anything outside that set
// yields no link rather than a rewritten one.
func validWebLinkRepository(repo string) bool {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') &&
				r != '.' && r != '-' && r != '_' {
				return false
			}
		}
	}
	return true
}

// isWebLinkPathChar reports the characters a repo-relative path segment may
// carry in a rendered link: unreserved URL characters plus the safe
// sub-delimiters that appear in ordinary file names. No other character —
// space, colon, percent, backslash, URL metacharacter, or control character
// — may appear, because the path travels verbatim in the URL and is never
// cleaned, trimmed, or percent-encoded to force a match.
func isWebLinkPathChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
		r == '.' || r == '-' || r == '_' || r == '~' || r == '+'
}

// validWebLinkPath reports whether path is a clean repo-relative path: rooted
// at the repository, one or more non-empty segments, none of them "." or
// "..", every character one web links may carry verbatim, and within the
// length bound. Anything else yields no link rather than a rewritten one.
func validWebLinkPath(p string) bool {
	if p == "" || len(p) > maxWebLinkPathLen {
		return false
	}
	for _, r := range p {
		if r != '/' && !isWebLinkPathChar(r) {
			return false
		}
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// validWebLinkRevision reports whether revision is a commit SHA or short
// revision the web front resolves: lowercase or uppercase hex, four to
// sixty-four characters. A branch name, a tag, or a shorter abbreviation
// yields no link.
func validWebLinkRevision(revision string) bool {
	return revisionPattern.MatchString(revision)
}

// WebCommitURL renders the forge's web URL for one commit of a repository,
// or the empty string when a safe link cannot be rendered for the given
// identity. endpoint is the forge API base serving the repository (see
// webBase); the web front is derived from it, never from the repository's
// own identity.
func WebCommitURL(endpoint, repo, sha string) string {
	web := webBase(endpoint)
	if web == "" || !validWebLinkRepository(repo) || !validWebLinkRevision(sha) {
		return ""
	}
	return web + "/" + repo + "/commit/" + sha
}

// WebFileURL renders the forge's web URL for one repo-relative path at an
// exact revision, or the empty string when a safe link cannot be rendered
// for the given identity. The path is placed verbatim after the revision;
// it is never cleaned or rewritten.
func WebFileURL(endpoint, repo, path, ref string) string {
	web := webBase(endpoint)
	if web == "" || !validWebLinkRepository(repo) || !validWebLinkPath(path) || !validWebLinkRevision(ref) {
		return ""
	}
	return web + "/" + repo + "/blob/" + ref + "/" + path
}

var _ controller.WorkItemLinker = Observer{}

// endpointFor returns the forge API endpoint that serves a repository under
// the observer's routing rules, reusing the exact registration lookup a read
// routes through (registrationFor): a loaded registry decides the
// destination, an unset registry falls back to the observer's own client
// destination, and a repository the loaded registry cannot settle yields no
// endpoint. A nil client yields none.
func (o Observer) endpointFor(repository string) (string, bool) {
	if o.Client == nil {
		return "", false
	}
	registration, err := o.registrationFor(repository)
	if err != nil {
		return "", false
	}
	if registration == nil {
		return o.Client.baseURL.String(), true
	}
	return registration.Endpoint, true
}

// CommitLink renders the forge's web URL for one commit of a repository,
// routed to the endpoint serving that repository. It implements the
// reconciler's WorkItemLinker seam; the empty string is the "cannot render a
// safe link" answer the caller renders as plain text.
func (o Observer) CommitLink(repo, sha string) string {
	endpoint, ok := o.endpointFor(repo)
	if !ok {
		return ""
	}
	return WebCommitURL(endpoint, repo, sha)
}

// FileLink renders the forge's web URL for one repo-relative path at an
// exact revision, routed to the endpoint serving that repository. It
// implements the reconciler's WorkItemLinker seam.
func (o Observer) FileLink(repo, path, ref string) string {
	endpoint, ok := o.endpointFor(repo)
	if !ok {
		return ""
	}
	return WebFileURL(endpoint, repo, path, ref)
}
