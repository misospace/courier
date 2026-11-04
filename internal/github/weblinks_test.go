package github

import (
	"strings"
	"testing"
)

func TestWebCommitURL(t *testing.T) {
	const sha = "abc123def4567890"
	tests := []struct {
		name     string
		endpoint string
		repo     string
		sha      string
		want     string
	}{
		{name: "empty endpoint is the GitHub.com default", endpoint: "", repo: "acme/demo", sha: sha, want: "https://github.com/acme/demo/commit/" + sha},
		{name: "github api base", endpoint: "https://api.github.com", repo: "acme/demo", sha: sha, want: "https://github.com/acme/demo/commit/" + sha},
		{name: "github api base with trailing slash", endpoint: "https://api.github.com/", repo: "acme/demo", sha: sha, want: "https://github.com/acme/demo/commit/" + sha},
		{name: "github api base with path", endpoint: "https://api.github.com/api/v3", repo: "acme/demo", sha: sha, want: "https://github.com/acme/demo/commit/" + sha},
		{name: "github api base without scheme", endpoint: "api.github.com", repo: "acme/demo", sha: sha, want: "https://github.com/acme/demo/commit/" + sha},
		{name: "enterprise endpoint renders its own host", endpoint: "https://ghe.example.com/api/v3", repo: "acme/demo", sha: sha, want: "https://ghe.example.com/acme/demo/commit/" + sha},
		{name: "enterprise endpoint without api path renders its own host", endpoint: "https://ghe.example.com", repo: "acme/demo", sha: sha, want: "https://ghe.example.com/acme/demo/commit/" + sha},
		{name: "minimum sha length", endpoint: "", repo: "acme/demo", sha: "abcd", want: "https://github.com/acme/demo/commit/abcd"},
		{name: "maximum sha length", endpoint: "", repo: "acme/demo", sha: strings.Repeat("aB3f", 16), want: "https://github.com/acme/demo/commit/" + strings.Repeat("aB3f", 16)},
		{name: "repo without a name is refused", endpoint: "", repo: "a", sha: sha, want: ""},
		{name: "repo with three parts is refused", endpoint: "", repo: "a/b/c", sha: sha, want: ""},
		{name: "repo with a percent escape is refused", endpoint: "", repo: "a/%20b", sha: sha, want: ""},
		{name: "repo carrying a url is refused", endpoint: "", repo: "https://evil/x", sha: sha, want: ""},
		{name: "repo with a space is refused", endpoint: "", repo: "a/b c", sha: sha, want: ""},
		{name: "repo with a colon is refused", endpoint: "", repo: "a:b/c", sha: sha, want: ""},
		{name: "repo with an at sign is refused", endpoint: "", repo: "a/@b", sha: sha, want: ""},
		{name: "non-hex sha is refused", endpoint: "", repo: "acme/demo", sha: "zzz", want: ""},
		{name: "sha shorter than four is refused", endpoint: "", repo: "acme/demo", sha: "123", want: ""},
		{name: "sha longer than sixty-four is refused", endpoint: "", repo: "acme/demo", sha: strings.Repeat("a", 65), want: ""},
		{name: "word is refused as sha", endpoint: "", repo: "acme/demo", sha: "shashash", want: ""},
		{name: "relative endpoint is refused", endpoint: "api/v3", repo: "acme/demo", sha: sha, want: ""},
		{name: "leading-path endpoint is refused", endpoint: "/api/v3", repo: "acme/demo", sha: sha, want: ""},
		{name: "foreign scheme endpoint is refused", endpoint: "ftp://ghe.example.com/api/v3", repo: "acme/demo", sha: sha, want: ""},
		{name: "plain-http endpoint is refused, never rewritten up", endpoint: "http://ghe.example.com/api/v3", repo: "acme/demo", sha: sha, want: ""},
		{name: "userinfo endpoint is refused", endpoint: "https://user:pass@ghe.example.com/api/v3", repo: "acme/demo", sha: sha, want: ""},
		{name: "schemeless non-github host is refused", endpoint: "ghe.example.com", repo: "acme/demo", sha: sha, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WebCommitURL(tt.endpoint, tt.repo, tt.sha); got != tt.want {
				t.Fatalf("WebCommitURL(%q, %q, %q) = %q, want %q", tt.endpoint, tt.repo, tt.sha, got, tt.want)
			}
		})
	}
}

func TestWebFileURL(t *testing.T) {
	const ref = "abc123def4567890"
	tests := []struct {
		name     string
		endpoint string
		repo     string
		path     string
		ref      string
		want     string
	}{
		{name: "default endpoint", endpoint: "", repo: "acme/demo", path: "docs/readme.md", ref: ref, want: "https://github.com/acme/demo/blob/" + ref + "/docs/readme.md"},
		{name: "enterprise endpoint", endpoint: "https://ghe.example.com/api/v3", repo: "acme/demo", path: "src/main.go", ref: ref, want: "https://ghe.example.com/acme/demo/blob/" + ref + "/src/main.go"},
		{name: "deep clean path", endpoint: "", repo: "acme_.demo-1/demo_2", path: "a/b/c+d.go", ref: ref, want: "https://github.com/acme_.demo-1/demo_2/blob/" + ref + "/a/b/c+d.go"},
		{name: "empty path is refused", endpoint: "", repo: "acme/demo", path: "", ref: ref, want: ""},
		{name: "absolute path is refused", endpoint: "", repo: "acme/demo", path: "/abs", ref: ref, want: ""},
		{name: "leading dot-dot is refused", endpoint: "", repo: "acme/demo", path: "../x", ref: ref, want: ""},
		{name: "inner dot-dot is refused", endpoint: "", repo: "acme/demo", path: "a/../b", ref: ref, want: ""},
		{name: "dot segment is refused", endpoint: "", repo: "acme/demo", path: "a/./b", ref: ref, want: ""},
		{name: "empty segment is refused", endpoint: "", repo: "acme/demo", path: "a//b", ref: ref, want: ""},
		{name: "trailing slash is refused", endpoint: "", repo: "acme/demo", path: "a/b/", ref: ref, want: ""},
		{name: "backslash is refused", endpoint: "", repo: "acme/demo", path: `a\b`, ref: ref, want: ""},
		{name: "colon is refused", endpoint: "", repo: "acme/demo", path: "a:b", ref: ref, want: ""},
		{name: "percent is refused", endpoint: "", repo: "acme/demo", path: "a%20b", ref: ref, want: ""},
		{name: "space is refused", endpoint: "", repo: "acme/demo", path: "a b", ref: ref, want: ""},
		{name: "fragment marker is refused", endpoint: "", repo: "acme/demo", path: "a#b", ref: ref, want: ""},
		{name: "query marker is refused", endpoint: "", repo: "acme/demo", path: "a?b", ref: ref, want: ""},
		{name: "control character is refused", endpoint: "", repo: "acme/demo", path: "a\x00b", ref: ref, want: ""},
		{name: "path beyond the bound is refused", endpoint: "", repo: "acme/demo", path: strings.Repeat("x", maxWebLinkPathLen+1), ref: ref, want: ""},
		{name: "path at the bound renders", endpoint: "", repo: "acme/demo", path: strings.Repeat("x", maxWebLinkPathLen), ref: ref, want: "https://github.com/acme/demo/blob/" + ref + "/" + strings.Repeat("x", maxWebLinkPathLen)},
		{name: "bad revision is refused", endpoint: "", repo: "acme/demo", path: "src/main.go", ref: "main", want: ""},
		{name: "bad repo is refused", endpoint: "", repo: "a/b/c", path: "src/main.go", ref: ref, want: ""},
		{name: "bad endpoint is refused", endpoint: "ftp://ghe.example.com", repo: "acme/demo", path: "src/main.go", ref: ref, want: ""},
		{name: "plain-http endpoint is refused", endpoint: "http://ghe.example.com/api/v3", repo: "acme/demo", path: "src/main.go", ref: ref, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WebFileURL(tt.endpoint, tt.repo, tt.path, tt.ref); got != tt.want {
				t.Fatalf("WebFileURL(%q, %q, %q, %q) = %q, want %q", tt.endpoint, tt.repo, tt.path, tt.ref, got, tt.want)
			}
		})
	}
}

func TestObserverWebLinksUseTheDefaultEndpoint(t *testing.T) {
	client, err := New("token")
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{Client: client}
	if got, want := observer.CommitLink("acme/demo", "abc123"), "https://github.com/acme/demo/commit/abc123"; got != want {
		t.Fatalf("CommitLink() = %q, want %q", got, want)
	}
	if got, want := observer.FileLink("acme/demo", "docs/readme.md", "abc123"), "https://github.com/acme/demo/blob/abc123/docs/readme.md"; got != want {
		t.Fatalf("FileLink() = %q, want %q", got, want)
	}
}

func TestObserverWebLinksRouteThroughRegistrationEndpoint(t *testing.T) {
	client, err := New("token")
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{Client: client, Registrations: testForgeRegistry(t, "https://ghe.example.com/api/v3")}
	if got, want := observer.CommitLink("acme/demo", "abc123"), "https://ghe.example.com/acme/demo/commit/abc123"; got != want {
		t.Fatalf("CommitLink() = %q, want the registration host, not the GitHub.com default", got)
	}
	if got, want := observer.FileLink("acme/demo", "docs/readme.md", "abc123"), "https://ghe.example.com/acme/demo/blob/abc123/docs/readme.md"; got != want {
		t.Fatalf("FileLink() = %q, want the registration host, not the GitHub.com default", got)
	}
}

func TestObserverWebLinksFailClosedOutsideTheRegistry(t *testing.T) {
	client, err := New("token")
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{Client: client, Registrations: testForgeRegistry(t, "https://ghe.example.com/api/v3")}
	if got := observer.CommitLink("other/demo", "abc123"); got != "" {
		t.Fatalf("CommitLink() = %q, want no link for a repository the registry cannot settle", got)
	}
	if got := observer.FileLink("other/demo", "docs/readme.md", "abc123"); got != "" {
		t.Fatalf("FileLink() = %q, want no link for a repository the registry cannot settle", got)
	}
}

func TestObserverWebLinksNilClientRendersNothing(t *testing.T) {
	observer := Observer{}
	if got := observer.CommitLink("acme/demo", "abc123"); got != "" {
		t.Fatalf("CommitLink() = %q, want no link without a client", got)
	}
	if got := observer.FileLink("acme/demo", "docs/readme.md", "abc123"); got != "" {
		t.Fatalf("FileLink() = %q, want no link without a client", got)
	}
}
