package log

import (
	"fmt"
	"strings"
	"testing"
)

// Fake secrets used by every redaction test. Test failures must never print
// these values or any emitted output containing them: assertions report
// which named constant leaked, not the value itself.
const (
	fakeGitToken     = "sup3r-secret-git-token-9f2c"
	fakeForgeToken   = "github_pat_11FAKE0000notARealPatValue00"
	fakeProviderKey  = "sk-fake-provider-key-0123456789abcdef"
	fakeDispatchKey  = "dispatch-shared-secret-a1b2c3d4"
	fakeBearerToken  = "b3ar3r-t0ken-value-778811"
	fakeAccessKeyID  = "AKIAIOSFODNN7EXAMPLE"
	fakeSlackish     = "xoxb-123456789012-abcdef"
	fakeModelKey     = "model-api-key-5566778899001122"
	fakeShortSecret  = "tiny"
	fakeUserPassword = "hunter2-password"
)

func TestRedactorRemovesRegisteredSecrets(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  string
		secret string
	}{
		{name: "git token in error text", value: "clone failed for token " + fakeGitToken + " upstream", secret: fakeGitToken},
		{name: "forge token, two occurrences", value: fakeForgeToken + " then " + fakeForgeToken, secret: fakeForgeToken},
		{name: "provider key inside JSON body", value: `{"api_key":"` + fakeProviderKey + `"}`, secret: fakeProviderKey},
		{name: "dispatch key in header", value: "Authorization: Bearer " + fakeDispatchKey, secret: fakeDispatchKey},
		{name: "secret embedded with no separators", value: "prefix" + fakeModelKey + "suffix", secret: fakeModelKey},
	} {
		t.Run(test.name, func(t *testing.T) {
			redactor := NewRedactor()
			redactor.Register(test.secret)
			got := redactor.Redact(test.value)
			if strings.Contains(got, test.secret) {
				t.Fatalf("registered secret leaked after redaction (secret name: %s)", test.name)
			}
			if !strings.Contains(got, RedactedPlaceholder) {
				t.Fatalf("redacted output lost its placeholder (secret name: %s)", test.name)
			}
		})
	}
}

func TestRedactorRedactsLongestSecretFirst(t *testing.T) {
	redactor := NewRedactor()
	redactor.Register(fakeGitToken)
	redactor.Register(fakeGitToken + "-suffix")
	got := redactor.Redact(fakeGitToken + "-suffix and " + fakeGitToken)
	if strings.Contains(got, fakeGitToken) {
		t.Fatal("longest-secret replacement left a fragment of a registered secret")
	}
	if got != RedactedPlaceholder+" and "+RedactedPlaceholder {
		t.Fatalf("unexpected redaction result shape (want two placeholders)")
	}
}

func TestRedactorIgnoresShortRegistrations(t *testing.T) {
	redactor := NewRedactor()
	redactor.Register(fakeShortSecret)
	got := redactor.Redact("the tiny value stayed intact")
	if got != "the tiny value stayed intact" {
		t.Fatal("short registration should be ignored, not mangle normal text")
	}
}

func TestRedactorRedactsCredentialPatterns(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "bearer header", value: "Authorization: Bearer " + fakeBearerToken},
		{name: "bearer header lowercase", value: "authorization: bearer " + fakeBearerToken},
		{name: "authorization without scheme", value: "Authorization: " + fakeBearerToken},
		{name: "authorization JSON field", value: `"authorization": "Bearer ` + fakeBearerToken + `"`},
		{name: "proxy-authorization header", value: "proxy-authorization: Basic " + fakeBearerToken},
		{name: "url user and password", value: "https://user:" + fakeUserPassword + "@example.com/repo.git"},
		{name: "url token as username", value: "https://" + fakeGitToken + "@example.com/repo.git"},
		{name: "url token as password only", value: "https://x-access-token:" + fakeGitToken + "@git.example/acme.git"},
		{name: "query access token", value: "https://git.example/pull?access_token=" + fakeBearerToken + "&page=2"},
		{name: "query api key", value: "https://api.example/v1?key=" + fakeProviderKey},
		{name: "query token", value: "https://api.example/v1/items?token=" + fakeDispatchKey},
		{name: "forge pat format", value: "using " + fakeForgeToken + " from the environment"},
		{name: "cloud access key id", value: "credentials " + fakeAccessKeyID + " rejected"},
		{name: "chat token format", value: "post with " + fakeSlackish + " failed"},
		{name: "provider key in transcript text", value: `set key to ` + fakeProviderKey + ` and retry`},
	} {
		t.Run(test.name, func(t *testing.T) {
			redactor := NewRedactor()
			got := redactor.Redact(test.value)
			// The value must not survive verbatim: a pattern match always
			// substitutes the placeholder, so an unchanged output means the
			// pattern missed. Failures report the case name, never values.
			if got == test.value {
				t.Fatalf("credential pattern %s was not redacted (input length %d)", test.name, len(test.value))
			}
		})
	}
}

func TestRedactorPatternsLeaveNormalStringsIntact(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "plain url", value: "https://github.com/acme/widgets.git"},
		{name: "url with query but no credentials", value: "https://api.example/search?q=structured+logging&page=2&sort=key"},
		{name: "email in query", value: "https://example.com/assign?email=dev@example.com"},
		{name: "email address", value: "contact courier@example.com for access"},
		{name: "prose mentioning bearer", value: "the Bearer of bad news: CI failed again"},
		{name: "short lookalike prefixes", value: "sk-learn pipeline and AKIA discussion notes"},
		{name: "non credential key value", value: "sort_key=name&layout=compact&tag=v1.2.3"},
		{name: "authorization with short value", value: "Authorization: none"},
		{name: "branch name", value: "courier/resolve-issue/acme-widgets/7"},
	} {
		t.Run(test.name, func(t *testing.T) {
			redactor := NewRedactor()
			if got := redactor.Redact(test.value); got != test.value {
				t.Fatalf("over-aggressive redaction changed %s (length %d -> %d)", test.name, len(test.value), len(got))
			}
		})
	}
}

func TestRedactionIsIdempotent(t *testing.T) {
	redactor := NewRedactor()
	redactor.Register(fakeGitToken)
	redactor.Register(fakeForgeToken)
	mixed := "Authorization: Bearer " + fakeBearerToken + " url https://" + fakeGitToken +
		"@example.com/x.git pat " + fakeForgeToken + " q ?token=" + fakeDispatchKey
	once := redactor.Redact(mixed)
	twice := redactor.Redact(once)
	if once != twice {
		t.Fatalf("redaction is not idempotent (first pass length %d, second pass length %d)", len(once), len(twice))
	}
}

func TestRedactorRegisterEnvironmentUsesNameShape(t *testing.T) {
	environ := []string{
		"GITHUB_TOKEN=" + fakeForgeToken,
		"MY_SERVICE_API_KEY=" + fakeModelKey,
		"DISPATCH_SHARED_SECRET=" + fakeDispatchKey,
		"HOME=/home/courier",
		"PATH=/usr/local/bin:/usr/bin",
		"COURIER_LOG_LEVEL=debug",
		"EMPTY_TOKEN=",
		"=" + fakeGitToken,
	}
	redactor := NewRedactor()
	redactor.RegisterEnvironment(environ)
	got := redactor.Redact("t=" + fakeForgeToken + " k=" + fakeModelKey + " d=" + fakeDispatchKey)
	if strings.Contains(got, fakeForgeToken) || strings.Contains(got, fakeModelKey) || strings.Contains(got, fakeDispatchKey) {
		t.Fatal("environment registration missed a secret-shaped variable")
	}
	if got != "t="+RedactedPlaceholder+" k="+RedactedPlaceholder+" d="+RedactedPlaceholder {
		t.Fatalf("unexpected redaction shape for environment secrets (length %d)", len(got))
	}
}

func TestIsSecretEnvName(t *testing.T) {
	for _, test := range []struct {
		name string
		want bool
	}{
		{name: "GITHUB_TOKEN", want: true},
		{name: "ANTHROPIC_API_KEY", want: true},
		{name: "APIKEY", want: true},
		{name: "DB_PASSWORD", want: true},
		{name: "SERVICE_ACCOUNT_CREDENTIALS", want: true},
		{name: "VAULT_SECRET", want: true},
		{name: "LDAP_PASS", want: true},
		{name: "PATH", want: false},
		{name: "HOME", want: false},
		{name: "COURIER_REPO", want: false},
		{name: "COURIER_LOG_LEVEL", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isSecretEnvName(test.name); got != test.want {
				t.Fatalf("isSecretEnvName(%q) = %t, want %t", test.name, got, test.want)
			}
		})
	}
}

func TestRedactorConcurrentUse(t *testing.T) {
	redactor := NewRedactor()
	redactor.Register(fakeGitToken)
	value := fmt.Sprintf("payload %s payload", fakeGitToken)
	done := make(chan string, 8)
	for i := 0; i < 8; i++ {
		go func() {
			done <- redactor.Redact(value)
		}()
	}
	for i := 0; i < 8; i++ {
		if got := <-done; strings.Contains(got, fakeGitToken) {
			t.Fatal("concurrent redaction leaked a registered secret")
		}
	}
}
