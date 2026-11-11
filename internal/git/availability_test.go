package git

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestAvailabilityScpLikeTargetSplitsGitScpForms(t *testing.T) {
	cases := []struct {
		name      string
		remoteURL string
		want      string
	}{
		{"userHostPath", "git@github.com:owner/repo.git", "git@github.com"},
		{"bareHostPath", "gitlab.example.com:group/project.git", "gitlab.example.com"},
		{"hostOnlyColon", "host:", "host"},
		{"portLikePathStillSplitsAtFirstColon", "localhost:9418/repo", "localhost"},
		{"ipv6LikeFirstSegmentSplitsAsGitDoes", "fe80::1:path", "fe80"},
		{"emptyRemote", "", ""},
		{"colonFirst", ":path", ""},
		{"noColonLocalPath", "/var/git/repo.git", ""},
		{"slashBeforeColonIsLocalPath", "relative/path:tag", ""},
		{"spaceBeforeColon", "a b:c", ""},
		{"backslashBeforeColon", `a\b:c`, ""},
		{"bracketedIpv6Rejected", "[::1]:path", ""},
		{"emptyFirstIpv6SegmentRejected", "::1:path", ""},
		{"leadingAtRejected", "@host:path", ""},
		{"emptyHostAfterAtRejected", "user@:path", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scpLikeTarget(tc.remoteURL); got != tc.want {
				t.Errorf("scpLikeTarget(%q) = %q, want %q", tc.remoteURL, got, tc.want)
			}
		})
	}
}

func TestAvailabilitySplitShellTokensQuoting(t *testing.T) {
	cases := []struct {
		name    string
		command string
		want    []string
		wantOK  bool
	}{
		{"plainOptions", "ssh -i /keys/id_ed25519 -o IdentitiesOnly=yes", []string{"ssh", "-i", "/keys/id_ed25519", "-o", "IdentitiesOnly=yes"}, true},
		{"singleQuotedSpace", `ssh -F 'my config' host`, []string{"ssh", "-F", "my config", "host"}, true},
		{"doubleQuotedSpace", `ssh -F "my config" host`, []string{"ssh", "-F", "my config", "host"}, true},
		{"adjacentPartsConcatenate", `git"-"ssh' x'`, []string{"git-ssh x"}, true},
		{"emptyQuotedToken", `ssh '' x`, []string{"ssh", "", "x"}, true},
		{"blanksAndTabsCollapse", "  ssh\t\t-x  y ", []string{"ssh", "-x", "y"}, true},
		{"emptyLineNoTokens", "", nil, true},
		{"singleQuotesKeepEverythingLiteral", `ssh -c 'a$b|&;(){} ' x`, []string{"ssh", "-c", "a$b|&;(){} ", "x"}, true},
		{"unterminatedSingleQuote", `ssh 'x`, nil, false},
		{"unterminatedDoubleQuote", `ssh "x`, nil, false},
		{"semicolonSeparator", "ssh; echo", nil, false},
		{"parameterExpansion", "ssh -o P=$PWD", nil, false},
		{"commandSubstitution", "ssh -F $(mktemp)", nil, false},
		{"globChar", "ssh* x", nil, false},
		{"tilde", "ssh ~root", nil, false},
		{"backslashEscape", `ssh \x`, nil, false},
		{"newlineSeparator", "ssh x\nssh y", nil, false},
		{"carriageReturn", "ssh x\ry", nil, false},
		{"expansionInsideDoubleQuotes", `ssh -F "a$b" x`, nil, false},
		{"backtickInsideDoubleQuotes", "ssh -F \"a`b\" x", nil, false},
		{"backslashInsideDoubleQuotes", `ssh -F "a\b" x`, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := splitShellTokens(tc.command)
			if ok != tc.wantOK {
				t.Fatalf("splitShellTokens(%q) ok = %v, want %v (tokens %q)", tc.command, ok, tc.wantOK, got)
			}
			if !tc.wantOK {
				return
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("splitShellTokens(%q) = %q, want %q", tc.command, got, tc.want)
			}
		})
	}

	for _, meta := range []string{"$", "|", "&", ";", "<", ">", "(", ")", "*", "?", "[", "]", "{", "}", "~", `\`, "#", "`", "\n", "\r"} {
		command := fmt.Sprintf("ssh%sx", meta)
		t.Run(fmt.Sprintf("unquotedMetaEscapes_%q", meta), func(t *testing.T) {
			if _, ok := splitShellTokens(command); ok {
				t.Errorf("splitShellTokens(%q) = ok, want !ok", command)
			}
		})
	}
}

func TestAvailabilitySSHCommandLineFirstTokenGating(t *testing.T) {
	cases := []struct {
		name     string
		command  string
		wantBin  string
		wantArgs []string
		wantOK   bool
	}{
		{"plainCommand", "ssh -o ProxyJump=bastion -i /k/id", "ssh", []string{"-o", "ProxyJump=bastion", "-i", "/k/id"}, true},
		{"quotedPathWithSpace", `'/usr/local/bin/my ssh' -F /cfg`, "/usr/local/bin/my ssh", []string{"-F", "/cfg"}, true},
		{"firstTokenIsFlagRejected", "-x ssh", "", nil, false},
		{"multiCommandLineRejected", "ls; ssh", "", nil, false},
		{"emptyLineHasNoBinary", "", "", nil, false},
		{"expansionRejected", `ssh -F "$(mktemp)"`, "", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bin, args, ok := sshCommandLine(context.Background(), tc.command)
			if ok != tc.wantOK {
				t.Fatalf("sshCommandLine(%q) ok = %v, want %v (bin %q args %q)", tc.command, ok, tc.wantOK, bin, args)
			}
			if !tc.wantOK {
				return
			}
			if bin != tc.wantBin || !slices.Equal(args, tc.wantArgs) {
				t.Errorf("sshCommandLine(%q) = (%q, %q), want (%q, %q)", tc.command, bin, args, tc.wantBin, tc.wantArgs)
			}
		})
	}
}

func TestAvailabilitySSHEffectiveEndpointReadsSSHDump(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want string
		ok   bool
	}{
		{
			name: "representativeDump",
			out:  "user git\nhostname github.com\nport 22\naddressfamily any\n",
			want: "github.com:22",
			ok:   true,
		},
		{
			name: "portBeforeHostname",
			out:  "port 2222\nhostname ssh.example.com\n",
			want: "ssh.example.com:2222",
			ok:   true,
		},
		{
			name: "firstValueWinsPerKeyword",
			out:  "hostname a.example\nhostname b.example\nport 22\nport 2222\n",
			want: "a.example:22",
			ok:   true,
		},
		{
			name: "trailingWhitespaceTrimmed",
			out:  "hostname host.example \r\nport 22\t\n",
			want: "host.example:22",
			ok:   true,
		},
		{
			name: "proxycommandValueYieldsNoEndpoint",
			out:  "hostname github.com\nport 22\nproxycommand ssh -W %h:%p bastion\n",
			want: "",
			ok:   false,
		},
		{
			name: "proxyjumpValueYieldsNoEndpoint",
			out:  "proxyjump jump1,jump2\nhostname github.com\nport 22\n",
			want: "",
			ok:   false,
		},
		{
			name: "proxyKeywordNoneMeansNoProxy",
			out:  "hostname github.com\nport 22\nproxycommand None \r\nproxyjump none\n",
			want: "github.com:22",
			ok:   true,
		},
		{"emptyOutput", "", "", false},
		{"missingHostname", "port 22\n", "", false},
		{"missingPort", "hostname github.com\n", "", false},
		{"portZeroRejected", "hostname github.com\nport 0\n", "", false},
		{"portTooLargeRejected", "hostname github.com\nport 65536\n", "", false},
		{"portNotNumericRejected", "hostname github.com\nport ssh\n", "", false},
		{"uppercaseKeywordNotParsed", "HostName github.com\nPort 22\n", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := sshEffectiveEndpoint([]byte(tc.out))
			if ok != tc.ok || got != tc.want {
				t.Errorf("sshEffectiveEndpoint(%q) = (%q, %v), want (%q, %v)", tc.out, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestAvailabilityPktLineFramesLengthPrefix(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"emptyPayloadIsLengthOnlyPacket", "", "0004"},
		{"asciiPayload", "want abc\n", "000dwant abc\n"},
		{"lengthCrossesIntoThreeHexDigits", strings.Repeat("x", 252), "0100" + strings.Repeat("x", 252)},
		{"probeRequestPayload", "git-upload-pack /probe-invalid\x00host=example.com\x00",
			"0034git-upload-pack /probe-invalid\x00host=example.com\x00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pktLine(tc.payload); got != tc.want {
				t.Errorf("pktLine(%q) = %q, want %q", tc.payload, got, tc.want)
			}
		})
	}
	if pktLine("") == "0000" {
		t.Error(`pktLine("") must not emit the flush packet "0000"`)
	}
}

func TestAvailabilityProbeDeadlineBoundsProbeIO(t *testing.T) {
	t.Run("noCallerDeadlineUsesFreshBound", func(t *testing.T) {
		before := time.Now()
		got := probeDeadline(context.Background())
		after := time.Now()
		if !got.After(before.Add(remoteProbeTimeout-time.Second)) || got.After(after.Add(remoteProbeTimeout+time.Second)) {
			t.Errorf("probeDeadline(background) = %s, want ~%s from now", got, remoteProbeTimeout)
		}
	})
	t.Run("shortCallerDeadlineIsInherited", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		want, _ := ctx.Deadline()
		if got := probeDeadline(ctx); !got.Equal(want) {
			t.Errorf("probeDeadline = %s, want caller deadline %s", got, want)
		}
	})
	t.Run("farCallerDeadlineKeepsProbeBound", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		before := time.Now()
		got := probeDeadline(ctx)
		after := time.Now()
		if !got.After(before.Add(remoteProbeTimeout-time.Second)) || got.After(after.Add(remoteProbeTimeout+time.Second)) {
			t.Errorf("probeDeadline = %s, want the probe's own ~%s bound, not the caller's hour", got, remoteProbeTimeout)
		}
	})
	t.Run("callerDeadlineInPastIsKept", func(t *testing.T) {
		past := time.Now().Add(-time.Minute)
		ctx, cancel := context.WithDeadline(context.Background(), past)
		defer cancel()
		if got := probeDeadline(ctx); !got.Equal(past) {
			t.Errorf("probeDeadline = %s, want the caller's past deadline %s", got, past)
		}
	})
}

func TestAvailabilityCallerDeadlineEndedDecidesFromInstant(t *testing.T) {
	if callerDeadlineEnded(context.Background()) {
		t.Error("callerDeadlineEnded(background) = true, want false: a caller with no deadline left the probe's own bound to end the wait")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if callerDeadlineEnded(ctx) {
		t.Errorf("callerDeadlineEnded = true, want false while the deadline (%v) is still in the future", time.Now().Add(time.Hour))
	}
	expired, cancelExpired := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelExpired()
	time.Sleep(20 * time.Millisecond)
	if !callerDeadlineEnded(expired) {
		t.Error("callerDeadlineEnded = false after the caller's deadline passed, want true")
	}
}
