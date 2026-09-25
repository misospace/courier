package executor

import (
	"reflect"
	"testing"
)

func TestMCPStatusCommand(t *testing.T) {
	tests := []struct {
		name   string
		binary string
		want   Command
	}{
		{name: "default binary", binary: "", want: Command{Binary: "opencode", Args: []string{"mcp", "list"}}},
		{name: "blank binary", binary: "   ", want: Command{Binary: "opencode", Args: []string{"mcp", "list"}}},
		{name: "configured binary", binary: "/usr/local/bin/opencode", want: Command{Binary: "/usr/local/bin/opencode", Args: []string{"mcp", "list"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := (OpenCode{Binary: test.binary}).MCPStatusCommand()
			if got.Binary != test.want.Binary {
				t.Fatalf("binary = %q, want %q", got.Binary, test.want.Binary)
			}
			if !sameStrings(got.Args, test.want.Args) {
				t.Fatalf("args = %#v, want %#v", got.Args, test.want.Args)
			}
		})
	}
}

func TestParseMCPStatus(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []MCPCapability
	}{
		{
			name:  "observed failed output",
			input: "✗ github failed\n    SSE error: Non-200 status code (400)\n",
			want:  []MCPCapability{{Name: "github", Available: false, Reason: "SSE error: Non-200 status code (400)"}},
		},
		{
			name:  "crlf failed output",
			input: "✗ github failed\r\n    SSE error: Non-200 status code (400)\r\n",
			want:  []MCPCapability{{Name: "github", Available: false, Reason: "SSE error: Non-200 status code (400)"}},
		},
		{
			name:  "healthy multi-server",
			input: "✓ github connected\n✓ context7 connected\n",
			want:  []MCPCapability{{Name: "github", Available: true}, {Name: "context7", Available: true}},
		},
		{
			name:  "mixed healthy and failed",
			input: "✓ github connected\n✗ metrics failed\n    connection refused\n",
			want:  []MCPCapability{{Name: "github", Available: true}, {Name: "metrics", Available: false, Reason: "connection refused"}},
		},
		{
			name:  "glyph-less success",
			input: "github connected",
			want:  []MCPCapability{{Name: "github", Available: true}},
		},
		{
			name:  "name token with trailing punctuation",
			input: "github: connected\n",
			want:  []MCPCapability{{Name: "github", Available: true}},
		},
		{
			name:  "glyph-less failure with reason",
			input: "github failed\n    boom",
			want:  []MCPCapability{{Name: "github", Available: false, Reason: "boom"}},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "prose only",
			input: "No MCP servers configured\n",
			want:  nil,
		},
		{
			name:  "duplicate keeps last occurrence",
			input: "✓ github connected\n✗ github failed\n    boom\n",
			want:  []MCPCapability{{Name: "github", Available: false, Reason: "boom"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ParseMCPStatus(test.input)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ParseMCPStatus() = %#v, want %#v", got, test.want)
			}
		})
	}
}
