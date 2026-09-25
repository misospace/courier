package executor

import "strings"

// MCPCapability is the availability of one configured MCP server observed by a
// bounded preflight using the run's own OpenCode config/environment. Reason is
// a short, secret-free failure note (for example an SSE/HTTP error) and is
// empty when the server is available. It never carries config contents or
// credentials.
type MCPCapability struct {
	Name      string
	Available bool
	Reason    string
}

// mcpReasonMaxRunes bounds a failure reason to a concise diagnostic.
const mcpReasonMaxRunes = 200

var (
	mcpSuccessGlyphs = []string{"✓", "✔", "+"}
	mcpFailureGlyphs = []string{"✗", "✘", "×"}
	mcpSuccessWords  = []string{"connected", "ready", "enabled"}
	mcpFailureWords  = []string{"failed", "error", "disabled"}

	mcpStatusGlyphs = append(append([]string{}, mcpSuccessGlyphs...), mcpFailureGlyphs...)
)

// MCPStatusCommand returns the OpenCode command vector that reports MCP
// connection status for the same config the run uses. It uses the run's own
// OpenCode binary (defaulting like OpenCode.Command does) so the probe reflects
// the run's environment. Args are exactly ["mcp", "list"].
func (o OpenCode) MCPStatusCommand() Command {
	binary := strings.TrimSpace(o.Binary)
	if binary == "" {
		binary = DefaultOpenCode().Binary
	}
	return Command{Binary: binary, Args: []string{"mcp", "list"}}
}

// ParseMCPStatus parses the textual `opencode mcp list` output into per-server
// capability status. A line is a server entry when it starts at column 0 and
// carries a success signal (a leading glyph ✓, ✔, or +, or the word
// connected, ready, or enabled) or a failure signal (a leading glyph ✗, ✘, or
// ×, or the word failed, error, or disabled), case-insensitively. When both a
// success and a failure signal appear, the entry is a failure. The server name
// is the first whitespace-delimited token with any leading status glyph and
// surrounding punctuation removed, skipping tokens that are status words. For a failed entry, an
// indented, non-empty line immediately following it supplies the reason. Lines
// that cannot be classified (headers, blank lines, "no servers configured"
// prose) are ignored. A repeated name keeps its last occurrence, and result
// order follows first appearance. Reasons are capped at mcpReasonMaxRunes
// runes. It never interprets or returns secrets; callers redact before
// emission. It returns nil when nothing is recognized.
func ParseMCPStatus(output string) []MCPCapability {
	lines := strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n")
	positions := make(map[string]int)
	var result []MCPCapability
	for i := 0; i < len(lines); i++ {
		capability, ok := classifyMCPLine(lines[i])
		if !ok {
			continue
		}
		if !capability.Available && i+1 < len(lines) && isIndentedMCPLine(lines[i+1]) {
			capability.Reason = capMCPReason(strings.TrimSpace(lines[i+1]))
			i++
		}
		if index, seen := positions[capability.Name]; seen {
			result[index] = capability
		} else {
			positions[capability.Name] = len(result)
			result = append(result, capability)
		}
	}
	return result
}

// classifyMCPLine reports whether line is a server entry and, if so, its
// capability. Indented lines are continuation prose, never server entries.
func classifyMCPLine(line string) (MCPCapability, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
		return MCPCapability{}, false
	}
	success := hasMCPGlyph(line, mcpSuccessGlyphs)
	failure := hasMCPGlyph(line, mcpFailureGlyphs)
	tokens := mcpStatusTokens(trimmed)
	for _, token := range tokens {
		switch word := mcpStatusWord(token); {
		case containsWord(mcpSuccessWords, word):
			success = true
		case containsWord(mcpFailureWords, word):
			failure = true
		}
	}
	if !success && !failure {
		return MCPCapability{}, false
	}
	name := ""
	for _, token := range tokens {
		if token == "" || isMCPStatusWord(token) {
			continue
		}
		name = strings.Trim(token, mcpTokenPunctuation)
		break
	}
	if name == "" {
		return MCPCapability{}, false
	}
	return MCPCapability{Name: name, Available: success && !failure}, true
}

// mcpStatusTokens splits line into whitespace-delimited tokens and removes any
// leading status glyph from the first token.
func mcpStatusTokens(line string) []string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	for _, glyph := range mcpStatusGlyphs {
		for strings.HasPrefix(fields[0], glyph) {
			fields[0] = strings.TrimPrefix(fields[0], glyph)
		}
	}
	return fields
}

// mcpTokenPunctuation is the surrounding punctuation stripped from tokens
// when normalizing status words and choosing a server name.
const mcpTokenPunctuation = ".,:;()[]{}<>\"'"

// mcpStatusWord normalizes a token for status-word comparison.
func mcpStatusWord(token string) string {
	return strings.ToLower(strings.Trim(token, mcpTokenPunctuation))
}

func isMCPStatusWord(token string) bool {
	word := mcpStatusWord(token)
	return containsWord(mcpSuccessWords, word) || containsWord(mcpFailureWords, word)
}

func containsWord(words []string, word string) bool {
	for _, candidate := range words {
		if candidate == word {
			return true
		}
	}
	return false
}

func hasMCPGlyph(line string, glyphs []string) bool {
	for _, glyph := range glyphs {
		if strings.HasPrefix(line, glyph) {
			return true
		}
	}
	return false
}

// isIndentedMCPLine reports whether line is an indented, non-empty line.
func isIndentedMCPLine(line string) bool {
	if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
		return false
	}
	return strings.TrimSpace(line) != ""
}

// capMCPReason shortens reason to mcpReasonMaxRunes runes at a rune boundary.
func capMCPReason(reason string) string {
	runes := []rune(reason)
	if len(runes) <= mcpReasonMaxRunes {
		return reason
	}
	return string(runes[:mcpReasonMaxRunes])
}
