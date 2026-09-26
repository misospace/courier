package executor

import (
	"strings"
	"unicode"
)

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

// mcpNameMaxRunes bounds a server name to a concise diagnostic.
const mcpNameMaxRunes = 128

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
// capability status. Each line is first stripped of ANSI escape sequences and
// of a leading box-drawing/bullet frame rune (● │ ┌ └) with its following
// spaces, and is then classified on that normalized content. A line is a
// server entry when it carries a success signal (a leading glyph ✓, ✔, or +,
// or the word connected, ready, or enabled) or a failure signal (a leading
// glyph ✗, ✘, or ×, or the word failed, error, or disabled),
// case-insensitively. When both a success and a failure signal appear, the
// entry is a failure. The server name is the first whitespace-delimited token
// that is not a status word, with any leading status glyph and surrounding
// punctuation removed; a server whose name is itself a status word is
// preserved with its status, while a lone status word with no name is treated
// as prose and ignored. For a failed entry, a continuation line immediately
// following it — one that is whitespace-indented, or that starts with a │
// frame rune — supplies the reason. Lines that cannot be classified (headers,
// blank lines, "no servers configured" prose) are ignored. A repeated name
// keeps its last occurrence, and result order follows first appearance. Names
// and reasons are stripped of control characters; names are capped at
// mcpNameMaxRunes runes and reasons at mcpReasonMaxRunes runes. It never
// interprets or returns secrets; callers redact before emission. It returns
// nil when nothing is recognized.
func ParseMCPStatus(output string) []MCPCapability {
	lines := strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n")
	positions := make(map[string]int)
	var result []MCPCapability
	for i := 0; i < len(lines); i++ {
		capability, ok := classifyMCPLine(stripMCPPrefix(stripANSI(lines[i])))
		if !ok {
			continue
		}
		if !capability.Available && i+1 < len(lines) {
			next := stripANSI(lines[i+1])
			reason := strings.TrimSpace(stripMCPPrefix(next))
			if reason != "" && (isIndentedMCPLine(next) || strings.HasPrefix(next, "│")) {
				capability.Reason = capMCPReason(reason)
				i++
			}
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
	hasGlyph := hasMCPGlyph(line, mcpStatusGlyphs)
	firstNonStatus := ""
	firstName := ""
	nonEmpty := 0
	for _, token := range tokens {
		candidate := strings.Trim(stripMCPControl(token), mcpTokenPunctuation)
		if candidate == "" {
			continue
		}
		nonEmpty++
		if firstName == "" {
			firstName = candidate
		}
		if !isMCPStatusWord(candidate) {
			firstNonStatus = candidate
			break
		}
	}
	name := firstNonStatus
	if name == "" {
		name = firstName
	}
	if name == "" {
		return MCPCapability{}, false
	}
	if firstNonStatus == "" && !hasGlyph && nonEmpty < 2 {
		return MCPCapability{}, false
	}
	return MCPCapability{Name: capMCPName(name), Available: success && !failure}, true
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

// mcpFramePrefixes are the box-drawing/bullet runes that frame real
// `opencode mcp list` output.
var mcpFramePrefixes = []rune{'●', '│', '┌', '└'}

// stripANSI removes ANSI/VT escape sequences from s. On an ESC byte it
// consumes an escape sequence introduced by [ or ] through its final byte in
// the 0x40-0x7E range, and a single following byte for any other introducer;
// all other bytes are emitted verbatim.
func stripANSI(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		if runes[i] == 0x1b {
			if i+1 < len(runes) {
				if runes[i+1] == '[' || runes[i+1] == ']' {
					i += 2
					for i < len(runes) && (runes[i] < 0x40 || runes[i] > 0x7e) {
						i++
					}
				} else {
					i++
				}
			}
			continue
		}
		b.WriteRune(runes[i])
	}
	return b.String()
}

// stripMCPPrefix removes a single leading box/bullet frame rune from
// already-ANSI-stripped s, plus the spaces and tabs that follow it. A line
// that does not start with a frame rune is returned unchanged, so indented
// lines keep their indentation.
func stripMCPPrefix(s string) string {
	runes := []rune(s)
	if len(runes) == 0 {
		return s
	}
	leading := runes[0]
	isPrefix := false
	for _, p := range mcpFramePrefixes {
		if leading == p {
			isPrefix = true
			break
		}
	}
	if !isPrefix {
		return s
	}
	return strings.TrimLeft(string(runes[1:]), " \t")
}

// stripMCPControl removes every control rune from s.
func stripMCPControl(s string) string {
	var b strings.Builder
	for _, r := range s {
		if !unicode.IsControl(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// capMCPName bounds name to mcpNameMaxRunes runes at a rune boundary. Control
// characters and surrounding punctuation are removed earlier, when the name
// candidate is chosen.
func capMCPName(name string) string {
	runes := []rune(name)
	if len(runes) > mcpNameMaxRunes {
		return string(runes[:mcpNameMaxRunes])
	}
	return name
}

// capMCPReason strips control runes and shortens reason to mcpReasonMaxRunes
// runes at a rune boundary.
func capMCPReason(reason string) string {
	reason = stripMCPControl(reason)
	runes := []rune(reason)
	if len(runes) <= mcpReasonMaxRunes {
		return reason
	}
	return string(runes[:mcpReasonMaxRunes])
}
