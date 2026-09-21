package executor

import "encoding/json"

const (
	opencodeConfigAnnotation = "courier.misospace.dev/opencode-config"
	opencodeConfigMountPath  = "/etc/courier/opencode"
	opencodeConfigFilename   = "opencode.json"
)

type openCodeConfig struct {
	Agents    map[string]openCodeAgent `json:"agent"`
	Permission map[string]string       `json:"permission,omitempty"`
	MCP       map[string]openCodeMCP   `json:"mcp,omitempty"`
}

type openCodeAgent struct {
	Mode  string `json:"mode"`
	Model string `json:"model"`
}

type openCodeMCP struct {
	Type    string            `json:"type"`
	URL     string            `json:"url"`
	Enabled bool              `json:"enabled"`
	OAuth   *bool             `json:"oauth,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

func marshalOpenCodeConfig(roles map[string]string, githubURL, context7URL, metricsURL string) ([]byte, error) {
	denyMerge := map[string]string{
		"github_merge_pull_request": "deny",
		"github_merge*":             "deny",
	}
	agents := make(map[string]openCodeAgent, len(roles))
	for role, model := range roles {
		agents[role] = openCodeAgent{Mode: "all", Model: model}
	}
	config := openCodeConfig{
		Agents:    agents,
		Permission: denyMerge,
	}
	config.MCP = make(map[string]openCodeMCP)
	if githubURL != "" {
		config.MCP["github"] = openCodeMCP{
			Type:    "remote",
			URL:     githubURL,
			Enabled: true,
			OAuth:   boolPtr(false),
			Headers: map[string]string{"Authorization": "Bearer {env:GITHUB_TOKEN}"},
		}
	}
	if context7URL != "" {
		config.MCP["context7"] = openCodeMCP{
			Type:    "remote",
			URL:     context7URL,
			Enabled: true,
			OAuth:   boolPtr(false),
			Headers: map[string]string{"CONTEXT7_API_KEY": "{env:CONTEXT7_API_KEY}"},
		}
	}
	if metricsURL != "" {
		config.MCP["metrics"] = openCodeMCP{
			Type:    "remote",
			URL:     metricsURL,
			Enabled: true,
		}
	}
	if len(config.MCP) == 0 {
		config.MCP = nil
	}
	return json.Marshal(config)
}
