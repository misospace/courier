package v1alpha1

// TokenUsage is a per-run token tally summed over the coordinator's own
// model steps. Subagent steps are not observable from the coordinator's
// stdout, so they are not included.
type TokenUsage struct {
	Input      int64 `json:"input,omitempty"`
	Output     int64 `json:"output,omitempty"`
	Reasoning  int64 `json:"reasoning,omitempty"`
	CacheRead  int64 `json:"cacheRead,omitempty"`
	CacheWrite int64 `json:"cacheWrite,omitempty"`
	Total      int64 `json:"total,omitempty"`
}

// SubagentTally is the per (agent, model) accounting of `task` tool calls.
// It records calls, errors, and aggregate wall time; subagent tokens are
// not observable from the coordinator's stdout and are deliberately absent.
type SubagentTally struct {
	Agent         string `json:"agent"`
	Model         string `json:"model,omitempty"`
	Calls         int    `json:"calls,omitempty"`
	Errors        int    `json:"errors,omitempty"`
	RuntimeMillis int64  `json:"runtimeMillis,omitempty"`
}

// RunTelemetry is the compact per-run summary written to CoderRun.status
// and included in the source lifecycle report (#172). It is deliberately
// small: it rides the pod termination-message budget (4 KiB) alongside the
// termination reason, so it keeps only run-level totals and a bounded
// per-agent subagent list.
type RunTelemetry struct {
	ModelCalls     int             `json:"modelCalls,omitempty"`
	ToolCalls      int             `json:"toolCalls,omitempty"`
	Sessions       int             `json:"sessions,omitempty"`
	MaxContext     int64           `json:"maxContext,omitempty"`
	Tokens         *TokenUsage     `json:"tokens,omitempty"`
	Subagents      []SubagentTally `json:"subagents,omitempty"`
	DurationMillis int64           `json:"durationMillis,omitempty"`
	Continuations  int             `json:"continuations,omitempty"`
}
