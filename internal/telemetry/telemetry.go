// Package telemetry tallies the coordinator's OpenCode stdout event stream
// into per-run telemetry (#172). It is a pure aggregator: it ingests raw
// event lines and produces a rich log Summary plus a compact RunTelemetry. The
// executor consumes it; only the compact form crosses into CoderRun.status and
// the source report, via the terminal handoff.
package telemetry

import (
	"encoding/json"
	"sort"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// Summary is the full per-run telemetry for the run.summary log event.
// Richer than the compact RunTelemetry written to status: it keeps
// per-tool counts and per-session detail too large for the
// termination-message budget.
type Summary struct {
	ModelCalls      int                             `json:"model_calls"`
	ToolCalls       int                             `json:"tool_calls"`
	ToolCallsByName map[string]int                  `json:"tool_calls_by_name"`
	Tokens          courierv1alpha1.TokenUsage      `json:"tokens"`
	MaxContext      int64                           `json:"max_context"`
	Sessions        []SessionStat                   `json:"sessions"`
	Subagents       []courierv1alpha1.SubagentTally `json:"subagents"`
	DurationMillis  int64                           `json:"duration_millis"`
	Continuations   int                             `json:"continuations,omitempty"`
}

// SessionStat is one session's contribution to a run summary.
type SessionStat struct {
	SessionID      string                     `json:"session_id"`
	ModelCalls     int                        `json:"model_calls"`
	ToolCalls      int                        `json:"tool_calls"`
	MaxContext     int64                      `json:"max_context"`
	Tokens         courierv1alpha1.TokenUsage `json:"tokens"`
	DurationMillis int64                      `json:"duration_millis"`
}

// maxSubagents bounds the subagent list in the compact status form so it
// cannot blow the pod termination-message budget.
const maxSubagents = 24

// openCodeEvent is the subset of an OpenCode stdout event line the tracker
// consumes. Every event carries a top-level sessionID; the part carries the
// payload.
type openCodeEvent struct {
	SessionID string `json:"sessionID"`
	Part      *part  `json:"part"`
}

type part struct {
	Type   string      `json:"type"` // e.g. "step-finish", "tool", "text"; not branched on
	Tool   string      `json:"tool"` // tool name, present on tool_use parts
	Tokens *partTokens `json:"tokens"`
	State  *partState  `json:"state"`
}

type partTokens struct {
	Input     int64            `json:"input"`
	Output    int64            `json:"output"`
	Reasoning int64            `json:"reasoning"`
	Cache     *partTokensCache `json:"cache"`
}

type partTokensCache struct {
	Read  int64 `json:"read"`
	Write int64 `json:"write"`
}

type partTime struct {
	Start float64 `json:"start"` // epoch milliseconds
	End   float64 `json:"end"`
}

type partInput struct {
	SubagentType string `json:"subagent_type"`
}

type partModel struct {
	ModelID string `json:"modelID"`
}

type partMetadata struct {
	Model     *partModel `json:"model"`
	SessionID string     `json:"sessionId"`
}

type partState struct {
	Status   string        `json:"status"` // "completed" | "error" | ...
	Time     *partTime     `json:"time"`
	Input    partInput     `json:"input"`
	Metadata *partMetadata `json:"metadata"`
}

// sessionState is one top-level session's observed activity.
type sessionState struct {
	modelCalls int
	toolCalls  int
	tokens     courierv1alpha1.TokenUsage
	maxContext int64
	minStartMs float64
	maxEndMs   float64
	sawTime    bool
}

// subagentKey is the (agent, model) pair of a `task` tool call.
type subagentKey struct {
	agent string
	model string
}

type subagentState struct {
	calls     int
	errors    int
	runtimeMs float64
}

// Tracker tallies one run's event stream.
type Tracker struct {
	modelCalls int
	toolCalls  int
	toolByName map[string]int
	tokens     courierv1alpha1.TokenUsage
	maxContext int64

	sessions         map[string]*sessionState
	topSessions      map[string]struct{} // distinct non-empty top-level session ids
	coordinator      string              // first non-empty top-level session id
	subagentSessions map[string]struct{} // distinct subagent task metadata.sessionId

	subagents map[subagentKey]*subagentState

	continuations int
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{
		toolByName:       make(map[string]int),
		sessions:         make(map[string]*sessionState),
		topSessions:      make(map[string]struct{}),
		subagentSessions: make(map[string]struct{}),
		subagents:        make(map[subagentKey]*subagentState),
	}
}

// Observe ingests one stdout event line (bytes, may lack a trailing
// newline). Lines that are not valid JSON or that do not match a
// recognized event shape are silently ignored. Observe never errors and
// never retains the line bytes.
func (t *Tracker) Observe(line []byte) {
	var ev openCodeEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return
	}
	if ev.SessionID != "" {
		if t.coordinator == "" {
			t.coordinator = ev.SessionID
		}
		t.topSessions[ev.SessionID] = struct{}{}
	}
	if ev.Part == nil {
		return
	}
	if ev.Part.Tokens != nil {
		t.observeModelStep(ev.SessionID, ev.Part.Tokens)
	}
	if ev.Part.Tool != "" {
		t.observeTool(ev.SessionID, ev.Part)
	}
}

// SetContinuations records how many executor continuations the run used.
func (t *Tracker) SetContinuations(n int) {
	t.continuations = n
}

// Summary returns the full summary for the log event.
func (t *Tracker) Summary() Summary {
	stats := make([]SessionStat, 0, len(t.sessions))
	for id, s := range t.sessions {
		stats = append(stats, SessionStat{
			SessionID:      id,
			ModelCalls:     s.modelCalls,
			ToolCalls:      s.toolCalls,
			MaxContext:     s.maxContext,
			Tokens:         s.tokens,
			DurationMillis: s.durationMillis(),
		})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].SessionID < stats[j].SessionID })

	toolByName := make(map[string]int, len(t.toolByName))
	for name, n := range t.toolByName {
		toolByName[name] = n
	}

	return Summary{
		ModelCalls:      t.modelCalls,
		ToolCalls:       t.toolCalls,
		ToolCallsByName: toolByName,
		Tokens:          t.tokens,
		MaxContext:      t.maxContext,
		Sessions:        stats,
		Subagents:       t.sortedSubagents(),
		DurationMillis:  t.coordinatorDurationMillis(),
		Continuations:   t.continuations,
	}
}

// Compact returns the compact status/report form. Returns nil when the
// run recorded no activity at all (no model steps and no tool steps), so
// callers can omit an empty summary.
func (t *Tracker) Compact() *courierv1alpha1.RunTelemetry {
	if t.modelCalls == 0 && t.toolCalls == 0 {
		return nil
	}
	subagents := t.sortedSubagents()
	if len(subagents) > maxSubagents {
		subagents = subagents[:maxSubagents]
	}
	rt := &courierv1alpha1.RunTelemetry{
		ModelCalls:     t.modelCalls,
		ToolCalls:      t.toolCalls,
		Sessions:       len(t.topSessions) + len(t.subagentSessions),
		MaxContext:     t.maxContext,
		Subagents:      subagents,
		DurationMillis: t.coordinatorDurationMillis(),
		Continuations:  t.continuations,
	}
	// Omit the token object entirely on tool-only runs: every field is
	// zero and carries omitempty, so an empty Tokens would serialize as a
	// useless "tokens":{} on the size-bounded termination path.
	if t.tokens != (courierv1alpha1.TokenUsage{}) {
		tokens := t.tokens
		rt.Tokens = &tokens
	}
	return rt
}

func (t *Tracker) observeModelStep(sessionID string, tok *partTokens) {
	var cacheRead, cacheWrite int64
	if tok.Cache != nil {
		cacheRead = tok.Cache.Read
		cacheWrite = tok.Cache.Write
	}
	stepTokens := courierv1alpha1.TokenUsage{
		Input:      tok.Input,
		Output:     tok.Output,
		Reasoning:  tok.Reasoning,
		CacheRead:  cacheRead,
		CacheWrite: cacheWrite,
		Total:      tok.Input + tok.Output + tok.Reasoning + cacheRead + cacheWrite,
	}
	stepContext := tok.Input + cacheRead + cacheWrite

	t.modelCalls++
	addTokens(&t.tokens, stepTokens)
	if stepContext > t.maxContext {
		t.maxContext = stepContext
	}
	if sessionID != "" {
		s := t.session(sessionID)
		s.modelCalls++
		addTokens(&s.tokens, stepTokens)
		if stepContext > s.maxContext {
			s.maxContext = stepContext
		}
	}
}

func (t *Tracker) observeTool(sessionID string, p *part) {
	t.toolCalls++
	t.toolByName[p.Tool]++
	if sessionID != "" {
		t.session(sessionID).toolCalls++
	}
	if p.State != nil && p.State.Time != nil && sessionID != "" {
		s := t.session(sessionID)
		if !s.sawTime {
			s.minStartMs = p.State.Time.Start
			s.maxEndMs = p.State.Time.End
			s.sawTime = true
		} else {
			if p.State.Time.Start < s.minStartMs {
				s.minStartMs = p.State.Time.Start
			}
			if p.State.Time.End > s.maxEndMs {
				s.maxEndMs = p.State.Time.End
			}
		}
	}
	if p.Tool != "task" {
		return
	}

	// Subagent call: per (agent, model) accounting.
	var agent, model string
	var st *subagentState
	if p.State != nil {
		agent = p.State.Input.SubagentType
		if p.State.Metadata != nil && p.State.Metadata.Model != nil {
			model = p.State.Metadata.Model.ModelID
		}
	}
	key := subagentKey{agent: agent, model: model}
	st = t.subagents[key]
	if st == nil {
		st = &subagentState{}
		t.subagents[key] = st
	}
	st.calls++
	if p.State != nil {
		if p.State.Status == "error" {
			st.errors++
		}
		if p.State.Time != nil {
			st.runtimeMs += p.State.Time.End - p.State.Time.Start
		}
		if p.State.Metadata != nil && p.State.Metadata.SessionID != "" {
			t.subagentSessions[p.State.Metadata.SessionID] = struct{}{}
		}
	}
}

func (t *Tracker) session(id string) *sessionState {
	s, ok := t.sessions[id]
	if !ok {
		s = &sessionState{}
		t.sessions[id] = s
	}
	return s
}

func (t *Tracker) coordinatorDurationMillis() int64 {
	if t.coordinator == "" {
		return 0
	}
	s, ok := t.sessions[t.coordinator]
	if !ok {
		return 0
	}
	return s.durationMillis()
}

func (s *sessionState) durationMillis() int64 {
	if !s.sawTime {
		return 0
	}
	return int64(s.maxEndMs - s.minStartMs)
}

func (t *Tracker) sortedSubagents() []courierv1alpha1.SubagentTally {
	out := make([]courierv1alpha1.SubagentTally, 0, len(t.subagents))
	for k, st := range t.subagents {
		out = append(out, courierv1alpha1.SubagentTally{
			Agent:         k.agent,
			Model:         k.model,
			Calls:         st.calls,
			Errors:        st.errors,
			RuntimeMillis: int64(st.runtimeMs),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RuntimeMillis != out[j].RuntimeMillis {
			return out[i].RuntimeMillis > out[j].RuntimeMillis
		}
		if out[i].Agent != out[j].Agent {
			return out[i].Agent < out[j].Agent
		}
		return out[i].Model < out[j].Model
	})
	return out
}

func addTokens(dst *courierv1alpha1.TokenUsage, src courierv1alpha1.TokenUsage) {
	dst.Input += src.Input
	dst.Output += src.Output
	dst.Reasoning += src.Reasoning
	dst.CacheRead += src.CacheRead
	dst.CacheWrite += src.CacheWrite
	dst.Total += src.Total
}
