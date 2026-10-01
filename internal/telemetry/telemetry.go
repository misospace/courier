// Package telemetry tallies the coordinator's OpenCode stdout event stream
// into per-run telemetry (#172). It is a pure aggregator: it ingests raw
// event lines and produces a rich log Summary plus a compact RunTelemetry. The
// executor consumes it; only the compact form crosses into CoderRun.status and
// the source report, via the terminal handoff.
//
// OpenCode re-emits the same line for the same part id as a part evolves (a
// tool part at start and again at completion, a step-finish part more than
// once). The tracker is therefore an idempotent projection over the LATEST
// view of each unique part id, not an increment-per-line tally: re-emissions
// of a part id collapse to one entry, so tool calls, subagent calls, runtime,
// and tokens are never double-counted.
package telemetry

import (
	"encoding/json"
	"fmt"
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

// maxSubagents bounds the subagent list in the compact status form by count.
const maxSubagents = 24

// maxCompactBytes bounds the whole compact form by serialized size so it can
// never push the COURIER_TERMINATION line past the 4 KiB termination-message
// budget on its own. 3072 leaves headroom for the reason (capped at 1024)
// plus phase/result/exit/overhead, under 4096.
const maxCompactBytes = 3072

// openCodeEvent is the subset of an OpenCode stdout event line the tracker
// consumes. Every event carries a top-level sessionID; the part carries the
// payload.
type openCodeEvent struct {
	SessionID string `json:"sessionID"`
	Part      *part  `json:"part"`
}

type part struct {
	ID     string      `json:"id"`   // stable part id; re-emissions share it
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

// partKind is the stable kind of a part, fixed at first sight. A part is
// either a model step (carries tokens) or a tool call (carries a tool name);
// the two never co-occur.
type partKind int

const (
	kindUnknown partKind = iota
	kindModel
	kindTool
)

// partView is the latest observed view of one unique part id. Observe
// upserts it in place; Summary and Compact project over the set of views, so
// re-emissions of a part id contribute exactly once.
type partView struct {
	kind      partKind
	sessionID string
	toolName  string
	tokens    courierv1alpha1.TokenUsage // latest tokens, for model parts
	status    string
	sawTime   bool
	startMs   float64
	endMs     float64
	// task-tool subagent identity
	agent      string
	model      string
	subSession string
}

// subagentKey is the (agent, model) pair of a `task` tool call.
type subagentKey struct {
	agent string
	model string
}

// Tracker tallies one run's event stream.
type Tracker struct {
	// parts is the latest view per unique part id; the single source of
	// truth every tally is projected from.
	parts map[string]*partView
	// sessionIDs is the single deduped set of every distinct session id
	// seen: top-level event sessionIDs plus subagent task metadata
	// sessionIds. It drives the Sessions total.
	sessionIDs map[string]struct{}
	// coordinator is the first non-empty top-level session id.
	coordinator string
	// idless is a monotonic counter minting unique keys for id-less
	// emissions, so each is distinct (preserving prior per-line behavior
	// for fixtures that omit part ids).
	idless int

	continuations int
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{
		parts:      make(map[string]*partView),
		sessionIDs: make(map[string]struct{}),
	}
}

// Observe ingests one stdout event line (bytes, may lack a trailing
// newline). Lines that are not valid JSON or that do not match a
// recognized event shape are silently ignored. Observe never errors and
// never retains the line bytes.
//
// It upserts the part's view keyed by part id (or a synthesized unique key
// when the part has no id) and records the session id in the deduped set.
// It does not maintain any global counters: those are derived in Summary and
// Compact by projecting over the part views.
func (t *Tracker) Observe(line []byte) {
	var ev openCodeEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return
	}
	if ev.SessionID != "" {
		if t.coordinator == "" {
			t.coordinator = ev.SessionID
		}
		t.sessionIDs[ev.SessionID] = struct{}{}
	}
	if ev.Part == nil {
		return
	}
	p := ev.Part
	// A recognized part carries tokens (a model step) or a tool name (a
	// tool call); anything else is not tallied.
	if p.Tokens == nil && p.Tool == "" {
		return
	}

	key := p.ID
	if key == "" {
		t.idless++
		key = fmt.Sprintf("idless-%d", t.idless)
	}
	pv := t.parts[key]
	if pv == nil {
		pv = &partView{}
		t.parts[key] = pv
		if p.Tokens != nil {
			pv.kind = kindModel
		} else {
			pv.kind = kindTool
		}
	}

	// Latest wins: refresh every field this emission carries.
	pv.sessionID = ev.SessionID
	if p.Tokens != nil {
		pv.tokens = tokensToUsage(p.Tokens)
	}
	if p.Tool != "" {
		pv.toolName = p.Tool
	}
	if p.State != nil {
		if p.State.Status != "" {
			pv.status = p.State.Status
		}
		if p.State.Time != nil {
			pv.sawTime = true
			if p.State.Time.Start != 0 {
				pv.startMs = p.State.Time.Start
			}
			if p.State.Time.End != 0 {
				pv.endMs = p.State.Time.End
			}
		}
		if p.Tool == "task" {
			pv.agent = p.State.Input.SubagentType
			if p.State.Metadata != nil {
				if p.State.Metadata.Model != nil {
					pv.model = p.State.Metadata.Model.ModelID
				}
				if p.State.Metadata.SessionID != "" {
					pv.subSession = p.State.Metadata.SessionID
					t.sessionIDs[p.State.Metadata.SessionID] = struct{}{}
				}
			}
		}
	}
}

// SetContinuations records how many executor continuations the run used.
func (t *Tracker) SetContinuations(n int) {
	t.continuations = n
}

// sessAgg accumulates one session's projection over its part views.
type sessAgg struct {
	modelCalls int
	toolCalls  int
	tokens     courierv1alpha1.TokenUsage
	maxContext int64
	minStartMs float64
	maxEndMs   float64
	sawTime    bool
}

// subAgg accumulates one (agent, model) subagent group's projection.
type subAgg struct {
	calls   int
	errors  int
	runtime float64
}

// agg is the single-pass projection over the part views that Summary and
// Compact both consume.
type agg struct {
	modelCalls int
	toolCalls  int
	toolByName map[string]int
	tokens     courierv1alpha1.TokenUsage
	maxContext int64
	sessions   map[string]*sessAgg
	subagents  map[subagentKey]*subAgg
	coordDur   int64
}

// aggregate projects the run tallies over the latest view of each part.
func (t *Tracker) aggregate() agg {
	a := agg{
		toolByName: make(map[string]int),
		sessions:   make(map[string]*sessAgg),
		subagents:  make(map[subagentKey]*subAgg),
	}
	for _, pv := range t.parts {
		switch pv.kind {
		case kindModel:
			a.modelCalls++
			addTokens(&a.tokens, pv.tokens)
			if ctx := pv.tokens.Input + pv.tokens.CacheRead + pv.tokens.CacheWrite; ctx > a.maxContext {
				a.maxContext = ctx
			}
		case kindTool:
			a.toolCalls++
			a.toolByName[pv.toolName]++
			if pv.toolName == "task" {
				k := subagentKey{agent: pv.agent, model: pv.model}
				st := a.subagents[k]
				if st == nil {
					st = &subAgg{}
					a.subagents[k] = st
				}
				st.calls++
				if pv.status == "error" {
					st.errors++
				}
				if pv.sawTime {
					st.runtime += pv.endMs - pv.startMs
				}
			}
		}
		if pv.sessionID != "" {
			s := a.sessions[pv.sessionID]
			if s == nil {
				s = &sessAgg{}
				a.sessions[pv.sessionID] = s
			}
			switch pv.kind {
			case kindModel:
				s.modelCalls++
				addTokens(&s.tokens, pv.tokens)
				if ctx := pv.tokens.Input + pv.tokens.CacheRead + pv.tokens.CacheWrite; ctx > s.maxContext {
					s.maxContext = ctx
				}
			case kindTool:
				s.toolCalls++
			}
			if pv.sawTime {
				if !s.sawTime {
					s.minStartMs = pv.startMs
					s.maxEndMs = pv.endMs
					s.sawTime = true
				} else {
					if pv.startMs < s.minStartMs {
						s.minStartMs = pv.startMs
					}
					if pv.endMs > s.maxEndMs {
						s.maxEndMs = pv.endMs
					}
				}
			}
		}
	}
	if t.coordinator != "" {
		if s, ok := a.sessions[t.coordinator]; ok && s.sawTime {
			a.coordDur = int64(s.maxEndMs - s.minStartMs)
		}
	}
	return a
}

// Summary returns the full summary for the log event.
func (t *Tracker) Summary() Summary {
	a := t.aggregate()

	stats := make([]SessionStat, 0, len(a.sessions))
	for id, s := range a.sessions {
		var dur int64
		if s.sawTime {
			dur = int64(s.maxEndMs - s.minStartMs)
		}
		stats = append(stats, SessionStat{
			SessionID:      id,
			ModelCalls:     s.modelCalls,
			ToolCalls:      s.toolCalls,
			MaxContext:     s.maxContext,
			Tokens:         s.tokens,
			DurationMillis: dur,
		})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].SessionID < stats[j].SessionID })

	return Summary{
		ModelCalls:      a.modelCalls,
		ToolCalls:       a.toolCalls,
		ToolCallsByName: a.toolByName,
		Tokens:          a.tokens,
		MaxContext:      a.maxContext,
		Sessions:        stats,
		Subagents:       sortedSubagents(a.subagents),
		DurationMillis:  a.coordDur,
		Continuations:   t.continuations,
	}
}

// Compact returns the compact status/report form. Returns nil when the
// run recorded no activity at all (no model steps and no tool steps), so
// callers can omit an empty summary.
func (t *Tracker) Compact() *courierv1alpha1.RunTelemetry {
	a := t.aggregate()
	if a.modelCalls == 0 && a.toolCalls == 0 {
		return nil
	}
	subagents := sortedSubagents(a.subagents)
	if len(subagents) > maxSubagents {
		subagents = subagents[:maxSubagents]
	}
	rt := &courierv1alpha1.RunTelemetry{
		ModelCalls:     a.modelCalls,
		ToolCalls:      a.toolCalls,
		Sessions:       len(t.sessionIDs),
		MaxContext:     a.maxContext,
		Subagents:      subagents,
		DurationMillis: a.coordDur,
		Continuations:  t.continuations,
	}
	// Omit the token object entirely on tool-only runs: every field is
	// zero and carries omitempty, so an empty Tokens would serialize as a
	// useless "tokens":{} on the size-bounded termination path.
	if a.tokens != (courierv1alpha1.TokenUsage{}) {
		tokens := a.tokens
		rt.Tokens = &tokens
	}
	// Bound the whole form by serialized bytes too: the count cap alone
	// cannot stop a few very long agent/model strings from blowing the
	// termination budget. Drop the lowest-runtime subagent entries until it
	// fits; the scalar fields alone are tiny, so an empty subagent list
	// always fits.
	for {
		b, err := json.Marshal(rt)
		if err != nil || len(b) <= maxCompactBytes || len(rt.Subagents) == 0 {
			break
		}
		rt.Subagents = rt.Subagents[:len(rt.Subagents)-1]
	}
	return rt
}

// tokensToUsage flattens a part's token block into the shared TokenUsage,
// computing the part's total.
func tokensToUsage(tok *partTokens) courierv1alpha1.TokenUsage {
	var cacheRead, cacheWrite int64
	if tok.Cache != nil {
		cacheRead = tok.Cache.Read
		cacheWrite = tok.Cache.Write
	}
	return courierv1alpha1.TokenUsage{
		Input:      tok.Input,
		Output:     tok.Output,
		Reasoning:  tok.Reasoning,
		CacheRead:  cacheRead,
		CacheWrite: cacheWrite,
		Total:      tok.Input + tok.Output + tok.Reasoning + cacheRead + cacheWrite,
	}
}

func sortedSubagents(subs map[subagentKey]*subAgg) []courierv1alpha1.SubagentTally {
	out := make([]courierv1alpha1.SubagentTally, 0, len(subs))
	for k, st := range subs {
		out = append(out, courierv1alpha1.SubagentTally{
			Agent:         k.agent,
			Model:         k.model,
			Calls:         st.calls,
			Errors:        st.errors,
			RuntimeMillis: int64(st.runtime),
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
