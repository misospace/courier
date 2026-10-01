package telemetry

import (
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

func observeAll(t *testing.T, lines ...string) *Tracker {
	t.Helper()
	tr := New()
	for _, line := range lines {
		tr.Observe([]byte(line))
	}
	return tr
}

func TestRunSummary(t *testing.T) {
	tr := observeAll(t,
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"step-finish","tokens":{"input":1000,"output":200,"reasoning":50,"cache":{"read":4000,"write":100}}}}`+"\n",
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"tool","tool":"bash","state":{"status":"completed","time":{"start":1000,"end":3000}}}}`+"\n",
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"step-finish","tokens":{"input":2000,"output":300,"reasoning":0,"cache":{"read":8000,"write":0}}}}`+"\n",
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"tool","tool":"edit","state":{"status":"completed","time":{"start":5000,"end":6000}}}}`+"\n",
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"tool","tool":"task","state":{"status":"completed","time":{"start":6000,"end":66000},"input":{"subagent_type":"coder-local"},"metadata":{"model":{"modelID":"litellm/qwen"},"sessionId":"ses_sub1"}}}}`+"\n",
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"tool","tool":"task","state":{"status":"error","time":{"start":66000,"end":66500},"input":{"subagent_type":"coder-local"},"metadata":{"model":{"modelID":"litellm/qwen"},"sessionId":"ses_sub2"}}}}`+"\n",
		`{"type":"message.part.updated","sessionID":"ses_c","part":{"type":"step-finish","tokens":{"input":500,"output":100,"reasoning":0,"cache":{"read":10000,"write":0}}}}`+"\n",
		"not-json-this-line-is-ignored\n",
	)
	tr.SetContinuations(2)

	s := tr.Summary()
	c := tr.Compact()
	if c == nil {
		t.Fatal("Compact() = nil, want non-nil")
	}

	if s.ModelCalls != 3 {
		t.Errorf("Summary.ModelCalls = %d, want 3", s.ModelCalls)
	}
	if c.ModelCalls != 3 {
		t.Errorf("Compact.ModelCalls = %d, want 3", c.ModelCalls)
	}
	if s.ToolCalls != 4 {
		t.Errorf("Summary.ToolCalls = %d, want 4", s.ToolCalls)
	}
	if c.ToolCalls != 4 {
		t.Errorf("Compact.ToolCalls = %d, want 4", c.ToolCalls)
	}
	wantByName := map[string]int{"bash": 1, "edit": 1, "task": 2}
	for name, want := range wantByName {
		if got := s.ToolCallsByName[name]; got != want {
			t.Errorf("Summary.ToolCallsByName[%q] = %d, want %d", name, got, want)
		}
	}
	if len(s.ToolCallsByName) != len(wantByName) {
		t.Errorf("Summary.ToolCallsByName = %v, want %v", s.ToolCallsByName, wantByName)
	}

	wantTokens := courierv1alpha1.TokenUsage{
		Input:      3500,
		Output:     600,
		Reasoning:  50,
		CacheRead:  22000,
		CacheWrite: 100,
		Total:      26250,
	}
	if s.Tokens != wantTokens {
		t.Errorf("Summary.Tokens = %+v, want %+v", s.Tokens, wantTokens)
	}
	if c.Tokens == nil {
		t.Fatal("Compact.Tokens = nil, want non-nil")
	}
	if *c.Tokens != wantTokens {
		t.Errorf("Compact.Tokens = %+v, want %+v", *c.Tokens, wantTokens)
	}

	if s.MaxContext != 10500 {
		t.Errorf("Summary.MaxContext = %d, want 10500", s.MaxContext)
	}
	if c.MaxContext != 10500 {
		t.Errorf("Compact.MaxContext = %d, want 10500", c.MaxContext)
	}

	if len(s.Sessions) != 1 {
		t.Fatalf("len(Summary.Sessions) = %d, want 1", len(s.Sessions))
	}
	ses := s.Sessions[0]
	if ses.SessionID != "ses_c" {
		t.Errorf("Summary.Sessions[0].SessionID = %q, want %q", ses.SessionID, "ses_c")
	}
	if ses.ModelCalls != 3 {
		t.Errorf("Summary.Sessions[0].ModelCalls = %d, want 3", ses.ModelCalls)
	}
	if ses.ToolCalls != 4 {
		t.Errorf("Summary.Sessions[0].ToolCalls = %d, want 4", ses.ToolCalls)
	}
	if ses.MaxContext != 10500 {
		t.Errorf("Summary.Sessions[0].MaxContext = %d, want 10500", ses.MaxContext)
	}
	if ses.DurationMillis != 65500 {
		t.Errorf("Summary.Sessions[0].DurationMillis = %d, want 65500", ses.DurationMillis)
	}
	if ses.Tokens != wantTokens {
		t.Errorf("Summary.Sessions[0].Tokens = %+v, want %+v", ses.Tokens, wantTokens)
	}

	if c.Sessions != 3 {
		t.Errorf("Compact.Sessions = %d, want 3", c.Sessions)
	}

	wantSubagents := []courierv1alpha1.SubagentTally{
		{Agent: "coder-local", Model: "litellm/qwen", Calls: 2, Errors: 1, RuntimeMillis: 60500},
	}
	if len(s.Subagents) != 1 {
		t.Fatalf("len(Summary.Subagents) = %d, want 1", len(s.Subagents))
	}
	if s.Subagents[0] != wantSubagents[0] {
		t.Errorf("Summary.Subagents[0] = %+v, want %+v", s.Subagents[0], wantSubagents[0])
	}
	if len(c.Subagents) != 1 {
		t.Fatalf("len(Compact.Subagents) = %d, want 1", len(c.Subagents))
	}
	if c.Subagents[0] != wantSubagents[0] {
		t.Errorf("Compact.Subagents[0] = %+v, want %+v", c.Subagents[0], wantSubagents[0])
	}

	if s.DurationMillis != 65500 {
		t.Errorf("Summary.DurationMillis = %d, want 65500", s.DurationMillis)
	}
	if c.DurationMillis != 65500 {
		t.Errorf("Compact.DurationMillis = %d, want 65500", c.DurationMillis)
	}
	if s.Continuations != 2 {
		t.Errorf("Summary.Continuations = %d, want 2", s.Continuations)
	}
	if c.Continuations != 2 {
		t.Errorf("Compact.Continuations = %d, want 2", c.Continuations)
	}
}

func TestNewTrackerIsEmpty(t *testing.T) {
	tr := New()
	if c := tr.Compact(); c != nil {
		t.Errorf("Compact() = %+v, want nil", c)
	}
	s := tr.Summary()
	if s.ModelCalls != 0 {
		t.Errorf("Summary.ModelCalls = %d, want 0", s.ModelCalls)
	}
	if s.ToolCalls != 0 {
		t.Errorf("Summary.ToolCalls = %d, want 0", s.ToolCalls)
	}
}

func TestNonPartJSONIsIgnored(t *testing.T) {
	tr := observeAll(t, `{"hello":"world"}`+"\n")
	s := tr.Summary()
	if s.ModelCalls != 0 {
		t.Errorf("Summary.ModelCalls = %d, want 0", s.ModelCalls)
	}
	if s.ToolCalls != 0 {
		t.Errorf("Summary.ToolCalls = %d, want 0", s.ToolCalls)
	}
	if c := tr.Compact(); c != nil {
		t.Errorf("Compact() = %+v, want nil", c)
	}
}

func TestModelStepWithoutCache(t *testing.T) {
	tr := observeAll(t, `{"sessionID":"s","part":{"type":"step-finish","tokens":{"input":10,"output":5,"reasoning":0}}}`+"\n")
	s := tr.Summary()
	if s.Tokens.CacheRead != 0 || s.Tokens.CacheWrite != 0 {
		t.Errorf("Summary.Tokens = %+v, want CacheRead/CacheWrite 0", s.Tokens)
	}
	if s.Tokens.Total != 15 {
		t.Errorf("Summary.Tokens.Total = %d, want 15", s.Tokens.Total)
	}
	if s.MaxContext != 10 {
		t.Errorf("Summary.MaxContext = %d, want 10", s.MaxContext)
	}
}

func TestTaskWithoutMetadata(t *testing.T) {
	tr := observeAll(t, `{"sessionID":"s","part":{"type":"tool","tool":"task","state":{"status":"completed","time":{"start":100,"end":250}}}}`+"\n")
	s := tr.Summary()
	if s.ToolCalls != 1 {
		t.Errorf("Summary.ToolCalls = %d, want 1", s.ToolCalls)
	}
	want := []courierv1alpha1.SubagentTally{
		{Agent: "", Model: "", Calls: 1, Errors: 0, RuntimeMillis: 150},
	}
	if len(s.Subagents) != 1 {
		t.Fatalf("len(Summary.Subagents) = %d, want 1", len(s.Subagents))
	}
	if s.Subagents[0] != want[0] {
		t.Errorf("Summary.Subagents[0] = %+v, want %+v", s.Subagents[0], want[0])
	}
	c := tr.Compact()
	if c == nil {
		t.Fatal("Compact() = nil, want non-nil")
	}
	if c.Sessions != 1 {
		t.Errorf("Compact.Sessions = %d, want 1", c.Sessions)
	}
}

func TestNullPartIsIgnored(t *testing.T) {
	tr := observeAll(t, `{"sessionID":"s","part":null}`+"\n")
	if c := tr.Compact(); c != nil {
		t.Errorf("Compact() = %+v, want nil", c)
	}
}
