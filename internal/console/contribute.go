package console

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultContributeTimeout = 20 * time.Second
	maxContributeResponse    = 64 * 1024
	contributeErrorFallback  = "the admission API refused the contribution"
)

type contributeSubmission struct {
	Lane          string `json:"lane"`
	BaseRepo      string `json:"baseRepo"`
	BaseRef       string `json:"baseRef"`
	WorkRepo      string `json:"workRepo"`
	Goal          string `json:"goal"`
	UpstreamIssue string `json:"upstreamIssue"`
	// IdempotencyKey is the submission-intent key the handler mints once
	// per attempt and replays across retries. It rides the submission so
	// the client can never silently send an empty key: the server would
	// then mint its own digest key, and a later retry under the stored
	// key could duplicate the run the first attempt created.
	IdempotencyKey string `json:"idempotencyKey"`
}

type contributeOutcome string

const (
	outcomeCreated          contributeOutcome = "created"
	outcomeResumed          contributeOutcome = "resumed"
	outcomeRefused          contributeOutcome = "refused"
	outcomeCredentialDenied contributeOutcome = "credentialDenied"
	outcomeAPIMismatch      contributeOutcome = "apiMismatch"
	outcomeUnreachable      contributeOutcome = "unreachable"
	outcomeUncertain        contributeOutcome = "uncertain"
)

type contributeResult struct {
	Outcome    contributeOutcome
	Run        string
	Namespace  string
	WorkItemID string
	Message    string
}

type contributeClient interface {
	Submit(ctx context.Context, sub contributeSubmission) contributeResult
}

type httpContributeClient struct {
	url     string
	token   []byte
	timeout time.Duration
	client  *http.Client
}

func newHTTPContributeClient(base string, token []byte, timeout time.Duration) (*httpContributeClient, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(base, "#") {
		return nil, errors.New("console: invalid contribution admission URL")
	}
	if timeout <= 0 {
		timeout = defaultContributeTimeout
	}
	return &httpContributeClient{
		url: strings.TrimRight(base, "/"), token: append([]byte(nil), token...), timeout: timeout,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func (c *httpContributeClient) Submit(ctx context.Context, sub contributeSubmission) contributeResult {
	body, err := json.Marshal(sub)
	if err != nil {
		return contributeResult{Outcome: outcomeUncertain}
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.url+"/v1/contributions", bytes.NewReader(body))
	if err != nil {
		return contributeResult{Outcome: outcomeUncertain}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+string(c.token))
	resp, err := c.client.Do(req)
	if err != nil {
		return classifyContributeError(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxContributeResponse))
	if err != nil {
		return contributeResult{Outcome: outcomeUncertain, Message: "may have been admitted"}
	}
	return classifyContributeResponse(resp.StatusCode, data)
}

func classifyContributeResponse(status int, body []byte) contributeResult {
	decode := func(dst any) bool { return json.Unmarshal(body, dst) == nil }
	switch {
	case status == http.StatusCreated || status == http.StatusOK:
		var result struct {
			Run        string `json:"run"`
			Namespace  string `json:"namespace"`
			WorkItemID string `json:"workItemId"`
		}
		if !decode(&result) {
			return contributeResult{Outcome: outcomeUncertain}
		}
		outcome := outcomeCreated
		if status == http.StatusOK {
			outcome = outcomeResumed
		}
		return contributeResult{Outcome: outcome, Run: result.Run, Namespace: result.Namespace, WorkItemID: result.WorkItemID}
	case status == http.StatusBadRequest || status == http.StatusForbidden || status == http.StatusConflict:
		var result struct {
			Error string `json:"error"`
		}
		message := contributeErrorFallback
		if decode(&result) && result.Error != "" {
			message = result.Error
		}
		return contributeResult{Outcome: outcomeRefused, Message: message}
	case status == http.StatusUnauthorized:
		return contributeResult{Outcome: outcomeCredentialDenied, Message: "the console's contribution credential was not accepted by the admission API"}
	case status >= 400 && status < 500:
		return contributeResult{Outcome: outcomeAPIMismatch, Message: "the configured contribution API URL does not serve the expected admission API"}
	default:
		return contributeResult{Outcome: outcomeUncertain, Message: "may have been admitted"}
	}
}

func classifyContributeError(err error) contributeResult {
	if provablyUnreached(err) {
		return contributeResult{Outcome: outcomeUnreachable}
	}
	return contributeResult{Outcome: outcomeUncertain, Message: "may have been admitted"}
}

func provablyUnreached(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}

type intentEntry struct {
	key     string
	expires time.Time
}
type intentStore struct {
	mu      sync.Mutex
	entries map[string]intentEntry
}

func (s *intentStore) set(sessionID, key string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]intentEntry)
	}
	for id, entry := range s.entries {
		if now.After(entry.expires) {
			delete(s.entries, id)
		}
	}
	s.entries[sessionID] = intentEntry{key: key, expires: now.Add(sessionTTL)}
}

func (s *intentStore) get(sessionID string, now time.Time) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[sessionID]
	if !ok {
		return "", false
	}
	if now.After(entry.expires) {
		delete(s.entries, sessionID)
		return "", false
	}
	return entry.key, true
}

func (s *intentStore) clear(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, sessionID)
}

func mintContributeKey() (string, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("mint contribution key: %w", err)
	}
	return "adhoc-console-" + hex.EncodeToString(nonce), nil
}

type contributeData struct {
	Title         string
	Active        string
	Authenticated bool
	CSRF          string
	State         string
	StateClass    string
	Message       string
	Values        contributeSubmission
	Key           string
	HasIntent     bool
	Run           string
	Namespace     string
	WorkItemID    string
	Resumed       bool
}

func (s *Server) contributePage(r *http.Request, sessionID string) contributeData {
	common := s.pageData(r, "Contribute", "contribute")
	data := contributeData{Title: common.Title, Active: common.Active, Authenticated: common.Authenticated, CSRF: common.CSRF}
	switch {
	case s.auth.mode == AuthModeHeader:
		data.State = "requires token-mode sign-in"
	case s.contribute == nil:
		data.State = "not configured"
	default:
		data.State = "form"
		data.Key, data.HasIntent = s.intents.get(sessionID, s.auth.now())
	}
	return data
}

func (s *Server) handleContribute(w http.ResponseWriter, r *http.Request) {
	sessionID, _ := s.auth.sessionAndIdentity(r)
	data := s.contributePage(r, sessionID)
	s.render(w, http.StatusOK, "contribute", data)
}

func (s *Server) handleContributeSubmit(w http.ResponseWriter, r *http.Request) {
	sessionID, _ := s.auth.sessionAndIdentity(r)
	data := s.contributePage(r, sessionID)
	if data.State != "form" {
		s.render(w, http.StatusOK, "contribute", data)
		return
	}
	if !csrfVerify(s.auth.key, sessionID, r.PostFormValue("csrf")) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	sub := contributeSubmission{
		Lane: r.PostForm.Get("lane"), BaseRepo: r.PostForm.Get("baseRepo"), BaseRef: r.PostForm.Get("baseRef"),
		WorkRepo: r.PostForm.Get("workRepo"), Goal: r.PostForm.Get("goal"), UpstreamIssue: r.PostForm.Get("upstreamIssue"),
	}
	data.Values = sub
	if message := validateContributeSubmission(sub); message != "" {
		data.State, data.StateClass, data.Message = "refused", "state-error", message
		s.render(w, http.StatusOK, "contribute", data)
		return
	}
	key, ok := s.intents.get(sessionID, s.auth.now())
	if !ok {
		var err error
		key, err = mintContributeKey()
		if err != nil {
			http.Error(w, "could not prepare contribution", http.StatusInternalServerError)
			return
		}
		s.intents.set(sessionID, key, s.auth.now())
	}
	sub.IdempotencyKey = key
	result := s.contribute.Submit(r.Context(), sub)
	data.HasIntent = false
	switch result.Outcome {
	case outcomeCreated, outcomeResumed:
		s.intents.clear(sessionID)
		data.State, data.StateClass = "success", "state-ok"
		data.Run, data.Namespace, data.WorkItemID = result.Run, result.Namespace, result.WorkItemID
		data.Resumed = result.Outcome == outcomeResumed
	case outcomeRefused:
		s.intents.clear(sessionID)
		data.State, data.StateClass, data.Message = "refused", "state-error", result.Message
	case outcomeCredentialDenied:
		s.intents.clear(sessionID)
		data.State, data.StateClass, data.Message = "credentialDenied", "state-error", result.Message
	case outcomeAPIMismatch:
		s.intents.clear(sessionID)
		data.State, data.StateClass, data.Message = "apiMismatch", "state-error", result.Message
	case outcomeUnreachable:
		s.intents.clear(sessionID)
		data.State, data.StateClass, data.Message = "unreachable", "state-error", "nothing was admitted; the contribution API could not be reached"
	default:
		// Uncertain keeps the stored intent: the POST response's message
		// already names the key, and the next GET shows the unsettled
		// notice through the fresh contributePage computation.
		data.State, data.StateClass, data.Message = "uncertain", "state-error", "may have been admitted — resubmitting retries the same idempotency key ("+sub.IdempotencyKey+"), so no duplicate run can be created"
	}
	s.render(w, http.StatusOK, "contribute", data)
}

func validateContributeSubmission(sub contributeSubmission) string {
	for _, field := range []struct{ name, value string }{{"lane", sub.Lane}, {"baseRepo", sub.BaseRepo}, {"baseRef", sub.BaseRef}, {"workRepo", sub.WorkRepo}, {"goal", sub.Goal}} {
		if strings.TrimSpace(field.value) == "" {
			return field.name + " is required"
		}
	}
	for _, field := range []struct {
		name, value string
		limit       int
	}{
		{"goal", sub.Goal, 16384}, {"baseRef", sub.BaseRef, 255}, {"lane", sub.Lane, 512},
		{"baseRepo", sub.BaseRepo, 512}, {"workRepo", sub.WorkRepo, 512}, {"upstreamIssue", sub.UpstreamIssue, 2048},
	} {
		if len(field.value) > field.limit {
			return fmt.Sprintf("%s exceeds %d bytes", field.name, field.limit)
		}
	}
	return ""
}

func (s *Server) handleContributeNew(w http.ResponseWriter, r *http.Request) {
	sessionID, _ := s.auth.sessionAndIdentity(r)
	data := s.contributePage(r, sessionID)
	if data.State != "form" {
		s.render(w, http.StatusOK, "contribute", data)
		return
	}
	if !csrfVerify(s.auth.key, sessionID, r.PostFormValue("csrf")) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	s.intents.clear(sessionID)
	http.Redirect(w, r, "/contribute", http.StatusSeeOther)
}
