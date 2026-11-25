package console

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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
	maxContributeBodyBytes   = 32 * 1024 // Stay below the admission server's 64 KiB request-body cap.
	contributeErrorFallback  = "the admission API refused the contribution"
	contributeIntentCookie   = "courier_console_contribute_intent"
	contributeIntentTTL      = 2 * time.Minute
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
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(base, "#") {
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
	case status == http.StatusRequestEntityTooLarge:
		return contributeResult{Outcome: outcomeRefused, Message: "the admission API rejected the submission body as too large"}
	case status == http.StatusUnsupportedMediaType:
		return contributeResult{Outcome: outcomeRefused, Message: "the admission API rejected the request media type"}
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
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return false
	}
	return errors.Is(opErr.Err, syscall.ECONNREFUSED) || errors.Is(opErr.Err, syscall.EHOSTUNREACH) || errors.Is(opErr.Err, syscall.ENETUNREACH)
}

type contributeMinterEntry struct {
	key     string
	expires time.Time
}

type contributeMinter struct {
	mu      sync.Mutex
	entries map[string]contributeMinterEntry
}

// contributeMinterSlot scopes a retained key to the operator session that
// owns the attempt. Content identity alone must never share a key across
// sessions: an identical payload arriving from another session — with no
// intent cookie of its own — is an independent submission and must mint its
// own key, never resume the first session's admitted run. Within one session
// the slot ties this attempt's retries and identical resubmissions to one
// key; the explicit new-attempt action frees it.
func contributeMinterSlot(sessionID, digest string) string {
	return sessionID + "\x00" + digest
}

func (m *contributeMinter) forget(sessionID, digest string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, contributeMinterSlot(sessionID, digest))
}

// remember retains the session-and-digest-to-key mapping past the two-minute
// in-flight TTL, for the rest of the session: a definitive outcome means the
// admission server has bound this key to this content, so an identical
// resubmission in this session after the browser's settled cookie was
// overwritten must replay the same key and let the server deduplicate —
// never mint a new key that could admit a duplicate run/branch/PR. The
// mapping lives only in the submitting session's slot, so it never describes
// another session's attempt, and this session's explicit new-attempt action
// frees the slot settled or not, making a deliberately new attempt
// distinguishable from a retry of a settled one.
func (m *contributeMinter) remember(sessionID, digest, key string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[string]contributeMinterEntry)
	}
	for held, entry := range m.entries {
		if !now.Before(entry.expires) {
			delete(m.entries, held)
		}
	}
	if len(m.entries) >= contributeMinterCap {
		oldest, oldestExpires := "", time.Time{}
		for held, entry := range m.entries {
			if oldestExpires.IsZero() || entry.expires.Before(oldestExpires) {
				oldest, oldestExpires = held, entry.expires
			}
		}
		delete(m.entries, oldest)
	}
	m.entries[contributeMinterSlot(sessionID, digest)] = contributeMinterEntry{key: key, expires: now.Add(sessionTTL)}
}

// contributeSettled records attempt keys whose most recent submission
// reached a definitive outcome, keyed by key — never by content digest, so
// one session's settlement can never describe another session's live
// attempt. The submit response has already flushed its headers by then, so
// it cannot mark the browser's retained intent cookie settled; a later page
// view uses this registry to sign the settled flag and drop the stale
// unsettled-attempt notice instead.
type contributeSettled struct {
	mu   sync.Mutex
	keys map[string]time.Time
}

const contributeSettledCap = 4096

// contributeMinterCap bounds retained session-scoped slots: definitive
// outcomes extend a slot's life from the two-minute in-flight TTL to one
// session TTL, so without a cap steady-state size would track every distinct
// submission of every live session for up to 12 hours. Eviction can only
// cost a session its local replay convenience — the browser's signed cookie
// stays the key's durable home — and never shares a key across sessions.
const contributeMinterCap = 4096

func (r *contributeSettled) mark(key string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.keys == nil {
		r.keys = make(map[string]time.Time)
	}
	for held, expires := range r.keys {
		if !now.Before(expires) {
			delete(r.keys, held)
		}
	}
	if len(r.keys) >= contributeSettledCap {
		// Evict the entry closest to its expiry, so the victim is the one
		// the registry would drop soonest anyway.
		oldest, oldestExpires := "", time.Time{}
		for held, expires := range r.keys {
			if oldestExpires.IsZero() || expires.Before(oldestExpires) {
				oldest, oldestExpires = held, expires
			}
		}
		delete(r.keys, oldest)
	}
	r.keys[key] = now.Add(sessionTTL)
}

func (r *contributeSettled) clear(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keys, key)
}

func (r *contributeSettled) contains(key string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	expires, ok := r.keys[key]
	if !ok {
		return false
	}
	if !now.Before(expires) {
		delete(r.keys, key)
		return false
	}
	return true
}

func (m *contributeMinter) keyFor(sessionID, digest string, now time.Time) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[string]contributeMinterEntry)
	}
	slot := contributeMinterSlot(sessionID, digest)
	for held, entry := range m.entries {
		if !now.Before(entry.expires) {
			delete(m.entries, held)
		}
	}
	if entry, ok := m.entries[slot]; ok {
		return entry.key, nil
	}
	key, err := mintContributeKey()
	if err != nil {
		return "", err
	}
	m.entries[slot] = contributeMinterEntry{key: key, expires: now.Add(contributeIntentTTL)}
	return key, nil
}

func contributeDigest(sub contributeSubmission) string {
	// Length-prefix every field: a bare separator would let a field
	// carrying the separator byte collide two different submissions.
	fields := []string{sub.Lane, sub.BaseRepo, sub.BaseRef, sub.WorkRepo, sub.Goal, sub.UpstreamIssue}
	var canonical bytes.Buffer
	for _, field := range fields {
		fmt.Fprintf(&canonical, "%d\x00%s\x00", len(field), field)
	}
	sum := sha256.Sum256(canonical.Bytes())
	return hex.EncodeToString(sum[:])[:16]
}

func contributeIntentValue(key, digest string, settled bool, sessionKey []byte) string {
	flag := "0"
	if settled {
		flag = "1"
	}
	prefix := key + "." + digest + "." + flag
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write([]byte("contribute-intent:" + prefix))
	return prefix + "." + hex.EncodeToString(mac.Sum(nil))
}

func verifyContributeIntent(value string, sessionKey []byte) (key, digest string, settled, ok bool) {
	if len(sessionKey) == 0 {
		return "", "", false, false
	}
	parts := strings.Split(value, ".")
	if len(parts) != 4 || !strings.HasPrefix(parts[0], "adhoc-console-") || len(parts[0]) != len("adhoc-console-")+16 || len(parts[1]) != 16 || (parts[2] != "0" && parts[2] != "1") || parts[3] == "" {
		return "", "", false, false
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(parts[0], "adhoc-console-")); err != nil {
		return "", "", false, false
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return "", "", false, false
	}
	flag := parts[2] == "1"
	expected := contributeIntentValue(parts[0], parts[1], flag, sessionKey)
	_, expectedSig, _ := strings.Cut(expected, parts[0]+"."+parts[1]+"."+parts[2]+".")
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(parts[3])) != 1 {
		return "", "", false, false
	}
	return parts[0], parts[1], flag, true
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
	// RefreshSeconds stays zero: the layout's meta-refresh only applies to
	// the overview page, and a contribution form must never reload itself.
	RefreshSeconds int
	State          string
	StateClass     string
	Message        string
	Values         contributeSubmission
	Key            string
	HasIntent      bool
	SettledIntent  bool
	Run            string
	Namespace      string
	WorkItemID     string
	Resumed        bool
	// StatusLink points the admitted run's contribution page. It is set
	// only when the status backend is configured and the admission response
	// named a run; the namespace is percent-escaped into the query, pinned
	// from the response the console already holds (the status DTO
	// deliberately never carries the client submission key, so correlation
	// stays on the admission side). Empty renders no link, never a guess.
	StatusLink string
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
		if cookie, err := r.Cookie(contributeIntentCookie); err == nil {
			if key, _, cookieSettled, valid := verifyContributeIntent(cookie.Value, s.auth.key); valid {
				if cookieSettled || s.settled.contains(key, s.auth.now()) {
					data.SettledIntent = true
				} else {
					data.Key, data.HasIntent = key, true
				}
			}
		}
	}
	return data
}

func (a *Authenticator) contributeIntentCookie(value string) *http.Cookie {
	cookie := a.sessionCookie()
	cookie.Name = contributeIntentCookie
	cookie.Value = value
	cookie.MaxAge = int(sessionTTL.Seconds())
	cookie.Expires = a.now().Add(sessionTTL)
	return cookie
}

func (a *Authenticator) expiredContributeIntentCookie() *http.Cookie {
	cookie := a.contributeIntentCookie("")
	cookie.Expires = a.now().Add(-time.Hour)
	cookie.MaxAge = -1
	return cookie
}

func (s *Server) handleContribute(w http.ResponseWriter, r *http.Request) {
	sessionID, _ := s.auth.sessionAndIdentity(r)
	data := s.contributePage(r, sessionID)
	if data.SettledIntent {
		if cookie, err := r.Cookie(contributeIntentCookie); err == nil {
			if key, digest, cookieSettled, valid := verifyContributeIntent(cookie.Value, s.auth.key); valid && !cookieSettled {
				// The definitive submit response had already flushed its headers,
				// so the settled flag makes the key's durable home the browser even
				// after this page view or a console restart.
				http.SetCookie(w, s.auth.contributeIntentCookie(contributeIntentValue(key, digest, true, s.auth.key)))
			}
		}
	}
	s.render(w, http.StatusOK, "contribute", data)
}

func (s *Server) handleContributeSubmit(w http.ResponseWriter, r *http.Request) {
	sessionID, _ := s.auth.sessionAndIdentity(r)
	data := s.contributePage(r, sessionID)
	data.HasIntent = false
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
	digest := contributeDigest(sub)
	key := ""
	// storedDigest records which attempt the browser's intent cookie holds,
	// so a definitive settlement clears only its own attempt and never an
	// unrelated unsettled one from changed content.
	storedDigest := ""
	storedKey := ""
	storedSettled := false
	if cookie, err := r.Cookie(contributeIntentCookie); err == nil {
		if cookieKey, candidateDigest, cookieSettled, valid := verifyContributeIntent(cookie.Value, s.auth.key); valid {
			storedDigest = candidateDigest
			storedKey = cookieKey
			storedSettled = cookieSettled || s.settled.contains(cookieKey, s.auth.now())
			if storedDigest == digest {
				key = cookieKey
			}
		}
	}
	if storedDigest != "" && storedDigest != digest && !storedSettled {
		// The browser's only durable key slot holds a different still-unsettled
		// attempt. Submitting this content would mint a key that lives nowhere
		// durable before the admission side effect: a lost uncertain response or
		// a console restart would orphan it, and an identical resubmission could
		// then duplicate an admitted run. Refuse before minting or contacting
		// the admission API; the operator resubmits unchanged or abandons the
		// earlier attempt explicitly.
		data.State, data.StateClass, data.Message = "blocked", "state-warn", "this browser still has an unsettled attempt (idempotency key "+storedKey+"); resubmit it unchanged to retry, or use Start a new contribution to abandon it before submitting different content. Abandoning discards its key: resubmitting that content later can duplicate an admitted run"
		s.render(w, http.StatusOK, "contribute", data)
		return
	}
	if key == "" {
		var err error
		key, err = s.minter.keyFor(sessionID, digest, s.auth.now())
		if err != nil {
			http.Error(w, "could not prepare contribution", http.StatusInternalServerError)
			return
		}
	}
	sub.IdempotencyKey = key
	body, err := json.Marshal(sub)
	if err != nil {
		http.Error(w, "could not prepare contribution", http.StatusInternalServerError)
		return
	}
	if len(body) > maxContributeBodyBytes {
		data.State, data.StateClass, data.Message = "refused", "state-error", "the submission is too large after JSON encoding for the admission API's size cap; shorten the goal or other fields"
		s.render(w, http.StatusOK, "contribute", data)
		return
	}
	// Deliver the signed intent before the admission side effect: commit and
	// flush the response headers so a lost response or console restart cannot
	// leave the attempt's only key in the two-minute minter, which would let
	// an identical resubmission mint a new key and duplicate an admitted run.
	// Every submission that reaches this point may carry the cookie: the
	// blocked case above — a different still-unsettled attempt owning the
	// browser's only durable key slot — returns before minting or contacting
	// the admission API.
	http.SetCookie(w, s.auth.contributeIntentCookie(contributeIntentValue(key, digest, false, s.auth.key)))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(w).Flush()
	result := s.contribute.Submit(r.Context(), sub)
	switch result.Outcome {
	case outcomeCreated, outcomeResumed:
		s.minter.remember(sessionID, digest, key, s.auth.now())
		s.settled.mark(key, s.auth.now())
		data.State, data.StateClass = "success", "state-ok"
		data.Run, data.Namespace, data.WorkItemID = result.Run, result.Namespace, result.WorkItemID
		data.Resumed = result.Outcome == outcomeResumed
		// The run the admission response named is reachable on the status
		// page from here, with the response's namespace pinned — but only
		// values the console's own request validators accept, so the link
		// never ships a destination the status page would itself reject. A
		// blank or hostile namespace drops the pin, not the link: the
		// status page then carries the endpoint's default and pins the
		// canonical echo.
		if s.contributions != nil && validContributeRunName(result.Run) {
			data.StatusLink = "/contributions/" + url.PathEscape(result.Run)
			if result.Namespace != "" && validContributeNamespace(result.Namespace) {
				data.StatusLink += "?namespace=" + url.QueryEscape(result.Namespace)
			}
		}
	case outcomeRefused:
		s.minter.remember(sessionID, digest, key, s.auth.now())
		s.settled.mark(key, s.auth.now())
		data.State, data.StateClass, data.Message = "refused", "state-error", result.Message
	case outcomeCredentialDenied:
		s.minter.remember(sessionID, digest, key, s.auth.now())
		s.settled.mark(key, s.auth.now())
		data.State, data.StateClass, data.Message = "credentialDenied", "state-error", result.Message
	case outcomeAPIMismatch:
		s.minter.remember(sessionID, digest, key, s.auth.now())
		s.settled.mark(key, s.auth.now())
		data.State, data.StateClass, data.Message = "apiMismatch", "state-error", result.Message
	case outcomeUnreachable:
		s.minter.remember(sessionID, digest, key, s.auth.now())
		s.settled.mark(key, s.auth.now())
		data.State, data.StateClass, data.Message = "unreachable", "state-error", "nothing was admitted; the contribution API could not be reached"
	default:
		// An uncertain attempt is not settled: an earlier settlement record
		// for a replayed key no longer describes it, and a later page view
		// must keep the retry key, not clear it.
		s.settled.clear(key)
		data.State, data.StateClass, data.Message = "uncertain", "state-error", "may have been admitted — resubmitting retries the same idempotency key ("+key+"). Use Start a new contribution to abandon this attempt before submitting different content; the abandoned attempt may still have been admitted."
	}
	s.renderBody(w, "contribute", data)
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
	data.HasIntent = false
	if data.State != "form" {
		s.render(w, http.StatusOK, "contribute", data)
		return
	}
	if !csrfVerify(s.auth.key, sessionID, r.PostFormValue("csrf")) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if cookie, err := r.Cookie(contributeIntentCookie); err == nil {
		if key, digest, _, valid := verifyContributeIntent(cookie.Value, s.auth.key); valid {
			// "Start a new contribution" is this session's explicit
			// new-attempt action: it frees the session's key slot whether
			// the attempt is settled or not, so resubmitting identical
			// content afterwards mints a fresh key and requests genuinely
			// new work. Retrying a settled attempt needs no action — the
			// retained cookie replays its key — which is what makes the
			// deliberate new attempt distinguishable from a retry. An
			// abandoned attempt that had been admitted keeps its run: the
			// action ends this browser's attempt, never the server-side
			// work.
			s.minter.forget(sessionID, digest)
			s.settled.clear(key)
		}
	}
	http.SetCookie(w, s.auth.expiredContributeIntentCookie())
	http.Redirect(w, r, "/contribute", http.StatusSeeOther)
}
