package adhoc

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"
)

// PathContributions is the admission route the server serves; per-run
// status reads hang off the same prefix (status.go). It is the HTTP twin
// of the courierctl contribute subcommand: the same adapter, the same
// server-side validation, a thinner frontend
// (#708, docs/external-contributions.md, "Admission surface").
const PathContributions = "/v1/contributions"

const (
	// maxAdmissionBodyBytes bounds the JSON admission body. An admission is
	// a handful of short strings (a goal is prose, not a document), so a
	// body anywhere near this cap is a client bug or an attack, and it is
	// rejected before any parse or cluster contact.
	maxAdmissionBodyBytes = 64 * 1024

	// admissionTimeout bounds one admission's cluster round trips, the same
	// bound the CLI applies (cmd/courierctl defaultAdmissionTimeout): a
	// stalled API server that accepts the connection but never answers
	// maps to a 503 instead of holding the connection open forever.
	admissionTimeout = 30 * time.Second

	// routeNotFoundMessage answers a miss on a server whose only surface
	// is admission; a server without a read token must not announce the
	// status route it will never serve.
	routeNotFoundMessage = "contribution admission serves " + PathContributions + " only"

	// routeNotFoundMessageWithReads answers a miss when status reads are
	// configured and therefore genuinely part of the surface.
	routeNotFoundMessageWithReads = "contribution API serves " + PathContributions +
		" and GET " + statusReadPrefix + "<run> only"

	// httpStatus constants for the fixed messages below.
	unauthorizedMessage  = "contribution admission requires a valid bearer token"
	timeoutMessage       = "contribution admission timed out before the API server answered"
	internalErrorMessage = "contribution admission failed"
)

// errBodyTooLarge is the decode-stage size sentinel: the body outran the
// MaxBytesReader mid-read (a chunked upload that lied about its
// ContentLength), which maps to 413 like a declared overflow, not to the
// 400 a malformed body gets.
var errBodyTooLarge = errors.New("contribution admission request exceeds the size cap")

// Admitter is the admission verb the server fronts. Adapter satisfies it;
// the indirection keeps the server testable against a fake without a
// Kubernetes client.
type Admitter interface {
	Admit(ctx context.Context, in Admission) (Admitted, error)
}

// AdmissionServerConfig is the construction input for an AdmissionServer.
// Every field except the namespace defaults is required; the constructor
// fails closed on a missing one so a wiring mistake surfaces at startup,
// never as a listener that authenticates nothing.
type AdmissionServerConfig struct {
	// Admitter performs the actual admission. Required.
	Admitter Admitter
	// Token is the static bearer token frontends present. Required; it is
	// compared in constant time and never logged or echoed.
	Token string
	// Bind is the listener address (e.g. ":8083"). Required.
	Bind string
	// DefaultNamespace is applied when a request omits its namespace.
	// Empty leaves resolution to the adapter, which rejects a namespace
	// that is still empty.
	DefaultNamespace string
	// OwnedNamespaces is the deployment-ownership boundary (#632): the
	// namespaces this release may admit runs into. Non-empty restricts
	// admission to exactly these namespaces; empty means every namespace,
	// the single-controller default. It scopes status reads too.
	OwnedNamespaces []string
	// Status and ReadToken wire the read-only lifecycle surface (#819).
	// They come as a pair: a reader without its own credential would
	// invite a later wiring mistake that authenticates reads with the
	// admission token, and a token without a reader serves nothing. Set
	// both to serve GET /v1/contributions/<run>; leave both empty to
	// leave reads off.
	Status    StatusReader
	ReadToken string
}

// AdmissionServer serves the HTTP admission API for ad-hoc contributions
// (#708). It is a thin frontend over the adapter's Admit: it owns
// transport concerns only — authentication, content negotiation, body
// bounds, and error mapping — and performs no authority validation of its
// own, because the adapter and the controller admission path own all of it
// (docs/external-contributions.md, "Admission surface"). The response
// carries run identity only; the goal, issue text, and token never appear
// in it. When a read token and reader are configured, the same listener
// also serves the read-only contribution status endpoint (#819) under an
// independent credential; see status.go.
type AdmissionServer struct {
	admitter         Admitter
	token            string
	bind             string
	defaultNamespace string
	ownedNamespaces  []string
	statusReader     StatusReader
	readToken        string
	// statusSlots is the counting semaphore behind the status read
	// concurrency bound: a token per in-flight read, sized at
	// construction so the handler never allocates per request.
	statusSlots chan struct{}
}

// NewAdmissionServer builds the server, failing closed when a required
// input is missing: a server without an admitter, a token, or a bind
// address would either do nothing or authenticate nothing, and that must
// be a startup error rather than a runtime surprise. The status reader and
// its read token are optional, but only together: a half-configured read
// surface is the same class of wiring mistake.
func NewAdmissionServer(config AdmissionServerConfig) (*AdmissionServer, error) {
	switch {
	case config.Admitter == nil:
		return nil, errors.New("adhoc: admission server requires an admitter")
	case config.Token == "":
		return nil, errors.New("adhoc: admission server requires a bearer token")
	case config.Bind == "":
		return nil, errors.New("adhoc: admission server requires a bind address")
	case (config.Status == nil) != (config.ReadToken == ""):
		return nil, errors.New("adhoc: admission server status reads require both a status reader and a read token")
	}
	return &AdmissionServer{
		admitter:         config.Admitter,
		token:            config.Token,
		bind:             config.Bind,
		defaultNamespace: config.DefaultNamespace,
		ownedNamespaces:  config.OwnedNamespaces,
		statusReader:     config.Status,
		readToken:        config.ReadToken,
		statusSlots:      make(chan struct{}, maxConcurrentStatusReads),
	}, nil
}

// NeedLeaderElection reports that the server must run on every replica.
// Like the evidence intake, it is a network listener, not a reconciler: it
// has no per-resource bookkeeping to serialize, and a leader-election-gated
// instance would fail to bind its port, leaving a hole the other replicas'
// listeners (all running) would have covered. Duplicate submissions stay
// idempotent at the adapter, not here.
func (s *AdmissionServer) NeedLeaderElection() bool { return false }

// Start binds the listener and serves the admission API until the context
// is done, then shuts down gracefully. The shape mirrors the evidence
// intake's Start. The bind address is never empty (the constructor
// requires it), so there is no disabled mode to short-circuit.
func (s *AdmissionServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.bind)
	if err != nil {
		return fmt.Errorf("adhoc: admission server listening on %s: %w", s.bind, err)
	}
	srv := &http.Server{
		// No request wall-clock timeout: the handler bounds each admission
		// with its own deadline. ReadHeaderTimeout and ReadTimeout bound the
		// connection's read half; IdleTimeout reclaims idle keep-alives.
		// The write timeout exceeds the admission deadline by a margin, so
		// the 503 (or any near-deadline admission) still gets written: in
		// net/http the write deadline starts when the handler begins, and a
		// window equal to the admission deadline would expire exactly when
		// the deadline-path response is due, dropping the connection instead
		// of answering it.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      admissionTimeout + 10*time.Second,
		IdleTimeout:       120 * time.Second,
		Handler:           s.Handler(),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
		return nil
	case err := <-errCh:
		_ = ln.Close()
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

// Handler returns the server's HTTP handler: the single entry point for
// every request, with all authentication, negotiation, bounding, and
// error mapping inside it.
func (s *AdmissionServer) Handler() http.Handler {
	return http.HandlerFunc(s.handle)
}

// handle routes the two surfaces before any request touches a pipeline:
//
//   - POST /v1/contributions                    admission (#708)
//   - GET  /v1/contributions/<run>  status read (#819)
//
// A right path with the wrong method gets its own honest 405: a collection
// GET (a listing would be the arbitrary-CR-search proxy the read contract
// forbids) and a POST to a run path are different mistakes and say so.
// With no read token configured, the run path is not a route at all: the
// server never announces, method-maps, or serves a status surface it will
// not answer, and the miss message says so too.
func (s *AdmissionServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == PathContributions {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "contribution admission accepts POST only")
			return
		}
		s.handleAdmission(w, r)
		return
	}
	if run, ok := statusRoute(r.URL.Path); ok && s.readToken != "" {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, statusMethodMessage)
			return
		}
		s.handleStatus(w, r, run)
		return
	}
	if s.readToken != "" {
		writeError(w, http.StatusNotFound, routeNotFoundMessageWithReads)
		return
	}
	writeError(w, http.StatusNotFound, routeNotFoundMessage)
}

// AdmissionRequest is the JSON body of one admission POST. Field names are
// camelCase; Namespace, UpstreamIssue, IdempotencyKey, Fresh, and Debug
// are optional, everything else is required (validated by the adapter).
// Goal and UpstreamIssue are untrusted data: they ride into the run as
// context and can never change the approved fork, base, or publication
// destination.
type AdmissionRequest struct {
	Namespace      string `json:"namespace"`
	Lane           string `json:"lane"`
	BaseRepo       string `json:"baseRepo"`
	BaseRef        string `json:"baseRef"`
	WorkRepo       string `json:"workRepo"`
	Goal           string `json:"goal"`
	UpstreamIssue  string `json:"upstreamIssue"`
	IdempotencyKey string `json:"idempotencyKey"`
	// Fresh mints a fresh idempotency key for an intentional new
	// contribution, even for byte-identical content. It contradicts an
	// explicit IdempotencyKey in the same request and is rejected with it.
	Fresh bool `json:"fresh"`
	// Debug, when true, raises the run's pod log level only.
	Debug bool `json:"debug"`
}

// AdmissionResult is the success response body: the identity of the
// CoderRun the contribution was admitted to, and nothing else. It
// deliberately carries no goal, issue text, or credential — identity
// fields are all a thin frontend needs to report progress.
type AdmissionResult struct {
	// Run is the created-or-found CoderRun object name.
	Run string `json:"run"`
	// Namespace is the resolved namespace the run lives in.
	Namespace string `json:"namespace"`
	// WorkItemID is the idempotency key persisted in spec.workItemID.
	WorkItemID string `json:"workItemId"`
	// Resumed is true when an existing run already owned the key (HTTP
	// 200); false means a run was created (HTTP 201).
	Resumed bool `json:"resumed"`
}

// handleAdmission runs the admission pipeline in a fixed order, mirroring
// the intake's stage discipline: each stage either proceeds or rejects
// with a mapped status code, cheap structural checks run before expensive
// ones, and every cluster contact happens last, after the request is
// fully shaped:
//
//  1. auth        static bearer token             (401)
//  2. media type  application/json                (415)
//  3. body bound  ContentLength + MaxBytesReader  (413)
//  4. decode      strict single JSON object       (400)
//  5. shape       fresh vs. explicit key          (400)
//  6. ownership   owned-namespace boundary        (403)
//  7. admission   adapter Admit under a deadline  (400/409/503/500)
//
// Stage 1 uses one constant rejection message for missing, malformed, and
// wrong tokens alike, so an unauthenticated caller learns nothing about
// which part failed; the token itself is never logged or echoed anywhere.
// Stages 5 and 6 run before any cluster contact, so a contradictory or
// out-of-bounds request leaves the cluster untouched.
func (s *AdmissionServer) handleAdmission(w http.ResponseWriter, r *http.Request) {
	// Stage 1. Static bearer token, compared in constant time. One
	// constant message covers missing, malformed, and wrong tokens: the
	// response must not tell an unauthenticated caller which part of the
	// credential failed. The token is never written to any response.
	token, ok := bearerToken(r)
	if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
		writeError(w, http.StatusUnauthorized, unauthorizedMessage)
		return
	}

	// Stage 2. The API consumes application/json only; a form POST or a
	// raw body is a client bug. Parameters (charset and the like) are
	// ignored, and the comparison is case-insensitive.
	if contentType(r) != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "contribution admission requires application/json")
		return
	}

	// Stage 3. Bound the body before parsing so an oversized upload cannot
	// exhaust memory. ContentLength is only a claim: a chunked upload
	// reports -1, so the read itself is capped too and every byte that
	// enters the handler is bounded, not just the declared ones.
	if r.ContentLength > maxAdmissionBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "contribution admission request exceeds the size cap")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAdmissionBodyBytes)

	// Stage 4. One strict JSON object: malformed JSON, a non-object shape,
	// an unknown field, or trailing garbage is a 400 — a silent typo must
	// never become a silently ignored input.
	req, err := decodeAdmissionRequest(r)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		writeError(w, code, err.Error())
		return
	}

	// Stage 5. The fresh/explicit-key contradiction is rejected here, on
	// the raw key, before any cluster contact — the same rule the CLI
	// applies at parse time. Letting one silently win would hide the
	// caller's mistake about which idempotency strategy they meant.
	if req.Fresh && req.IdempotencyKey != "" {
		writeError(w, http.StatusBadRequest, "contribution admission cannot combine fresh and an explicit idempotency key; pick one idempotency strategy")
		return
	}

	// Stage 6. Ownership boundary (#632), before any cluster contact: this
	// release never admits a run into a namespace its reconciler refuses
	// to own. The namespace is trimmed here so what the server records is
	// exactly what the adapter validates and persists, which trims its own
	// inputs again.
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = s.defaultNamespace
	}
	if len(s.ownedNamespaces) > 0 && !slices.Contains(s.ownedNamespaces, namespace) {
		writeError(w, http.StatusForbidden, "contribution admission targets a namespace outside the owned namespaces")
		return
	}

	// Stage 7. Admission under a deadline, so a stalled API server maps to
	// a 503 instead of holding the connection open. Fields are handed to
	// the adapter untrimmed: the adapter trims and validates its own
	// inputs, and the server adding a second trimming rule would risk the
	// two disagreeing.
	key := req.IdempotencyKey
	if req.Fresh {
		minted, err := mintFreshKey()
		if err != nil {
			writeError(w, http.StatusInternalServerError, internalErrorMessage)
			return
		}
		key = minted
	}
	admitCtx, cancel := context.WithTimeout(r.Context(), admissionTimeout)
	defer cancel()
	admitted, err := s.admitter.Admit(admitCtx, Admission{
		Namespace:      namespace,
		Lane:           req.Lane,
		BaseRepo:       req.BaseRepo,
		BaseRef:        req.BaseRef,
		WorkRepo:       req.WorkRepo,
		Goal:           req.Goal,
		UpstreamIssue:  req.UpstreamIssue,
		IdempotencyKey: key,
		Debug:          req.Debug,
	})
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			// Named before the generic branch so a stalled cluster reads
			// as the timeout it is, not an opaque failure.
			writeError(w, http.StatusServiceUnavailable, timeoutMessage)
		case errors.Is(err, ErrInvalidAdmission):
			// Safe to surface verbatim: the message names fields and
			// shapes, never secrets.
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, ErrForeignRun):
			writeError(w, http.StatusConflict, err.Error())
		default:
			// Arbitrary cluster error text is not for unauthenticated-ish
			// callers: the fixed message says the admission failed and
			// leaves diagnosis to the server's own logs.
			writeError(w, http.StatusInternalServerError, internalErrorMessage)
		}
		return
	}

	status := http.StatusCreated
	if admitted.Resumed {
		status = http.StatusOK
	}
	writeJSON(w, status, AdmissionResult{
		Run:        admitted.RunName,
		Namespace:  namespace,
		WorkItemID: admitted.WorkItemID,
		Resumed:    admitted.Resumed,
	})
}

// decodeAdmissionRequest reads one strict JSON object from the request
// body. Unknown fields are rejected so a client bug never silently drops
// an input, and trailing content after the object is rejected so a
// doubled or padded body cannot smuggle a second payload past the first
// successful parse. A body that outran the MaxBytesReader maps to the
// size-cap error, not a parse error.
func decodeAdmissionRequest(r *http.Request) (AdmissionRequest, error) {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req AdmissionRequest
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return AdmissionRequest{}, errBodyTooLarge
		}
		return AdmissionRequest{}, errors.New("contribution admission requires exactly one JSON object with the expected fields")
	}
	if dec.More() {
		return AdmissionRequest{}, errors.New("contribution admission requires exactly one JSON object with the expected fields")
	}
	return req, nil
}

// mintFreshKey mints the idempotency key a fresh request carries: an
// "adhoc-api-" prefix plus 8 random bytes in hex — well within the
// adapter's key shape and length bound, and shaped like the CLI's
// "adhoc-cli-" keys so logs can tell the frontends apart. Randomness
// rather than a content digest is the point: fresh must guarantee a new
// run even for byte-identical content.
func mintFreshKey() (string, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("mint fresh idempotency key: %w", err)
	}
	return "adhoc-api-" + hex.EncodeToString(nonce), nil
}

// bearerToken extracts the bearer token from the Authorization header,
// the same shape the evidence intake accepts.
func bearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || strings.ToLower(scheme) != "bearer" || token == "" {
		return "", false
	}
	return token, true
}

// contentType returns the request's media type without parameters,
// lowercased; an unparseable header maps to an empty string, which never
// matches the accepted type.
func contentType(r *http.Request) string {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return ""
	}
	return strings.ToLower(media)
}

// writeError writes a JSON error response with the given status code. The
// message is safe to surface: callers pass fixed strings or the adapter's
// field-validation messages, which name no secrets or tokens.
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// writeJSON writes one JSON response with the application/json content
// type.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
