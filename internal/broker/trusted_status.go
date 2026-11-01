package broker

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// The trusted status handler answers one request with a TokenReview, live run
// and pod reads, and — for a lastCommit confirmation — live forge reads, all
// of them sequential and all of them before its compare-and-set write. The
// listener's write deadline starts when the request headers are read, so the
// handler's own budget must sit strictly inside that deadline with room left
// to write the response: a request that exhausts its budget answers a
// retryable 503 instead of a response the write deadline truncates.
const (
	// StatusResponseReserve is the share of the listener's write budget kept
	// for writing the response once the handler's own work is done.
	StatusResponseReserve = 5 * time.Second
	// StatusRequestBudget is the handler's own work budget.
	StatusRequestBudget = InboundWriteTimeout - StatusResponseReserve
)

// NewTrustedStatusHandler constructs the trusted status route on a separate
// handler. It must be served only by the isolated trusted control/broker path;
// this handler is not mounted on Server.Handler(). The route itself is not a
// security boundary: #123 process and network isolation must secure its listener.
func NewTrustedStatusHandler(authenticator RequestAuthenticator, writer *StatusWriter, validate StatusValidator) (http.Handler, error) {
	if authenticator == nil || writer == nil || validate == nil {
		return nil, errors.New("trusted status requires live authentication, writer, and live-world validator")
	}
	return &trustedStatusHandler{authenticator: authenticator, writer: writer, validate: validate, budget: StatusRequestBudget}, nil
}

type trustedStatusHandler struct {
	authenticator RequestAuthenticator
	writer        *StatusWriter
	validate      StatusValidator
	// budget bounds the handler's own work. It ends before the listener's
	// write deadline, with the response reserve in between, so the write
	// bound can never truncate an answer the handler produced.
	budget time.Duration
}

func (h *trustedStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Every step below runs under the handler's own budget: it starts with
	// the request and ends before the listener's write deadline, so a
	// backend that cannot answer in time produces a complete, retryable
	// response rather than a response the write deadline truncates.
	ctx, cancel := context.WithTimeout(r.Context(), h.budget)
	defer cancel()
	// The body read is part of that budget. Without this bound the
	// listener's own read bound — longer than its write bound — would let a
	// client that sends its headers and then stalls its body hold the
	// request past the write deadline, and the answer would be written into
	// an expired deadline. A writer that does not support deadlines keeps
	// the listener's own read bound; the handler still answers inside its
	// budget.
	if deadline, ok := ctx.Deadline(); ok {
		_ = http.NewResponseController(w).SetReadDeadline(deadline)
	}
	controlRequest := canonicalControlRequestPath(r) && r.Method == http.MethodGet
	if !controlRequest && (!canonicalStatusPath(r) || r.Method != http.MethodPost) {
		http.NotFound(w, r)
		return
	}
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	identity, err := h.authenticator.Authenticate(ctx, token)
	if err != nil {
		if requestUnfinished(err) {
			// The review failed because the request ended — the budget ran
			// out or the caller hung up — so the credential is unproven
			// rather than refused: the answer stays retryable.
			http.Error(w, "status update unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if identity.RunUID == "" || identity.RunName != h.writer.runName || identity.Namespace != h.writer.namespace || identity.ControlPod == "" || identity.ControlPodUID == "" || identity.ServiceAccount == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if controlRequest {
		h.serveControlRequest(w, r, ctx, identity)
		return
	}
	var patch HarnessPatch
	if err := decodeJSONBody(r, &patch); err != nil {
		if requestUnfinished(err) || readTimedOut(err) {
			// The body could not be read in time. The request is incomplete,
			// not malformed, so the answer stays retryable.
			http.Error(w, "status update unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if patch.Heartbeat != nil && (patch.Heartbeat.At.IsZero() || (patch.Heartbeat.Kind != "stream" && patch.Heartbeat.Kind != "tool") || patch.Heartbeat.CoordinatorPodUID == "") {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := h.writer.Write(ctx, StatusIdentity{
		RunUID: identity.RunUID, ControlPod: identity.ControlPod,
		ControlPodUID: identity.ControlPodUID, ControlServiceAccount: identity.ServiceAccount,
	}, patch, h.validate); err != nil {
		// Contention and backend unavailability are transient, not denial:
		// 503 lets the caller's own retry machinery treat them as retryable,
		// while a definite identity or live-world mismatch stays 422.
		var conflict *StatusConflictError
		var unavailable *StatusUnavailableError
		if errors.As(err, &conflict) {
			http.Error(w, "status update conflicted", http.StatusServiceUnavailable)
			return
		}
		if errors.As(err, &unavailable) {
			http.Error(w, "status update unavailable", http.StatusServiceUnavailable)
			return
		}
		if requestUnfinished(err) {
			// The budget ran out before the write could answer. The update is
			// unproven, not refused, so the answer stays retryable.
			http.Error(w, "status update unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "status update denied", http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// requestUnfinished reports whether the failure is the request ending before
// the handler could answer it: the handler's own budget ran out, or the
// caller hung up. Such a failure proves nothing about the request, so it must
// never be answered as a definite denial.
func requestUnfinished(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// readTimedOut reports whether a body read failed because the read deadline
// passed rather than because the body was malformed.
func readTimedOut(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func bearerToken(value string) (string, bool) {
	if !strings.HasPrefix(value, "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(value, "Bearer ")
	return token, token != "" && strings.TrimSpace(token) == token
}

// canonicalPath reports whether the request addresses exactly one unescaped
// spelling of want with no query: the conservative canonicality the trusted
// routes enforce on every request.
func canonicalPath(r *http.Request, want string) bool {
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	return err == nil && decoded == r.URL.Path && r.URL.RawPath == "" && r.URL.Path == want && !strings.Contains(r.URL.Path, "//") && !strings.Contains(r.URL.Path, "\\") && r.URL.RawQuery == ""
}

func canonicalStatusPath(r *http.Request) bool {
	return canonicalPath(r, PathTrustedStatus)
}

func canonicalControlRequestPath(r *http.Request) bool {
	return canonicalPath(r, PathControlRequest)
}

// serveControlRequest answers the authenticated control incarnation's read of
// the operator's fenced control request. The record is returned verbatim only
// when it fences to this exact incarnation; any other case is a bare 404 that
// reveals nothing about the record.
func (h *trustedStatusHandler) serveControlRequest(w http.ResponseWriter, r *http.Request, ctx context.Context, identity Identity) {
	if r.Body != nil && r.ContentLength != 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	run := &courier.CoderRun{}
	if err := h.writer.reader.Get(ctx, types.NamespacedName{Namespace: h.writer.namespace, Name: h.writer.runName}, run); err != nil {
		if apierrors.IsNotFound(err) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "control request unavailable", http.StatusServiceUnavailable)
		return
	}
	request := run.Status.ControlRequest
	if run.UID != identity.RunUID || request == nil || request.TargetPodUID != string(identity.ControlPodUID) {
		http.NotFound(w, r)
		return
	}
	respond(w, request, nil)
}
