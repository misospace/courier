package broker

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// NewTrustedStatusHandler constructs the trusted status route on a separate
// handler. It must be served only by the isolated trusted control/broker path;
// this handler is not mounted on Server.Handler(). The route itself is not a
// security boundary: #123 process and network isolation must secure its listener.
func NewTrustedStatusHandler(authenticator RequestAuthenticator, writer *StatusWriter, validate StatusValidator) (http.Handler, error) {
	if authenticator == nil || writer == nil || validate == nil {
		return nil, errors.New("trusted status requires live authentication, writer, and live-world validator")
	}
	return &trustedStatusHandler{authenticator: authenticator, writer: writer, validate: validate}, nil
}

type trustedStatusHandler struct {
	authenticator RequestAuthenticator
	writer        *StatusWriter
	validate      StatusValidator
}

func (h *trustedStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	identity, err := h.authenticator.Authenticate(r.Context(), token)
	if err != nil || identity.RunUID == "" || identity.RunName != h.writer.runName || identity.Namespace != h.writer.namespace || identity.ControlPod == "" || identity.ControlPodUID == "" || identity.ServiceAccount == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if controlRequest {
		h.serveControlRequest(w, r, identity)
		return
	}
	var patch HarnessPatch
	if !decodeJSON(w, r, &patch) {
		return
	}
	if patch.Heartbeat != nil && (patch.Heartbeat.At.IsZero() || (patch.Heartbeat.Kind != "stream" && patch.Heartbeat.Kind != "tool") || patch.Heartbeat.CoordinatorPodUID == "") {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := h.writer.Write(r.Context(), StatusIdentity{
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
		http.Error(w, "status update denied", http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
func (h *trustedStatusHandler) serveControlRequest(w http.ResponseWriter, r *http.Request, identity Identity) {
	if r.Body != nil && r.ContentLength != 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	run := &courier.CoderRun{}
	if err := h.writer.reader.Get(r.Context(), types.NamespacedName{Namespace: h.writer.namespace, Name: h.writer.runName}, run); err != nil {
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
