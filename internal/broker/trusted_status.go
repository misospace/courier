package broker

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
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
	if !canonicalStatusPath(r) || r.Method != http.MethodPost {
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
	var patch HarnessPatch
	if !decodeJSON(w, r, &patch) {
		return
	}
	if patch.Heartbeat != nil && (patch.Heartbeat.At.IsZero() || (patch.Heartbeat.Kind != "stream" && patch.Heartbeat.Kind != "tool")) {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := h.writer.Write(r.Context(), StatusIdentity{
		RunUID: identity.RunUID, ControlPod: identity.ControlPod,
		ControlPodUID: identity.ControlPodUID, ControlServiceAccount: identity.ServiceAccount,
	}, patch, h.validate); err != nil {
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

func canonicalStatusPath(r *http.Request) bool {
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	return err == nil && decoded == r.URL.Path && r.URL.RawPath == "" && r.URL.Path == PathTrustedStatus && !strings.Contains(r.URL.Path, "//") && !strings.Contains(r.URL.Path, "\\") && r.URL.RawQuery == ""
}
