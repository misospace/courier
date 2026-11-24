package console

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	data := s.pageData(r, "Run "+name, "runs")
	data.RunName = name
	if !validRunName(name) {
		data.RunNotFound = true
		s.render(w, http.StatusOK, "run", data)
		return
	}
	result, err := s.backend.Run(r.Context(), name)
	if err != nil {
		var backendErr *BackendError
		if errors.As(err, &backendErr) && backendErr.Status == http.StatusNotFound {
			data.RunNotFound = true
		} else {
			data.RunErr = runErrorMessage(err)
		}
		s.render(w, http.StatusOK, "run", data)
		return
	}
	data.Run = result
	s.render(w, http.StatusOK, "run", data)
}

func runErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrBackendUnavailable):
		return "Run detail reads the operator read API, which is not configured for this console."
	case errors.Is(err, ErrBackendTokenMissing):
		return "Run detail needs a read API bearer token. This console has a read API URL but no token, so it cannot authenticate."
	case errors.Is(err, ErrBackendUnexpectedResponse):
		return "The read API answered, but not with run detail. Check that the console's read API URL points at the read API and not through a proxy that rewrites the response."
	}
	var backendErr *BackendError
	if errors.As(err, &backendErr) {
		switch backendErr.Status {
		case http.StatusUnauthorized:
			return "The read API refused this console's bearer token. Check the console's read API token against the read API's configured token."
		case http.StatusBadRequest:
			return "The read API rejected the run detail request."
		case http.StatusServiceUnavailable:
			return "The read API is at its read budget or past its own deadline. Try again shortly."
		default:
			return fmt.Sprintf("The read API returned an unexpected status (%d).", backendErr.Status)
		}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return "The read API request timed out before it answered. Raise --backend-timeout or try again."
	}
	return "The operator read API could not be reached."
}
