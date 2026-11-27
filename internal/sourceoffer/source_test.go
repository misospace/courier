package sourceoffer

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSourceOffer(t *testing.T) {
	const source = "https://example.org/releases/source.tar.gz"
	t.Setenv("COURIER_SOURCE_URL", source)
	req := httptest.NewRequest(http.MethodGet, "/source?url=https://wrong.example", nil)
	w := httptest.NewRecorder()
	Serve(w, req)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != source {
		t.Fatalf("source redirect: %d %q", w.Code, w.Header().Get("Location"))
	}
	w = httptest.NewRecorder()
	Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || w.Header().Get("Link") != "<"+source+">; rel=\"source\"" {
		t.Fatalf("API response: %d %q", w.Code, w.Header().Get("Link"))
	}
}

func TestSourceOfferRejectsUnsafeOrMissingURL(t *testing.T) {
	for _, raw := range []string{"", "//example.org", "javascript:alert(1)", "https://user:secret@example.org", "https://example.org/a>", "https://example.org/a\r\nX-Bad: yes"} {
		t.Setenv("COURIER_SOURCE_URL", raw)
		w := httptest.NewRecorder()
		Serve(w, httptest.NewRequest(http.MethodGet, "/source", nil))
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Location") != "" {
			t.Errorf("accepted invalid source URL: %q", raw)
		}
	}
}
