package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCorrespondingSourceOffer(t *testing.T) {
	const source = "https://example.org/source.tar.gz"
	t.Setenv("COURIER_SOURCE_URL", source)
	server, err := NewServer(Config{
		Mode:           AuthModeToken,
		SessionKey:     []byte("test-source-offer-key"),
		OperatorTokens: [][]byte{[]byte("operator")},
	})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/login", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `href="/source"`) {
		t.Fatalf("missing login-page source offer: %d", w.Code)
	}
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/source", nil))
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != source {
		t.Fatalf("source offer: %d %q", w.Code, w.Header().Get("Location"))
	}
}
