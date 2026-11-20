package console

import "testing"

// TestCSRFTokenRoundTrip pins the derivation and the verifier: the token is
// a function of the session key and the session id, and only the exact pair
// verifies.
func TestCSRFTokenRoundTrip(t *testing.T) {
	key := []byte("session-key-material")
	token := csrfToken(key, "session-a")
	if len(token) == 0 {
		t.Fatal("csrfToken produced an empty token")
	}
	if !csrfVerify(key, "session-a", token) {
		t.Fatal("csrfVerify refused the session's own token")
	}
	for _, testCase := range []struct {
		name      string
		key       []byte
		sessionID string
		token     string
	}{
		{name: "other session", key: key, sessionID: "session-b", token: token},
		{name: "other key", key: []byte("other-key"), sessionID: "session-a", token: token},
		{name: "empty token", key: key, sessionID: "session-a", token: ""},
	} {
		if csrfVerify(testCase.key, testCase.sessionID, testCase.token) {
			t.Fatalf("csrfVerify accepted the %q case", testCase.name)
		}
	}
	// Empty keys and session ids fail closed rather than deriving a
	// predictable token.
	if csrfToken(nil, "session-a") == token {
		t.Fatal("an empty session key must not derive the real session's token")
	}
}
