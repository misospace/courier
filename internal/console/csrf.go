package console

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
)

// csrfScopeSuffix names the CSRF derivation under the session key: the
// per-session CSRF token is HMAC(sessionKey, sessionID+":csrf"), so a token
// is bound to exactly one session and never doubles as a session signature.
const csrfScopeSuffix = ":csrf"

// csrfToken derives the per-session CSRF token. This is groundwork for the
// future mutating controls: every form the console renders embeds the token
// and every state-changing POST verifies it.
func csrfToken(key []byte, sessionID string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(sessionID + csrfScopeSuffix))
	return hex.EncodeToString(mac.Sum(nil))
}

// csrfVerify reports whether the presented token is the session's CSRF
// token. It refuses an empty presentation and compares in constant time.
func csrfVerify(key []byte, sessionID, token string) bool {
	if token == "" || len(key) == 0 {
		return false
	}
	expected := csrfToken(key, sessionID)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(token)) == 1
}
