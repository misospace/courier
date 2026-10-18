package harness

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
)

// BrokerTokenSource supplies the bearer token for one authenticated broker
// request. It is consulted per request so kubelet rotation of the projected
// token file is picked up without rebuilding control identity or restarting
// the run. Implementations must not embed the token bytes in an error, and
// must be safe to hold by value: a caller detects an unwired source by a nil
// interface only, so a typed nil must never reach a method here. A caller may
// hold the returned slice across the request and compare it against a later
// read, so every return must be freshly owned bytes: never a shared or reused
// buffer.
//
// There is deliberately no cache and no expiry bookkeeping: a per-request read
// of a projected volume is the simplest correct cadence, and a cached copy is
// exactly the frozen-credential bug this type exists to remove. The file read
// is a cheap local open on a projected volume, and a rotation is picked up on
// the next request without touching identity or restarting anything.
type BrokerTokenSource interface {
	BrokerToken() ([]byte, error)
}

// fileBrokerTokenSource reads the projected broker-audience token file on
// every call. Rotation is kubelet's: it swaps the projection's `..data`
// symlink underneath this process, the next read resolves it and sees the new
// bytes.
type fileBrokerTokenSource struct{ path string }

// NewFileBrokerTokenSource reads path on every BrokerToken call so kubelet
// rotation is picked up immediately. An absent file, an unreadable file, and
// an empty or whitespace-only read each fail with their own fixed category; no
// error carries the file's contents, its path, or the underlying *fs.PathError
// text. Surrounding whitespace is trimmed — the broker rejects a bearer value
// that is not already trimmed, so the bytes handed to it must be clean — and
// each call returns a defensive copy.
func NewFileBrokerTokenSource(path string) BrokerTokenSource {
	return fileBrokerTokenSource{path: path}
}

func (s fileBrokerTokenSource) BrokerToken() ([]byte, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.New("harness: broker token file is absent")
		}
		// A *fs.PathError carries the path, so the raw error never travels:
		// a projection control cannot read is one fixed condition, not a
		// per-filename diagnosis.
		return nil, errors.New("harness: broker token file is unreadable")
	}
	token := bytes.TrimSpace(raw)
	if len(token) == 0 {
		return nil, errors.New("harness: broker token file is empty")
	}
	return bytes.Clone(token), nil
}

// staticBrokerTokenSource serves fixed bytes. It exists for tests and for
// callers that genuinely hold immutable bytes; nothing in the control path
// uses it, because the control path's token rotates underneath it.
type staticBrokerTokenSource struct{ token []byte }

// NewStaticBrokerTokenSource serves a copy of token on every call. It trims
// and rejects a blank token exactly as the file source does, so an unwired
// test cannot quietly authenticate with `Bearer `.
func NewStaticBrokerTokenSource(token []byte) BrokerTokenSource {
	return staticBrokerTokenSource{token: bytes.Clone(token)}
}

func (s staticBrokerTokenSource) BrokerToken() ([]byte, error) {
	token := bytes.TrimSpace(s.token)
	if len(token) == 0 {
		return nil, errors.New("harness: static broker token is empty")
	}
	return bytes.Clone(token), nil
}

// resolveBrokerToken fetches one request's bearer token, failing closed when
// no source is wired. A nil source is a wiring fault, not a credential
// failure, so it gets the same fixed diagnostic as an unreadable projection:
// callers never send an unauthenticated broker request.
func resolveBrokerToken(source BrokerTokenSource) ([]byte, error) {
	if source == nil {
		return nil, errors.New("harness: broker token is unavailable")
	}
	return source.BrokerToken()
}
