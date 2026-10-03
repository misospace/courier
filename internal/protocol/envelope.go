// Package protocol implements the signed control-to-worker task protocol
// (HARNESS.md §5): typed dispatch and cancellation envelopes signed by the
// trusted control pod's per-incarnation Ed25519 key, verified by an honest
// worker listener that keeps in-memory replay state and cancellation
// tombstones.
//
// The worker listener is not a security boundary — a compromised worker may
// bypass its own verifier and fabricate results — but it protects the honest
// worker from stale, foreign, or replayed dispatch. Everything a worker
// returns is untrusted data that control must validate independently.
package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Envelope kinds.
const (
	KindDispatch = "dispatch"
	KindCancel   = "cancel"
)

// Envelope is the signed, canonical dispatch or cancellation record. Every
// field is bound to the signature; a worker verifies the exact run,
// control-pod, and worker-pod incarnation, the payload digest, the replay
// state, and the admission expiration. The expiration bounds admission and
// replay only — it is never an operation-duration limit.
type Envelope struct {
	RunUID        string    `json:"runUID"`
	ControlPodUID string    `json:"controlPodUID"`
	WorkerPodUID  string    `json:"workerPodUID"`
	BriefID       string    `json:"briefID"`
	OpID          string    `json:"opID"`
	Kind          string    `json:"kind"`
	PayloadDigest string    `json:"payloadDigest"`
	Sequence      uint64    `json:"sequence"`
	Nonce         string    `json:"nonce"`
	ExpiresAt     time.Time `json:"expiresAt"`
}

// canonical returns the exact bytes the signature covers: the deterministic
// JSON encoding of the envelope struct. Field order is fixed by declaration,
// so both sides canonicalize identically.
func (e Envelope) canonical() ([]byte, error) {
	return json.Marshal(e)
}

// Sign returns the Ed25519 signature over the envelope's canonical form.
func (e Envelope) Sign(key ed25519.PrivateKey) ([]byte, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("protocol: signing key has the wrong size")
	}
	canonical, err := e.canonical()
	if err != nil {
		return nil, fmt.Errorf("protocol: canonicalize envelope: %w", err)
	}
	return ed25519.Sign(key, canonical), nil
}

// Verify checks the envelope's signature against the operator-provisioned
// public key.
func (e Envelope) Verify(key ed25519.PublicKey, signature []byte) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("protocol: verification key has the wrong size")
	}
	canonical, err := e.canonical()
	if err != nil {
		return fmt.Errorf("protocol: canonicalize envelope: %w", err)
	}
	if !ed25519.Verify(key, canonical, signature) {
		return errors.New("protocol: envelope signature is invalid")
	}
	return nil
}

// Validate checks the envelope's structural invariants before any signature
// work: kind, sequence rules (dispatch is sequence 0, cancellation strictly
// greater), identity binding, digest shape, and required identifiers.
func (e Envelope) Validate(now time.Time) error {
	switch e.Kind {
	case KindDispatch:
		if e.Sequence != 0 {
			return errors.New("protocol: dispatch must carry sequence 0")
		}
		if e.BriefID == "" {
			return errors.New("protocol: dispatch requires a brief ID")
		}
	case KindCancel:
		if e.Sequence == 0 {
			return errors.New("protocol: cancellation sequence must be strictly greater than zero")
		}
	default:
		return fmt.Errorf("protocol: unknown envelope kind %q", e.Kind)
	}
	if e.RunUID == "" || e.ControlPodUID == "" || e.WorkerPodUID == "" {
		return errors.New("protocol: envelope must bind run, control, and worker incarnations")
	}
	if e.OpID == "" {
		return errors.New("protocol: envelope requires an operation ID")
	}
	if len(e.PayloadDigest) != 64 || !isHex(e.PayloadDigest) {
		return errors.New("protocol: payload digest must be a SHA-256 hex string")
	}
	if len(e.Nonce) < 16 {
		return errors.New("protocol: nonce is too short")
	}
	if e.ExpiresAt.IsZero() {
		return errors.New("protocol: admission expiration is required")
	}
	if now.After(e.ExpiresAt) {
		return fmt.Errorf("protocol: envelope expired at %s", e.ExpiresAt.UTC().Format(time.RFC3339))
	}
	return nil
}

// PayloadDigestOf computes the digest a dispatch envelope must carry.
func PayloadDigestOf(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// CancelPayloadDigest is the digest carried by cancellation envelopes, which
// have no payload.
var CancelPayloadDigest = PayloadDigestOf(nil)

// NewEnvelope fills an envelope's nonce, digest, and admission expiration.
// The caller sets the identity fields, kind, sequence, and identifiers.
func NewEnvelope(now time.Time, admissionWindow time.Duration, payload []byte) Envelope {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		panic("protocol: entropy unavailable")
	}
	return Envelope{
		PayloadDigest: PayloadDigestOf(payload),
		Nonce:         hex.EncodeToString(nonce),
		ExpiresAt:     now.Add(admissionWindow),
	}
}

// EncodeKey renders an Ed25519 public key for immutable pod-creation
// configuration (worker environment).
func EncodeKey(key ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(key)
}

// DecodeKey parses a base64-encoded Ed25519 public key.
func DecodeKey(value string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("protocol: decode public key: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("protocol: public key has the wrong size")
	}
	return ed25519.PublicKey(raw), nil
}

// GenerateKey creates a per-control-incarnation signing keypair. The private
// key is provisioned only into trusted control; the public key is passed to
// the worker in immutable pod-creation configuration.
func GenerateKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("protocol: generate signing key: %w", err)
	}
	return pub, priv, nil
}

func isHex(value string) bool {
	_, err := hex.DecodeString(value)
	return err == nil
}
