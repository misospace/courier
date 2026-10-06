package executor

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

// The environment names the pod builder writes for failure-evidence capture
// (#115). The token name ends in TOKEN so the executor's name-shape redactor
// registers it without per-deployment configuration.
const (
	// EnvEvidenceURL is the operator intake endpoint the executor POSTs a
	// bounded dirty-run bundle to when capture is enabled.
	EnvEvidenceURL = "COURIER_EVIDENCE_URL"
	// EnvEvidenceToken is the per-incarnation bearer token that authenticates a
	// capture to exactly one intake slot.
	EnvEvidenceToken = "COURIER_EVIDENCE_TOKEN"
	// EnvEvidenceNonce is the random nonce bound into the token; the intake
	// derives the per-incarnation Secret slot name from it.
	EnvEvidenceNonce = "COURIER_EVIDENCE_NONCE"
	// EnvPodUID carries the pod's own UID via the downward API, for human
	// correlation of a stored bundle to the incarnation that produced it.
	EnvPodUID = "COURIER_POD_UID"
)

// EvidenceTerminationGracePeriodSeconds is the capture grace: the 20-second
// capture deadline plus the child-kill wait plus exit margin, sized so a
// SIGTERM capture can complete before the kubelet force-kills the pod.
const EvidenceTerminationGracePeriodSeconds int64 = 45

// EvidenceNonceBytes is the nonce length: long enough to keep an
// incarnation's secret unguessable — neither its slot name nor its token can
// be predicted or forged — short enough to fit a slot name.
const EvidenceNonceBytes = 16

// NewEvidenceNonce returns a fresh random nonce for one pod incarnation. The
// nonce is what makes a token per-incarnation: the intake derives the slot
// name from it, so two incarnations of the same run can never address each
// other's slot.
func NewEvidenceNonce() ([]byte, error) {
	nonce := make([]byte, EvidenceNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

// EvidenceToken builds the per-incarnation bearer token the executor presents
// to the intake and the intake validates. It returns
// base64(nonce ‖ HMAC-SHA256(key, namespace+"/"+name+"/"+runUID+"/"+hex(nonce))).
// This is the wire contract shared with the evidence intake (issue #200): the
// intake recomputes the HMAC from a live read of the run and compares it with
// hmac.Equal, and derives the per-incarnation Secret slot name from the first
// 8 hex characters of the nonce.
func EvidenceToken(key []byte, namespace, name, runUID string, nonce []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(namespace + "/" + name + "/" + runUID + "/" + hex.EncodeToString(nonce)))
	return base64.StdEncoding.EncodeToString(mac.Sum(nonce))
}
