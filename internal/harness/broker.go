package harness

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	couriergit "github.com/misospace/courier/internal/git"
)

// Control-side broker API paths. They mirror internal/broker's routes; the
// client is transport only — every semantic check happens broker-side against
// the persisted policy, and control supplies OIDs it derived itself. The
// status path is served by the broker's separate trusted listener.
const (
	brokerPathSnapshot = "/v1/git/snapshot"
	brokerPathImport   = "/v1/bundles/import"
	brokerPathPublish  = "/v1/publication"
	brokerPathStatus   = "/trusted/v1/status"
)

// Broker API header names.
const (
	headerSnapshotTip      = "X-Courier-Snapshot-Tip"
	headerSnapshotBaseTip  = "X-Courier-Snapshot-Base-Tip"
	headerSnapshotWorkRef  = "X-Courier-Snapshot-Work-Ref-Exists"
	headerSnapshotAtAnchor = "X-Courier-Snapshot-At-Anchor"
	headerProposedOID      = "X-Courier-Proposed-OID"
	headerExpectedTip      = "X-Courier-Expected-Tip"
)

// BrokerCallError classifies one broker call failure. Transport-class and
// 5xx/429/408 failures are retryable; other 4xx responses are definite
// denials. Response bodies are never echoed: only a fixed category and the
// status code travel.
type BrokerCallError struct {
	StatusCode int
	Category   string
	Retryable  bool
}

func (e *BrokerCallError) Error() string {
	return "harness: broker " + e.Category
}

func brokerTransportError() *BrokerCallError {
	return &BrokerCallError{Category: "is unreachable", Retryable: true}
}

func brokerStatusError(status int) *BrokerCallError {
	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
	category := "denied the operation"
	if retryable {
		category = "is temporarily unavailable"
	}
	return &BrokerCallError{StatusCode: status, Category: category, Retryable: retryable}
}

func isBrokerRetryable(err error) bool {
	var callErr *BrokerCallError
	return errors.As(err, &callErr) && callErr.Retryable
}

// BrokerSnapshot is one live world observation plus the bundle that seeds a
// fresh integration tree from it.
type BrokerSnapshot struct {
	Data    []byte
	Tip     string
	BaseTip string
	// WorkRefExists reports whether the pinned work ref currently exists.
	// A false value with an initially-absent policy is what licenses the
	// expected-tip-absent publication of the run's first commit.
	WorkRefExists bool
	// AtAnchor reports whether the observed work tip is the admission
	// anchor. A seeded tip at the anchor is ordinary pre-existing state —
	// never a publication of this run; a tip beyond the anchor may be this
	// run's own unconfirmed publication and is confirmable by the broker.
	AtAnchor bool
}

// BrokerClient reaches the run's broker's typed publication API over TLS
// with the control pod's projected broker-audience token.
type BrokerClient struct {
	BaseURL string
	Token   []byte
	CA      []byte
	HTTP    *http.Client
}

func (b *BrokerClient) transport() (*http.Client, error) {
	if b.HTTP != nil {
		return b.HTTP, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b.CA) {
		return nil, &BrokerCallError{Category: "CA is unreadable"}
	}
	return &http.Client{
		Timeout:   5 * time.Minute,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: hostOnly(b.BaseURL), MinVersion: tls.VersionTLS13}},
	}, nil
}

func (b *BrokerClient) do(ctx context.Context, method, path string, headers map[string]string, body []byte) (*http.Response, error) {
	transport, err := b.transport()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(b.BaseURL, "/")+path, strings.NewReader(string(body)))
	if err != nil {
		return nil, &BrokerCallError{Category: "request is malformed"}
	}
	req.Header.Set("Authorization", "Bearer "+string(b.Token))
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := transport.Do(req)
	if err != nil {
		return nil, brokerTransportError()
	}
	if !okStatus(resp.StatusCode) {
		resp.Body.Close()
		return nil, brokerStatusError(resp.StatusCode)
	}
	return resp, nil
}

// okStatus reports a successful typed-API response. Every typed endpoint
// answers 200 with a body; the trusted status route answers 204 on write.
func okStatus(status int) bool {
	return status == http.StatusOK || status == http.StatusNoContent
}

// Snapshot fetches the broker's live world observation and the seed bundle
// rendered from it: the pinned work tip (or the base tip when the work ref
// is absent), the pinned base tip, and a self-contained bundle of both.
func (b *BrokerClient) Snapshot(ctx context.Context) (BrokerSnapshot, error) {
	resp, err := b.do(ctx, http.MethodGet, brokerPathSnapshot, nil, nil)
	if err != nil {
		return BrokerSnapshot{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxArtifactBundleBytes+1<<20))
	if err != nil {
		return BrokerSnapshot{}, &BrokerCallError{Category: "snapshot response is unreadable", Retryable: true}
	}
	if len(data) == 0 || len(data) > MaxArtifactBundleBytes {
		return BrokerSnapshot{}, &BrokerCallError{Category: "snapshot exceeds the bundle bound"}
	}
	tip := resp.Header.Get(headerSnapshotTip)
	baseTip := resp.Header.Get(headerSnapshotBaseTip)
	if !couriergit.ValidOID(tip) || !couriergit.ValidOID(baseTip) {
		return BrokerSnapshot{}, &BrokerCallError{Category: "snapshot observation is malformed"}
	}
	return BrokerSnapshot{
		Data:          data,
		Tip:           tip,
		BaseTip:       baseTip,
		WorkRefExists: resp.Header.Get(headerSnapshotWorkRef) == "true",
		AtAnchor:      resp.Header.Get(headerSnapshotAtAnchor) == "true",
	}, nil
}

// ImportBundle imports a proposed publication into the broker's private
// object store. The broker independently re-observes the world and checks
// the expected tip against its policy anchors before importing.
func (b *BrokerClient) ImportBundle(ctx context.Context, bundle []byte, proposedOID, expectedTip string) error {
	resp, err := b.do(ctx, http.MethodPost, brokerPathImport, map[string]string{
		headerProposedOID: proposedOID,
		headerExpectedTip: expectedTip,
	}, bundle)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// PostStatus sends one trusted status patch to the broker's separate status
// listener. The broker validates the patch against the authenticated control
// incarnation and applies it under resourceVersion CAS; its response, never
// the transport outcome, decides whether the write happened.
func (b *BrokerClient) PostStatus(ctx context.Context, patch StatusPatch) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return &BrokerCallError{Category: "status request is malformed"}
	}
	resp, err := b.do(ctx, http.MethodPost, brokerPathStatus, map[string]string{"Content-Type": "application/json"}, body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Publication is the broker's confirmed publication result.
type Publication struct {
	OID              string
	AlreadyPublished bool
}

// Publish asks the broker to publish the proposed commit to the pinned work
// ref. The broker preflights, pushes non-force, and confirms by post-push
// observation; its response, never the transport outcome, decides.
func (b *BrokerClient) Publish(ctx context.Context, expectedWorkOID, proposedOID string) (Publication, error) {
	body, err := json.Marshal(struct {
		ExpectedWorkOID string `json:"expectedWorkOID"`
		ProposedOID     string `json:"proposedOID"`
	}{ExpectedWorkOID: expectedWorkOID, ProposedOID: proposedOID})
	if err != nil {
		return Publication{}, &BrokerCallError{Category: "request is malformed"}
	}
	resp, err := b.do(ctx, http.MethodPost, brokerPathPublish, map[string]string{"Content-Type": "application/json"}, body)
	if err != nil {
		return Publication{}, err
	}
	defer resp.Body.Close()
	var result Publication
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return Publication{}, &BrokerCallError{Category: "publication response is malformed", Retryable: true}
	}
	if result.OID != proposedOID {
		return Publication{}, &BrokerCallError{Category: "publication response does not match the proposal"}
	}
	return result, nil
}

// PublicationBlockedError reports a publication the world will not allow: a
// foreign work tip, a changed pinned identity, an unwritable or protected
// destination. It is a NeedsHuman classification, not an infrastructure
// failure, and its reason names identities and categories — never credential
// or provider response data.
type PublicationBlockedError struct {
	Reason string
}

func (e *PublicationBlockedError) Error() string {
	return "harness: publication blocked: " + e.Reason
}

// TransientPublicationError reports publication transport-class failure that
// persisted past bounded retries: relaunching the coordinator reconciles
// against the world.
type TransientPublicationError struct {
	Err error
}

func (e *TransientPublicationError) Error() string {
	return "harness: publication unavailable: " + e.Err.Error()
}

func (e *TransientPublicationError) Unwrap() error { return e.Err }

// publishRetryRounds bounds transport-class retries inside one broker call,
// and publishRetryDelay spaces them. They are availability retries, never a
// duration limit on the work.
const (
	publishRetryRounds = 3
	publishRetryDelay  = 2 * time.Second
)

// worldReconcileRounds bounds how many times one publication re-observes the
// world and reclassifies before it refuses to guess: a tip that keeps moving
// is foreign, and the run stops rather than racing it.
const worldReconcileRounds = 3
