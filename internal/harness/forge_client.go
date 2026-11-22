package harness

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/misospace/courier/internal/forge"
)

const brokerPathForgeCall = "/v1/forge/call"

// BrokerForgeClient routes model-influenced forge reads through the trusted
// run broker. It holds no forge credentials and delegates all policy to broker.
type BrokerForgeClient struct {
	BaseURL     string
	TokenSource BrokerTokenSource
	CA          []byte
	HTTP        *http.Client

	mu    sync.Mutex
	owned *http.Client
}

func (b *BrokerForgeClient) transport() (*http.Client, error) {
	if b.HTTP != nil {
		return b.HTTP, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.owned != nil {
		return b.owned, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b.CA) {
		return nil, &BrokerCallError{Category: "CA is unreadable"}
	}
	b.owned = &http.Client{
		Timeout: 5 * time.Minute,
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{RootCAs: pool, ServerName: hostOnly(b.BaseURL), MinVersion: tls.VersionTLS13},
			MaxIdleConns:        brokerMaxIdleConns,
			MaxIdleConnsPerHost: brokerMaxIdleConnsPerHost,
			IdleConnTimeout:     brokerIdleConnTimeout,
		},
	}
	return b.owned, nil
}

// CloseIdleConnections releases idle connections on the client's owned pool.
func (b *BrokerForgeClient) CloseIdleConnections() {
	b.mu.Lock()
	owned := b.owned
	b.mu.Unlock()
	if owned != nil {
		owned.CloseIdleConnections()
	}
}

// forgeReadOperations is the closed read vocabulary this client serves. It
// mirrors the broker route's allow-list: anything else is refused before any
// request is built, so a model-supplied write name can never reach the wire.
var forgeReadOperations = map[string]bool{
	"read-work-item":    true,
	"read-pull-request": true,
	"list-reviews":      true,
	"list-comments":     true,
	"read-checks":       true,
}

// maxBrokerForgeArgumentsBytes bounds the model-supplied arguments blob before
// it is buffered into a request; the broker applies its own bound as well.
const maxBrokerForgeArgumentsBytes = 16 << 10

// Call submits one model-requested forge read to the broker's closed vocabulary.
func (b *BrokerForgeClient) Call(ctx context.Context, operation string, arguments string) (string, error) {
	if !forgeReadOperations[operation] {
		return "", &BrokerCallError{Category: "forge operation is not supported"}
	}
	if len(arguments) > maxBrokerForgeArgumentsBytes {
		return "", &BrokerCallError{Category: "forge request is malformed"}
	}
	var args json.RawMessage
	if err := json.Unmarshal([]byte(arguments), &args); err != nil || len(args) == 0 || args[0] != '{' {
		return "", &BrokerCallError{Category: "forge request is malformed"}
	}
	body, err := json.Marshal(struct {
		Operation string          `json:"operation"`
		Arguments json.RawMessage `json:"arguments"`
	}{Operation: operation, Arguments: args})
	if err != nil {
		return "", &BrokerCallError{Category: "forge request is malformed"}
	}
	token, err := resolveBrokerToken(b.TokenSource)
	if err != nil {
		return "", &BrokerCallError{Category: "token is unavailable", Retryable: true}
	}
	transport, err := b.transport()
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(b.BaseURL, "/")+brokerPathForgeCall, bytes.NewReader(body))
	if err != nil {
		return "", &BrokerCallError{Category: "request is malformed"}
	}
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Content-Type", "application/json")
	resp, err := transport.Do(req)
	if err != nil {
		return "", brokerTransportError()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", b.statusError(resp.StatusCode, token)
	}
	result, err := io.ReadAll(io.LimitReader(resp.Body, maxBrokerForgeResponseBytes+1))
	if err != nil {
		return "", &BrokerCallError{Category: "forge response is unreadable", Retryable: true}
	}
	if len(result) == 0 || len(result) > maxBrokerForgeResponseBytes || !json.Valid(result) {
		return "", &BrokerCallError{Category: "forge response is malformed", Retryable: true}
	}
	if err := validateForgeResult(operation, result); err != nil {
		return "", &BrokerCallError{Category: "forge response is malformed", Retryable: true}
	}
	return string(bytes.TrimSpace(result)), nil
}

const maxBrokerForgeResponseBytes = 4 << 20

func validateForgeResult(operation string, data []byte) error {
	switch operation {
	case "read-work-item":
		var value forge.WorkItem
		return json.Unmarshal(data, &value)
	case "read-pull-request":
		var value forge.PullRequest
		return json.Unmarshal(data, &value)
	case "list-reviews":
		var value []forge.Review
		return json.Unmarshal(data, &value)
	case "list-comments":
		var value []forge.Comment
		return json.Unmarshal(data, &value)
	case "read-checks":
		var value []forge.Check
		return json.Unmarshal(data, &value)
	default:
		return errors.New("unsupported forge operation")
	}
}

func (b *BrokerForgeClient) statusError(status int, sent []byte) error {
	if status == http.StatusUnauthorized && rotatedUnder(b.TokenSource, sent) {
		return &BrokerCallError{StatusCode: status, Category: "refused the rotated-out credential", Retryable: true}
	}
	return brokerStatusError(status)
}

var _ ForgeOps = (*BrokerForgeClient)(nil)
