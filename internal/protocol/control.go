package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Client is the trusted control-side transport for the signed worker
// protocol. It signs every dispatch and cancellation with the current
// control incarnation's key. All data a worker returns through it is
// untrusted.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func (c *Client) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

// Dispatch delivers one signed task. It returns the worker's accepted status
// ("running") or an error describing the rejection. A transport failure is
// reported as such: control must reconcile via Cancel and Result polling, not
// blindly redeliver, because the worker may have accepted the task.
func (c *Client) Dispatch(ctx context.Context, key ed25519.PrivateKey, envelope Envelope, payload []byte) (string, error) {
	signature, err := envelope.Sign(key)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(signedRequest{Envelope: envelope, Signature: signature, Payload: payload})
	if err != nil {
		return "", fmt.Errorf("protocol: encode dispatch: %w", err)
	}
	return c.post(ctx, "/v1/tasks", body, envelope.OpID)
}

// Cancel delivers one signed cancellation for the same opID as its dispatch.
// It returns once the worker has verified process termination (or confirmed
// the operation never started).
func (c *Client) Cancel(ctx context.Context, key ed25519.PrivateKey, envelope Envelope) (string, error) {
	signature, err := envelope.Sign(key)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(signedRequest{Envelope: envelope, Signature: signature})
	if err != nil {
		return "", fmt.Errorf("protocol: encode cancel: %w", err)
	}
	return c.post(ctx, "/v1/tasks/cancel", body, envelope.OpID)
}

func (c *Client) post(ctx context.Context, path string, body []byte, opID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("protocol: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Courier-OpID", opID)
	resp, err := c.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("protocol: transport: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("protocol: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &TransportError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	}
	var ack struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &ack); err != nil {
		return "", fmt.Errorf("protocol: decode ack: %w", err)
	}
	return ack.Status, nil
}

// TransportError carries the worker's rejection status code and short
// message. The message is the worker's fixed diagnostic string, never task
// output.
type TransportError struct {
	StatusCode int
	Message    string
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("protocol: worker rejected the request (%d): %s", e.StatusCode, e.Message)
}

// ErrConflict reports that the worker refused the operation because the opID
// is already registered or tombstoned.
var ErrConflict = errors.New("protocol: operation conflicts with worker state")

// ResultState is a result poll answer.
type ResultState struct {
	Status string
	Result *Result
}

// Result polls one operation's state. A missing operation (a replacement
// worker that never saw the opID) reports Status "" with IsNotFound.
func (c *Client) Result(ctx context.Context, opID string) (ResultState, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/tasks/"+opID+"/result", nil)
	if err != nil {
		return ResultState{}, fmt.Errorf("protocol: build request: %w", err)
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return ResultState{}, fmt.Errorf("protocol: transport: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
	case http.StatusNotFound:
		return ResultState{}, nil
	default:
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return ResultState{}, &TransportError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	}
	var state ResultState
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxArtifactBytes+(maxArtifactBytes/2))).Decode(&state); err != nil {
		return ResultState{}, fmt.Errorf("protocol: decode result: %w", err)
	}
	return state, nil
}

// UploadSnapshot stores the sanitized workspace snapshot on the worker after
// digest verification.
func (c *Client) UploadSnapshot(ctx context.Context, data []byte) error {
	body, err := json.Marshal(Snapshot{Digest: PayloadDigestOf(data), Data: data})
	if err != nil {
		return fmt.Errorf("protocol: encode snapshot: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/snapshot", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("protocol: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("protocol: transport: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return &TransportError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	}
	return nil
}
