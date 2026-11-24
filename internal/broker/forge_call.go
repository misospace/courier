package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/misospace/courier/internal/forge"
	couriergit "github.com/misospace/courier/internal/git"
)

const maxForgeCallResponseBytes = 4 << 20

// ForgeReader is the broker's narrow, run-pinned model-facing forge read surface.
// In App mode the broker wires this interface to the read-scoped
// installation-token profile, so a route-vocabulary regression still cannot
// reach a write-capable credential; static mode has a single deployment
// credential for everything.
type ForgeReader interface {
	WorkItem(context.Context, string, string) (forge.WorkItem, error)
	ReadPullRequest(context.Context, string, int) (forge.PullRequest, error)
	Reviews(context.Context, string, int) ([]forge.Review, error)
	Comments(context.Context, string, int) ([]forge.Comment, error)
	Checks(context.Context, string, int, string) ([]forge.Check, error)
}

type forgeCallRequest struct {
	Operation string          `json:"operation"`
	Arguments json.RawMessage `json:"arguments"`
}

type forgeCallArguments struct {
	Repo    string  `json:"repo"`
	ID      *string `json:"id"`
	Number  *int    `json:"number"`
	HeadSHA *string `json:"headSHA"`
}

func (s *Server) forgeCall(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	var request forgeCallRequest
	if !decodeJSON(w, r, &request) || request.Operation == "" || len(request.Arguments) == 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	operation := request.Operation
	switch operation {
	case "read-work-item", "read-pull-request", "list-reviews", "list-comments", "read-checks":
	default:
		http.Error(w, "operation denied", http.StatusBadRequest)
		return
	}
	var rawArgs map[string]json.RawMessage
	if err := json.Unmarshal(request.Arguments, &rawArgs); err != nil || !validForgeArgumentKeys(operation, rawArgs) {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var args forgeCallArguments
	if err := strictDecodeBytes(request.Arguments, &args); err != nil || strings.TrimSpace(args.Repo) == "" {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	switch operation {
	case "read-work-item":
		id, err := strconv.Atoi(stringValue(args.ID))
		if err != nil || id <= 0 {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.WorkItem(r.Context(), args.Repo, strconv.Itoa(id))
		respondForgeRead(w, value, err)
	case "read-pull-request":
		if !validForgePRArgs(args) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.ReadPullRequest(r.Context(), args.Repo, *args.Number)
		respondForgeRead(w, value, err)
	case "list-reviews":
		if !validForgePRArgs(args) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.Reviews(r.Context(), args.Repo, *args.Number)
		respondForgeRead(w, value, err)
	case "list-comments":
		if !validForgePRArgs(args) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.Comments(r.Context(), args.Repo, *args.Number)
		respondForgeRead(w, value, err)
	case "read-checks":
		if !validForgePRNumber(args) || !couriergit.ValidOID(stringValue(args.HeadSHA)) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.Checks(r.Context(), args.Repo, *args.Number, *args.HeadSHA)
		respondForgeRead(w, value, err)
	}
}

func validForgeArgumentKeys(operation string, args map[string]json.RawMessage) bool {
	allowed := map[string]bool{"repo": true}
	switch operation {
	case "read-work-item":
		allowed["id"] = true
	case "read-checks":
		allowed["number"], allowed["headSHA"] = true, true
	default:
		allowed["number"] = true
	}
	if _, ok := args["repo"]; !ok {
		return false
	}
	for key := range args {
		if !allowed[key] {
			return false
		}
	}
	return true
}

func validForgePRArgs(args forgeCallArguments) bool {
	return validForgePRNumber(args) && args.ID == nil && args.HeadSHA == nil
}

func validForgePRNumber(args forgeCallArguments) bool {
	return args.Number != nil && *args.Number > 0 && *args.Number <= 10_000_000 && args.ID == nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func strictDecodeBytes(data []byte, target any) error {
	return strictDecodeReader(bytes.NewReader(data), target)
}

func strictDecodeReader(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("invalid request")
	}
	return nil
}

func respondForgeRead(w http.ResponseWriter, value any, err error) {
	if err != nil {
		// A transient provider failure means the read never happened: the
		// answer stays retryable so control's retry machinery reruns it. A
		// definite refusal — policy, capability, or a completed-but-failed
		// read — is a denial and never retries.
		if forge.IsTransientAvailability(err) {
			http.Error(w, "forge read is temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "operation denied", http.StatusUnprocessableEntity)
		return
	}
	// The encoder marshals the value once and writes it in a single call,
	// so the bounded buffer rejects an oversize answer before a response
	// copy is buffered; it does not bound the encoder's own marshal.
	body := &forgeResponseBuffer{remaining: maxForgeCallResponseBytes}
	if err := json.NewEncoder(body).Encode(value); err != nil {
		http.Error(w, "response unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body.Bytes())
}

// errForgeResponseOversize stops the encoder the moment a forge read's answer
// outgrows the response bound.
var errForgeResponseOversize = errors.New("forge response exceeds the bound")

// forgeResponseBuffer accepts encoded bytes only while they fit the bound, so
// an oversized answer fails mid-encode instead of being fully buffered.
type forgeResponseBuffer struct {
	bytes.Buffer
	remaining int64
}

func (b *forgeResponseBuffer) Write(p []byte) (int, error) {
	if int64(len(p)) > b.remaining {
		return 0, errForgeResponseOversize
	}
	b.remaining -= int64(len(p))
	return b.Buffer.Write(p)
}
