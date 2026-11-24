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

// ForgeReadBudget bounds the provider read of one model-facing forge read
// request. A listing fans out into paginated provider requests, each with the
// provider's own timeout, so an unbounded fan-out can outlive the listener's
// write deadline and have its answer cut off mid-write. The budget starts
// before the read is dispatched and ends inside that deadline with the
// response reserve left over, so the route always answers a whole status. The
// phases before the read are bounded apart: authentication runs under the
// bare request context and the body read under the socket's own read bound
// (apiBodyReadBudget), a longer window that likewise ends inside the write
// deadline, so a stalled body can outlive this budget but never push an
// answer past the deadline. A read that cannot finish in budget is cancelled
// while it can still be answered. It is exported so the harness client can
// pin its own timeout above it and below the write deadline.
const ForgeReadBudget = InboundWriteTimeout - 2*apiResponseReserve

// ForgeReader is the broker's narrow, run-pinned model-facing forge read surface.
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
	ctx, cancel := context.WithTimeout(r.Context(), ForgeReadBudget)
	defer cancel()
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
		value, err := s.forgeReader.WorkItem(ctx, args.Repo, strconv.Itoa(id))
		respondForgeRead(w, value, err)
	case "read-pull-request":
		if !validForgePRArgs(args) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.ReadPullRequest(ctx, args.Repo, *args.Number)
		respondForgeRead(w, value, err)
	case "list-reviews":
		if !validForgePRArgs(args) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.Reviews(ctx, args.Repo, *args.Number)
		respondForgeRead(w, value, err)
	case "list-comments":
		if !validForgePRArgs(args) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.Comments(ctx, args.Repo, *args.Number)
		respondForgeRead(w, value, err)
	case "read-checks":
		if !validForgePRNumber(args) || !couriergit.ValidOID(stringValue(args.HeadSHA)) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		value, err := s.forgeReader.Checks(ctx, args.Repo, *args.Number, *args.HeadSHA)
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

// respondForgeRead answers one model-facing forge read with the status that
// says what happened, so the client retries only what a retry can change:
//
//	503  the read never happened — a transient provider failure, a read
//	     cancelled before it completed (client disconnect, shutdown, or the
//	     caller), or a read the route budget cut short. The world was not
//	     observed, so retrying is legitimate.
//	422  the read was refused, or completed and failed. A definite answer that
//	     no retry changes.
//	413  the answer outgrew the response bound. The same read outgrows it
//	     again, so this never retries.
//	501  the provider does not implement this read. Retrying a capability the
//	     provider never had can only loop.
func respondForgeRead(w http.ResponseWriter, value any, err error) {
	if err != nil {
		// A definite refusal — policy, capability, or a completed-but-failed
		// read — is a denial and never retries. A capability the provider
		// never had answers apart from a refusal so an operator can tell
		// "not supported here" from "you may not have it".
		if errors.Is(err, forge.ErrUnsupported) {
			http.Error(w, "forge read is not supported", http.StatusNotImplemented)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || forge.IsTransientAvailability(err) {
			// DeadlineExceeded is the route's own budget; Canceled is the
			// caller's context ending — a client disconnect, shutdown, or run
			// teardown. Either way the read completed nothing, so the answer
			// is not the definite denial a 422 claims.
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
		if errors.Is(err, errForgeResponseOversize) {
			http.Error(w, "forge read answer is too large", http.StatusRequestEntityTooLarge)
			return
		}
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
