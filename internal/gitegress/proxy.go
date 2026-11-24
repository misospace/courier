package gitegress

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
)

// ServeHTTP answers exactly the resolved slice of the pinned git surface:
// it verifies a push's ref updates before any byte reaches the forge,
// injects the run's credential into the upstream request, and never echoes
// a credential, an upstream auth challenge, or a client-supplied
// Authorization header in either direction. Every refusal is one fixed
// status line with a fixed reason: the guard has no credential-bearing
// detail to leak, and a compromised model already knows what it tried.
func (g *Guard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bound, endpoint, err := g.resolve(r.URL, r.Method)
	if err != nil {
		refuse(w, err)
		return
	}
	var body io.Reader
	if r.Method != "GET" && r.Method != "HEAD" {
		body = r.Body
	}
	if endpoint == serviceReceivePack {
		head, rest, err := g.checkReceivePack(r.Body)
		if err != nil {
			refuse(w, err)
			return
		}
		body = io.MultiReader(bytes.NewReader(head), rest)
	}
	credential, err := g.credentials.Credentials(r.Context(), bound.identity)
	if err != nil || credential.Token == "" || credential.Username == "" {
		http.Error(w, "courier: installation git credential unavailable", http.StatusBadGateway)
		return
	}
	target := bound.upstream + "/" + endpoint
	if endpoint == endpointInfoRefs {
		target += "?service=" + url.QueryEscape(r.URL.Query().Get("service"))
	}
	request, err := http.NewRequestWithContext(r.Context(), r.Method, target, body)
	if err != nil {
		refuse(w, errors.New("refused: malformed request"))
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	// The guard repackages the request body it already read or is streaming,
	// so the inbound byte count still describes the outbound body exactly:
	// carrying it keeps the request unchunked for gateways (and CGIs) that
	// require a length. A chunked client body stays chunked.
	if r.ContentLength >= 0 && body != nil {
		request.ContentLength = r.ContentLength
	}
	// The guard parses the v1 ref-update section, so it pins the negotiation
	// to v1 in both directions: a v2 advertisement would invite a v2 push
	// body the guard refuses, turning a working push into a confusing one.
	request.Header.Set("Git-Protocol", "version=1")
	request.SetBasicAuth(credential.Username, credential.Token)
	response, err := g.upstream.Do(request)
	if err != nil {
		http.Error(w, "courier: forge transport unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	copyHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	// Smart-HTTP is a streaming protocol: flush each chunk so a long fetch
	// negotiates and progresses instead of buffering the whole response.
	flushCopy(w, response.Body)
}

func refuse(w http.ResponseWriter, err error) {
	http.Error(w, "courier: "+err.Error(), http.StatusForbidden)
}

var hopByHop = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Www-Authenticate":    true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// copyHeaders forwards the upstream response headers except the hop-by-hop
// set and any WWW-Authenticate challenge: the guard is the only authority
// the client may answer, and it never asks, so relaying a real host's
// challenge could only feed a retry loop the pod cannot win.
func copyHeaders(dst, src http.Header) {
	for name, values := range src {
		if hopByHop[http.CanonicalHeaderKey(name)] {
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

// flushCopy streams the upstream body to the client, flushing as data
// arrives. A failed write ends the copy: the client is gone.
func flushCopy(dst io.Writer, src io.Reader) {
	buffer := make([]byte, 32*1024)
	flusher, canFlush := dst.(http.Flusher)
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			if _, writeErr := dst.Write(buffer[:n]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			return
		}
	}
}
