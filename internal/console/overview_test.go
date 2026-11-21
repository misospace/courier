package console

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/readapi"
)

// The overview fixture set renders the six states issue #824 names: a
// saturated lane, a stale reserved slot, a healthy-empty discovery source, a
// disconnected source, a provider cooldown, and an unsafe-workspace hold. The
// payloads below mirror the read API's wire contract (readapi dto.go) so the
// console's decoder types are pinned against the real projection shapes.

const fixtureOverviewJSON = `{"apiVersion":"courier.misospace.dev/v1","kind":"Overview","payload":{
	"namespace":"courier-system","generatedAt":"2026-11-21T20:00:00Z","freshness":"Fresh",
	"runs":{"total":7,"byPhase":{"Running":2,"Claimed":1,"Pending":2,"NeedsHuman":1,"Done":1}},
	"lanes":{"count":4,"saturated":true,"suspended":true,"cooldown":true,"unhealthy":3},
	"sources":{"count":4,"healthy":1,"empty":1,"stale":0,"unavailable":1,"blocked":1},
	"conditionRuns":{"LifecycleReported":2}}}`

const fixtureLanesJSON = `{"apiVersion":"courier.misospace.dev/v1","kind":"LaneHealthList","payload":{"items":[
	{"name":"ci","availability":"Healthy","freshness":"Unknown","configuredConcurrency":4,"admitted":4,"reserved":5,"saturated":false,
	 "pendingOldestSeconds":120,"derivedFrom":"counts derived from retained run state"},
	{"name":"ci-full","availability":"Healthy","freshness":"Unknown","configuredConcurrency":2,"admitted":2,"reserved":2,"saturated":true},
	{"name":"batch","availability":"Stale","freshness":"Unknown",
	 "notes":["an unadmitted run has held a discovery reservation past the staleness window"],
	 "configuredConcurrency":2,"admitted":0,"reserved":1,"pendingOldestSeconds":5400},
	{"name":"gpu","availability":"Blocked","freshness":"Unknown","notes":["capacity cooldown active"],
	 "configuredConcurrency":1,"cooldown":{"capacityID":"pool-a","reason":"provider quota exhausted","exhaustionCount":3,"nextEligibleAt":"2026-11-21T22:00:00Z"},
	 "suspended":true}]}}`

const fixtureSourcesJSON = `{"apiVersion":"courier.misospace.dev/v1","kind":"SourceHealthList","payload":{"items":[
	{"name":"github","availability":"Healthy","freshness":"Fresh","observedAt":"2026-11-21T19:59:40Z","totalRuns":5,"runsByPhase":{"Running":2}},
	{"name":"dispatch","availability":"Empty","freshness":"Unknown","totalRuns":0},
	{"name":"linear","availability":"Unavailable","freshness":"Stale","observedAt":"2026-11-21T18:00:00Z","totalRuns":2,
	 "notes":["run run-x recorded ForgeUnavailable"]},
	{"name":"jira","availability":"Blocked","freshness":"Fresh","observedAt":"2026-11-21T19:58:00Z","totalRuns":1,"oldestPendingSeconds":90,
	 "notes":["run run-y holds unsafe workspace evidence"]}]}}`

// fixtureCapture records the paths the console requested, so a test can pin
// that the overview page reads exactly the three allowlisted projections.
type fixtureCapture struct {
	paths []string
	auths []string
}

func newFixtureReadAPI(t *testing.T, responses map[string]string, capture *fixtureCapture) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if capture != nil {
			capture.paths = append(capture.paths, r.URL.Path)
			capture.auths = append(capture.auths, r.Header.Get("Authorization"))
		}
		body, ok := responses[r.URL.Path]
		if !ok {
			http.Error(w, `{"error":"unallowlisted path"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
}

// fixtureServer wires a console to a read API serving the given path → body
// responses, and returns the console base URL plus a logged-in client.
func fixtureServer(t *testing.T, responses map[string]string, capture *fixtureCapture) (string, *http.Client) {
	t.Helper()
	backend := newFixtureReadAPI(t, responses, capture)
	t.Cleanup(backend.Close)
	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "ov-read-token")
	client := login(t, base, secrets.token)
	return base, client
}

// TestOverviewRendersAllSixFixtureStates is the issue #824 done-condition
// sweep: the saturated lane, the stale reserved slot, the healthy-empty
// source, the disconnected source, the provider-cooldown lane, and the
// unsafe-workspace hold all render, each with its own badge, and none of the
// degraded states renders as healthy.
func TestOverviewRendersAllSixFixtureStates(t *testing.T) {
	base, client := fixtureServer(t, map[string]string{
		"/v1/overview": fixtureOverviewJSON,
		"/v1/lanes":    fixtureLanesJSON,
		"/v1/sources":  fixtureSourcesJSON,
	}, nil)

	resp, body := get(t, client, base+"/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}

	// Namespace and per-phase drilldowns.
	for _, want := range []string{
		"courier-system",
		`href="/runs?phase=Running"`,
		"Running: 2",
		"Pending: 2",
		"7 run(s) in the namespace",
		"LifecycleReported: 2 run(s)",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("overview missing %q:\n%s", want, body)
		}
	}

	// State 1: the saturated lane reads with its own tag and its real
	// occupancy, never a zero.
	if !strings.Contains(body, `>ci-full<`) || !strings.Contains(body, "2 admitted / 2 configured") || !strings.Contains(body, `class="tag">saturated<`) {
		t.Fatalf("saturated lane state missing:\n%s", body)
	}

	// State 2: the stale reserved slot (admission wedge) reads stale — amber,
	// never the healthy class — with its Pending age and a Pending drilldown.
	if !strings.Contains(body, `class="badge badge-stale">Stale<`) {
		t.Fatalf("stale reservation badge missing:\n%s", body)
	}
	if !strings.Contains(body, "discovery reservation past the staleness window") {
		t.Fatalf("wedge note missing:\n%s", body)
	}
	if !strings.Contains(body, `href="/runs?lane=batch&amp;phase=Pending"`) {
		t.Fatalf("Pending drilldown for the wedged lane missing:\n%s", body)
	}
	if !strings.Contains(body, ">1h30m<") {
		t.Fatalf("oldest Pending age (5400s) missing:\n%s", body)
	}

	// State 3: the healthy-empty discovery source reads Empty with unknown
	// freshness — distinct from Healthy, and with no invented observation.
	if !strings.Contains(body, `class="badge badge-empty">Empty<`) {
		t.Fatalf("empty source badge missing:\n%s", body)
	}
	if !strings.Contains(body, `>dispatch<`) {
		t.Fatalf("empty source row missing:\n%s", body)
	}

	// State 4: the disconnected source reads Unavailable, not zero runs.
	if !strings.Contains(body, `class="badge badge-unavailable">Unavailable<`) {
		t.Fatalf("unavailable source badge missing:\n%s", body)
	}
	if !strings.Contains(body, "ForgeUnavailable") {
		t.Fatalf("disconnected-source note missing:\n%s", body)
	}

	// State 5: the provider-cooldown lane reads Blocked with its cooldown
	// detail and its own tag.
	if !strings.Contains(body, `class="badge badge-blocked">Blocked<`) {
		t.Fatalf("blocked lane badge missing:\n%s", body)
	}
	if !strings.Contains(body, "provider quota exhausted") ||
		!strings.Contains(body, "pool-a") ||
		!strings.Contains(body, "next eligible 2026-11-21T22:00:00Z") ||
		!strings.Contains(body, `class="tag">provider cooldown<`) {
		t.Fatalf("cooldown detail missing:\n%s", body)
	}

	// State 6: the unsafe-workspace hold reads Blocked on the source with its
	// note.
	if !strings.Contains(body, "holds unsafe workspace evidence") {
		t.Fatalf("unsafe-workspace note missing:\n%s", body)
	}

	// The healthy rows still read Healthy, so the sweep is not "everything
	// red". The badge class attribute, not the bare word, is the assertion.
	if !strings.Contains(body, `class="badge badge-healthy">Healthy<`) {
		t.Fatalf("healthy lane/source badges missing:\n%s", body)
	}
	if !strings.Contains(body, `class="badge badge-fresh">Fresh<`) {
		t.Fatalf("fresh snapshot badge missing:\n%s", body)
	}
	// Stale and unavailable never carry the healthy class.
	for _, degraded := range []string{"badge badge-stale", "badge badge-unavailable", "badge badge-blocked"} {
		if strings.Contains(degraded, "healthy") {
			t.Fatalf("degraded class name contains healthy: %s", degraded)
		}
	}

	// The lane/source open links and the reserved-superset note.
	if !strings.Contains(body, `href="/runs?lane=ci-full"`) || !strings.Contains(body, `href="/runs?source=linear"`) {
		t.Fatalf("row drilldown links missing:\n%s", body)
	}
	if !strings.Contains(body, "never add the two counts") {
		t.Fatalf("reserved-superset note missing:\n%s", body)
	}

	// A healthy page refreshes on the bounded interval.
	if !strings.Contains(body, `http-equiv="refresh" content="60"`) {
		t.Fatalf("healthy overview must auto-refresh:\n%s", body)
	}
}

// TestOverviewReadsOnlyAllowlistedProjections pins the requests the overview
// page emits: three GETs on the allowlisted projection paths, each carrying
// the bearer token, and nothing else.
func TestOverviewReadsOnlyAllowlistedProjections(t *testing.T) {
	capture := &fixtureCapture{}
	base, client := fixtureServer(t, map[string]string{
		"/v1/overview": fixtureOverviewJSON,
		"/v1/lanes":    fixtureLanesJSON,
		"/v1/sources":  fixtureSourcesJSON,
	}, capture)
	if _, body := get(t, client, base+"/"); strings.Contains(body, "state-error") {
		t.Fatalf("fixture overview rendered an error state:\n%s", body)
	}
	want := []string{"/v1/overview", "/v1/lanes", "/v1/sources"}
	if strings.Join(capture.paths, ",") != strings.Join(want, ",") {
		t.Fatalf("backend paths = %v, want %v", capture.paths, want)
	}
	for _, auth := range capture.auths {
		if auth != "Bearer ov-read-token" {
			t.Fatalf("backend Authorization = %q, want the bearer token", auth)
		}
	}
}

// TestOverviewPartialSectionFailure pins that one failing detail projection
// replaces only its own section: the runs summary and the surviving list keep
// rendering, and the failed section states its own outage.
func TestOverviewPartialSectionFailure(t *testing.T) {
	canned := map[string]string{
		"/v1/overview": fixtureOverviewJSON,
		"/v1/sources":  fixtureSourcesJSON,
	}
	secrets := newTestSecrets(t)
	backends := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/lanes" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"busy"}`, http.StatusServiceUnavailable)
			return
		}
		body, ok := canned[r.URL.Path]
		if !ok {
			http.Error(w, `{"error":"no"}`, http.StatusNotFound)
			return
		}
		fmt.Fprint(w, body)
	}))
	defer backends.Close()
	_, base := newTestServerWithToken(t, secrets, backends.URL, "tok")
	client := login(t, base, secrets.token)

	_, body := get(t, client, base+"/")
	if !strings.Contains(body, "read budget or past its own deadline") {
		t.Fatalf("lanes section must state its own outage:\n%s", body)
	}
	if !strings.Contains(body, "7 run(s) in the namespace") || !strings.Contains(body, "badge badge-unavailable") {
		t.Fatalf("surviving sections must keep rendering:\n%s", body)
	}
	if strings.Contains(body, "No lane profiles configured") {
		t.Fatalf("a failed lanes read must not render as zero lanes:\n%s", body)
	}
}

// TestOverviewNoDataRendersDistinctEmpty pins the no-data state: a healthy
// read API over an empty namespace renders explicit empties, not an error and
// not a green board.
func TestOverviewNoDataRendersDistinctEmpty(t *testing.T) {
	base, client := fixtureServer(t, map[string]string{
		"/v1/overview": `{"apiVersion":"courier.misospace.dev/v1","kind":"Overview","payload":{"namespace":"ns","freshness":"Fresh","runs":{"total":0},"lanes":{"count":0},"sources":{"count":0}}}`,
		"/v1/lanes":    `{"apiVersion":"courier.misospace.dev/v1","kind":"LaneHealthList","payload":{"items":[]}}`,
		"/v1/sources":  `{"apiVersion":"courier.misospace.dev/v1","kind":"SourceHealthList","payload":{"items":[]}}`,
	}, nil)
	_, body := get(t, client, base+"/")
	for _, want := range []string{
		"No runs in this namespace yet.",
		"No lane profiles configured",
		"No sources configured",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("empty-namespace overview missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "state-error") {
		t.Fatalf("an empty namespace is not an error:\n%s", body)
	}
}

// TestOverviewTruncatedIsStated pins that a truncated namespace scan is
// displayed, so windowed counts are never read as whole-namespace counts.
func TestOverviewTruncatedIsStated(t *testing.T) {
	base, client := fixtureServer(t, map[string]string{
		"/v1/overview": `{"apiVersion":"courier.misospace.dev/v1","kind":"Overview","payload":{"namespace":"ns","freshness":"Fresh","truncated":true,"runs":{"total":2000,"byPhase":{"Running":2000}},"lanes":{"count":0},"sources":{"count":0}}}`,
		"/v1/lanes":    `{"apiVersion":"courier.misospace.dev/v1","kind":"LaneHealthList","payload":{"items":[]}}`,
		"/v1/sources":  `{"apiVersion":"courier.misospace.dev/v1","kind":"SourceHealthList","payload":{"items":[]}}`,
	}, nil)
	_, body := get(t, client, base+"/")
	if !strings.Contains(body, "truncated window") {
		t.Fatalf("truncation must be stated:\n%s", body)
	}
}

// TestOverviewRefusedTokenStatesAuthDenied pins the auth-denied backend
// state: a 401 from the read API renders its own message with the slow
// refresh, and the backend body never echoes.
func TestOverviewRefusedTokenStatesAuthDenied(t *testing.T) {
	const marker = "OVERVIEW-401-BODY-MARKER-5e91"
	secrets := newTestSecrets(t)
	unauthorized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, marker, http.StatusUnauthorized)
	}))
	defer unauthorized.Close()
	_, deniedBase := newTestServerWithToken(t, secrets, unauthorized.URL, "wrong-token")
	deniedClient := login(t, deniedBase, secrets.token)
	_, body := get(t, deniedClient, deniedBase+"/")
	if !strings.Contains(body, "refused this console&#39;s bearer token") {
		t.Fatalf("401 from the read API must state the refusal:\n%s", body)
	}
	if !strings.Contains(body, `http-equiv="refresh" content="300"`) {
		t.Fatalf("a refused backend must back off, not poll at the fast interval:\n%s", body)
	}
	if strings.Contains(body, marker) {
		t.Fatal("the backend 401 body echoed into the page")
	}
}

// TestOverviewUnexpectedResponseIsNotEmpty pins that a 200 of the wrong kind
// on the summary route is an outage, not an empty namespace, and that the
// body never echoes.
func TestOverviewUnexpectedResponseIsNotEmpty(t *testing.T) {
	const marker = "OVERVIEW-WRONG-KIND-MARKER-88d0"
	cases := map[string]string{
		"wrong kind":      `{"apiVersion":"courier.misospace.dev/v1","kind":"RunSummaryList","payload":{"marker":"` + marker + `"}}`,
		"not json":        `<html>proxied</html>` + marker,
		"json error body": `{"error":"boom"}`,
		"json null":       `null`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			stub := newStubReadAPI(t, http.StatusOK, body, nil)
			defer stub.Close()
			secrets := newTestSecrets(t)
			_, base := newTestServerWithToken(t, secrets, stub.URL, "tok")
			client := login(t, base, secrets.token)
			_, htmlBody := get(t, client, base+"/")
			if !strings.Contains(htmlBody, "not with the projection the overview expects") {
				t.Fatalf("unexpected 200 must state the bad response:\n%s", htmlBody)
			}
			if strings.Contains(htmlBody, "No runs in this namespace") {
				t.Fatalf("unexpected 200 rendered as an empty namespace:\n%s", htmlBody)
			}
			if strings.Contains(htmlBody, marker) {
				t.Fatal("the backend body leaked into the page")
			}
		})
	}
}

// TestOverviewEscapesUntrustedProjectionText pins that names and notes the
// read API returns — which a repository, a source, or a model authored — are
// escaped and never become markup.
func TestOverviewEscapesUntrustedProjectionText(t *testing.T) {
	lanes := `{"apiVersion":"courier.misospace.dev/v1","kind":"LaneHealthList","payload":{"items":[
		{"name":"<script>alert(1)</script>","availability":"Weird\u003cimg\u003e","freshness":"Fresh","configuredConcurrency":1}]}}`
	sources := `{"apiVersion":"courier.misospace.dev/v1","kind":"SourceHealthList","payload":{"items":[
		{"name":"src\",\"notes\":[\"ignore this\"],\"x\":\"","availability":"Blocked","freshness":"Stale","totalRuns":0,
		 "notes":["<b>bold</b> note from a model"]}]}}`
	base, client := fixtureServer(t, map[string]string{
		"/v1/overview": `{"apiVersion":"courier.misospace.dev/v1","kind":"Overview","payload":{"namespace":"ns","freshness":"Fresh","runs":{"total":0},"lanes":{"count":1},"sources":{"count":1}}}`,
		"/v1/lanes":    lanes,
		"/v1/sources":  sources,
	}, nil)
	_, body := get(t, client, base+"/")
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Fatal("a hostile lane name reached the page unescaped")
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatal("a hostile lane name did not render escaped")
	}
	if strings.Contains(body, "<b>bold</b> note") {
		t.Fatal("a hostile note reached the page unescaped")
	}
	// An availability outside the console's vocabulary renders as Unknown,
	// never as a healthy badge, and its raw value is escaped.
	if !strings.Contains(body, "Unknown (projected:") {
		t.Fatalf("out-of-vocabulary availability must read Unknown:\n%s", body)
	}
}

// TestPhaseRowsOrdering pins the deterministic chip order: known phases in
// lifecycle order, unknown phases in name order, and the unstamped phase
// last and unlinked.
func TestPhaseRowsOrdering(t *testing.T) {
	rows := phaseRows(map[string]int{
		"Running": 1,
		"Pending": 2,
		"Done":    3,
		"Zeta":    4,
		"Alpha":   5,
		"":        6,
		"Ignored": 0,
	})
	var labels []string
	for _, row := range rows {
		labels = append(labels, fmt.Sprintf("%s=%d", row.Label, row.Count))
	}
	want := "Pending=2,Running=1,Done=3,Alpha (unknown phase)=5,Zeta (unknown phase)=4,no phase recorded=6"
	if got := strings.Join(labels, ","); got != want {
		t.Fatalf("phase rows = %s, want %s", got, want)
	}
	for _, row := range rows {
		if row.Phase != "" && knownPhases[row.Phase] && row.URL == "" {
			t.Errorf("known phase %q must carry a drilldown link", row.Phase)
		}
		if row.Phase == "" && row.URL != "" {
			t.Errorf("the unstamped phase must not carry a link the explorer would reject")
		}
	}
}

// TestOverviewBackendErrorContract pins the Backend methods' no-backend and
// no-token states directly, mirroring the explorer's contract.
func TestOverviewBackendErrorContract(t *testing.T) {
	ctx := context.Background()
	if _, err := NewBackend("", nil, time.Second).Overview(ctx); err != ErrBackendUnavailable {
		t.Fatalf("Overview without a backend URL = %v, want ErrBackendUnavailable", err)
	}
	if _, err := NewBackend("", nil, time.Second).Lanes(ctx); err != ErrBackendUnavailable {
		t.Fatalf("Lanes without a backend URL = %v, want ErrBackendUnavailable", err)
	}
	if _, err := NewBackend("", nil, time.Second).Sources(ctx); err != ErrBackendUnavailable {
		t.Fatalf("Sources without a backend URL = %v, want ErrBackendUnavailable", err)
	}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no request may leave without a token, got %s", r.URL.Path)
	}))
	defer stub.Close()
	if _, err := NewBackend(stub.URL, nil, time.Second).Overview(ctx); err != ErrBackendTokenMissing {
		t.Fatalf("Overview without a token = %v, want ErrBackendTokenMissing", err)
	}
	if _, err := NewBackend(stub.URL, nil, time.Second).Lanes(ctx); err != ErrBackendTokenMissing {
		t.Fatalf("Lanes without a token = %v, want ErrBackendTokenMissing", err)
	}
	if _, err := NewBackend(stub.URL, nil, time.Second).Sources(ctx); err != ErrBackendTokenMissing {
		t.Fatalf("Sources without a token = %v, want ErrBackendTokenMissing", err)
	}
}

// TestOverviewWireContractMirrorsReadAPI guards the console's local path and
// kind constants against drift from the read API package it deliberately does
// not import.
func TestOverviewWireContractMirrorsReadAPI(t *testing.T) {
	pins := []struct {
		name     string
		got      string
		wantFunc func() string
	}{
		{"backendOverviewPath", backendOverviewPath, func() string { return readapi.PathOverview }},
		{"backendLanesPath", backendLanesPath, func() string { return readapi.PathLanes }},
		{"backendSourcesPath", backendSourcesPath, func() string { return readapi.PathSources }},
		{"kindOverview", kindOverview, func() string { return readapi.KindOverview }},
		{"kindLaneList", kindLaneList, func() string { return readapi.KindLaneList }},
		{"kindSourceList", kindSourceList, func() string { return readapi.KindSourceList }},
	}
	for _, pin := range pins {
		if pin.got != pin.wantFunc() {
			t.Errorf("%s = %q, want %q from the read API", pin.name, pin.got, pin.wantFunc())
		}
	}
}
