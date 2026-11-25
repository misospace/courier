# Operator console

The operator console (`courier-console`) is a small, server-rendered web UI
for authenticated operators. It is a standalone Go binary, separate from the
manager. The overview page shows source health, lane capacity, and admission
wedges from the operator read API. The run explorer lists CoderRuns from the
operator read API with server-side search and pagination, and each run has a
read-only detail page with status and a bounded event timeline. The console
never talks to the Kubernetes API. It reads the operator read API over a fixed
allowlist of GET paths: the `/healthz` probe, and the overview, lane, source,
run-list, and named run-detail projections when `--backend-url` and a read API
token are both configured. In token mode it can also submit contributions to
the separate admission API, the only mutating surface.

## Authentication modes

The console serves authenticated operators only. There are no per-user
roles; `IsOperator` in `internal/console/auth.go` is the single seam where a
future per-identity policy would land.

**Token mode** (default). Operators POST a shared token to `/login`; a token
that matches one of the accepted operator tokens exchanges for a session
cookie (a random id, HMAC-signed with the session key, `HttpOnly`,
`SameSite=Lax`, `Secure`, 12-hour expiry). Sessions live in the console
process's memory: a restart signs everyone out. Unauthenticated requests to
any page are redirected to `/login`.

**Header mode**. The console trusts an upstream authenticating proxy and
takes the operator identity from one configured header
(`--trusted-header`, e.g. `X-Forwarded-User`). The mode is safe only when
the proxy is the only client that can reach the console, so the Helm chart
enforces that rule instead of documenting it. Header mode requires
`operatorConsole.auth.headerAllowedPeers`, and the chart renders a
NetworkPolicy named `courier-console-header-ingress` under the release's
fullname prefix. That policy selects the console pods, declares
`policyTypes: [Ingress]`, and admits only the peers you configured. A
workload elsewhere in the cluster has no admitted route to the console, so
a forged identity header never reaches it. NetworkPolicy enforcement needs
a CNI that enforces NetworkPolicy; verify your cluster enforces it before
you rely on the boundary. The render fails when the peer list is missing or
malformed. A request without the header gets a plain 403 page.

CSRF groundwork is in place: each session's CSRF token is derived as
`HMAC(sessionKey, sessionID + ":csrf")`, embedded in rendered forms, and
verified on state-changing POSTs. Logout enforces it today; future
mutating controls must too. The first login POST carries no CSRF token yet,
because a fresh browser session has none to present. Add login-CSRF
protection before the console gains state a login can change.

The session and contribution-intent cookies carry `Secure`. The console does
not infer the flag from its own listener. In every chart-supported deployment
the browser's TLS ends at a proxy, and the hop that reaches the console is
plaintext HTTP. An inference from the listener would leave the flag permanently
off, so the posture is configuration, not observation.
`--cookie-secure=false` (`operatorConsole.auth.cookieSecure=false` in the chart)
opts out explicitly. Do not run the console over a network you do not control
without that flag set to its default.

Every response carries `Content-Security-Policy: default-src 'self';
frame-ancestors 'none'; form-action 'self'; base-uri 'self'`,
`X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, and
`Referrer-Policy: no-referrer`; authenticated pages are
`Cache-Control: no-store`. There is no JavaScript, no external CDN
reference, and no font download.

## Submitting contributions (#829)

Token-mode consoles can render a bounded server-side form for approved one-off
contributions. The console submits `POST /v1/contributions` to the operator's
contribution admission API (#708). It does no authority validation; the same
server-side admission path used by `courierctl contribute` decides everything.

The form fields are lane, base repository (`owner/name`), base ref, approved
head/fork repository (`owner/name`), natural-language goal, and an optional
upstream issue URL or number as untrusted context. Supplying an issue is not an
ownership claim and never mutates the upstream issue. The form does not expose
target host, git or API credentials, namespace, or fresh/debug idempotency
escapes.

The form is available only in token mode, because the form needs the
session-bound CSRF token that only token mode has. A manually deployed
header-mode console renders an unavailable state; the chart goes further and
fails the render when the contribution pair is set with header mode, so a
header-mode release can never ship the unused write credential. All
state-changing requests are CSRF-verified. The bearer token is a file-mounted
Secret read only by the console process; it is never rendered or proxied to
the browser. The console uses no JavaScript.

A signed intent cookie carries the idempotency key per browser, bound to the
submitted content, across reloads, retries, and console restarts; once the
attempt definitively settles, a later page view re-signs the cookie so the
key stays in the browser instead of being dropped. Every submission that reaches the
admission call commits and flushes the cookie first, so even a lost response
or a console restart mid-submission leaves the browser holding the key. A submission
whose content differs from a still-unsettled attempt already in the cookie
is refused before a key is minted or the admission API is contacted, because
the browser's single durable key slot already belongs to that attempt. The
operator resubmits the earlier content unchanged, or abandons the attempt
with the explicit "start a new contribution" action, which clears the cookie
and frees this session's server-side key slot — settled or not — so
resubmitting that content afterwards mints a fresh key and requests
genuinely new work (an abandoned attempt that had been admitted keeps its
run: the action ends the browser's attempt, never the server-side work).
Retrying a settled attempt needs no action: the retained cookie replays its
key, so an accidental identical resubmission cannot duplicate a run, and the
explicit action is what distinguishes a deliberate new attempt from such a
retry. Key reuse is scoped to the authenticated operator session: identical
content arriving from a different session with no intent cookie is an
independent submission and mints its own key, so one operator's submission
can never resume another's admitted run. The
settled flag is signed while this console holds the settlement record; a
restart before that page view leaves the attempt looking
unsettled, and resubmitting the content unchanged replays the key and
re-settles it. The console supports exactly one replica because contribution attempt
state is per-process. The cookie signature uses the session key, so
rotating that key discards the browser's intent cookie, and the rotation's
pod restart also drops every in-memory session and minter slot: the operator
re-logs in under a new session id and an identical resubmission starts a
fresh attempt with a new key. (An earlier definitive outcome whose other key
record lived only in that restart-lost memory can therefore admit a
duplicate run on such a resubmission; the browser's signed cookie is the
key's durable home, and rotation destroys it.) A
timeout or server error has an uncertain outcome and is shown as "may have been
admitted"; while the key survives, resubmitting identical content retries with
the same key, so it cannot create a duplicate run. Different content stays
blocked until the earlier attempt settles or the operator abandons it with
the explicit "start a new contribution" action. A submission the console
cannot encode under its own 32 KiB request
bound is refused locally, before any network contact.

Outcomes stay distinct: created (`201`), resumed (`200`), policy or validation
refusal (`400`, `403`, or `409`, with the server's message), rejected
credentials (`401`), unreachable contribution API (nothing admitted only when
failure is proven to precede sending the request, such as DNS resolution or a
dial-phase network failure), and uncertain (may have been admitted). Timeouts
and server errors remain uncertain. Admission is never presented as PR success.
The result links to the run's status view at `/runs/<run>`. The bearer hop to the
admission API is plaintext when its configured URL uses `http`; configure a
cluster-internal Service URL.

Enable the manager's `contributeAPI.enabled` with its `keySecret`, then set
`operatorConsole.contribute.url` and `tokenSecret` together. The chart render
fails if only one is set; a raw `--contribute-url` or `--contribute-token-file`
arg in `controllers.console.containers.console.args` counts toward that pair,
and a raw token-file path is the operator's own mount to provide. The form is
off by default.

```sh
kubectl create secret generic courier-contribute-token \
  --from-literal=token="$(head -c 32 /dev/urandom | base64)" \
  --namespace courier-console-ns

helm upgrade --install courier charts/courier \
  --namespace courier-console-ns \
  --set contributeAPI.enabled=true \
  --set contributeAPI.keySecret=courier-contribute-api \
  --set operatorConsole.enabled=true \
  --set operatorConsole.auth.sessionKeySecret=courier-console-session \
  --set operatorConsole.auth.operatorTokensSecret=courier-console-tokens \
  --set operatorConsole.contribute.url=http://courier-contribute-api \
  --set operatorConsole.contribute.tokenSecret=courier-contribute-token
```

## Enabling via Helm

The console is disabled by default. Enabling it renders a second controller
Deployment and a ClusterIP-only Service named `courier-console` (port 80 →
the console port). No Ingress is rendered. In header mode the chart also
renders the `courier-console-header-ingress` NetworkPolicy described above;
token mode renders no policy. Exposing the console is a deployment concern,
and the default posture is ClusterIP-only with no public ingress.

```sh
kubectl create namespace courier-console-ns  # or let --create-namespace do it

# Provision the secrets in the release namespace first.
kubectl create secret generic courier-console-session \
  --from-literal=key="$(head -c 32 /dev/urandom | base64)" \
  --namespace courier-console-ns
kubectl create secret generic courier-console-tokens \
  --from-literal=tokens="$(head -c 32 /dev/urandom | base64)" \
  --namespace courier-console-ns

helm upgrade --install courier charts/courier \
  --namespace courier-console-ns \
  --create-namespace \
  --set operatorConsole.enabled=true \
  --set operatorConsole.auth.sessionKeySecret=courier-console-session \
  --set operatorConsole.auth.operatorTokensSecret=courier-console-tokens
```

The rendered chart fails closed at upgrade time when the console is enabled
without the secret references its auth mode requires. The guards follow the
mode the binary will actually run: a raw `--auth-mode` override in
`controllers.console.containers.console.args` wins over
`operatorConsole.auth.mode`, and the secret requirements, mounts, and
injected flags follow that effective mode. In header mode, set
the mode, the header, and at least one proxy peer instead; no secret volumes
are mounted in that mode:

```sh
helm upgrade --install courier charts/courier \
  --namespace courier-console-ns \
  --set operatorConsole.enabled=true \
  --set operatorConsole.auth.mode=header \
  --set operatorConsole.auth.trustedHeader=X-Forwarded-User \
  --set-json 'operatorConsole.auth.headerAllowedPeers=[{"namespaceSelector":{"kubernetes.io/metadata.name":"proxy-ns"},"podSelector":{"app.kubernetes.io/name":"oauth2-proxy"}}]'
```

Point the peer selectors at the namespace and pods of your authenticating
proxy. A proxy that runs with hostNetwork cannot be a NetworkPolicy peer,
because NetworkPolicy cannot select node-traffic sources. Run that proxy as
pods, or add an ipBlock policy beside the chart's.

Secrets are read from files only. The binary takes `--session-key-file`,
`--operator-tokens-file`, and (for the explorer) `--readapi-token-file` paths,
never environment values. Rotate a session key by replacing the Secret and
rolling the console pod; rotating the tokens file signs nobody out (sessions
are independent of tokens).

## Overview

The Overview page reads three projections on the server: the namespace
summary (`GET /v1/overview`), the lane health list (`GET /v1/lanes`), and the
source health list (`GET /v1/sources`). It renders them as a run summary with
per-phase links, a lane table, and a source table. There is no JavaScript and
no Kubernetes watch. The page reloads itself with a meta-refresh tag: every
60 seconds when the read API answers, every 300 seconds when any projection
call fails, and never when the console has no configured read API URL or
token.

Every state carries its own label and color. Stale, unavailable, blocked, and
unknown readings never look green. A source with no runs reads Empty, which
is distinct from Healthy. Data the read API cannot project, such as a
source's discovery polling state or a lane profile that does not exist, reads
Unknown or Unsupported. The page never shows it as a zero.

Each state links to the runs it covers. A phase chip opens the run explorer
filtered to that phase. A lane row opens the explorer filtered to that lane,
and a lane with a Pending run also links to `Pending` runs on that lane, so a
stale reservation or admission wedge is one click from the runs that hold it.
A source row opens the explorer filtered to that source.

The lane table shows configured concurrency, admitted runs, reserved runs, and
the age of the oldest Pending run. Reserved includes admitted, so the page
never adds the two. Cooldown and suspension appear as their own labels with
the cooldown detail (capacity id, reason, exhaustion count, and next eligible
time). The source table shows status, freshness, last run activity, the last
recorded success (most recent `Done` or `AlreadyDone` run) and the last
recorded error (most recent `Failed` run), run count, and the oldest Pending
run, plus the notes the projection supplies, such as a forge-unavailable
diagnostic or an unsafe workspace hold. A source with runs but no recorded
outcome of a kind says so in words, and the healthy-empty source reads "no
runs yet" — a wording no source with runs gets. Success and error come from
run outcomes only: runs parked for review, waiting on a human, or blocked
externally are neither, and a source adapter's discovery polling state lives
in operator metrics, not the read API.

## Run explorer

The Run page lists CoderRuns from the operator read API. It reads the read
API's run projection (`GET /v1/runs`) on the server and renders it. There is
no JavaScript, so the filters (lane, source, phase) and the page number live
in the URL. Every view is a link an operator can share.

To turn the explorer on, point the console at the read API and give it the
read API's bearer token. The token Secret holds the same bearer token the read
API uses; its entry key defaults to `token`.

```sh
helm upgrade --install courier charts/courier \
  --namespace courier-console-ns \
  --set operatorConsole.enabled=true \
  --set operatorConsole.auth.sessionKeySecret=courier-console-session \
  --set operatorConsole.auth.operatorTokensSecret=courier-console-tokens \
  --set operatorConsole.backendURL=http://courier-read-api \
  --set operatorConsole.readAPITokenSecret=courier-read-api-token
```

The token Secret is optional. With no token, or with no `--backend-url`, the
Overview and Run pages say their data is not configured, and the console sends
no unauthenticated projection request. The token reaches the console only as a
file path read from a mounted Secret, and it never travels to the browser.

Rotate the read API token by replacing the Secret and rolling the console pod.
The console reads the token once at startup and mounts it with `subPath`, which
Kubernetes does not refresh in a running pod, so a rotated token takes effect
only after the pod restarts. Until then the Overview and Run pages report that
the read API refused the token.

A read API that is down, busy, or that rejects the token shows its own message.
An operator never reads an outage as an empty namespace.

Each row links to that run's detail page and shows the run's phase, source,
lane, age, pull-request number, and restart count. The detail page shows the
allowlisted per-run status blocks, retention holds, and a bounded timeline of
recorded timestamps. Each timeline entry names the run-object field that holds
its timestamp and the authority that writes it — the operator, the run's
coordinator, or the API server — and the page states separately when the read
model observed the run, so a recorded event time and an observation time are
never the same claim. The retention block states the phase's retention window and
the instant it expires, and it names an unsafe-workspace or capacity-recovery
hold when the operator is still holding that attempt's partial work. The
per-repository audit floor is not a per-run fact, so the page mentions it only
for the phases that consume a floor slot and never implies it elsewhere. The
console
escapes every value the read API returns, including a name or label that a
repository or model authored. It shows the pull request as a number, not a
link, because the run projection carries no repository URL to trust.

The console reads a fixed set of GET paths on the configured backend:
`GET /healthz`, `GET /v1/overview`, `GET /v1/lanes`, `GET /v1/sources`,
`GET /v1/runs`, and `GET /v1/runs/{name}`. The detail name must pass
DNS-subdomain validation before it is composed into the path; it cannot add a
path segment or steer the request to another host. Only the query string of the
run-list request varies, and the console bounds it before it leaves.

The console reaches the read API inside the cluster, so the read API Service
must accept traffic from the console pod. The chart's own NetworkPolicies do
not block this (the console policy is ingress-only, so the console's egress
stays open). If you restrict read API ingress with a NetworkPolicy of your own,
allow the console pod as a source, or the explorer reports that the read API
could not be reached.

## Contribution status

The console tracks one contribution run at a time. Two pages serve it.
`/contributions` looks a run up by name and lists the contributions this
browser viewed most recently. `/contributions/<run>` shows one run's status.
An operator adds `?namespace=` to pin the admission namespace; a successful
contribution submission links straight to its run's status page with the
namespace from the admission response pinned, so an admitted or resumed run
is one click from its status on any deployment that configures the status
backend. Every view is a
link, so a shared link opens the same view and a reload keeps it. The console
renders these pages on the server and runs no client-side JavaScript. If a
read's `observedAt` sits far from the console's own clock — a cached or
stalled upstream, or badly skewed clocks — the page keeps the values but
labels the read stale; a same-name response from a different namespace, or a
stamp that does not parse, is treated as a misconfiguration rather than
rendered.

The status comes from the contribution status API, not the operator read API.
The console calls `GET /v1/contributions/<run>` on the manager's contribution
listener, the same listener that admits contributions when
`contributeAPI.enabled`. The call uses the status-read bearer token — the
same token the Secret named by `contributeAPI.readKeySecret` holds (see
[External contributions](./external-contributions.md)). The token is separate
on purpose. A status-read token cannot admit a contribution, and an admission
token cannot read status. The console sends the token server-side only, and it
never reaches the browser.

To turn the view on, the deployment already runs the contribution listener,
then you point the console at it. Set `contributeAPI.enabled` with its
`readKeySecret` first. Then set `operatorConsole.contributeStatus.url` and
`operatorConsole.contributeStatus.tokenSecret` together. The chart fails the
render when you set only one. The token Secret holds the same bearer token
that `contributeAPI.readKeySecret` names, and its entry key defaults to
`token`. A raw `--contribute-status-url` or `--contribute-status-token-file`
arg in `controllers.console.containers.console.args` wins over these values.
The Secret is the only thing that puts the token file in the container, so a
raw `--contribute-status-token-file` arg also needs `tokenSecret` set and must
name `/etc/courier/console/contribute-status-token`, the only path the chart
mounts the Secret at; without `tokenSecret`, or with any other path, the chart
fails the render. With no URL or token the page says it is not
configured and sends no request.

```sh
helm upgrade --install courier charts/courier \
  --namespace courier-console-ns \
  --set operatorConsole.enabled=true \
  --set operatorConsole.auth.sessionKeySecret=courier-console-session \
  --set operatorConsole.auth.operatorTokensSecret=courier-console-tokens \
  --set contributeAPI.enabled=true \
  --set contributeAPI.readKeySecret=courier-contribute-read-key \
  --set operatorConsole.contributeStatus.url=http://courier-contribute \
  --set operatorConsole.contributeStatus.tokenSecret=courier-contribute-status-token
```

The page reports what the run recorded and never guesses a phase:

- `Pending`, `Claimed`, and `Running` show as admitted or executing.
- `Verifying` shows verifying. A `Verifying` run with a CI park says it waits
  on CI.
- `AwaitingReview` and `HandedOff` say a pull request awaits review.
- `NeedsHuman` says an operator must act.
- `BlockedExternal` says Courier waits on something outside Courier.
- `Done` and `AlreadyDone` say the run finished.
- `Failed` shows the bounded cause, category and outcome, when the run
  recorded one.
- An unrecognized phase renders as the raw word marked unknown, and a run
  with no phase stamped yet renders as unknown.

A published pull request and a `Done` run both mean one thing only: Courier
opened a pull request. The page never says "merged" or "successful". Courier
does not track merges. It shows the pull request as a link only when the value
is a plain `https` URL with no embedded credentials, and renders anything else
as escaped text. When the pull request links that way, the base repository, the
head repository, and the commit link too. The console builds these links only
from the pull request link's own host, a repository of the form `owner/name`,
and a hexadecimal commit SHA. The branch and any other value render as text.
The console builds every page with `html/template` and escapes every
string a user or a model wrote. Nothing from the API becomes markup or a URL
scheme.

The page tells how fresh its data is. It shows the read's `observedAt` time and
the controller's recorded admitted, started, phase-entered, and finished times.
It shows the age of the current phase, which is approximate and clamped.
Auto-refresh is a meta-refresh tag, not JavaScript. A fast interval applies
while the run is active. A slow interval applies while the run is parked for
review, for a human, for CI, or on something external. The tag is off once the run is
terminal, and off in a static state such as not configured, a refused
credential, not found, or a namespace outside Courier. A transient backend
failure retries on a slow backoff, so the page does not hammer the listener.

An error status renders honestly. A 404 covers three cases, and the page does
not say which: the run was never admitted, the run finished and retention
reaped it, or the run is not a contribution run. A run owned by another source
reads the same way. A 403 says the namespace is outside the namespaces Courier
owns. A 401 says the console's status credential was refused. A 503 says the
read is busy or timed out, and the page retries slowly. A `200` that does not
describe the requested run, such as an empty JSON object from a wrong status
URL, shows a static misconfiguration message. The page does not render its
fields as a run with blank values, and it stores no history entry for it.

A successful read stores a short history. The console sets a signed cookie
(HMAC with the session key, `HttpOnly`, `SameSite=Lax`) that holds up to a few
of the most recent namespace-and-run pairs for this browser. The cookie holds
links only, never lifecycle data. A cookie whose signature does not verify is
dropped whole, and a signed entry that is not a valid namespace-and-run pair is
skipped. The cookie is cleared on sign-out. It never carries the run's goal
text or any credential. A header-mode console has no session key, so it has no
history list, and its detail page still works.

The run explorer links an ad-hoc run's row, source `adhoc`, to its
contribution status page, pinned to the namespace the row names. The console's
other boundaries still hold: it imports
only the standard library, never talks to Kubernetes, renders no client-side
JavaScript, and reads tokens only from mounted files.

## Headless Courier is preserved

The console block is additive. With `operatorConsole.enabled=false` (the
default) the chart renders exactly what it rendered before: one manager
Deployment, no console Service, no secret mounts, no new flags. Headless
deployments — operators driving Courier through `kubectl` and forge reviews —
change nothing.

## Non-goals

The console does not include, yet: mutating controls beyond the contribution
form, any JavaScript, or an Ingress. The console reads the read API but changes
nothing; other features grow alongside it.
