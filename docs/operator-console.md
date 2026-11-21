# Operator console

The operator console (`courier-console`) is a small, server-rendered web UI
for authenticated operators. It is a standalone Go binary, separate from the
manager. The overview page shows source health, lane capacity, and admission
wedges from the operator read API. The run explorer lists CoderRuns from the
operator read API with server-side search and pagination. The run-detail page
is still a placeholder, and there are no mutating controls. The console never
talks to the Kubernetes API. It reads the operator read API over a fixed
allowlist of GET paths: the `/healthz` probe, and the overview, lane, source,
and run-list projections when `--backend-url` and a read API token are both
configured.

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

The session cookie always carries `Secure`. The console does not infer the
flag from its own listener. In every chart-supported deployment the browser's
TLS ends at a proxy, and the hop that reaches the console is plaintext HTTP.
An inference from the listener would leave the flag permanently off, so the
posture is configuration, not observation. `--cookie-secure=false`
(`operatorConsole.auth.cookieSecure=false` in the chart) opts out explicitly.
Do not run the console over a network you do not control without that flag
set to its default.

Every response carries `Content-Security-Policy: default-src 'self';
frame-ancestors 'none'; form-action 'self'; base-uri 'self'`,
`X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, and
`Referrer-Policy: no-referrer`; authenticated pages are
`Cache-Control: no-store`. There is no JavaScript, no external CDN
reference, and no font download.

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
60 seconds when the read API answers, every 300 seconds when it fails, and
never when the console has no configured read API.

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
time). The source table shows status, freshness, last run activity, run count,
and the oldest Pending run, plus the notes the projection supplies, such as a
forge-unavailable diagnostic or an unsafe workspace hold.

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

Each row links to that run's detail page (a separate view) and shows the run's
phase, source, lane, age, pull-request number, and restart count. The console
escapes every value the read API returns, including a name or label that a
repository or model authored. It shows the pull request as a number, not a
link, because the run projection carries no repository URL to trust.

The console reads only a fixed set of GET paths on the configured backend:
`GET /healthz`, `GET /v1/overview`, `GET /v1/lanes`, `GET /v1/sources`, and
`GET /v1/runs`. The path never comes from the browser, so a filter value
cannot steer the request to another path or host. Only the query string of the
run-list request varies, and the console bounds it before it leaves.

The console reaches the read API inside the cluster, so the read API Service
must accept traffic from the console pod. The chart's own NetworkPolicies do
not block this (the console policy is ingress-only, so the console's egress
stays open). If you restrict read API ingress with a NetworkPolicy of your own,
allow the console pod as a source, or the explorer reports that the read API
could not be reached.

## Headless Courier is preserved

The console block is additive. With `operatorConsole.enabled=false` (the
default) the chart renders exactly what it rendered before: one manager
Deployment, no console Service, no secret mounts, no new flags. Headless
deployments — operators driving Courier through `kubectl` and forge reviews —
change nothing.

## Non-goals

The console does not include, yet: run-detail data, a contribution form,
mutating controls, any JavaScript, or an Ingress. The run explorer reads the
read API but changes nothing; the other features grow alongside it.
