# Operator console

The operator console (`courier-console`) is a small, server-rendered web UI
for authenticated operators. It is a standalone Go binary, separate from the
manager. The console never talks to the Kubernetes API. It can probe the
operator read API's `/healthz` path when `--backend-url` is configured and,
in token mode, submit contributions to the separate admission API.

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

The form is available only in token mode. Header mode renders an unavailable
state because the form needs the session-bound CSRF token that only token mode
has. All state-changing requests are CSRF-verified. The bearer token is a
file-mounted Secret read only by the console process; it is never rendered or
proxied to the browser. The console uses no JavaScript.

A signed intent cookie carries the idempotency key per browser, bound to the
submitted content, across reloads, retries, and console restarts until the
attempt is definitively settled. A timeout or server error has an uncertain
outcome and is shown as "may have been admitted"; resubmitting identical
content retries with the same key, so it cannot create a duplicate run. Changing
the content starts a new attempt. A deliberate new contribution is the separate
explicit "start a new contribution" action.

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
fails if only one is set. The form is off by default.

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

Secrets are read from files only — the binary takes
`--session-key-file` and `--operator-tokens-file` paths, never environment
values. Rotate a session key by replacing the Secret and rolling the console
pod; rotating the tokens file signs nobody out (sessions are independent of
tokens).

## Headless Courier is preserved

The console block is additive. With `operatorConsole.enabled=false` (the
default) the chart renders exactly what it rendered before: one manager
Deployment, no console Service, no secret mounts, no new flags. Headless
deployments — operators driving Courier through `kubectl` and forge reviews —
change nothing.

## Non-goals

The console does not include a read API client, run tables or detail data,
mutating controls beyond the contribution form, JavaScript, or an Ingress.
