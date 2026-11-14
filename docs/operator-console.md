# Operator console

The operator console (`courier-console`) is a small, server-rendered web UI
for authenticated operators. It is a standalone Go binary, separate from the
manager. It is a scaffold: every page is a placeholder until the operator
read API exists. There is no read API, no data table, and no mutating
control yet. The console never talks to the Kubernetes API. It can only
probe the operator read API's `/healthz` path, and only when
`--backend-url` is configured.

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
NetworkPolicy named `courier-console-header-ingress`. That policy selects
the console pods, declares `policyTypes: [Ingress]`, and admits only the
peers you configured. A workload elsewhere in the cluster has no route to
the console, so a forged identity header never reaches it. The render fails
when the peer list is missing or malformed. A request without the header
gets a plain 403 page.

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
without the secret references its auth mode requires. In header mode, set
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

The console deliberately does not include, yet: a read API client, run
tables or detail data, contribution-admission UI, mutating controls, any
JavaScript, or an Ingress. It is the scaffold those features will land on,
not an early version of them.
