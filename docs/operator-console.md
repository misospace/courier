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
`SameSite=Lax`, `Secure` under TLS, 12-hour expiry). Sessions live in the
console process's memory: a restart signs everyone out. Unauthenticated
requests to any page are redirected to `/login`.

**Header mode**. The console trusts an upstream authenticating proxy and
takes the operator identity from one configured header
(`--trusted-header`, e.g. `X-Forwarded-User`). Only the proxy that strips or
sets that header may reach the console. A client that reaches the console
directly can forge an identity. A request without the header gets a plain
403 page.

CSRF groundwork is in place: each session's CSRF token is derived as
`HMAC(sessionKey, sessionID + ":csrf")`, embedded in rendered forms, and
verified on state-changing POSTs. Logout enforces it today; future
mutating controls must too. The first login POST carries no CSRF token yet,
because a fresh browser session has none to present. Add login-CSRF
protection before the console gains state a login can change.

The session cookie's `Secure` flag keys off TLS that terminates on the
console pod itself. Behind the default plaintext, ClusterIP-only Service
the flag is not set, so keep the console off any path that crosses an
untrusted network until TLS terminates on the pod or the proxy model
covers the transport.

Every response carries `Content-Security-Policy: default-src 'self';
frame-ancestors 'none'; form-action 'self'; base-uri 'self'`,
`X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, and
`Referrer-Policy: no-referrer`; authenticated pages are
`Cache-Control: no-store`. There is no JavaScript, no external CDN
reference, and no font download.

## Enabling via Helm

The console is disabled by default. Enabling it renders a second controller
Deployment and a ClusterIP-only Service named `courier-console` (port 80 →
the console port). No Ingress is rendered. Exposing the console is a
deployment concern, and the default posture is ClusterIP-only with no public
ingress.

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
`operatorConsole.auth.mode=header` and
`operatorConsole.auth.trustedHeader=X-Forwarded-User` instead; no secret
volumes are mounted in that mode.

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
