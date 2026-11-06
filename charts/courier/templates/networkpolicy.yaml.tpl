{{- /*
  Deployment-level model-gateway egress policy (#763).

  The operator's per-run NetworkPolicy covers one run's pods and one run's
  destinations. The deployment's model gateway belongs to no run, so a
  secure install whose gateway path the operator's ipBlock slot does not
  cover needs a policy that lets the trusted control/coordinator pods
  reach it.

  The source selector is not configurable: the chart hardcodes the
  operator's coordinator label keys (courier.misospace.dev/component and
  app.kubernetes.io/component = coordinator). A chart test asserts the
  rendered selector's key set is exactly those keys, that each key is
  stamped on a coordinator pod with the same value, and that the selector
  matches no worker, broker, or probe pod.

  The destination comes from values: secure.modelGateway.networkPolicy
  carries the gateway's namespace, pod selector, and port. Every value is
  rendered as a string, and every key and value is validated as a label
  key and a label value, so a malformed selector fails the render instead
  of the API server.

  The policy is rendered into secure.runNamespace — where the operator
  creates the run's control pods — and not into the release namespace,
  because a NetworkPolicy only ever constrains pods in its own namespace.

  The egress rules of several policies that select the same pod are unioned,
  so this policy adds the gateway path to the per-run control egress and
  never replaces it. It declares policyTypes: [Egress] only and carries no
  ingress rule, so it never restricts ingress on the selected pods.
*/}}
{{- if .Values.secure.enabled }}
{{- $secure := .Values.secure }}
{{- $modelGateway := default (dict) $secure.modelGateway }}
{{- $networkPolicy := default (dict) $modelGateway.networkPolicy }}
{{- $namespace := toString (default "" $networkPolicy.namespace) }}
{{- $selectorKind := kindOf $networkPolicy.podSelector }}
{{- if and (ne $selectorKind "invalid") (ne $selectorKind "map") }}
{{- fail "secure.modelGateway.networkPolicy.podSelector must be a map of label keys to values" }}
{{- end }}
{{- $podSelector := default (dict) $networkPolicy.podSelector }}
{{- $portRaw := $networkPolicy.port }}
{{- $portKind := kindOf $portRaw }}
{{- $portPresent := ne $portKind "invalid" }}
{{- if and $portPresent (eq $portKind "string") }}
{{- $portPresent = ne $portRaw "" }}
{{- end }}
{{- if $portPresent }}
{{- $portIsNumber := or (eq $portKind "int64") (eq $portKind "int") (eq $portKind "float64") }}
{{- if and (eq $portKind "string") (regexMatch "^[0-9]+$" $portRaw) }}
{{- $portIsNumber = true }}
{{- end }}
{{- if not $portIsNumber }}
{{- fail "secure.modelGateway.networkPolicy.port must be a number" }}
{{- end }}
{{- end }}
{{- $port := int (default 0 $portRaw) }}
{{- $namespaceSet := ne $namespace "" }}
{{- $selectorSet := gt (len $podSelector) 0 }}
{{- $portSet := ne $port 0 }}
{{- if or $namespaceSet $selectorSet $portSet }}
{{- if not $namespaceSet }}
{{- fail "secure.modelGateway.networkPolicy.namespace is required when the block is configured" }}
{{- end }}
{{- if not $selectorSet }}
{{- fail "secure.modelGateway.networkPolicy.podSelector is required when the block is configured" }}
{{- end }}
{{- if not $portSet }}
{{- fail "secure.modelGateway.networkPolicy.port is required when the block is configured" }}
{{- end }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $namespace)) (gt (len $namespace) 63) }}
{{- fail (printf "secure.modelGateway.networkPolicy.namespace %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $namespace) }}
{{- end }}
{{- if ne (float64 $portRaw) (float64 $port) }}
{{- fail "secure.modelGateway.networkPolicy.port must be a whole number" }}
{{- end }}
{{- if or (lt $port 1) (gt $port 65535) }}
{{- fail "secure.modelGateway.networkPolicy.port must be between 1 and 65535" }}
{{- end }}
{{- $runNamespace := toString $secure.runNamespace }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $runNamespace)) (gt (len $runNamespace) 63) }}
{{- fail (printf "secure.runNamespace %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $runNamespace) }}
{{- end }}
{{- $renderedSelector := dict }}
{{- range $key, $value := $podSelector }}
{{- $keyName := $key }}
{{- $keyPrefix := "" }}
{{- $keyHasPrefix := contains "/" $key }}
{{- if $keyHasPrefix }}
{{- $parts := splitList "/" $key }}
{{- $keyName = last $parts }}
{{- $keyPrefix = join "/" (initial $parts) }}
{{- end }}
{{- if or (not $key) (gt (len $key) 253) (regexMatch "\\s" $key) }}
{{- fail (printf "secure.modelGateway.networkPolicy.podSelector key %q is not a valid label key" $key) }}
{{- end }}
{{- if or (not (regexMatch "^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$" $keyName)) (gt (len $keyName) 63) }}
{{- fail (printf "secure.modelGateway.networkPolicy.podSelector key %q is not a valid label key" $key) }}
{{- end }}
{{- if $keyHasPrefix }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$" $keyPrefix)) (gt (len $keyPrefix) 253) }}
{{- fail (printf "secure.modelGateway.networkPolicy.podSelector key %q is not a valid label key" $key) }}
{{- end }}
{{- end }}
{{- $valueText := toString $value }}
{{- if and $valueText (or (not (regexMatch "^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$" $valueText)) (gt (len $valueText) 63)) }}
{{- fail (printf "secure.modelGateway.networkPolicy.podSelector value %q for key %q is not a valid label value" $valueText $key) }}
{{- end }}
{{- $_ := set $renderedSelector $key $valueText }}
{{- end }}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ .Release.Name }}-model-gateway-egress
  namespace: {{ $runNamespace | quote }}
  labels:
    app.kubernetes.io/managed-by: courier
spec:
  podSelector:
    matchLabels:
      courier.misospace.dev/component: coordinator
      app.kubernetes.io/component: coordinator
  policyTypes:
    - Egress
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {{ $namespace }}
          podSelector:
            matchLabels:
              {{- toYaml $renderedSelector | nindent 14 }}
      ports:
        - protocol: TCP
          port: {{ $port }}
{{- end }}
{{- end }}
