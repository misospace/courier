{{- /*
Header-mode ingress boundary for the operator console (#821 review).
header mode adopts the configured identity header verbatim, so the mode's
authentication is only as strong as its source network: only the
authenticating proxy may reach the console. An enabled header mode renders
this policy selecting the console pods, declaring policyTypes: [Ingress],
and allowing only the operator's proxy peers from
operatorConsole.auth.headerAllowedPeers. A NetworkPolicy of Ingress type
that selects pods denies every other source, so a direct ClusterIP call
that forges the header from another in-cluster workload never reaches the
console. The chart fails the header-mode render without at least one peer,
so the boundary ships with the mode rather than in prose.
Peers: each entry is a NetworkPolicy ingress peer with an optional
namespaceSelector and podSelector; each selector is a non-empty label map,
and both selectors in one entry combine with AND. Keys and values are
validated as label keys and label values, and an unknown entry key fails
the render, because a typo must not widen the boundary. The policy declares
Ingress only, so the console's egress backend probe keeps working. kubelet
probes originate on nodes, and NetworkPolicy does not constrain node
traffic. A hostNetwork proxy cannot be a peer here: NetworkPolicy cannot
select node-traffic sources, so run the proxy as pods or add an ipBlock
policy beside this one (docs/operator-console.md).
Token mode renders no policy: it authenticates a shared token over the
wire, not the network position.
*/}}
{{- $operatorConsole := .Values.operatorConsole }}
{{- /* The boundary follows the mode the binary will actually run, not only
operatorConsole.auth.mode: a raw --auth-mode/--trusted-header override in
controllers.console args wins (the hasPrefix guards in common.yaml.tpl skip
the matching injected flag), and controllers.console.enabled without an
operatorConsole block still starts console pods. The effective mode drives
the guards and the policy. cookieSecure is type-checked here with the same
message common.yaml.tpl uses, so a header-mode console can never render
without the check whichever template fails first. */}}
{{- $effectiveMode := $operatorConsole.auth.mode }}
{{- $effectiveTrustedHeader := $operatorConsole.auth.trustedHeader }}
{{- $consoleController := dict }}
{{- $controllers := index .Values "controllers" }}
{{- if $controllers }}
{{- $named := index $controllers "console" }}
{{- if $named }}{{- $consoleController = $named }}{{- end }}
{{- end }}
{{- $rawArgs := list }}
{{- $containers := index $consoleController "containers" }}
{{- if $containers }}
{{- $namedContainer := index $containers "console" }}
{{- if $namedContainer }}{{- $rawArgs = default (list) (index $namedContainer "args") }}{{- end }}
{{- end }}
{{- range $arg := $rawArgs }}
{{- if hasPrefix "--auth-mode=" $arg }}{{- $effectiveMode = trimPrefix "--auth-mode=" $arg }}{{- end }}
{{- if hasPrefix "--trusted-header=" $arg }}{{- $effectiveTrustedHeader = trimPrefix "--trusted-header=" $arg }}{{- end }}
{{- end }}
{{- $consoleControllerEnabled := false }}
{{- $enabledRaw := index $consoleController "enabled" }}
{{- if eq (toString $enabledRaw) "true" }}{{- $consoleControllerEnabled = true }}{{- end }}
{{- $consoleLive := or $operatorConsole.enabled $consoleControllerEnabled }}
{{- if and $consoleLive (eq $effectiveMode "header") $effectiveTrustedHeader }}
{{- if not (kindIs "bool" $operatorConsole.auth.cookieSecure) }}
{{- fail "operatorConsole.auth.cookieSecure must be a boolean" }}
{{- end }}
{{- $peers := $operatorConsole.auth.headerAllowedPeers }}
{{- if not $peers }}
{{- fail "operatorConsole.enabled in header mode requires operatorConsole.auth.headerAllowedPeers: the chart enforces the proxy-only boundary with a default-deny-ingress NetworkPolicy" }}
{{- end }}
{{- $renderedPeers := list }}
{{- range $peer := $peers }}
{{- if not (kindIs "map" $peer) }}
{{- fail "operatorConsole.auth.headerAllowedPeers entries must be maps with namespaceSelector and/or podSelector" }}
{{- end }}
{{- $entry := dict }}
{{- range $selectorName, $selector := $peer }}
{{- if not (has $selectorName (list "namespaceSelector" "podSelector")) }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers entry key %q is not namespaceSelector or podSelector" $selectorName) }}
{{- end }}
{{- if not (kindIs "map" $selector) }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers %s must be a map of label keys and values" $selectorName) }}
{{- end }}
{{- if not $selector }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers %s must carry at least one label: an empty selector matches every namespace and pod" $selectorName) }}
{{- end }}
{{- $matchLabels := dict }}
{{- range $key, $value := $selector }}
{{- $keyName := $key }}
{{- $keyPrefix := "" }}
{{- $keyHasPrefix := contains "/" $key }}
{{- if $keyHasPrefix }}
{{- $parts := splitList "/" $key }}
{{- $keyName = last $parts }}
{{- $keyPrefix = join "/" (initial $parts) }}
{{- end }}
{{- if or (not $key) (gt (len $key) 253) (regexMatch "\\s" $key) }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers %s key %q is not a valid label key" $selectorName $key) }}
{{- end }}
{{- if or (not (regexMatch "^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$" $keyName)) (gt (len $keyName) 63) }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers %s key %q is not a valid label key" $selectorName $key) }}
{{- end }}
{{- if $keyHasPrefix }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$" $keyPrefix)) (gt (len $keyPrefix) 253) }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers %s key %q is not a valid label key" $selectorName $key) }}
{{- end }}
{{- end }}
{{- $valueText := toString $value }}
{{- if and $valueText (or (not (regexMatch "^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$" $valueText)) (gt (len $valueText) 63)) }}
{{- fail (printf "operatorConsole.auth.headerAllowedPeers %s value %q for key %q is not a valid label value" $selectorName $valueText $key) }}
{{- end }}
{{- $_ := set $matchLabels $key $valueText }}
{{- end }}
{{- $_ := set $entry $selectorName (dict "matchLabels" $matchLabels) }}
{{- end }}
{{- if not $entry }}
{{- fail "each operatorConsole.auth.headerAllowedPeers entry must set namespaceSelector and/or podSelector" }}
{{- end }}
{{- $renderedPeers = append $renderedPeers $entry }}
{{- end }}
{{- include "bjw-s.common.loader.init" . }}
{{- $fullname := include "bjw-s.common.lib.chart.names.fullname" . }}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ printf "%s-console-header-ingress" $fullname }}
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/controller: console
    {{- include "bjw-s.common.lib.metadata.allLabels" . | trim | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/controller: console
      {{- include "bjw-s.common.lib.metadata.selectorLabels" . | nindent 6 }}
  policyTypes:
    - Ingress
  ingress:
    - from:
        {{- toYaml $renderedPeers | nindent 8 }}
      ports:
        - protocol: TCP
          port: {{ int $operatorConsole.port }}
{{- end }}
