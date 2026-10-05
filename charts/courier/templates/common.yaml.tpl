---
{{- include "bjw-s.common.loader.init" . }}

{{- /*
  The common library rejects an empty image tag with no digest. The chart
  keeps tag "" in values.yaml (consumers pin the release they want), so
  default the tag to the chart appVersion at render time when neither tag
  nor digest is set.
*/}}
{{- $container := .Values.controllers.main.containers.main }}
{{- $img := $container.image }}
{{- if and (not $img.tag) (not $img.digest) }}
{{- $_ := set $img "tag" .Chart.AppVersion }}
{{- end }}
{{- $args := concat (list) ($container.args | default (list)) }}
{{- $hasExecutorImage := false }}
{{- $hasOpenCodeAgent := false }}
{{- $hasGithubMCPURL := false }}
{{- $hasContext7MCPURL := false }}
{{- $hasMetricsMCPURL := false }}
{{- $hasDispatchEnabled := false }}
{{- $hasDispatchBaseURL := false }}
{{- $hasDispatchAgentName := false }}
{{- $hasDispatchQueueLane := false }}
{{- $hasDispatchLane := false }}
{{- $hasDispatchLaneBinding := false }}
{{- $hasDispatchPollInterval := false }}
{{- $hasDispatchHTTPTimeout := false }}
{{- range $arg := $args }}
{{- if hasPrefix "--executor-image=" $arg }}
{{- $hasExecutorImage = true }}
{{- end }}
{{- if hasPrefix "--opencode-agent=" $arg }}
{{- $hasOpenCodeAgent = true }}
{{- end }}
{{- if hasPrefix "--github-mcp-url=" $arg }}
{{- $hasGithubMCPURL = true }}
{{- end }}
{{- if hasPrefix "--context7-mcp-url=" $arg }}
{{- $hasContext7MCPURL = true }}
{{- end }}
{{- if hasPrefix "--metrics-mcp-url=" $arg }}
{{- $hasMetricsMCPURL = true }}
{{- end }}
{{- if or (eq $arg "--dispatch-enabled") (hasPrefix "--dispatch-enabled=" $arg) }}
{{- $hasDispatchEnabled = true }}
{{- end }}
{{- if hasPrefix "--dispatch-base-url=" $arg }}
{{- $hasDispatchBaseURL = true }}
{{- end }}
{{- if hasPrefix "--dispatch-agent-name=" $arg }}
{{- $hasDispatchAgentName = true }}
{{- end }}
{{- if hasPrefix "--dispatch-queue-lane=" $arg }}
{{- $hasDispatchQueueLane = true }}
{{- end }}
{{- if hasPrefix "--dispatch-lane=" $arg }}
{{- $hasDispatchLane = true }}
{{- end }}
{{- if hasPrefix "--dispatch-lane-binding=" $arg }}
{{- $hasDispatchLaneBinding = true }}
{{- end }}
{{- if hasPrefix "--dispatch-poll-interval=" $arg }}
{{- $hasDispatchPollInterval = true }}
{{- end }}
{{- if hasPrefix "--dispatch-http-timeout=" $arg }}
{{- $hasDispatchHTTPTimeout = true }}
{{- end }}
{{- end }}
{{- if not $hasExecutorImage }}
{{- $args = append $args (printf "--executor-image=ghcr.io/misospace/courier-opencode:%s" .Chart.AppVersion) }}
{{- end }}
{{- $agent := .Values.executor.openCode.agent }}
{{- if and (not $hasOpenCodeAgent) $agent }}
{{- $args = append $args (printf "--opencode-agent=%s" $agent) }}
{{- end }}
{{- $mcp := .Values.executor.mcp }}
{{- if and (not $hasGithubMCPURL) $mcp.githubURL }}
{{- $args = append $args (printf "--github-mcp-url=%s" $mcp.githubURL) }}
{{- end }}
{{- if and (not $hasContext7MCPURL) $mcp.context7URL }}
{{- $args = append $args (printf "--context7-mcp-url=%s" $mcp.context7URL) }}
{{- end }}
{{- if and (not $hasMetricsMCPURL) $mcp.metricsURL }}
{{- $args = append $args (printf "--metrics-mcp-url=%s" $mcp.metricsURL) }}
{{- end }}
{{- $dispatch := .Values.dispatch }}
{{- $lanes := $dispatch.lanes | default (list) }}
{{- if $dispatch.enabled }}
{{- if or (not $dispatch.baseURL) (not $dispatch.agentName) (not $dispatch.tokenSecret.name) (not $dispatch.tokenSecret.key) }}
{{- fail "dispatch.enabled requires baseURL, agentName, tokenSecret.name, and tokenSecret.key" }}
{{- end }}
{{- if $lanes }}
{{- if or $dispatch.queueLane $dispatch.laneProfile }}
{{- fail "dispatch.lanes cannot be combined with dispatch.queueLane or dispatch.laneProfile" }}
{{- end }}
{{- $seen := dict }}
{{- range $binding := $lanes }}
{{- if or (not $binding.queueLane) (not $binding.laneProfile) }}
{{- fail "each dispatch.lanes entry requires queueLane and laneProfile" }}
{{- end }}
{{- if hasKey $seen $binding.queueLane }}
{{- fail (printf "duplicate queueLane %q in dispatch.lanes" $binding.queueLane) }}
{{- end }}
{{- $_ := set $seen $binding.queueLane true }}
{{- if or (contains ":" $binding.queueLane) (contains ":" $binding.laneProfile) }}
{{- fail (printf "queueLane and laneProfile must not contain a colon (got %q)" $binding.queueLane) }}
{{- end }}
{{- end }}
{{- else if or (not $dispatch.queueLane) (not $dispatch.laneProfile) }}
{{- fail "dispatch.enabled requires dispatch.lanes or both dispatch.queueLane and dispatch.laneProfile" }}
{{- end }}
{{- if not $hasDispatchEnabled }}{{- $args = append $args "--dispatch-enabled=true" }}{{- end }}
{{- if not $hasDispatchBaseURL }}{{- $args = append $args (printf "--dispatch-base-url=%s" $dispatch.baseURL) }}{{- end }}
{{- if not $hasDispatchAgentName }}{{- $args = append $args (printf "--dispatch-agent-name=%s" $dispatch.agentName) }}{{- end }}
{{- if $lanes }}
{{- if not $hasDispatchLaneBinding }}
{{- range $binding := $lanes }}
{{- $args = append $args (printf "--dispatch-lane-binding=%s:%s" $binding.queueLane $binding.laneProfile) }}
{{- end }}
{{- end }}
{{- else }}
{{- if not $hasDispatchQueueLane }}{{- $args = append $args (printf "--dispatch-queue-lane=%s" $dispatch.queueLane) }}{{- end }}
{{- if not $hasDispatchLane }}{{- $args = append $args (printf "--dispatch-lane=%s" $dispatch.laneProfile) }}{{- end }}
{{- end }}
{{- if not $hasDispatchPollInterval }}{{- $args = append $args (printf "--dispatch-poll-interval=%s" $dispatch.pollInterval) }}{{- end }}
{{- if not $hasDispatchHTTPTimeout }}{{- $args = append $args (printf "--dispatch-http-timeout=%s" $dispatch.httpTimeout) }}{{- end }}
{{- $env := deepCopy ($container.env | default (dict)) }}
{{- $_ := set $env "DISPATCH_AGENT_TOKEN" (dict "valueFrom" (dict "secretKeyRef" (dict "name" $dispatch.tokenSecret.name "key" $dispatch.tokenSecret.key))) }}
{{- $_ := set $container "env" $env }}
{{- end }}
{{- $secure := .Values.secure }}
{{- if $secure.enabled }}
{{- if not $secure.runNamespace }}
{{- fail "secure.enabled requires secure.runNamespace" }}
{{- end }}
{{- if not $secure.providers }}
{{- fail "secure.enabled requires at least one entry in secure.providers" }}
{{- end }}
{{- $seenProviders := dict }}
{{- range $registration := $secure.providers }}
{{- if or (not $registration.name) (not $registration.type) (not $registration.endpoint) }}
{{- fail "each secure.providers entry requires name, type, and endpoint" }}
{{- end }}
{{- if hasKey $seenProviders $registration.name }}
{{- fail (printf "duplicate provider name %q in secure.providers" $registration.name) }}
{{- end }}
{{- $_ := set $seenProviders $registration.name true }}
{{- if not $registration.serves }}
{{- fail (printf "provider %q requires at least one serves pattern" $registration.name) }}
{{- end }}
{{- if or (not $registration.credentials) (not (index $registration.credentials "forge-api")) }}
{{- fail (printf "provider %q requires credentials.forge-api with secretName and key" $registration.name) }}
{{- end }}
{{- end }}
{{- $hasSecureMode := false }}
{{- $hasProvidersFile := false }}
{{- $hasRunNamespace := false }}
{{- $hasHarnessImage := false }}
{{- $hasProbeImage := false }}
{{- $hasCacheService := false }}
{{- $hasCachePort := false }}
{{- range $arg := $args }}
{{- if hasPrefix "--secure-mode=" $arg }}{{- $hasSecureMode = true }}{{- end }}
{{- if hasPrefix "--forge-providers-file=" $arg }}{{- $hasProvidersFile = true }}{{- end }}
{{- if hasPrefix "--run-namespace=" $arg }}{{- $hasRunNamespace = true }}{{- end }}
{{- if hasPrefix "--harness-image=" $arg }}{{- $hasHarnessImage = true }}{{- end }}
{{- if hasPrefix "--probe-image=" $arg }}{{- $hasProbeImage = true }}{{- end }}
{{- if hasPrefix "--dependency-cache-service=" $arg }}{{- $hasCacheService = true }}{{- end }}
{{- if hasPrefix "--dependency-cache-port=" $arg }}{{- $hasCachePort = true }}{{- end }}
{{- end }}
{{- if not $hasSecureMode }}{{- $args = append $args "--secure-mode=true" }}{{- end }}
{{- if not $hasProvidersFile }}{{- $args = append $args "--forge-providers-file=/etc/courier/forge-providers.json" }}{{- end }}
{{- if not $hasRunNamespace }}{{- $args = append $args (printf "--run-namespace=%s" $secure.runNamespace) }}{{- end }}
{{- $harnessRepository := $secure.harnessImage.repository }}
{{- $harnessTag := $secure.harnessImage.tag }}
{{- if not $harnessTag }}{{- $harnessTag = .Chart.AppVersion }}{{- end }}
{{- if not $hasHarnessImage }}{{- $args = append $args (printf "--harness-image=%s:%s" $harnessRepository $harnessTag) }}{{- end }}
{{- if not $hasProbeImage }}{{- $args = append $args (printf "--probe-image=%s" $secure.probeImage) }}{{- end }}
{{- if $secure.dependencyCacheService }}
{{- if not $hasCacheService }}{{- $args = append $args (printf "--dependency-cache-service=%s" $secure.dependencyCacheService) }}{{- end }}
{{- if $secure.dependencyCachePort }}
{{- if not $hasCachePort }}{{- $args = append $args (printf "--dependency-cache-port=%d" (int $secure.dependencyCachePort)) }}{{- end }}
{{- end }}
{{- end }}
{{- if $secure.modelGateway }}
{{- if $secure.modelGateway.url }}
{{- $args = append $args (printf "--model-gateway-url=%s" $secure.modelGateway.url) }}
{{- if $secure.modelGateway.keySecret }}
{{- $args = append $args (printf "--model-gateway-key-secret=%s" $secure.modelGateway.keySecret) }}
{{- if $secure.modelGateway.keySecretKey }}
{{- $args = append $args (printf "--model-gateway-key-name=%s" $secure.modelGateway.keySecretKey) }}
{{- end }}
{{- end }}
{{- end }}
{{- if $secure.modelGateway.cidr }}
{{- $args = append $args (printf "--model-gateway-cidr=%s" $secure.modelGateway.cidr) }}
{{- if $secure.modelGateway.port }}
{{- $args = append $args (printf "--model-gateway-port=%d" (int $secure.modelGateway.port)) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- if $secure.enabled }}
{{- $persistence := deepCopy (.Values.persistence | default dict) }}
{{- $_ := set $persistence "forge-providers" (dict
  "type" "configMap"
  "name" (printf "%s-forge-providers" .Release.Name)
  "globalMounts" (list (dict "path" "/etc/courier" "readOnly" true))) }}
{{- $_ := set .Values "persistence" $persistence }}
{{- end }}
{{- $_ := set $container "args" $args }}

{{- include "bjw-s.common.loader.generate" . }}
