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
{{- if $dispatch.enabled }}
{{- if or (not $dispatch.baseURL) (not $dispatch.agentName) (not $dispatch.queueLane) (not $dispatch.laneProfile) (not $dispatch.tokenSecret.name) (not $dispatch.tokenSecret.key) }}
{{- fail "dispatch.enabled requires baseURL, agentName, queueLane, laneProfile, tokenSecret.name, and tokenSecret.key" }}
{{- end }}
{{- if not $hasDispatchEnabled }}{{- $args = append $args "--dispatch-enabled=true" }}{{- end }}
{{- if not $hasDispatchBaseURL }}{{- $args = append $args (printf "--dispatch-base-url=%s" $dispatch.baseURL) }}{{- end }}
{{- if not $hasDispatchAgentName }}{{- $args = append $args (printf "--dispatch-agent-name=%s" $dispatch.agentName) }}{{- end }}
{{- if not $hasDispatchQueueLane }}{{- $args = append $args (printf "--dispatch-queue-lane=%s" $dispatch.queueLane) }}{{- end }}
{{- if not $hasDispatchLane }}{{- $args = append $args (printf "--dispatch-lane=%s" $dispatch.laneProfile) }}{{- end }}
{{- if not $hasDispatchPollInterval }}{{- $args = append $args (printf "--dispatch-poll-interval=%s" $dispatch.pollInterval) }}{{- end }}
{{- if not $hasDispatchHTTPTimeout }}{{- $args = append $args (printf "--dispatch-http-timeout=%s" $dispatch.httpTimeout) }}{{- end }}
{{- $env := deepCopy ($container.env | default (dict)) }}
{{- $_ := set $env "DISPATCH_AGENT_TOKEN" (dict "valueFrom" (dict "secretKeyRef" (dict "name" $dispatch.tokenSecret.name "key" $dispatch.tokenSecret.key))) }}
{{- $_ := set $container "env" $env }}
{{- end }}
{{- $_ := set $container "args" $args }}

{{- include "bjw-s.common.loader.generate" . }}
