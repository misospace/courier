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
{{- $args := $container.args | default (list) }}
{{- $hasExecutorImage := false }}
{{- $hasOpenCodeAgent := false }}
{{- $hasGithubMCPURL := false }}
{{- $hasContext7MCPURL := false }}
{{- $hasMetricsMCPURL := false }}
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
{{- $_ := set $container "args" $args }}

{{- include "bjw-s.common.loader.generate" . }}
