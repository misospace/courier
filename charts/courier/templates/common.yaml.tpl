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
{{- range $arg := $args }}
{{- if hasPrefix "--executor-image=" $arg }}
{{- $hasExecutorImage = true }}
{{- end }}
{{- end }}
{{- if not $hasExecutorImage }}
{{- $_ := set $container "args" (append $args (printf "--executor-image=ghcr.io/misospace/courier-opencode:%s" .Chart.AppVersion)) }}
{{- end }}

{{- include "bjw-s.common.loader.generate" . }}
