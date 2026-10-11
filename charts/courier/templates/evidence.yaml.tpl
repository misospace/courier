{{- /*
  Write-only failure-evidence intake listener Service (#115). Coordinator
  pods POST bounded failure-evidence bundles to this ClusterIP Service,
  which routes to the operator's intake port. Restricting its ingress to
  coordinator pods with a NetworkPolicy is a deployment concern
  (DESIGN.md #115). The Service selector uses the common library's shared
  selector labels so it tracks the controller pods.
*/}}
{{- if .Values.evidence.enabled }}
{{- /* Initialize the merged common-library values so its label helpers work. */}}
{{- include "bjw-s.common.loader.init" . }}
{{- $name := printf "%s-evidence" .Release.Name }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/service: {{ $name }}
    {{- include "bjw-s.common.lib.metadata.allLabels" . | trim | nindent 4 }}
spec:
  type: ClusterIP
  ports:
    - name: evidence
      port: 80
      targetPort: {{ .Values.evidence.port }}
      protocol: TCP
  selector:
    {{- include "bjw-s.common.lib.metadata.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/controller: main
{{- end }}
