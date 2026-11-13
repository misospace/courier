{{- /*
  Read-only projection API Service (issue #820). Consumers GET
  namespace-scoped CoderRun/LaneProfile projections from this ClusterIP
  Service, which routes to the operator's read-api port. Restricting its
  ingress to trusted consumers with a NetworkPolicy or an ingress allowlist
  is a deployment concern. The Service selector uses the common library's
  shared selector labels so it tracks the controller pods.
*/}}
{{- if .Values.readAPI.enabled }}
{{- /* Initialize the merged common-library values so its label helpers work. */}}
{{- include "bjw-s.common.loader.init" . }}
{{- $name := printf "%s-read-api" .Release.Name }}
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
    - name: read-api
      port: 80
      targetPort: {{ .Values.readAPI.port }}
      protocol: TCP
  selector:
    {{- include "bjw-s.common.lib.metadata.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/controller: main
{{- end }}
