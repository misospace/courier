{{- /*
  Write-only contribution-admission API Service (#708). Non-kubectl
  frontends such as Dispatch POST bounded contribution admissions to this
  ClusterIP Service, which routes to the operator's admission port.
  Restricting its ingress to trusted frontends with a NetworkPolicy or an
  ingress allowlist is a deployment concern. The Service selector uses the
  common library's shared selector labels so it tracks the controller pods.
*/}}
{{- if .Values.contributeAPI.enabled }}
{{- /* Initialize the merged common-library values so its label helpers work. */}}
{{- include "bjw-s.common.loader.init" . }}
{{- $name := printf "%s-contribute-api" .Release.Name }}
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
    - name: contribute-api
      port: 80
      targetPort: {{ .Values.contributeAPI.port }}
      protocol: TCP
  selector:
    {{- include "bjw-s.common.lib.metadata.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/controller: main
{{- end }}
