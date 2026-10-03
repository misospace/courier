{{- if .Values.secure.enabled }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-forge-providers
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/managed-by: courier
data:
  forge-providers.json: |
{{- $providers := list }}
{{- range $registration := .Values.secure.providers }}
{{- $entry := dict "name" $registration.name "type" $registration.type "endpoint" $registration.endpoint }}
{{- if $registration.gitEndpoint }}{{- $_ := set $entry "gitEndpoint" $registration.gitEndpoint }}{{- end }}
{{- if $registration.gitUsername }}{{- $_ := set $entry "gitUsername" $registration.gitUsername }}{{- end }}
{{- $_ := set $entry "credentials" $registration.credentials }}
{{- $_ := set $entry "serves" $registration.serves }}
{{- $providers = append $providers $entry }}
{{- end }}
{{- toPrettyJson (dict "providers" $providers) | nindent 4 }}
---
# The only cluster-scoped authority a run's broker service account holds:
# TokenReview creation. Per-run ClusterRoleBindings to this role are created
# (and garbage-collected) by the operator with the run's topology.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: courier-broker-tokenreview
  labels:
    app.kubernetes.io/managed-by: courier
rules:
  - apiGroups: [authentication.k8s.io]
    resources: [tokenreviews]
    verbs: [create]
{{- end }}
