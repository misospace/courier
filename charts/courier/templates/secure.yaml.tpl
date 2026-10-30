{{- if .Values.secure.enabled }}
{{- /* Initialize the merged common-library values so the serviceAccount helper resolves. */}}
{{- include "bjw-s.common.loader.init" . }}
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
  name: {{ .Values.secure.brokerTokenReviewClusterRole | default "courier-broker-tokenreview" }}
  labels:
    app.kubernetes.io/managed-by: courier
rules:
  - apiGroups: [authentication.k8s.io]
    resources: [tokenreviews]
    verbs: [create]
---
{{- $brokerRole := .Values.secure.brokerTokenReviewClusterRole | default "courier-broker-tokenreview" }}
{{- $operatorSA := (include "bjw-s.common.lib.serviceAccount.getByIdentifier" (dict "rootContext" . "id" "main") | fromYaml) }}
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ .Release.Name }}-broker-tokenreview-binder
  labels:
    app.kubernetes.io/managed-by: courier
rules:
  - apiGroups: [rbac.authorization.k8s.io]
    resources: [clusterroles]
    verbs: [bind]
    resourceNames: [{{ $brokerRole }}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ .Release.Name }}-broker-tokenreview-binder
  labels:
    app.kubernetes.io/managed-by: courier
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ .Release.Name }}-broker-tokenreview-binder
subjects:
  - kind: ServiceAccount
    name: {{ $operatorSA.name }}
    namespace: {{ .Release.Namespace }}
{{- end }}
