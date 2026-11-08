{{- $cache := .Values.secure.dependencyCache }}
{{- if gt (int $cache.replicas) 1 }}
{{- fail (printf "secure.dependencyCache.replicas %d must be 1: the store PVC is ReadWriteOnce, so a second replica cannot attach the volume on another node (Multi-Attach error) and its pod would strand in ContainerCreating; set replicas back to 1" (int $cache.replicas)) }}
{{- end }}
{{- if and .Values.secure.enabled $cache.enabled }}
{{- /* The dedicated read-only dependency cache (#136): a pre-populated
  module store served over the read-only GOPROXY subset to secure workers.
  The store volume mounts read-only in the serving pod — a compromised cache
  process cannot rewrite approved artifacts — and the NetworkPolicy denies
  all cache egress: the server has no outbound client at all, and the policy
  is defense in depth (HARNESS.md §2). Population travels the administrator's
  trusted path (see secure.dependencyCache in values.yaml), never a worker. */}}
{{- $imageTag := $cache.image.tag }}
{{- if not $imageTag }}{{- $imageTag = .Chart.AppVersion }}{{- end }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .Release.Name }}-dependency-cache
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/managed-by: courier
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/component: dependency-cache
  annotations:
    # The cache holds no identity: it serves files and needs no API access.
    kubernetes.io/enforce-mountable-secrets: "true"
automountServiceAccountToken: false
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ .Release.Name }}-dependency-cache
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/managed-by: courier
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/component: dependency-cache
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: {{ $cache.storage }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .Release.Name }}-dependency-cache
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/managed-by: courier
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/component: dependency-cache
spec:
  type: ClusterIP
  ports:
    - name: http
      port: {{ $cache.port }}
      targetPort: http
      protocol: TCP
  selector:
    app.kubernetes.io/managed-by: courier
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/component: dependency-cache
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}-dependency-cache
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/managed-by: courier
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/component: dependency-cache
spec:
  replicas: {{ $cache.replicas }}
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/managed-by: courier
      app.kubernetes.io/instance: {{ .Release.Name }}
      app.kubernetes.io/component: dependency-cache
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: courier
        app.kubernetes.io/instance: {{ .Release.Name }}
        app.kubernetes.io/component: dependency-cache
    spec:
      serviceAccountName: {{ .Release.Name }}-dependency-cache
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: dependency-cache
          {{- if $cache.image.digest }}
          image: {{ $cache.image.repository }}@{{ $cache.image.digest }}
          {{- else }}
          image: {{ $cache.image.repository }}:{{ $imageTag }}
          {{- end }}
          imagePullPolicy: {{ $cache.image.pullPolicy }}
          # distroless image; the binary is the whole process. The store is
          # the only state it reads.
          command: ["/usr/local/bin/courier-dependency-cache"]
          args:
            - --addr=:{{ $cache.port }}
            - --root=/var/lib/courier/dependency-cache
          ports:
            - name: http
              containerPort: {{ $cache.port }}
              protocol: TCP
          volumeMounts:
            - name: store
              mountPath: /var/lib/courier/dependency-cache
              readOnly: true
          resources:
{{ toYaml $cache.resources | indent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
            periodSeconds: 10
      volumes:
        - name: store
          persistentVolumeClaim:
            claimName: {{ .Release.Name }}-dependency-cache
---
# Ingress admits only the secure runs' worker pods on the cache port; the
# control and broker pods have no path here. Egress is fully denied: a cache
# pod that starts dialing is a compromised pod, not a feature.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ .Release.Name }}-dependency-cache
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/managed-by: courier
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/component: dependency-cache
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/managed-by: courier
      app.kubernetes.io/instance: {{ .Release.Name }}
      app.kubernetes.io/component: dependency-cache
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {{ .Values.secure.runNamespace }}
          podSelector:
            matchLabels:
              courier.misospace.dev/component: worker
      ports:
        - protocol: TCP
          port: {{ $cache.port }}
  # Egress stays nil: default deny.
{{- end }}
