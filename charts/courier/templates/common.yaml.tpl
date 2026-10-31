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
{{- $hasLabelSourceEnabled := false }}
{{- $hasLabelSourceRepos := false }}
{{- $hasLabelSourceTriggerLabel := false }}
{{- $hasLabelSourceFixLabel := false }}
{{- $hasLabelSourceLane := false }}
{{- $hasLabelSourcePollInterval := false }}
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
{{- if or (eq $arg "--label-source-enabled") (hasPrefix "--label-source-enabled=" $arg) }}
{{- $hasLabelSourceEnabled = true }}
{{- end }}
{{- if hasPrefix "--label-source-repos=" $arg }}
{{- $hasLabelSourceRepos = true }}
{{- end }}
{{- if hasPrefix "--label-source-trigger-label=" $arg }}
{{- $hasLabelSourceTriggerLabel = true }}
{{- end }}
{{- if hasPrefix "--label-source-fix-label=" $arg }}
{{- $hasLabelSourceFixLabel = true }}
{{- end }}
{{- if hasPrefix "--label-source-lane=" $arg }}
{{- $hasLabelSourceLane = true }}
{{- end }}
{{- if hasPrefix "--label-source-poll-interval=" $arg }}
{{- $hasLabelSourcePollInterval = true }}
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
{{- $seenAgents := dict }}
{{- $multiple := gt (len $lanes) 1 }}
{{- $queueCounts := dict }}
{{- range $binding := $lanes }}
{{- if not $binding.queueLane }}{{- fail "each dispatch.lanes entry requires queueLane and laneProfile" }}{{- end }}
{{- $_ := set $queueCounts $binding.queueLane (add (index $queueCounts $binding.queueLane | default 0) 1) }}
{{- end }}
{{- range $binding := $lanes }}
{{- if or (not $binding.queueLane) (not $binding.laneProfile) }}
{{- fail "each dispatch.lanes entry requires queueLane and laneProfile" }}
{{- end }}
{{- if or (contains ":" $binding.queueLane) (contains ":" $binding.laneProfile) }}
{{- fail "queueLane and laneProfile must not contain a colon" }}
{{- end }}
{{- $agent := $binding.agentName | default (ternary (printf "%s-%s" $dispatch.agentName $binding.queueLane) $dispatch.agentName $multiple) }}
{{- if or (contains ":" $agent) (regexMatch `[^A-Za-z0-9._-]` $agent) }}
{{- fail (printf "invalid dispatch agentName %q" $agent) }}
{{- end }}
{{- if hasKey $seenAgents $agent }}
{{- fail (printf "duplicate agentName %q in dispatch.lanes" $agent) }}
{{- end }}
{{- $_ := set $seenAgents $agent true }}
{{- if and (gt (index $queueCounts $binding.queueLane) 1) (not $binding.agentName) }}
{{- fail (printf "repeated queueLane %q requires explicit distinct agentName values" $binding.queueLane) }}
{{- end }}
{{- end }}
{{- else if or (not $dispatch.queueLane) (not $dispatch.laneProfile) }}
{{- fail "dispatch.enabled requires dispatch.lanes or both dispatch.queueLane and dispatch.laneProfile" }}
{{- end }}
{{- if not $hasDispatchEnabled }}{{- $args = append $args "--dispatch-enabled=true" }}{{- end }}
{{- if $dispatch.prFixExclusions }}{{- $args = append $args "--dispatch-pr-fix-exclusions=true" }}{{- end }}
{{- if not $hasDispatchBaseURL }}{{- $args = append $args (printf "--dispatch-base-url=%s" $dispatch.baseURL) }}{{- end }}
{{- if not $hasDispatchAgentName }}{{- $args = append $args (printf "--dispatch-agent-name=%s" $dispatch.agentName) }}{{- end }}
{{- if $lanes }}
{{- if not $hasDispatchLaneBinding }}
{{- range $binding := $lanes }}
{{- $bindingArg := printf "%s:%s" $binding.queueLane $binding.laneProfile }}
{{- if $binding.agentName }}{{- $bindingArg = printf "%s:%s" $bindingArg $binding.agentName }}{{- end }}
{{- $args = append $args (printf "--dispatch-lane-binding=%s" $bindingArg) }}
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
{{- $labelSource := .Values.labelSource }}
{{- if $labelSource.enabled }}
{{- if or (not $labelSource.repos) (not $labelSource.lane) }}
{{- fail "labelSource.enabled requires labelSource.repos and labelSource.lane" }}
{{- end }}
{{- $hasCredential := false }}
{{- range $arg := $args }}
{{- if hasPrefix "--git-credential-secret=" $arg }}
{{- if ne (trimPrefix "--git-credential-secret=" $arg) "" }}{{- $hasCredential = true }}{{- end }}
{{- end }}
{{- if hasPrefix "--github-credential-secret=" $arg }}
{{- if ne (trimPrefix "--github-credential-secret=" $arg) "" }}{{- $hasCredential = true }}{{- end }}
{{- end }}
{{- if hasPrefix "--observer-credential-secret=" $arg }}
{{- if ne (trimPrefix "--observer-credential-secret=" $arg) "" }}{{- $hasCredential = true }}{{- end }}
{{- end }}
{{- end }}
{{- if not $hasCredential }}
{{- fail "labelSource.enabled requires a non-empty --observer-credential-secret, --github-credential-secret, or --git-credential-secret arg: the label source reuses the operator's own forge credential and fails fast without one" }}
{{- end }}
{{- if not $hasLabelSourceEnabled }}{{- $args = append $args "--label-source-enabled=true" }}{{- end }}
{{- if not $hasLabelSourceRepos }}{{- $args = append $args (printf "--label-source-repos=%s" (join "," $labelSource.repos)) }}{{- end }}
{{- if and (not $hasLabelSourceTriggerLabel) $labelSource.triggerLabel }}{{- $args = append $args (printf "--label-source-trigger-label=%s" $labelSource.triggerLabel) }}{{- end }}
{{- if and (not $hasLabelSourceFixLabel) $labelSource.fixLabel }}{{- $args = append $args (printf "--label-source-fix-label=%s" $labelSource.fixLabel) }}{{- end }}
{{- if not $hasLabelSourceLane }}{{- $args = append $args (printf "--label-source-lane=%s" $labelSource.lane) }}{{- end }}
{{- if and (not $hasLabelSourcePollInterval) $labelSource.pollInterval }}{{- $args = append $args (printf "--label-source-poll-interval=%s" $labelSource.pollInterval) }}{{- end }}
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
{{- $hasBrokerTokenReviewRole := false }}
{{- $explicitBrokerTokenReviewRole := "" }}
{{- range $i, $arg := $args }}
{{- if hasPrefix "--secure-mode=" $arg }}{{- $hasSecureMode = true }}{{- end }}
{{- if hasPrefix "--forge-providers-file=" $arg }}{{- $hasProvidersFile = true }}{{- end }}
{{- if hasPrefix "--run-namespace=" $arg }}{{- $hasRunNamespace = true }}{{- end }}
{{- if hasPrefix "--harness-image=" $arg }}{{- $hasHarnessImage = true }}{{- end }}
{{- if hasPrefix "--probe-image=" $arg }}{{- $hasProbeImage = true }}{{- end }}
{{- if hasPrefix "--dependency-cache-service=" $arg }}{{- $hasCacheService = true }}{{- end }}
{{- if hasPrefix "--dependency-cache-port=" $arg }}{{- $hasCachePort = true }}{{- end }}
{{- if hasPrefix "--broker-tokenreview-cluster-role=" $arg }}{{- $hasBrokerTokenReviewRole = true }}{{- $explicitBrokerTokenReviewRole = $arg }}
{{- else if hasPrefix "-broker-tokenreview-cluster-role=" $arg }}{{- $hasBrokerTokenReviewRole = true }}{{- $explicitBrokerTokenReviewRole = printf "--%s" (trimPrefix "-" $arg) }}
{{- else if or (eq $arg "--broker-tokenreview-cluster-role") (eq $arg "-broker-tokenreview-cluster-role") }}
{{- $hasBrokerTokenReviewRole = true }}
{{- if and (lt (add1 $i) (len $args)) (not (hasPrefix "-" (index $args (add1 $i)))) }}
{{- $explicitBrokerTokenReviewRole = printf "--broker-tokenreview-cluster-role=%s" (index $args (add1 $i)) }}
{{- else }}
{{- fail "a bare --broker-tokenreview-cluster-role argument has no value at render time: the operator's flag parser consumes the next argument, so pass --broker-tokenreview-cluster-role=<name> or a value that does not start with '-'" }}
{{- end }}
{{- end }}
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
{{- $effectiveBrokerTokenReviewRole := $secure.brokerTokenReviewClusterRole | default "courier-broker-tokenreview" }}
{{- if $hasBrokerTokenReviewRole }}
{{- if ne $explicitBrokerTokenReviewRole (printf "--broker-tokenreview-cluster-role=%s" $effectiveBrokerTokenReviewRole) }}
{{- fail (printf "explicit arg %s contradicts the rendered broker TokenReview ClusterRole %q (secure.brokerTokenReviewClusterRole=%q): the chart renders the ClusterRole under a name the operator never binds" $explicitBrokerTokenReviewRole $effectiveBrokerTokenReviewRole $secure.brokerTokenReviewClusterRole) }}
{{- end }}
{{- else if $secure.brokerTokenReviewClusterRole }}
{{- $args = append $args (printf "--broker-tokenreview-cluster-role=%s" $effectiveBrokerTokenReviewRole) }}
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
{{- /* Bootstrap laneProfiles serialize into a chart ConfigMap mounted into
  the manager and passed through the source-neutral
  --bootstrap-lane-profiles-file seam. The library's built-in configMaps
  checksum annotates the pod template, so a values change rolls the manager
  on a normal upgrade. Helm never renders LaneProfile resources, so the
  CRD-ordering race stays structurally closed. */}}
{{- $laneProfiles := .Values.laneProfiles | default (dict) }}
{{- if $laneProfiles }}
{{- $hasBootstrapLaneFile := false }}
{{- range $arg := $args }}
{{- if or (eq $arg "--bootstrap-lane-profiles-file") (hasPrefix "--bootstrap-lane-profiles-file=" $arg) }}{{- $hasBootstrapLaneFile = true }}{{- end }}
{{- end }}
{{- if not $hasBootstrapLaneFile }}
{{- $profiles := list }}
{{- range $name, $spec := $laneProfiles }}
{{- if not (kindIs "map" $spec) }}
{{- fail (printf "laneProfiles entry %q must be a mapping of lane spec fields" $name) }}
{{- end }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name)) (gt (len $name) 63) }}
{{- fail (printf "laneProfiles name %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $name) }}
{{- end }}
{{- if not (index $spec "roles") }}
{{- fail (printf "laneProfiles entry %q requires at least one entry under roles" $name) }}
{{- end }}
{{- $entry := deepCopy $spec }}
{{- $_ := set $entry "name" $name }}
{{- $profiles = append $profiles $entry }}
{{- end }}
{{- $laneConfig := toYaml (dict "profiles" $profiles) }}
{{- $configMaps := deepCopy (.Values.configMaps | default dict) }}
{{- $_ := set $configMaps "bootstrap-lane-profiles" (dict
  "suffix" "bootstrap-lane-profiles"
  "data" (dict "profiles.yaml" $laneConfig)) }}
{{- $_ := set .Values "configMaps" $configMaps }}
{{- $persistence := deepCopy (.Values.persistence | default dict) }}
{{- $_ := set $persistence "bootstrap-lane-profiles" (dict
  "type" "configMap"
  "identifier" "bootstrap-lane-profiles"
  "globalMounts" (list (dict "path" "/etc/courier/lane-profiles" "readOnly" true))) }}
{{- $_ := set .Values "persistence" $persistence }}
{{- $args = append $args "--bootstrap-lane-profiles-file=/etc/courier/lane-profiles/profiles.yaml" }}
{{- end }}
{{- end }}
{{- /* Cron schedules serialize into a chart ConfigMap mounted into the
  manager and passed through the --cron-schedules-file seam, mirroring the
  bootstrap-lane-profiles transport: the library's built-in configMaps
  checksum rolls the manager on a values change, and an explicit
  --cron-schedules-file arg takes precedence. */}}
{{- $cronSchedules := dig "cron" "schedules" (dict) .Values }}
{{- if $cronSchedules }}
{{- $hasCronFile := false }}
{{- range $arg := $args }}
{{- if or (eq $arg "--cron-schedules-file") (hasPrefix "--cron-schedules-file=" $arg) }}{{- $hasCronFile = true }}{{- end }}
{{- end }}
{{- if not $hasCronFile }}
{{- $entries := list }}
{{- range $name, $spec := $cronSchedules }}
{{- if not (kindIs "map" $spec) }}
{{- fail (printf "cron.schedules entry %q must be a mapping of job fields" $name) }}
{{- end }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name)) (gt (len $name) 63) }}
{{- fail (printf "cron.schedules name %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $name) }}
{{- end }}
{{- if or (not $spec.repo) (not (regexMatch "^[^/]+/[^/]+$" $spec.repo)) }}
{{- fail (printf "cron.schedules entry %q requires repo in owner/name form" $name) }}
{{- end }}
{{- if kindIs "string" $spec.issue }}
{{- fail (printf "cron.schedules entry %q requires issue as a number" $name) }}
{{- end }}
{{- if or (not $spec.issue) (lt (float64 $spec.issue) 1.0) (ne (float64 $spec.issue) (floor (float64 $spec.issue))) }}
{{- fail (printf "cron.schedules entry %q requires a positive integer issue number" $name) }}
{{- end }}
{{- if not $spec.lane }}
{{- fail (printf "cron.schedules entry %q requires lane" $name) }}
{{- end }}
{{- if not $spec.schedule }}
{{- fail (printf "cron.schedules entry %q requires schedule" $name) }}
{{- end }}
{{- if and $spec.mode (not (or (eq $spec.mode "resolve-issue") (eq $spec.mode "fix-pr"))) }}
{{- fail (printf "cron.schedules entry %q mode must be resolve-issue or fix-pr" $name) }}
{{- end }}
{{- $entry := deepCopy $spec }}
{{- $_ := set $entry "name" $name }}
{{- $entries = append $entries $entry }}
{{- end }}
{{- $cronConfig := toYaml (dict "schedules" $entries) }}
{{- $configMaps := deepCopy (.Values.configMaps | default dict) }}
{{- $_ := set $configMaps "cron-schedules" (dict
  "suffix" "cron-schedules"
  "data" (dict "schedules.yaml" $cronConfig)) }}
{{- $_ := set .Values "configMaps" $configMaps }}
{{- $persistence := deepCopy (.Values.persistence | default dict) }}
{{- $_ := set $persistence "cron-schedules" (dict
  "type" "configMap"
  "identifier" "cron-schedules"
  "globalMounts" (list (dict "path" "/etc/courier/cron-schedules" "readOnly" true))) }}
{{- $_ := set .Values "persistence" $persistence }}
{{- $args = append $args "--cron-schedules-file=/etc/courier/cron-schedules/schedules.yaml" }}
{{- end }}
{{- $cronPollInterval := dig "cron" "pollInterval" "" .Values }}
{{- if $cronPollInterval }}
{{- $args = append $args (printf "--cron-poll-interval=%s" $cronPollInterval) }}
{{- end }}
{{- end }}
{{- /* Deployment-approved skill bundles serialize into a chart ConfigMap
  mounted into the manager and passed through the --skill-bundles-file
  seam, mirroring the bootstrap-lane-profiles transport: the library's
  built-in configMaps checksum annotates the pod template, so a values
  change rolls the manager on a normal upgrade, and an explicit
  --skill-bundles-file arg takes precedence. Each entry's source must be a
  plain directory tree mounted by the deployment; a bundle's interior must
  not contain symlinks. */}}
{{- $skillBundles := .Values.skillBundles | default (dict) }}
{{- if $skillBundles }}
{{- $hasSkillBundlesFile := false }}
{{- range $arg := $args }}
{{- if or (eq $arg "--skill-bundles-file") (hasPrefix "--skill-bundles-file=" $arg) }}{{- $hasSkillBundlesFile = true }}{{- end }}
{{- end }}
{{- if not $hasSkillBundlesFile }}
{{- $bundles := list }}
{{- range $name, $spec := $skillBundles }}
{{- if not (kindIs "map" $spec) }}
{{- fail (printf "skillBundles entry %q must be a mapping of bundle fields" $name) }}
{{- end }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name)) (gt (len $name) 63) }}
{{- fail (printf "skillBundles name %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $name) }}
{{- end }}
{{- if hasKey $spec "source" }}
{{- if or (not (kindIs "string" (index $spec "source"))) (not (index $spec "source")) }}
{{- fail (printf "skillBundles entry %q source must be a non-empty string" $name) }}
{{- end }}
{{- end }}
{{- $source := index $spec "source" | default (printf "/etc/courier/skill-bundles/%s" $name) }}
{{- if not (hasPrefix "/" $source) }}
{{- fail (printf "skillBundles entry %q source %q must be an absolute path starting with '/'" $name $source) }}
{{- end }}
{{- $bundles = append $bundles (dict "name" $name "source" $source) }}
{{- end }}
{{- $skillConfig := toYaml (dict "bundles" $bundles) }}
{{- $configMaps := deepCopy (.Values.configMaps | default dict) }}
{{- $_ := set $configMaps "bootstrap-skill-bundles" (dict
  "suffix" "bootstrap-skill-bundles"
  "data" (dict "bundles.yaml" $skillConfig)) }}
{{- $_ := set .Values "configMaps" $configMaps }}
{{- $persistence := deepCopy (.Values.persistence | default dict) }}
{{- $_ := set $persistence "bootstrap-skill-bundles" (dict
  "type" "configMap"
  "identifier" "bootstrap-skill-bundles"
  "globalMounts" (list (dict "path" "/etc/courier/skill-bundles-config" "readOnly" true))) }}
{{- $_ := set .Values "persistence" $persistence }}
{{- $args = append $args "--skill-bundles-file=/etc/courier/skill-bundles-config/bundles.yaml" }}
{{- end }}
{{- end }}
{{- $evidence := .Values.evidence }}
{{- if $evidence.enabled }}
{{- if not $evidence.keySecret }}
{{- fail "evidence.enabled requires evidence.keySecret" }}
{{- end }}
{{- $hasEvidenceKeySecret := false }}
{{- $hasEvidenceBind := false }}
{{- $hasEvidenceService := false }}
{{- range $arg := $args }}
{{- if hasPrefix "--evidence-intake-key-secret=" $arg }}{{- $hasEvidenceKeySecret = true }}{{- end }}
{{- if hasPrefix "--evidence-intake-bind=" $arg }}{{- $hasEvidenceBind = true }}{{- end }}
{{- if hasPrefix "--evidence-intake-service=" $arg }}{{- $hasEvidenceService = true }}{{- end }}
{{- end }}
{{- if not $hasEvidenceKeySecret }}{{- $args = append $args (printf "--evidence-intake-key-secret=%s" $evidence.keySecret) }}{{- end }}
{{- if not $hasEvidenceBind }}{{- $args = append $args (printf "--evidence-intake-bind=:%d" (int $evidence.port)) }}{{- end }}
{{- if not $hasEvidenceService }}{{- $args = append $args (printf "--evidence-intake-service=http://%s-evidence.%s.svc" .Release.Name .Release.Namespace) }}{{- end }}
{{- end }}
{{- $retention := .Values.retention }}
{{- if $retention }}
{{- $hasDoneRetention := false }}
{{- $hasFailedRetention := false }}
{{- $hasNeedsHumanRetention := false }}
{{- $hasAuditKeep := false }}
{{- range $arg := $args }}
{{- if hasPrefix "--done-retention=" $arg }}{{- $hasDoneRetention = true }}{{- end }}
{{- if hasPrefix "--failed-retention=" $arg }}{{- $hasFailedRetention = true }}{{- end }}
{{- if hasPrefix "--needs-human-retention=" $arg }}{{- $hasNeedsHumanRetention = true }}{{- end }}
{{- if hasPrefix "--terminal-audit-keep=" $arg }}{{- $hasAuditKeep = true }}{{- end }}
{{- end }}
{{- if and $retention.done (not $hasDoneRetention) }}{{- $args = append $args (printf "--done-retention=%s" $retention.done) }}{{- end }}
{{- if and $retention.failed (not $hasFailedRetention) }}{{- $args = append $args (printf "--failed-retention=%s" $retention.failed) }}{{- end }}
{{- if and $retention.needsHuman (not $hasNeedsHumanRetention) }}{{- $args = append $args (printf "--needs-human-retention=%s" $retention.needsHuman) }}{{- end }}
{{- if and $retention.auditKeep (not $hasAuditKeep) }}{{- $args = append $args (printf "--terminal-audit-keep=%d" (int $retention.auditKeep)) }}{{- end }}
{{- end }}
{{- $ownership := .Values.ownership | default (dict) }}
{{- $valueOwned := $ownership.namespaces | default (list) }}
{{- $hasLeaderElectionID := false }}
{{- $explicitOwned := list }}
{{- $hasExplicitOwned := false }}
{{- $argCount := len $args }}
{{- range $i, $arg := $args }}
{{- if hasPrefix "--leader-election-id=" $arg }}{{- $hasLeaderElectionID = true }}{{- end }}
{{- if hasPrefix "--owned-namespace=" $arg }}
{{- $hasExplicitOwned = true }}
{{- $explicitOwned = append $explicitOwned (trimPrefix "--owned-namespace=" $arg) }}
{{- else if eq $arg "--owned-namespace" }}
{{- $hasExplicitOwned = true }}
{{- if and (lt (add1 $i) $argCount) (not (hasPrefix "-" (index $args (add1 $i)))) }}
{{- $explicitOwned = append $explicitOwned (index $args (add1 $i)) }}
{{- else }}
{{- fail "a bare --owned-namespace argument has no value at render time: the operator's flag parser consumes the next argument, so pass --owned-namespace=<namespace> or a value that does not start with '-'" }}
{{- end }}
{{- end }}
{{- end }}
{{- if and $hasExplicitOwned $valueOwned }}
{{- range $namespace := $valueOwned }}
{{- if not (has $namespace $explicitOwned) }}
{{- fail (printf "ownership.namespaces %v are ignored because explicit --owned-namespace args are present and render the boundary %v: the two disagree" $valueOwned $explicitOwned) }}
{{- end }}
{{- end }}
{{- range $namespace := $explicitOwned }}
{{- if not (has $namespace $valueOwned) }}
{{- fail (printf "explicit --owned-namespace args %v disagree with ownership.namespaces %v: the values are ignored by the operator" $explicitOwned $valueOwned) }}
{{- end }}
{{- end }}
{{- end }}
{{- if not $hasExplicitOwned }}
{{- range $namespace := $valueOwned }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $namespace)) (gt (len $namespace) 63) }}
{{- fail (printf "ownership.namespaces entry %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $namespace) }}
{{- end }}
{{- $args = append $args (printf "--owned-namespace=%s" $namespace) }}
{{- end }}
{{- else }}
{{- range $namespace := $explicitOwned }}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $namespace)) (gt (len $namespace) 63) }}
{{- fail (printf "--owned-namespace entry %q must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters, starting and ending with an alphanumeric)" $namespace) }}
{{- end }}
{{- end }}
{{- end }}
{{- if $ownership.leaderElectionID }}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$" $ownership.leaderElectionID) }}
{{- fail (printf "ownership.leaderElectionID %q must be a lowercase DNS-1123 subdomain" $ownership.leaderElectionID) }}
{{- end }}
{{- if not $hasLeaderElectionID }}{{- $args = append $args (printf "--leader-election-id=%s" $ownership.leaderElectionID) }}{{- end }}
{{- end }}
{{- /* Mirror the operator's startup coupling (resolveOwnership) at render
  time so the boundary fails at upgrade instead of CrashLoopBackOff: secure
  mode's runs live in secure.runNamespace, and discovery materializes runs
  in the operator namespace (the release namespace). */}}
{{- $ownershipList := $explicitOwned }}
{{- if not $hasExplicitOwned }}{{- $ownershipList = $valueOwned }}{{- end }}
{{- $ownershipSource := "explicit --owned-namespace args" }}
{{- if not $hasExplicitOwned }}{{- $ownershipSource = "ownership.namespaces" }}{{- end }}
{{- $discoveryEnabled := or $dispatch.enabled $labelSource.enabled (ne (len ($cronSchedules | default (dict))) 0) }}
{{- if $ownershipList }}
{{- if and $secure.enabled (not (has $secure.runNamespace $ownershipList)) }}
{{- fail (printf "secure.runNamespace %q must appear in %s; a secure controller never reconciles runs outside its run namespace" $secure.runNamespace $ownershipSource) }}
{{- end }}
{{- if and $discoveryEnabled (not (has .Release.Namespace $ownershipList)) }}
{{- fail (printf "discovery sources materialize CoderRuns in the release namespace %q, which %s %v do not own; deploy this release inside an owned namespace or drop the boundary" .Release.Namespace $ownershipSource $ownershipList) }}
{{- end }}
{{- else }}
{{- if and $secure.enabled $discoveryEnabled (ne .Release.Namespace $secure.runNamespace) }}
{{- fail (printf "a secure release with discovery enabled derives an ownership boundary of just secure.runNamespace %q, so it must be deployed there (release namespace %q); list ownership.namespaces explicitly to choose otherwise" $secure.runNamespace .Release.Namespace) }}
{{- end }}
{{- end }}
{{- $_ := set $container "args" $args }}

{{- /* The namespaced evidence Secret authority renders only when the intake is enabled. */}}
{{- if not $evidence.enabled }}
{{- if hasKey .Values.rbac.roles "evidence" }}{{- $_ := unset .Values.rbac.roles "evidence" }}{{- end }}
{{- if hasKey .Values.rbac.bindings "evidence" }}{{- $_ := unset .Values.rbac.bindings "evidence" }}{{- end }}
{{- end }}

{{- include "bjw-s.common.loader.generate" . }}
