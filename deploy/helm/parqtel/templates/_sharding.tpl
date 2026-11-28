{{/*
Helpers for the sharded topology (mode: sharded).

One chart, three workloads: writers own the WAL and the ingest listeners, the
readers are stateless query pods, and the compactor runs block maintenance. The
role is chosen with `--role` in the container args, so every workload mounts the
same role-neutral ConfigMap.
*/}}
{{- define "parqtel.shardedName" -}}
{{- printf "%s-%s" (include "parqtel.fullname" .root) .role | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Component selector labels: one workload per component in one release. The
component defaults to the role; the alert evaluator sets it explicitly because
it runs with `--role reader`.
*/}}
{{- define "parqtel.shardedSelector" -}}
app.kubernetes.io/name: {{ include "parqtel.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component | default .role }}
{{- end }}

{{/*
The container every sharded workload runs.

Arguments: dict "root" $ "role" "writer|reader|compactor" plus optional
"component" (defaults to the role) and "alertEvaluator" (default false).
Every sharded workload pins PARQTEL__SERVER__ALERT_EVALUATOR, so only the
dedicated single-replica evaluator Deployment ever evaluates alerts.
*/}}
{{- define "parqtel.shardedContainer" -}}
- name: {{ .root.Chart.Name }}
  image: {{ include "parqtel.image" .root | quote }}
  imagePullPolicy: {{ .root.Values.image.pullPolicy }}
  args: ["serve", "--role", {{ .role | quote }}]
  securityContext:
    {{- toYaml .root.Values.containerSecurityContext | nindent 4 }}
  ports:
    - name: http
      containerPort: {{ .root.Values.service.port }}
      protocol: TCP
    {{- if and (eq .role "writer") .root.Values.ingestion.grpc.enabled }}
    - name: grpc
      containerPort: {{ .root.Values.ingestion.grpc.port }}
      protocol: TCP
    {{- end }}
  env:
    - name: PARQTEL_CONFIG
      value: "/etc/parqtel/parqtel.toml"
    - name: RUST_LOG
      value: {{ .root.Values.parqtel.telemetry.logLevel | quote }}
    - name: MALLOC_ARENA_MAX
      value: {{ .root.Values.parqtel.mallocArenaMax | default 2 | quote }}
    - name: K8S_POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: K8S_NAMESPACE
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
    - name: K8S_NODE_NAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: PARQTEL__QUERY__INTERNAL_AUTH_TOKEN
      valueFrom:
        secretKeyRef:
          name: {{ include "parqtel.internalTokenSecretName" .root }}
          key: {{ .root.Values.sharding.internalToken.secretKey }}
    # Readers/writers/compactors must never evaluate alerts: only the dedicated
    # single-replica evaluator may (otherwise every replica notifies).
    - name: PARQTEL__SERVER__ALERT_EVALUATOR
      value: {{ .alertEvaluator | default false | quote }}
    {{- with .root.Values.sharding.objectStore.existingSecret }}
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: {{ . }}
          key: AWS_ACCESS_KEY_ID
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: {{ . }}
          key: AWS_SECRET_ACCESS_KEY
    {{- end }}
    {{- with .root.Values.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .root.Values.livenessProbe }}
  livenessProbe:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .root.Values.readinessProbe }}
  readinessProbe:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- $probe := mergeOverwrite (dict "httpGet" (dict "path" "/health" "port" "http") "periodSeconds" 5) (deepCopy .root.Values.startupProbe) }}
  startupProbe:
    {{- $_ := set $probe "failureThreshold" 120 }}
    {{- toYaml $probe | nindent 4 }}
  resources:
    {{- $default := .root.Values.resources }}
    {{- $override := dict }}
    {{- $component := .component | default .role }}
    {{- if eq $component "writer" }}{{ $override = .root.Values.sharding.writer.resources }}{{ end }}
    {{- if eq $component "reader" }}{{ $override = .root.Values.sharding.reader.resources }}{{ end }}
    {{- if eq $component "compactor" }}{{ $override = .root.Values.sharding.compactor.resources }}{{ end }}
    {{- if eq $component "alert-evaluator" }}{{ $override = .root.Values.sharding.alertEvaluator.resources }}{{ end }}
    {{- toYaml (mergeOverwrite (deepCopy $default) $override) | nindent 4 }}
  {{- with .root.Values.lifecycle }}
  lifecycle:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  volumeMounts:
    - name: config
      mountPath: /etc/parqtel
      readOnly: true
    - name: tmp
      mountPath: /tmp
    {{- if eq .role "writer" }}
    # WAL staging plus the local block cache the writer uploads from.
    - name: data
      mountPath: /data
    {{- else }}
    # Readers and the compactor cache blocks from the bucket; nothing durable
    # lives on this pod.
    - name: data
      mountPath: /data
    {{- end }}
    {{- with .root.Values.extraVolumeMounts }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end }}

{{/* Name of the Secret holding the internal buffer token. */}}
{{- define "parqtel.internalTokenSecretName" -}}
{{- if .Values.sharding.internalToken.existingSecret }}
{{- .Values.sharding.internalToken.existingSecret }}
{{- else }}
{{- printf "%s-internal-token" (include "parqtel.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
