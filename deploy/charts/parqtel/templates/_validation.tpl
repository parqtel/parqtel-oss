{{- if .Values.autoscaling.enabled }}
{{- if gt (int .Values.autoscaling.minReplicas) 1 }}
{{- if not (or (contains "ReadWriteMany" (toString .Values.storage.accessMode)) .Values.storage.existingClaim) }}
{{- range $i := until 1 }}
{{- "WARNING: autoscaling with more than 1 replica requires shared storage (ReadWriteMany) or an external backend." | println }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- if .Values.ingress.enabled }}
{{- if not .Values.ingress.hosts }}
{{- fail "Ingress is enabled but no hosts are defined." }}
{{- end }}
{{- end }}

{{- if .Values.resources.limits.memory }}
{{- if and (not (contains "Gi" .Values.resources.limits.memory)) (lt (int (trimSuffix "Mi" .Values.resources.limits.memory)) 64) }}
{{- fail "Memory limit must not be below 64Mi." }}
{{- end }}
{{- end }}

{{- if and (not .Values.storage.enabled) (gt (int .Values.replicaCount) 1) }}
{{- "WARNING: Multiple replicas with storage disabled will result in ephemeral data silos." | println }}
{{- end }}

{{- if .Values.networkPolicy.enabled }}
{{- if and (not .Values.networkPolicy.ingressSelectorLabels) (not .Values.networkPolicy.monitoringNamespace) }}
{{- "WARNING: NetworkPolicy is enabled but no ingress selectors or monitoring namespace are defined. This may block all traffic." | println }}
{{- end }}
{{- end }}
