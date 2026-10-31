{{/*
Expand the name of the chart.
*/}}
{{- define "parqtel.mcp.name" -}}
{{- default .Chart.Name .Values.mcp.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "parqtel.mcp.fullname" -}}
{{- if .Values.mcp.fullnameOverride }}
{{- .Values.mcp.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.mcp.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "parqtel.mcp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "parqtel.mcp.labels" -}}
helm.sh/chart: {{ include "parqtel.mcp.chart" . }}
{{ include "parqtel.mcp.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "parqtel.mcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "parqtel.mcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
