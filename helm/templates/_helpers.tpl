{{/*
Expand the name of the chart.
*/}}
{{- define "databricks-specific-provisioner.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "databricks-specific-provisioner.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "databricks-specific-provisioner.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "databricks-specific-provisioner.labels" -}}
app.kubernetes.io/name: {{ include "databricks-specific-provisioner.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: databricks-specific-provisioner
app.kubernetes.io/part-of: witboost
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ include "databricks-specific-provisioner.chart" . }}
{{- if .Values.labels }}
{{ toYaml .Values.labels }}
{{- else if and .Values.global .Values.global.labels }}
{{ toYaml .Values.global.labels }}
{{- end }}
{{- end -}}

