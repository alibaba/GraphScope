{{/*
Create a default full qualified app name.
We truncate at 30 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "graphscope.fullname" -}}
{{- if contains .Chart.Name .Release.Name }}
{{- .Release.Name | trunc 30 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name .Chart.Name  | trunc 30 | trimSuffix "-" }}
{{- end }}
{{- end }}


{{/*
Create description information of chart as used by the label.
*/}}
{{- define "graphscope.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}


{{/*
Return the proper Docker Image Registry Secret Names
*/}}
{{- define "graphscope.imagePullSecrets" -}}
{{- if .Values.imagePullSecrets }}
imagePullSecrets:
{{- range .Values.imagePullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}


{{/*
Transform the Docker Image Registry Secret Names to string with comma separated.
*/}}
{{- define "graphscope.imagePullSecretsStr" -}}
{{- if .Values.imagePullSecrets }}
{{ join "," .Values.imagePullSecrets }}
{{- end }}
{{- end }}


{{/*
Unique Label of GraphScope Coordinator.
*/}}
{{- define "graphscope.coordinator.uniqueLabel" -}}
graphscope.coordinator.name: {{ include "graphscope.fullname" . }}-coordinator
{{- end }}

{{/*
Label Selector Corresponding to Unique Label of GraphScope Coordinator
*/}}
{{- define "graphscope.coordinator.labelSelector" -}}
graphscope.coordinator.name={{ include "graphscope.fullname" . }}-coordinator
{{- end }}

{{/*
Kubernetes labels of GraphScope Coordinator.
*/}}
{{- define "graphscope.coordinator.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
graphscope.components: coordinator
helm.sh/chart: {{ include "graphscope.chart" . }}
{{ include "graphscope.coordinator.uniqueLabel" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{/*
Return Service Annotations.
*/}}
{{- define "graphscope.service.annotations" -}}
annotations:
  "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-type": "tcp"
  "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-connect-timeout": "8"
  "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-healthy-threshold": "2"
  "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-unhealthy-threshold": "2"
  "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-interval": "1"
{{- end }}
