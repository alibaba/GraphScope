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
{{- if .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- range .Values.global.imagePullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}


{{/*
Transform the Docker Image Registry Secret Names to string with comma separated.
*/}}
{{- define "graphscope.imagePullSecretsStr" -}}
{{- if .Values.global.imagePullSecrets }}
{{ join "," .Values.global.imagePullSecrets }}
{{- end }}
{{- end }}


{{/*
Kubernetes Selector labels of GraphScope Coordinator.
*/}}
{{- define "graphscope.coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}


{{/*
Kubernetes labels of GraphScope Coordinator.
*/}}
{{- define "graphscope.coordinator.labels" -}}
graphscope.components: coordinator
helm.sh/chart: {{ include "graphscope.chart" . }}
{{ include "graphscope.coordinator.selectorLabels" . }}
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
