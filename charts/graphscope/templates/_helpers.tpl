{{/*
Create a default full qualified app name.
We truncate at 30 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "graphscope.fullname" -}}
{{- .Release.Name | trunc 30 | trimSuffix "-" }}
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
Return the proper image name
{{ include "graphscope.images.image" ( dict "imageRoot" .Values.path.to.the.image "DefaultTag" .DefaultTag "Component" .Values.path.to.component) }}
*/}}
{{- define "graphscope.images.image" -}}
{{- $registryName := .imageRoot.registry -}}
{{- $repositoryName := .imageRoot.repository -}}
{{- $tag := .imageRoot.tag | toString -}}
{{- $component := .Component.image.name -}}
{{- if not $tag }}
{{- if .DefaultTag }}
{{- $tag = .DefaultTag -}}
{{- else -}}
{{- $tag = "latest" -}}
{{- end -}}
{{- end -}}
{{- if $registryName }}
{{- printf "%s/%s/%s:%s" $registryName $repositoryName $component $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" $repositoryName $component $tag -}}
{{- end -}}
{{- end -}}


{{/*
Unique Label of GraphScope Coordinator.
*/}}
{{- define "graphscope.coordinator.uniqueLabel" -}}
graphscope.coordinator.name: coordinator-{{ include "graphscope.fullname" . }}
{{- end }}

{{/*
Label Selector Corresponding to Unique Label of GraphScope Coordinator
*/}}
{{- define "graphscope.coordinator.labelSelector" -}}
graphscope.coordinator.name=coordinator-{{ include "graphscope.fullname" . }}
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
