{{/*
Expand the name of the chart.
*/}}
{{- define "graphscope-store-service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "graphscope-store-service.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "graphscope-store-service.coordinator.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store-service.fullname" .) "coordinator" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-store-service.frontend.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store-service.fullname" .) "frontend" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-store-service.ingestor.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store-service.fullname" .) "ingestor" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-store-service.store.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store-service.fullname" .) "store" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "graphscope-store-service.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "graphscope-store-service.labels" -}}
helm.sh/chart: {{ include "graphscope-store-service.chart" . }}
{{ include "graphscope-store-service.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "graphscope-store-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "graphscope-store-service.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Return the proper graphscope-store-service image name
*/}}
{{- define "graphscope-store-service.image" -}}
{{ include "graphscope-store-service.images.image" .Values.image }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "graphscope-store-service.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "graphscope-store-service.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the configmap with the graphscope configuration
*/}}
{{- define "graphscope-store-service.configmapName" -}}
{{- if .Values.existingConfigmap -}}
    {{- printf "%s" (tpl .Values.existingConfigmap $) -}}
{{- else -}}
    {{- printf "%s" (include "graphscope-store-service.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a configmap object should be created for graphscope-service
*/}}
{{- define "graphscope-store-service.createConfigmap" -}}
{{- if not .Values.existingConfigmap }}
    {{- true -}}
{{- else -}}
{{- end -}}
{{- end -}}

{{/*
Return the proper image name (for the init container volume-permissions image)
*/}}
{{- define "graphscope-store-service.volumePermissions.image" -}}
{{ include "graphscope-store-service.images.image" .Values.volumePermissions.image }}
{{- end -}}

{{/*
Renders a value that contains template.
Usage:
{{ include "graphscope-store-service.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $) }}
*/}}
{{- define "graphscope-store-service.tplvalues.render" -}}
    {{- if typeIs "string" .value }}
        {{- tpl .value .context }}
    {{- else }}
        {{- tpl (.value | toYaml) .context }}
    {{- end }}
{{- end -}}


{{/*
Return the proper image name
{{ include "graphscope-store-service.images.image" .Values.path.to.the.image }}
*/}}
{{- define "graphscope-store-service.images.image" -}}
{{- $tag := .tag | toString -}}
{{- if .registry }}
{{- printf "%s/%s:%s" .registry .repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .repository $tag -}}
{{- end -}}
{{- end -}}


{{/*
Return  the proper Storage Class
{{ include "graphscope-store-service.storage.class" .Values.path.to.the.persistence }}
*/}}
{{- define "graphscope-store-service.storage.class" -}}

{{- $storageClass := .storageClass -}}
{{- if $storageClass -}}
  {{- if (eq "-" $storageClass) -}}
      {{- printf "storageClassName: \"\"" -}}
  {{- else }}
      {{- printf "storageClassName: %s" $storageClass -}}
  {{- end -}}
{{- end -}}

{{- end -}}

{{/*
Create a default fully qualified kafka name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "graphscope-store-service.kafka.fullname" -}}
{{- printf "%s-%s" .Release.Name "kafka" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Get full broker list.
*/}}
{{- define "graphscope-store-service.kafka.brokerlist" -}}

{{- $replicaCount := int .Values.kafka.replicaCount -}}
{{- $releaseNamespace := .Release.Namespace -}}
{{- $clusterDomain := .Values.clusterDomain -}}
{{- $fullname := include "graphscope-store-service.kafka.fullname" . -}}
{{- $servicePort := int .Values.kafka.service.port -}}

{{- $brokerList := list }}
{{- range $e, $i := until $replicaCount }}
{{- $brokerList = append $brokerList (printf "%s-%d.%s-headless.%s.svc.%s:%d" $fullname $i $fullname $releaseNamespace $clusterDomain $servicePort) }}
{{- end }}
{{- join "," $brokerList | printf "%s" -}}
{{- end -}}