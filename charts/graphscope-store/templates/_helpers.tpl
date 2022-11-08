{{/*
Expand the name of the chart.
*/}}
{{- define "graphscope-store.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "graphscope-store.fullname" -}}
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

{{- define "graphscope-store.coordinator.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store.fullname" .) "coordinator" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-store.frontend.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store.fullname" .) "frontend" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-store.ingestor.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store.fullname" .) "ingestor" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-store.store.fullname" -}}
{{- printf "%s-%s" (include "graphscope-store.fullname" .) "store" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "graphscope-store.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "graphscope-store.labels" -}}
helm.sh/chart: {{ include "graphscope-store.chart" . }}
{{ include "graphscope-store.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "graphscope-store.selectorLabels" -}}
app.kubernetes.io/name: {{ include "graphscope-store.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Return the proper graphscope-store image name
*/}}
{{- define "graphscope-store.image" -}}
{{ include "graphscope-store.images.image" (dict "imageRoot" .Values.image "global" .Values.global "DefaultTag" .Chart.AppVersion ) }}
{{- end -}}

{{/*
Return the proper image name
{{ include "graphscope-store.images.image" ( dict "imageRoot" .Values.path.to.the.image "global" $ "DefaultTag" .Chart.AppVersion ) }}
*/}}
{{- define "graphscope-store.images.image" -}}
{{- $registryName := .imageRoot.registry -}}
{{- $repositoryName := .imageRoot.repository -}}
{{- $tag := .imageRoot.tag | toString -}}
{{- if .global }}
    {{- if .global.imageRegistry }}
     {{- $registryName = .global.imageRegistry -}}
    {{- end -}}
{{- end -}}
{{- if not $tag }}
{{- if .imageRoot.DefaultTag }}
{{- $tag = .imageRoot.DefaultTag -}}
{{- else -}}
{{- $tag = "latest" -}}
{{- end -}}
{{- end -}}
{{- if $registryName }}
{{- printf "%s/%s:%s" $registryName $repositoryName $tag -}}
{{- else -}}
{{- printf "%s:%s" $repositoryName $tag -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "graphscope-store.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "graphscope-store.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the configmap with the graphscope configuration
*/}}
{{- define "graphscope-store.configmapName" -}}
{{- if .Values.existingConfigmap -}}
    {{- printf "%s" (tpl .Values.existingConfigmap $) -}}
{{- else -}}
    {{- printf "%s" (include "graphscope-store.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a configmap object should be created for graphscope-service
*/}}
{{- define "graphscope-store.createConfigmap" -}}
{{- if not .Values.existingConfigmap }}
    {{- true -}}
{{- else -}}
{{- end -}}
{{- end -}}


{{/*
Renders a value that contains template.
Usage:
{{ include "graphscope-store.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $) }}
*/}}
{{- define "graphscope-store.tplvalues.render" -}}
    {{- if typeIs "string" .value }}
        {{- tpl .value .context }}
    {{- else }}
        {{- tpl (.value | toYaml) .context }}
    {{- end }}
{{- end -}}




{{/*
Return the proper Storage Class
*/}}
{{- define "graphscope-store.storageClass" -}}
{{/*
Helm 2.11 supports the assignment of a value to a variable defined in a different scope,
but Helm 2.9 and 2.10 does not support it, so we need to implement this if-else logic.
*/}}
{{- if .Values.global -}}
    {{- if .Values.global.storageClass -}}
        {{- if (eq "-" .Values.global.storageClass) -}}
            {{- printf "storageClassName: \"\"" -}}
        {{- else }}
            {{- printf "storageClassName: %s" .Values.global.storageClass -}}
        {{- end -}}
    {{- else -}}
        {{- if .Values.persistence.storageClass -}}
              {{- if (eq "-" .Values.persistence.storageClass) -}}
                  {{- printf "storageClassName: \"\"" -}}
              {{- else }}
                  {{- printf "storageClassName: %s" .Values.persistence.storageClass -}}
              {{- end -}}
        {{- end -}}
    {{- end -}}
{{- else -}}
    {{- if .Values.persistence.storageClass -}}
        {{- if (eq "-" .Values.persistence.storageClass) -}}
            {{- printf "storageClassName: \"\"" -}}
        {{- else }}
            {{- printf "storageClassName: %s" .Values.persistence.storageClass -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified kafka name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "graphscope-store.kafka.fullname" -}}
{{- printf "%s-%s" .Release.Name "kafka" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Get full broker list.
*/}}
{{- define "graphscope-store.kafka.brokerlist" -}}

{{- $replicaCount := int .Values.kafka.replicaCount -}}
{{- $releaseNamespace := .Release.Namespace -}}
{{- $clusterDomain := .Values.clusterDomain -}}
{{- $fullname := include "graphscope-store.kafka.fullname" . -}}
{{- $servicePort := int .Values.kafka.service.port -}}

{{- $brokerList := list }}
{{- range $e, $i := until $replicaCount }}
{{- $brokerList = append $brokerList (printf "%s-%d.%s-headless.%s.svc.%s:%d" $fullname $i $fullname $releaseNamespace $clusterDomain $servicePort) }}
{{- end }}
{{- join "," $brokerList | printf "%s" -}}
{{- end -}}
