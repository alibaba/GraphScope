{{/*
Expand the name of the chart.
*/}}
{{- define "graphscope-interactive.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "graphscope-interactive.fullname" -}}
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

{{- define "graphscope-interactive.frontend.fullname" -}}
{{- printf "%s-%s" (include "graphscope-interactive.fullname" .) "frontend" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-interactive.engine.fullname" -}}
{{- printf "%s-%s" (include "graphscope-interactive.fullname" .) "engine" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "graphscope-interactive.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "graphscope-interactive.labels" -}}
helm.sh/chart: {{ include "graphscope-interactive.chart" . }}
{{ include "graphscope-interactive.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "graphscope-interactive.selectorLabels" -}}
app.kubernetes.io/name: {{ include "graphscope-interactive.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Return the proper graphscope-interactive frontend image name
*/}}
{{- define "graphscope-interactive.frontend.image" -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- with .Values.frontend.image -}}
{{- if .tag -}}
{{- $tag = .tag | toString -}}
{{- end -}}
{{- if .registry -}}
{{- printf "%s/%s:%s" .registry .repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .repository $tag -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Return the proper graphscope-interactive engine image name
*/}}
{{- define "graphscope-interactive.engine.image" -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- with .Values.engine.image -}}
{{- if .tag -}}
{{- $tag = .tag | toString -}}
{{- end -}}
{{- if .registry -}}
{{- printf "%s/%s:%s" .registry .repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .repository $tag -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{/*
Create the name of the service account to use
*/}}
{{- define "graphscope-interactive.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "graphscope-interactive.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the proper Storage Class
*/}}
{{- define "graphscope-interactive.storageClass" -}}
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
Return the configmap with the graphscope configuration
*/}}
{{- define "graphscope-interactive.configmapName" -}}
{{- if .Values.existingConfigmap -}}
    {{- printf "%s" (tpl .Values.existingConfigmap $) -}}
{{- else -}}
    {{- printf "%s-%s" (include "graphscope-interactive.fullname" .) "config" | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Return the engineConfigPath with the graphscope configuration
*/}}
{{- define "graphscope-interactive.engineConfigPath" -}}
/etc/interactive/interactive_config.yaml
{{- end -}}

{{/*
Return the realEngineConfigPath with the graphscope configuration, templated by frontend
*/}}
{{- define "graphscope-interactive.realEngineConfigPath" -}}
/etc/interactive/real_interactive_config.yaml
{{- end -}}

{{/*
Return the path to server_binary
*/}}
{{- define "graphscope-interactive.engineBinaryPath" -}}
/opt/flex/bin/interactive_server
{{- end -}}

{{/*
Return the path to compiler jar
*/}}
{{- define "graphscope-interactive.classPath" -}}
/opt/flex/lib/*
{{- end -}}

{{/*
Return the path to compiler jar
*/}}
{{- define "graphscope-interactive.libraryPath" -}}
/opt/flex/lib/
{{- end -}}

{{/*
Return the path to the default graph's schema file.
*/}}
{{- define "graphscope-interactive.graphSchemaPath" -}}
{{- if not .Values.defaultGraph}}
    {{- printf "%s/%s/%s/%s" (tpl .Values.workspace .) "data" "gs_interactive_default_graph" "graph.yaml" | trimSuffix "-" -}}
{{- else -}}
    {{- printf "%s/%s/%s/%s" (tpl .Values.workspace .) "data" (tpl .Values.defaultGraph .) "graph.yaml" | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a configmap object should be created for graphscope-service
*/}}
{{- define "graphscope-interactive.createConfigmap" -}}
{{- if not .Values.existingConfigmap }}
    {{- true -}}
{{- else -}}
{{- end -}}
{{- end -}}


{{/*
Renders a value that contains template.
Usage:
{{ include "graphscope-interactive.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $) }}
*/}}
{{- define "graphscope-interactive.tplvalues.render" -}}
    {{- if typeIs "string" .value }}
        {{- tpl .value .context }}
    {{- else }}
        {{- tpl (.value | toYaml) .context }}
    {{- end }}
{{- end -}}


{{/*
Return  the proper Storage Class
{{ include "graphscope-interactive.storage.class" .Values.path.to.the.persistence }}
*/}}
{{- define "graphscope-interactive.storage.class" -}}

{{- $storageClass := .storageClass -}}
{{- if $storageClass -}}
  {{- if (eq "-" $storageClass) -}}
      {{- printf "storageClassName: \"\"" -}}
  {{- else }}
      {{- printf "storageClassName: %s" $storageClass -}}
  {{- end -}}
{{- end -}}

{{- end -}}

