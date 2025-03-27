{{- define "graphscope-interactive.etcd.endpoint" -}}
{{- printf "http://%s-etcd.%s.svc.cluster.local:2379" .Release.Name .Release.Namespace | quote }}
{{- end -}}

{{- define "graphscope-interactive.master.workspace" -}}
{{- if .Values.workspace }}
{{- .Values.workspace }}
{{- else }}
{{- "/tmp/interactive_workspace/" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.master.codegenWorkDir" -}}
{{- printf "%s/codegen" (include "graphscope-interactive.master.workspace" .) }}
{{- end -}}

{{- define "graphscope-interactive.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "graphscope-interactive.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "graphscope-interacitve.engine.defaultGraphSchemaPath" -}}
{{- "/opt/flex/share/gs_interactive_default_graph/graph.yaml" }}
{{- end -}}


{{- define "graphscope-interactive.ossAccessKeyId" -}}
{{- if .Values.oss.accessKeyId }}
{{- .Values.oss.accessKeyId | quote }}
{{- else }}
{{- "" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.ossAccessKeySecret" -}}
{{- if .Values.oss.accessKeySecret }}
{{- .Values.oss.accessKeySecret | quote }}
{{- else }}
{{- "" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.ossEndpoint" -}}
{{- if .Values.oss.endpoint }}
{{- .Values.oss.endpoint | quote }}
{{- else }}
{{- "" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.ossBucketName" -}}
{{- if .Values.oss.bucketName }}
{{- .Values.oss.bucketName | quote }}
{{- else }}
{{- "" }}
{{- end -}}
{{- end -}}


{{- define "graphscope-interactive.engine.fullname" -}}
{{- printf "%s-%s" (include "graphscope-interactive.fullname" .) "engine" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "graphscope-interactive.master.fullname" -}}
{{- printf "%s-%s" (include "graphscope-interactive.fullname" .) "master" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-interactive.master.serviceName" -}}
{{- printf "%s-%s" (include "graphscope-interactive.master.fullname" .) "headless" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "graphscope-interactive.master.servicePort" -}}
{{- if .Values.master.service.adminPort }}
{{- .Values.master.service.adminPort }}
{{- else }}
{{- 7776 }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.master.endpoint" -}}
{{- printf "%s-0.%s.%s.svc.cluster.local:%s" (include "graphscope-interactive.master.fullname" .) (include "graphscope-interactive.master.serviceName" . ) .Release.Namespace (include "graphscope-interactive.master.servicePort" .) | trimSuffix "-" }}
{{- end -}}

{{- define "graphscope-interactive.master.entrypointMountPath" -}}
{{- if .Values.master.entrypointMountPath }}
{{- .Values.master.entrypointMountPath }}
{{- else }}
{{- "/etc/interactive/master_entrypoint.sh" }}
{{- end -}}
{{- end -}}


{{- define "graphscope-interactive.engine.entrypointMountPath" -}}
{{- if .Values.engine.entrypointMountPath }}
{{- .Values.engine.entrypointMountPath }}
{{- else }}
{{- "/etc/interactive/engine_entrypoint.sh" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.master.command" -}}
{{- if .Values.master.command }}
{{- toYaml .Values.master.command }}
{{- else }}
{{- include "graphscope-interactive.master.entrypointMountPath" . }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.engine.command" -}}
{{- if .Values.engine.command }}
{{- toYaml .Values.engine.command }}
{{- else }}
{{- include "graphscope-interactive.engine.entrypointMountPath" . }}
{{- end -}}
{{- end -}}



{{- define "graphscope-interactive.engine.metadataStoreUri" -}}
{{- if .Values.engine.metadataStoreUri -}}
{{- .Values.engine.metadataStoreUri }}
{{- else }}
{{- include "graphscope-interactive.etcd.endpoint" . }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.engine.compiler.metaReaderSchemaUri" -}}
{{- if .Values.engine.compiler.meta.reader.schema.uri -}}
{{- .Values.engine.compiler.meta.reader.schema.uri }}
{{- else }}
{{- printf "http://%s/v1/graph/%s/schema" (include "graphscope-interactive.master.endpoint" . ) "1"  | trimSuffix "-" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.engine.compiler.metaStatisticsUri" -}}
{{- if .Values.engine.compiler.meta.reader.statistics.uri -}}
{{- .Values.engine.compiler.meta.reader.statistics.uri }}
{{- else }}
{{- printf "http://%s/v1/graph/%s/statistics" (include "graphscope-interactive.master.endpoint" . )  "1" | trimSuffix "-" }}
{{- end -}}
{{- end -}}


{{- define "graphscope-interactive.engine.walUri" -}}
{{- if .Values.engine.walUri -}}
{{- .Values.engine.walUri }}
{{- else }}
{{- "file://{GRAPH_DATA_DIR}/wal" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.master.serviceRegistry.endpoint" -}}
{{- if .Values.master.serviceRegistry.endpoint }}
{{- .Values.master.serviceRegistry.endpoint }}
{{- else }}
{{- include "graphscope-interactive.etcd.endpoint" . }}
{{- end -}}
{{- end -}}


{{- define "graphscope-interactive.master.configFileMountPath" -}}
{{- if .Values.master.configFileMountPath }}
{{- .Values.master.configFileMountPath }}
{{- else }}
{{- "/opt/flex/share/interactive_config.yaml" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.engine.configFileMountPath" -}}
{{- if .Values.engine.configFileMountPath }}
{{- .Values.engine.configFileMountPath }}
{{- else }}
{{- "/opt/flex/share/interactive_config.yaml" }}
{{- end -}}
{{- end -}}

{{- define "graphscope-interactive.labels" -}}
helm.sh/chart: {{ include "graphscope-interactive.chart" . }}
{{ include "graphscope-interactive.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "graphscope-interactive.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "graphscope-interactive.selectorLabels" -}}
app.kubernetes.io/name: {{ include "graphscope-interactive.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "graphscope-interactive.master.image" -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- with .Values.master.image -}}
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

{{- define "graphscope-interactive.engine.image" -}}
{{- $tag := .Chart.AppVersion | toString -}}
{{- with .Values.engine.image -}}
{{- if .tag }}
{{- $tag = .tag | toString -}}
{{- end -}}
{{- if .registry -}}
{{- printf "%s/%s:%s" .registry .repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .repository $tag -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "graphscope-interactive.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "graphscope-interactive.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}


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


{{- define "graphscope-interactive.configmapName" -}}
{{- if .Values.existingConfigmap -}}
    {{- printf "%s" (tpl .Values.existingConfigmap $) -}}
{{- else -}}
    {{- printf "%s-%s" (include "graphscope-interactive.fullname" .) "config" | trimSuffix "-" -}}
{{- end -}}
{{- end -}}



{{- define "graphscope-interactive.engineBinaryPath" -}}
/opt/flex/bin/interactive_server
{{- end -}}

{{- define "graphscope-interactive.bulkLoaderBinaryPath" -}}
/opt/flex/bin/bulk_loader
{{- end -}}


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
    {{- end -}}
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

