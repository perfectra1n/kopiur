{{/*
Expand the name of the chart.
*/}}
{{- define "kopiur.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars (k8s name limit, DNS-1123).
*/}}
{{- define "kopiur.fullname" -}}
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

{{/*
Chart name and version, as used by the helm.sh/chart label.
*/}}
{{- define "kopiur.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "kopiur.labels" -}}
helm.sh/chart: {{ include "kopiur.chart" . }}
{{ include "kopiur.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: kopiur
{{- end }}

{{/*
Selector labels (stable across upgrades — never add version here).
*/}}
{{- define "kopiur.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kopiur.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Controller component selector labels.
*/}}
{{- define "kopiur.controller.selectorLabels" -}}
{{ include "kopiur.selectorLabels" . }}
app.kubernetes.io/component: controller
{{- end }}

{{/*
Webhook component selector labels.
*/}}
{{- define "kopiur.webhook.selectorLabels" -}}
{{ include "kopiur.selectorLabels" . }}
app.kubernetes.io/component: webhook
{{- end }}

{{/*
The name of the ServiceAccount to use.
*/}}
{{- define "kopiur.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kopiur.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Controller component name.
*/}}
{{- define "kopiur.controller.fullname" -}}
{{- printf "%s-controller" (include "kopiur.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Webhook component name.
*/}}
{{- define "kopiur.webhook.fullname" -}}
{{- printf "%s-webhook" (include "kopiur.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Resolve an image reference: registry/repository@digest if digest set, else
registry/repository:tag (tag defaults to .Chart.AppVersion).
Usage: include "kopiur.image" (dict "root" $ "img" .Values.image.controller)
*/}}
{{- define "kopiur.image" -}}
{{- $root := .root -}}
{{- $img := .img -}}
{{- $registry := $root.Values.image.registry -}}
{{- $repo := $img.repository -}}
{{- if $img.digest -}}
{{- printf "%s/%s@%s" $registry $repo $img.digest -}}
{{- else -}}
{{- $tag := default $root.Chart.AppVersion $img.tag -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- end -}}
{{- end }}

{{/*
Whether RBAC should be cluster-scoped.
*/}}
{{- define "kopiur.clusterScoped" -}}
{{- eq .Values.installScope "cluster" -}}
{{- end }}

{{/*
OTLP environment for the controller + webhook (and, via the controller, mover
Jobs). Emits the standard OTEL_EXPORTER_OTLP_* vars only when
observability.otlp.enabled. The env var NAMES match crates/telemetry/src/env.rs.
Usage: {{- include "kopiur.otlpEnv" . | nindent 12 }}
*/}}
{{- define "kopiur.otlpEnv" -}}
{{- if .Values.observability.otlp.enabled }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ required "observability.otlp.endpoint is required when observability.otlp.enabled" .Values.observability.otlp.endpoint | quote }}
{{- with .Values.observability.otlp.protocol }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ . | quote }}
{{- end }}
{{- with .Values.observability.otlp.headers }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  value: {{ . | quote }}
{{- end }}
{{- if .Values.observability.otlp.strict }}
- name: KOPIUR_OTEL_STRICT
  value: "true"
{{- end }}
{{- with .Values.observability.otlp.extraEnv }}
{{- toYaml . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Logging environment for the controller + webhook (and, via the controller, mover
Jobs — the controller forwards RUST_LOG + KOPIUR_LOG_FORMAT from its own env).
RUST_LOG resolves from logging.level, falling back to the deprecated
controller.logLevel. KOPIUR_LOG_FORMAT selects text|json. The env var NAMES match
crates/telemetry/src/env.rs.
Usage: {{- include "kopiur.loggingEnv" . | nindent 12 }}
*/}}
{{- define "kopiur.loggingEnv" -}}
- name: RUST_LOG
  value: {{ .Values.logging.level | default .Values.controller.logLevel | quote }}
- name: KOPIUR_LOG_FORMAT
  value: {{ .Values.logging.format | default "text" | quote }}
{{- end }}
