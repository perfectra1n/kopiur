{{- if and .Values.grafanaDashboard.enabled (not .Values.grafanaDashboard.grafanaOperator.enabled) -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "kopiur.fullname" . }}-dashboard
  namespace: {{ default .Release.Namespace .Values.grafanaDashboard.namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: dashboard
    {{ .Values.grafanaDashboard.label }}: {{ .Values.grafanaDashboard.labelValue | quote }}
    {{- with .Values.grafanaDashboard.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- if or .Values.grafanaDashboard.folderAnnotation .Values.grafanaDashboard.annotations }}
  annotations:
    {{- with .Values.grafanaDashboard.folderAnnotation }}
    {{ . }}: {{ $.Values.grafanaDashboard.folder | quote }}
    {{- end }}
    {{- with .Values.grafanaDashboard.annotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- end }}
data:
  kopiur.json: |-
    {{- .Files.Get "files/dashboards/kopiur.json" | nindent 4 }}
{{- end }}
