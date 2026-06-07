{{- if and .Values.grafanaDashboard.enabled .Values.grafanaDashboard.grafanaOperator.enabled -}}
apiVersion: grafana.integreatly.org/v1beta1
kind: GrafanaDashboard
metadata:
  name: {{ include "kopiur.fullname" . }}-dashboard
  namespace: {{ default .Release.Namespace .Values.grafanaDashboard.namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: dashboard
    {{- with .Values.grafanaDashboard.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .Values.grafanaDashboard.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  allowCrossNamespaceImport: {{ .Values.grafanaDashboard.grafanaOperator.allowCrossNamespaceImport }}
  resyncPeriod: {{ .Values.grafanaDashboard.grafanaOperator.resyncPeriod | quote }}
  {{- with .Values.grafanaDashboard.grafanaOperator.folder }}
  folder: {{ . | quote }}
  {{- end }}
  instanceSelector:
    matchLabels:
      {{- toYaml .Values.grafanaDashboard.grafanaOperator.matchLabels | nindent 6 }}
  json: |-
    {{- .Files.Get "files/dashboards/kopiur.json" | nindent 4 }}
{{- end }}
