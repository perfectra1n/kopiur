{{- if and .Values.metrics.enabled .Values.metrics.serviceMonitor.enabled -}}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "kopiur.controller.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    {{- with .Values.metrics.serviceMonitor.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "kopiur.controller.selectorLabels" . | nindent 6 }}
  namespaceSelector:
    matchNames:
      - {{ .Release.Namespace }}
  endpoints:
    - port: metrics
      path: /metrics
      interval: {{ .Values.metrics.serviceMonitor.interval }}
      scrapeTimeout: {{ .Values.metrics.serviceMonitor.scrapeTimeout }}
      {{- with .Values.metrics.serviceMonitor.relabelings }}
      relabelings:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.metrics.serviceMonitor.metricRelabelings }}
      metricRelabelings:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end }}
