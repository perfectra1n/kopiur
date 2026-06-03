{{- if and .Values.webhook.enabled .Values.webhook.serviceMonitor.enabled -}}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "kopiur.webhook.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: webhook
    {{- with .Values.webhook.serviceMonitor.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "kopiur.webhook.selectorLabels" . | nindent 6 }}
  namespaceSelector:
    matchNames:
      - {{ .Release.Namespace }}
  endpoints:
    # The webhook serves /metrics on its TLS port (443 → container 8443).
    - port: https
      scheme: https
      path: /metrics
      interval: {{ .Values.webhook.serviceMonitor.interval }}
      scrapeTimeout: {{ .Values.webhook.serviceMonitor.scrapeTimeout }}
      tlsConfig:
        insecureSkipVerify: {{ .Values.webhook.serviceMonitor.insecureSkipVerify }}
{{- end }}
