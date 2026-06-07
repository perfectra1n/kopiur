{{- if and .Values.webhook.enabled (eq .Values.webhook.tls.mode "cert-manager") -}}
{{- if not .Values.webhook.certManager.issuerRef.name }}
# Self-signed Issuer — only created when no external issuerRef is supplied.
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: {{ include "kopiur.webhook.fullname" . }}-selfsign
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
spec:
  selfSigned: {}
---
{{- end }}
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ include "kopiur.webhook.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
spec:
  # cert-manager writes the serving cert here; the webhook Deployment mounts it.
  secretName: {{ .Values.webhook.tls.secretName }}
  dnsNames:
    - {{ include "kopiur.webhook.fullname" . }}.{{ .Release.Namespace }}.svc
    - {{ include "kopiur.webhook.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local
  issuerRef:
    {{- if .Values.webhook.certManager.issuerRef.name }}
    name: {{ .Values.webhook.certManager.issuerRef.name }}
    kind: {{ .Values.webhook.certManager.issuerRef.kind }}
    {{- else }}
    name: {{ include "kopiur.webhook.fullname" . }}-selfsign
    kind: Issuer
    {{- end }}
    group: cert-manager.io
{{- end }}
