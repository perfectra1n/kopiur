{{- if .Values.webhook.enabled -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "kopiur.webhook.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: webhook
spec:
  type: ClusterIP
  ports:
    # The API server always calls admission webhooks on 443; map to the
    # container's TLS port (KOPIUR_WEBHOOK_ADDR, default 8443).
    - name: https
      port: 443
      targetPort: https
      protocol: TCP
  selector:
    {{- include "kopiur.webhook.selectorLabels" . | nindent 4 }}
{{- end }}
