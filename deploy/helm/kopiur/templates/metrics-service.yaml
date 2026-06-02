{{- if .Values.metrics.enabled -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "kopiur.controller.fullname" . }}-metrics
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
spec:
  type: ClusterIP
  ports:
    - name: metrics
      port: {{ .Values.metrics.port }}
      targetPort: metrics
      protocol: TCP
  selector:
    {{- include "kopiur.controller.selectorLabels" . | nindent 4 }}
{{- end }}
