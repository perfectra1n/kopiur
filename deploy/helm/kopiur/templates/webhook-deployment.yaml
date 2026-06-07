{{- if .Values.webhook.enabled -}}
{{- $mode := .Values.webhook.tls.mode | default "self" -}}
{{- if not (has $mode (list "self" "cert-manager" "manual")) -}}
{{- fail (printf "webhook.tls.mode must be one of self|cert-manager|manual, got %q" $mode) -}}
{{- end -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "kopiur.webhook.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: webhook
spec:
  replicas: {{ .Values.webhook.replicaCount }}
  selector:
    matchLabels:
      {{- include "kopiur.webhook.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "kopiur.webhook.selectorLabels" . | nindent 8 }}
        {{- with .Values.webhook.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .Values.webhook.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      serviceAccountName: {{ include "kopiur.serviceAccountName" . }}
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.webhook.priorityClassName }}
      priorityClassName: {{ . }}
      {{- end }}
      securityContext:
        {{- toYaml .Values.podSecurityContext | nindent 8 }}
      containers:
        - name: webhook
          image: {{ include "kopiur.image" (dict "root" $ "img" .Values.image.webhook) }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          env:
            {{- include "kopiur.loggingEnv" . | nindent 12 }}
            - name: KOPIUR_WEBHOOK_ADDR
              value: {{ .Values.webhook.listenAddr | quote }}
            - name: KOPIUR_WEBHOOK_TLS_CERT
              value: /tls/tls.crt
            - name: KOPIUR_WEBHOOK_TLS_KEY
              value: /tls/tls.key
            {{- include "kopiur.otlpEnv" . | nindent 12 }}
          ports:
            - name: https
              containerPort: {{ .Values.webhook.containerPort }}
              protocol: TCP
          livenessProbe:
            httpGet:
              path: /healthz
              port: https
              scheme: HTTPS
            initialDelaySeconds: 5
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /readyz
              port: https
              scheme: HTTPS
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            {{- toYaml .Values.webhook.resources | nindent 12 }}
          securityContext:
            {{- toYaml .Values.securityContext | nindent 12 }}
          volumeMounts:
            - name: tls
              mountPath: /tls
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: {{ .Values.webhook.tls.secretName }}
      {{- with .Values.webhook.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.webhook.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.webhook.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end }}
