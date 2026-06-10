{{- if .Values.webhook.enabled -}}
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: {{ include "kopiur.fullname" . }}-validating
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
  {{- if eq .Values.webhook.tls.mode "cert-manager" }}
  annotations:
    # cert-manager ca-injector populates caBundle on all webhooks below.
    cert-manager.io/inject-ca-from: {{ .Release.Namespace }}/{{ include "kopiur.webhook.fullname" . }}
  {{- end }}
webhooks:
  - name: validate.kopiur.home-operations.com
    admissionReviewVersions: ["v1"]
    sideEffects: None
    failurePolicy: {{ .Values.webhook.failurePolicy }}
    timeoutSeconds: {{ .Values.webhook.timeoutSeconds }}
    clientConfig:
      {{- if eq .Values.webhook.tls.mode "manual" }}
      caBundle: {{ .Values.webhook.caBundle | quote }}
      {{- end }}
      # self mode: the operator injects caBundle at runtime (omitted here so its
      # field manager owns it). cert-manager mode: ca-injector populates it.
      service:
        name: {{ include "kopiur.webhook.fullname" . }}
        namespace: {{ .Release.Namespace }}
        path: /admission
        port: 443
    rules:
      - apiGroups: ["kopiur.home-operations.com"]
        apiVersions: ["v1alpha1"]
        operations: ["CREATE", "UPDATE"]
        resources:
          - repositories
          - clusterrepositories
          - snapshotpolicies
          - snapshots
          - snapshotschedules
          - restores
          - maintenances
          - repositoryreplications
        scope: "*"
{{- end }}
