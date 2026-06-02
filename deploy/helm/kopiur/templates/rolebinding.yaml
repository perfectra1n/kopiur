{{- if eq .Values.installScope "namespaced" -}}
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "kopiur.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "kopiur.fullname" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "kopiur.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}
