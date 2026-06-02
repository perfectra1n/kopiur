{{- if eq .Values.installScope "cluster" -}}
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "kopiur.fullname" . }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "kopiur.fullname" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "kopiur.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}
