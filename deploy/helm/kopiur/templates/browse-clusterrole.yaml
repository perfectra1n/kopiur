{{- if .Values.rbac.browseRole -}}
# OPT-IN (rbac.browseRole): everything `kubectl kopiur ls/cat/download/browse`
# and `session end` need — and nothing more. The browse data-plane runs a
# read-only mover "session" Job in the snapshot's namespace and pod-execs a
# closed set of kopia read commands into it, so the user needs Job/ConfigMap
# create+delete and pods/exec, but NO `secrets` access: the session POD loads
# the repository credentials via envFrom; the browsing user never reads them.
# (The `--local` flag is the exception: it copies the credentials to the
# user's machine and therefore additionally needs `get secrets` — grant that
# separately and deliberately.)
#
# The chart renders only the ClusterRole; bind it to your users/groups with
# your own RoleBinding (per-namespace browse) or ClusterRoleBinding.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "kopiur.fullname" . }}-browse
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
rules:
  # Resolve the Snapshot → repository chain.
  - apiGroups:
      - kopiur.home-operations.com
    resources:
      - snapshots
      - repositories
    verbs: [get, list]
  - apiGroups:
      - kopiur.home-operations.com
    resources:
      - clusterrepositories
    verbs: [get]
  # Resolve the mover IMAGE from the controller Deployment (sessions must run
  # the exact mover the operator runs; the CLI refuses to guess otherwise).
  - apiGroups: [apps]
    resources:
      - deployments
    verbs: [get, list]
  # The session Job (find-or-create, end).
  - apiGroups: [batch]
    resources:
      - jobs
    verbs: [create, get, list, delete]
  # The session's work-spec ConfigMap (owned by the Job; created/deleted with it).
  - apiGroups: [""]
    resources:
      - configmaps
    verbs: [create, get, delete]
  # Wait for the session pod to become Ready; surface its logs on failure.
  - apiGroups: [""]
    resources:
      - pods
    verbs: [get, list, watch]
  - apiGroups: [""]
    resources:
      - pods/log
    verbs: [get]
  # The read path: exec the closed kopia session-command surface into the pod.
  - apiGroups: [""]
    resources:
      - pods/exec
    verbs: [create]
{{- end }}
