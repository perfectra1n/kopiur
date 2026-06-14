{{- if eq .Values.installScope "namespaced" -}}
# RBAC rules SYNCED from `cargo xtask gen-rbac` (deploy/rbac/operator-role.yaml).
# That xtask is the SOURCE OF TRUTH — it derives the kopiur.home-operations.com rules from the
# kube-rs Resource traits (ADR §4.12). If you edit kopiur.home-operations.com permissions, edit the
# xtask and re-run `cargo xtask gen-rbac`, then re-sync these rules. Names/labels
# are Helm-templated so the chart owns them.
#
# Note vs. the ClusterRole: a namespaced Role intentionally omits
# `clusterrepositories` (a cluster-scoped kind unreachable from a Role).
# ClusterRepository is only reconciled in installScope=cluster. The mover SA +
# RoleBinding minting rules ARE retained: the controller mints the mover RBAC in the
# (single, in-scope) workload namespace before each mover Job.
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "kopiur.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
rules:
  - apiGroups:
      - kopiur.home-operations.com
    resources:
      - repositories
      - snapshotpolicies
      - snapshots
      - snapshotschedules
      - restores
      - maintenances
      - repositoryreplications
    verbs: [get, list, watch, create, update, patch, delete]
  - apiGroups:
      - kopiur.home-operations.com
    resources:
      - repositories/status
      - repositories/finalizers
      - snapshotpolicies/status
      - snapshotpolicies/finalizers
      - snapshots/status
      - snapshots/finalizers
      - snapshotschedules/status
      - snapshotschedules/finalizers
      - restores/status
      - restores/finalizers
      - maintenances/status
      - maintenances/finalizers
      - repositoryreplications/status
      - repositoryreplications/finalizers
    verbs: [get, update, patch]
  - apiGroups: [""]
    resources:
      - pods
      - persistentvolumeclaims
      - configmaps
    verbs: [get, list, watch, create, update, patch, delete]
  - apiGroups: [""]
    resources:
      - pods/exec
    verbs: [create, get]
  # kube's Recorder writes events.k8s.io/v1 Events (not the legacy core Event),
  # so both api groups are required or the create is 403'd and the Event dropped.
  - apiGroups: ["", "events.k8s.io"]
    resources:
      - events
    verbs: [create, patch]
  # create+patch back the opt-in credential projection (spec.credentialProjection),
  # gated by secretProjection.enabled; read-only otherwise. See the ClusterRole for
  # the full rationale.
  # Secrets: read repository credentials, create+patch the opt-in credential
  # projection, AND create/patch/delete the operator-owned kopia web-UI auth Secret
  # (spec.server Generate mode) + the cross-namespace credentials mirror. The server
  # feature is presence-driven per-Repository, so the write verbs are granted
  # unconditionally (a deliberate escalation — see the generated operator-role and the
  # server addendum). Mirrors deploy/rbac/operator-role.yaml.
  - apiGroups: [""]
    resources:
      - secrets
    verbs: [get, list, watch, create, update, patch, delete]
  # Services exposing the kopia web-UI server (spec.server).
  - apiGroups: [""]
    resources:
      - services
    verbs: [get, list, watch, create, update, patch, delete]
  # Mover Jobs and the kopia web-UI server Deployment (spec.server).
  - apiGroups: [batch]
    resources:
      - jobs
    verbs: [get, list, watch, create, update, patch, delete]
  - apiGroups: [apps]
    resources:
      - deployments
    verbs: [get, list, watch, create, update, patch, delete]
  # CSI volume snapshots used as a consistent source for snapshotting (copyMethod:
  # Snapshot/Clone). `patch` backs the server-side apply that creates the staged
  # VolumeSnapshot. (Cluster-scoped VolumeSnapshotClasses/Contents + StorageClasses are
  # granted in the ClusterRole; a namespaced install cannot stage CSI snapshots.)
  - apiGroups: [snapshot.storage.k8s.io]
    resources:
      - volumesnapshots
    verbs: [get, list, watch, create, patch, delete]
  - apiGroups: [groupsnapshot.storage.k8s.io]
    resources:
      - volumegroupsnapshots
    verbs: [get, list, watch, create, delete]
  # Per-namespace mover RBAC minted by the controller (§4.12). Minted via
  # server-side apply (PATCH), so `patch`/`update` are required, not just create/get.
  # `list`/`watch` for workload identity (SA watch — see clusterrole.yaml).
  - apiGroups: [""]
    resources:
      - serviceaccounts
    verbs: [get, list, watch, create, update, patch]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources:
      - rolebindings
    verbs: [get, create, update, patch]
  {{- if eq (include "kopiur.webhook.selfManaged" .) "true" }}
  # Self-managed webhook TLS (webhook.tls.mode: self): writing the serving Secret
  # (namespace-local). create is unscoped (no resourceName at create time); the
  # rotation re-apply is scoped to the serving Secret by name. The cluster-scoped
  # webhook-config patch can't live in a Role — it's granted by the ClusterRole
  # below. SYNCED from xtask (webhook_cert_secret_rules).
  - apiGroups: [""]
    resources: [secrets]
    verbs: [create]
  - apiGroups: [""]
    resources: [secrets]
    resourceNames:
      - {{ .Values.webhook.tls.secretName }}
    verbs: [update, patch]
  {{- end }}
{{- end }}
{{- if and (eq .Values.installScope "namespaced") (eq (include "kopiur.webhook.selfManaged" .) "true") }}
---
# Webhook configurations are cluster-scoped, so even a namespaced install needs a
# (tightly resourceName-scoped) ClusterRole to inject their caBundle in self mode.
# This is the ONLY cluster-level grant a namespaced install carries.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "kopiur.fullname" . }}-webhook-cert
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
rules:
  - apiGroups: ["admissionregistration.k8s.io"]
    resources:
      - validatingwebhookconfigurations
      - mutatingwebhookconfigurations
    resourceNames:
      - {{ include "kopiur.fullname" . }}-validating
      - {{ include "kopiur.fullname" . }}-mutating
    verbs: [get, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "kopiur.fullname" . }}-webhook-cert
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "kopiur.fullname" . }}-webhook-cert
subjects:
  - kind: ServiceAccount
    name: {{ include "kopiur.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}
