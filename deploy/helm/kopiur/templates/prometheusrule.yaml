{{- if and .Values.metrics.enabled .Values.metrics.prometheusRule.enabled -}}
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: {{ include "kopiur.controller.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "kopiur.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
    {{- with .Values.metrics.prometheusRule.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  groups:
    - name: kopiur.rules
      rules:
        - alert: KopiurBackupConsecutiveFailures
          expr: kopiur_snapshot_consecutive_failures >= 3
          for: 15m
          labels:
            severity: warning
          annotations:
            summary: "Backups for {{`{{ $labels.namespace }}/{{ $labels.name }}`}} are failing"
            description: "{{`{{ $value }}`}} consecutive backup failures (>=3) for SnapshotPolicy {{`{{ $labels.namespace }}/{{ $labels.name }}`}}."
        - alert: KopiurBackupStale
          expr: time() - kopiur_snapshot_last_success_timestamp_seconds > {{ .Values.metrics.prometheusRule.backupStaleAfterSeconds }}
          for: 30m
          labels:
            severity: warning
          annotations:
            summary: "No recent successful backup for {{`{{ $labels.namespace }}/{{ $labels.name }}`}}"
            description: "Last successful backup was over {{ div .Values.metrics.prometheusRule.backupStaleAfterSeconds 3600 }}h ago."
        - alert: KopiurRepositoryNotReady
          expr: max by (namespace, name) (kopiur_resource_phase{kind=~"Repository|ClusterRepository", phase=~"Degraded|Failed"}) == 1
          for: 15m
          labels:
            severity: critical
          annotations:
            summary: "Repository {{`{{ $labels.namespace }}/{{ $labels.name }}`}} is {{`{{ $labels.phase }}`}}"
            description: "A kopiur repository has been Degraded/Failed for 15m; backups to it will not run."
        - alert: KopiurSnapshotFailed
          expr: max by (namespace, name) (kopiur_resource_phase{kind="Snapshot", phase="Failed"}) == 1
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Snapshot {{`{{ $labels.namespace }}/{{ $labels.name }}`}} failed"
            description: "A Snapshot CR has been in phase=Failed for 10m."
        - alert: KopiurRestoreFailed
          expr: max by (namespace, name) (kopiur_resource_phase{kind="Restore", phase="Failed"}) == 1
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Restore {{`{{ $labels.namespace }}/{{ $labels.name }}`}} failed"
            description: "A Restore CR has been in phase=Failed for 10m."
        - alert: KopiurReconcileErrorsHigh
          expr: sum by (kind) (rate(kopiur_controller_reconcile_errors_total[10m])) > 0.2
          for: 15m
          labels:
            severity: warning
          annotations:
            summary: "High reconcile error rate for {{`{{ $labels.kind }}`}}"
            description: "kopiur controller is erroring on {{`{{ $labels.kind }}`}} reconciles (>0.2/s over 10m)."
{{- end }}
