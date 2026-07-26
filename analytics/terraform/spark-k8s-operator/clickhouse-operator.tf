#---------------------------------------------------------------
# ClickHouse lab — Altinity Cluster Operator + tuned StorageClass
#---------------------------------------------------------------
# This file installs the two pieces of shared infrastructure that the
# ClickHouse lab depends on. The ClickHouse cluster + Keeper themselves
# are custom resources that the participant applies during the lab (see
# `analytics/clickhouse/`), so ClickHouse and Keeper capacity is only
# provisioned when someone actually runs the lab.
#
#   1. Altinity Cluster Operator (Helm)  — watches for the
#      ClickHouseInstallation and ClickHouseKeeperInstallation CRs and
#      reconciles them into StatefulSets + Services + PVCs.
#
#   2. `clickhouse-gp3` StorageClass       — tuned gp3 for ClickHouse
#                                            data volumes (16 000 IOPS /
#                                            1000 MiB/s, xfs, encrypted).
#                                            `reclaimPolicy: Retain` so a
#                                            stray PVC delete doesn't take
#                                            the underlying data with it.
#
# The dedicated ClickHouse Karpenter NodePool lives in
# `manifests/automode/nodepool-clickhouse.yaml`; it is picked up
# automatically by the `auto_mode_nodepools` fileset() discovery in
# `eks.tf`, so it deploys regardless of `enable_clickhouse_lab` (an
# untainted NodePool with no matching pods is inert and costs nothing).
#
# Toggle the operator + StorageClass with `var.enable_clickhouse_lab`
# (default true).
#---------------------------------------------------------------

locals {
  clickhouse_lab = {
    namespace          = "clickhouse"
    operator_version   = var.altinity_operator_version
    storage_class_name = "clickhouse-gp3"
    # ClickHouse's I/O profile is IOPS-sensitive (random reads across
    # MergeTree parts during selective queries) as well as throughput-
    # sensitive (background merges rewriting large parts). Provisioning
    # gp3 at 16 000 IOPS + 1000 MiB/s covers both cases and matches AWS's
    # ClickHouse-on-EBS guidance for the workshop shape. Bump to io2
    # Block Express only when benchmarks force the change — gp3 at the
    # ceiling is ~10x cheaper.
    storage_class_iops           = 16000
    storage_class_throughput_mib = 1000
  }
}

resource "helm_release" "altinity_clickhouse_operator" {
  count = var.enable_clickhouse_lab ? 1 : 0

  name             = "clickhouse-operator"
  namespace        = local.clickhouse_lab.namespace
  create_namespace = true
  repository       = "https://helm.altinity.com"
  chart            = "altinity-clickhouse-operator"
  version          = local.clickhouse_lab.operator_version
  timeout          = 600

  # The operator install is cluster-scoped by default. Only the
  # `clickhouse` namespace hosts CH + Keeper CRs in this workshop, but
  # the operator's RBAC watches everywhere so a participant can point
  # it at another namespace if they experiment.
  depends_on = [
    module.eks,
    kubectl_manifest.auto_mode_nodepools,
  ]
}

# Tuned gp3 StorageClass for ClickHouse data volumes.
#
# Why the parameters:
#   - `iops: 16000`   — gp3 ceiling. ClickHouse's random-read profile
#     during query benefits from IOPS more than raw throughput.
#   - `throughput: 1000` (MiB/s) — gp3 ceiling. Needed for the merge
#     path where a busy shard rewrites gigabytes of parts.
#   - `reclaimPolicy: Retain` — stateful data. A stray `kubectl delete
#     pvc` on `Delete` policy would silently take the volume with it.
#     `Retain` requires an explicit release step to reclaim storage.
#   - `allowVolumeExpansion: true` — in-place PVC resize when a shard
#     runs out of room, no rebuild required.
#   - `WaitForFirstConsumer` — critical for AZ correctness. Without
#     it, EBS provisions in a random AZ and the pod fails to mount if
#     it lands elsewhere.
resource "kubectl_manifest" "clickhouse_gp3_storageclass" {
  count = var.enable_clickhouse_lab ? 1 : 0

  yaml_body = yamlencode({
    apiVersion = "storage.k8s.io/v1"
    kind       = "StorageClass"
    metadata = {
      name = local.clickhouse_lab.storage_class_name
    }
    provisioner          = "ebs.csi.eks.amazonaws.com"
    volumeBindingMode    = "WaitForFirstConsumer"
    reclaimPolicy        = "Retain"
    allowVolumeExpansion = true
    parameters = {
      type       = "gp3"
      fsType     = "xfs"
      encrypted  = "true"
      iops       = tostring(local.clickhouse_lab.storage_class_iops)
      throughput = tostring(local.clickhouse_lab.storage_class_throughput_mib)
    }
  })
  wait = true

  depends_on = [module.eks]
}
