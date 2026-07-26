#---------------------------------------------------------------
# Flink lab — Apache Flink Kubernetes Operator + tuned StorageClass
#---------------------------------------------------------------
# This file installs the two pieces of shared infrastructure that the
# Flink lab depends on. The FlinkDeployment itself is a custom resource
# that the participant applies during the lab (see `analytics/flink/`),
# so JobManager / TaskManager capacity is only provisioned when someone
# actually runs the lab.
#
#   1. Apache Flink Kubernetes Operator (Helm) — watches for
#      `FlinkDeployment` and `FlinkSessionJob` CRs and reconciles them
#      into JobManager Deployments plus TaskManager pods managed via
#      Flink's native Kubernetes integration.
#
#   2. `flink-gp3` StorageClass — tuned gp3 for Flink stateful state
#      backends (RocksDB checkpoints, incremental savepoints). 6000 IOPS
#      / 500 MiB/s per volume; `reclaimPolicy: Retain` because savepoint
#      state is precious.
#
# Cert-manager is required for the Flink operator's admission webhooks.
# The workshop cluster already ships cert-manager as part of its addon
# stack, so this file does not install it.
#
# The dedicated Flink Karpenter NodePool lives in
# `manifests/automode/nodepool-flink.yaml`; it is picked up automatically
# by the `auto_mode_nodepools` fileset() discovery in `eks.tf`, so it
# deploys regardless of `enable_flink_lab` (an untainted NodePool with
# no matching pods is inert and costs nothing).
#
# Toggle the operator + StorageClass with `var.enable_flink_lab`
# (default true).
#---------------------------------------------------------------

locals {
  flink_lab = {
    namespace          = "flink"
    operator_version   = var.flink_operator_version
    storage_class_name = "flink-gp3"
    # Flink's checkpoint uploads are bursty (each interval, every
    # TaskManager writes its state delta). 6000 IOPS + 500 MiB/s covers
    # the common workshop shape; graduate to `io2` or bigger `gp3` if
    # your job's per-checkpoint state runs into the GB range.
    storage_class_iops           = 6000
    storage_class_throughput_mib = 500
  }
}

resource "helm_release" "flink_kubernetes_operator" {
  count = var.enable_flink_lab ? 1 : 0

  name             = "flink-kubernetes-operator"
  namespace        = local.flink_lab.namespace
  create_namespace = true
  # Apache publishes a Helm chart per operator version at
  #   https://downloads.apache.org/flink/flink-kubernetes-operator-<version>/
  # The chart name is always `flink-kubernetes-operator`.
  repository = "https://downloads.apache.org/flink/flink-kubernetes-operator-${local.flink_lab.operator_version}/"
  chart      = "flink-kubernetes-operator"
  version    = local.flink_lab.operator_version
  timeout    = 600

  # The operator watches all namespaces by default and installs
  # cluster-scoped CRDs. Participants apply FlinkDeployment CRs into
  # the `flink` namespace in the lab, but the operator's RBAC allows
  # experimentation elsewhere.
  depends_on = [
    module.eks,
    kubectl_manifest.auto_mode_nodepools,
  ]
}

# Tuned gp3 StorageClass for Flink state backend volumes.
#
# Why the parameters:
#   - `iops: 6000` — RocksDB compaction and checkpoint uploads are
#     write-heavy in bursts. gp3 default 3000 IOPS gets choked under
#     load; 6000 keeps us clear of the Kafka + ClickHouse allocation
#     ceilings while giving enough headroom for typical workshop jobs.
#   - `throughput: 500` MiB/s — enough for savepoint uploads to
#     s3-compatible checkpoint stores without saturating the per-volume
#     EBS baseline.
#   - `reclaimPolicy: Retain` — savepoint volumes are precious. A
#     stray `kubectl delete pvc` on `Delete` policy would silently take
#     the volume with it. Retain requires an explicit release step.
#   - `allowVolumeExpansion: true` — in-place PVC resize when state
#     grows past the initial size, no rebuild required.
#   - `WaitForFirstConsumer` — EBS volumes are AZ-scoped; deferring
#     provisioning until the pod is scheduled avoids AZ mismatches.
resource "kubectl_manifest" "flink_gp3_storageclass" {
  count = var.enable_flink_lab ? 1 : 0

  yaml_body = yamlencode({
    apiVersion = "storage.k8s.io/v1"
    kind       = "StorageClass"
    metadata = {
      name = local.flink_lab.storage_class_name
    }
    provisioner          = "ebs.csi.eks.amazonaws.com"
    volumeBindingMode    = "WaitForFirstConsumer"
    reclaimPolicy        = "Retain"
    allowVolumeExpansion = true
    parameters = {
      type       = "gp3"
      fsType     = "xfs"
      encrypted  = "true"
      iops       = tostring(local.flink_lab.storage_class_iops)
      throughput = tostring(local.flink_lab.storage_class_throughput_mib)
    }
  })
  wait = true

  depends_on = [module.eks]
}
