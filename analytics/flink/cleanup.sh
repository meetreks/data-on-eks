#!/usr/bin/env bash
set -euo pipefail

# Tears down the sample FlinkDeployment.
# The Flink Kubernetes Operator, the flink-gp3 StorageClass, the
# dedicated Flink NodePool, and the flink namespace are all managed
# by Terraform (enable_flink_lab = true) and are removed by
# `terraform destroy`. This script does NOT touch them.

NAMESPACE=flink

echo "Deleting FlinkDeployment (drains job gracefully; savepoint if any)..."
kubectl delete flinkdeployment state-machine -n "${NAMESPACE}" --ignore-not-found

echo ""
echo "Waiting up to 3 minutes for JobManager + TaskManager pods to terminate..."
kubectl -n "${NAMESPACE}" wait --for=delete pod \
  -l app=state-machine --timeout=180s || true

echo ""
echo "Deleting any persistent volume claims (only present if the job used PVCs for RocksDB state)..."
# Retain-policy PVs survive PVC deletion; the underlying EBS volumes have
# to be released separately if you want to stop paying for them. The
# workshop's `terraform destroy` handles that when it removes the
# StorageClass and namespace at the end.
kubectl -n "${NAMESPACE}" delete pvc --all --ignore-not-found

echo ""
echo "Flink job resources removed. The Flink Kubernetes Operator,"
echo "flink-gp3 StorageClass, and Karpenter NodePool remain"
echo "(Terraform-managed). Run 'terraform destroy' in"
echo "analytics/terraform/spark-k8s-operator/ to tear down the whole workshop."
