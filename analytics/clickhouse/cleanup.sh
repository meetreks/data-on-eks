#!/usr/bin/env bash
set -euo pipefail

# Tears down the in-lab ClickHouse cluster only.
# The Altinity operator, the clickhouse-gp3 StorageClass, the dedicated
# ClickHouse NodePool, and the clickhouse namespace are all managed by
# Terraform (enable_clickhouse_lab = true) and are removed by
# `terraform destroy`. This script does NOT touch them.

NAMESPACE=clickhouse

echo "Deleting ClickHouseInstallation (drains replicas gracefully)..."
kubectl delete chi cluster -n "${NAMESPACE}" --ignore-not-found

echo ""
echo "Deleting ClickHouseKeeperInstallation..."
kubectl delete chk keeper -n "${NAMESPACE}" --ignore-not-found

echo ""
echo "Waiting up to 3 minutes for CH + Keeper pods to terminate..."
kubectl -n "${NAMESPACE}" wait --for=delete pod \
  -l 'clickhouse.altinity.com/chi=cluster' --timeout=180s || true
kubectl -n "${NAMESPACE}" wait --for=delete pod \
  -l 'clickhouse-keeper.altinity.com/chk=keeper' --timeout=180s || true

echo ""
echo "Deleting persistent volume claims (ClickHouse + Keeper data)..."
# Retain-policy PVs survive PVC deletion; the underlying EBS volumes have
# to be released separately if you want to stop paying for them. The
# workshop's `terraform destroy` handles that when it removes the
# StorageClass and namespace at the end.
kubectl -n "${NAMESPACE}" delete pvc --all --ignore-not-found

echo ""
echo "ClickHouse cluster resources removed. The Altinity operator,"
echo "clickhouse-gp3 StorageClass, and Karpenter NodePool remain"
echo "(Terraform-managed). Run 'terraform destroy' in"
echo "analytics/terraform/spark-k8s-operator/ to tear down the whole workshop."
