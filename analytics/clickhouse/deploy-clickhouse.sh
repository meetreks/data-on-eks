#!/usr/bin/env bash
set -euo pipefail

# Deploys the ClickHouseKeeperInstallation + ClickHouseInstallation onto
# the workshop's EKS cluster. Prerequisites are managed by Terraform
# when enable_clickhouse_lab = true:
#   - Altinity ClickHouse Operator running in the "clickhouse" namespace
#   - "clickhouse-gp3" StorageClass
#   - Dedicated ClickHouse Karpenter NodePool with
#     workload=clickhouse:NoSchedule taint

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE=clickhouse

echo "Preflight: verifying operator, storageclass, and NodePool are in place..."
if ! kubectl -n "${NAMESPACE}" rollout status deploy/clickhouse-operator-altinity-clickhouse-operator --timeout=120s; then
  echo "ERROR: Altinity operator not ready in '${NAMESPACE}'."
  echo "       Re-apply Terraform with enable_clickhouse_lab=true — the operator,"
  echo "       StorageClass, and NodePool all come from that single toggle."
  exit 1
fi

if ! kubectl get storageclass clickhouse-gp3 &>/dev/null; then
  echo "ERROR: StorageClass 'clickhouse-gp3' not found."
  echo "       Re-apply Terraform with enable_clickhouse_lab=true."
  exit 1
fi

if ! kubectl get nodepool.karpenter.sh clickhouse &>/dev/null; then
  echo "ERROR: Karpenter NodePool 'clickhouse' not found."
  echo "       Re-apply Terraform (manifests/automode/nodepool-clickhouse.yaml)."
  exit 1
fi

echo ""
echo "Applying ClickHouse Keeper (3 replicas across AZs)..."
kubectl apply -f "${SCRIPT_DIR}/keeper.yaml"

echo ""
echo "Applying ClickHouse cluster (1 shard x 3 replicas across AZs)..."
kubectl apply -f "${SCRIPT_DIR}/cluster.yaml"

echo ""
echo "Waiting for the Keeper cluster to reach status=Completed (2-4 minutes typical)..."
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.status}'=Completed \
  chk/keeper --timeout=600s

echo ""
echo "Waiting for the ClickHouse cluster to reach status=Completed (2-4 minutes typical)..."
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.status}'=Completed \
  chi/cluster --timeout=600s

echo ""
kubectl get chi,chk -n "${NAMESPACE}"
echo ""
kubectl get pods -n "${NAMESPACE}" -o wide
echo ""
echo "Aggregated ClickHouse HTTP endpoint (in-cluster):"
echo "  http://clickhouse-cluster.${NAMESPACE}.svc.cluster.local:8123"
echo ""
echo "Native ClickHouse TCP endpoint (in-cluster):"
echo "  clickhouse-cluster.${NAMESPACE}.svc.cluster.local:9000"
