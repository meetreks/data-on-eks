#!/usr/bin/env bash
set -euo pipefail

# Deploys the sample FlinkDeployment onto the workshop's EKS cluster.
# Prerequisites are managed by Terraform when enable_flink_lab = true:
#   - Apache Flink Kubernetes Operator running in the "flink" namespace
#   - "flink-gp3" StorageClass
#   - Dedicated Flink Karpenter NodePool with
#     workload=flink:NoSchedule taint

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE=flink

echo "Preflight: verifying operator, storageclass, and NodePool are in place..."
if ! kubectl -n "${NAMESPACE}" rollout status deploy/flink-kubernetes-operator --timeout=120s; then
  echo "ERROR: Flink Kubernetes Operator not ready in '${NAMESPACE}'."
  echo "       Re-apply Terraform with enable_flink_lab=true — the operator,"
  echo "       StorageClass, and NodePool all come from that single toggle."
  exit 1
fi

if ! kubectl get storageclass flink-gp3 &>/dev/null; then
  echo "ERROR: StorageClass 'flink-gp3' not found."
  echo "       Re-apply Terraform with enable_flink_lab=true."
  exit 1
fi

if ! kubectl get nodepool.karpenter.sh flink &>/dev/null; then
  echo "ERROR: Karpenter NodePool 'flink' not found."
  echo "       Re-apply Terraform (manifests/automode/nodepool-flink.yaml)."
  exit 1
fi

echo ""
echo "Applying FlinkDeployment (StateMachineExample, parallelism 2)..."
kubectl apply -f "${SCRIPT_DIR}/flink-cluster.yaml"

echo ""
echo "Waiting for FlinkDeployment to reach STABLE (3-6 minutes typical while nodes provision)..."
# jsonpath wait against LIFECYCLE STATE. STABLE means the operator has
# reconciled the CR into a healthy JM + N TMs with the job RUNNING.
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.lifecycleState}'=STABLE \
  flinkdeployment/state-machine --timeout=600s

echo ""
kubectl get flinkdeployment -n "${NAMESPACE}"
echo ""
kubectl get pods -n "${NAMESPACE}" -o wide
echo ""
echo "JobManager REST endpoint (in-cluster):"
echo "  http://state-machine-rest.${NAMESPACE}.svc.cluster.local:8081"
echo ""
echo "Port-forward the Flink Web UI:"
echo "  kubectl -n ${NAMESPACE} port-forward svc/state-machine-rest 8081:8081"
