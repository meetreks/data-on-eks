# Apache Flink on EKS Auto Mode

An in-workshop [Apache Flink](https://flink.apache.org/) runtime that sits alongside the Spark labs — and, when deployed together, the Kafka and ClickHouse labs — on the **same** EKS Auto Mode cluster created by `analytics/terraform/spark-k8s-operator/`. No extra VPC, no separate cluster, no side install.

Under the hood: the [Apache Flink Kubernetes Operator](https://github.com/apache/flink-kubernetes-operator) manages a `FlinkDeployment` running the built-in `StateMachineExample.jar` from the official `flink:1.20` image. One JobManager plus two TaskManagers, in Application mode, on a dedicated Karpenter On-Demand NodePool.

## Architecture at a glance

Three design decisions shape this lab. Understanding them up-front makes the rest of the manifests obvious.

### 1. Flink runs on a dedicated NodePool, kept away from Spark, Kafka, and ClickHouse

A Flink JobManager owns the execution graph and checkpoint metadata; a TaskManager owns per-key state for its assigned partitions. Losing either forces a job restart from the last checkpoint — cheap for a demo, painful for a production stream that's built up hours of state. Same reasoning as Kafka and ClickHouse: keep them off Spot, off shared pools.

The workshop's Terraform creates a **dedicated Karpenter `NodePool`** for Flink (see `analytics/terraform/spark-k8s-operator/manifests/automode/nodepool-flink.yaml`) that runs **On-Demand only** and carries a `workload=flink:NoSchedule` taint. The `FlinkDeployment` in this folder carries the matching toleration and nodeSelector. Nothing else in the workshop cluster tolerates that taint, so nothing else lands on those nodes.

The routing uses **three complementary pieces** on top of Karpenter's usual scheduling:

| Direction | Mechanism | Effect |
|---|---|---|
| Non-Flink pods **off** the Flink pool | `workload=flink:NoSchedule` taint on the NodePool | Scheduler refuses to place them |
| Flink pods **allowed on** the pool | Matching toleration on the JM and TM pod templates | Scheduler accepts placement |
| Flink pods **routed to** the pool | `workload: flink` label on the pool + matching `nodeSelector` on the pod | Karpenter provisions from this pool, not from a higher-weighted general-purpose pool |

The nodeSelector matters as much as the taint. Without it, when a JobManager becomes pending, Karpenter picks the highest-weight *feasible* NodePool — since `general-purpose` weight=50 and our Flink pool has no weight, Flink would land on general-purpose and the dedicated pool would sit empty. Taint + toleration alone doesn't route.

### 2. Application mode, not Session mode

Two ways an operator can run Flink jobs:

- **Session mode** — one long-lived Flink cluster hosts many jobs. Legacy pattern, jobs share the JobManager, resource isolation is by slot rather than pod.
- **Application mode** — one Flink cluster per job. The FlinkDeployment CR = one JobManager + N TaskManagers, and the job runs until you delete the CR. Modern operator-native pattern.

This lab uses Application mode: `flink-cluster.yaml` defines a `FlinkDeployment` with an inline `job` block pointing at the StateMachine JAR. Deleting the CR removes the whole cluster — no leftover session waiting for another job.

### 3. State + checkpoints, not stateless

Every long-running stream processor has to answer two questions: where does per-key state live, and what happens when a pod dies?

- **State backend** — the sample uses the `hashmap` (in-memory) backend for simplicity. Fine for the ~15 KB state the state-machine keeps per key; graduate to `rocksdb` when state grows past what fits in the JVM heap.
- **Checkpoints** — periodic snapshots of the state. Written to `file:///tmp/flink-checkpoints` on the JM pod for the demo (so a restart loses recent progress), swap to `s3://` for real fault-tolerance.
- **Savepoints** — manually-triggered snapshots you can restart from. The path is set in `flinkConfiguration.state.savepoints.dir`.

For an HA JobManager you'd add `spec.jobManager.replicas: 2` and a `kubernetesHAOptions` block. The workshop keeps things single-JM to keep the demo readable.

## Prerequisites

The Spark-on-EKS workshop cluster is up (`analytics/terraform/spark-k8s-operator/` deployed) and `kubectl` targets it:

```sh
kubectl get nodes -o wide | head
kubectl get storageclass    # expect: gp3 (default), flink-gp3, plus kafka-gp3 / clickhouse-gp3 if those labs are enabled
kubectl get ns cert-manager # cert-manager is a prerequisite for the Flink operator's admission webhooks
```

`cert-manager` ships with the workshop's Terraform addon stack, so unless you disabled it, this last check should just pass.

## Files

```
analytics/flink/
├── README.md
├── deploy-flink.sh    # applies flink-cluster.yaml, waits for FlinkDeployment to reach LIFECYCLE STATE=STABLE
├── cleanup.sh         # removes the FlinkDeployment and its PVCs (Terraform owns the operator + NodePool + StorageClass)
└── flink-cluster.yaml # FlinkDeployment CR — StateMachineExample, Application mode, parallelism 2
```

The Flink Kubernetes Operator itself is installed exclusively by the workshop's Terraform (see `analytics/terraform/spark-k8s-operator/flink-operator.tf`). There is no bash-wrapper install script — helm is invoked from Terraform's `helm_release` resource, so operator version and lifecycle are managed alongside the rest of the workshop infrastructure.

## Deploy

### Confirm the operator is running

The Flink Kubernetes Operator is installed by the workshop's Terraform when `enable_flink_lab = true` (the default). Confirm it's ready before you apply the FlinkDeployment:

```sh
kubectl -n flink rollout status deploy/flink-kubernetes-operator
kubectl get storageclass flink-gp3
```

Both should be present. If either is missing, re-apply Terraform with `enable_flink_lab = true` — the operator, StorageClass, and NodePool all come from that single toggle.

### Apply the sample FlinkDeployment

```sh
./deploy-flink.sh
```

The Flink operator turns `flink-cluster.yaml` into:

- **1× JobManager Deployment** (`state-machine`), 1 CPU / 2 GiB — the control plane pod; accepts the JAR, plans the execution graph, spawns TaskManagers via Flink's native Kubernetes API
- **2× TaskManager pods** (`state-machine-taskmanager-1-{1,2}`), 1 CPU / 2 GiB each — the workers running the operators
- **REST/UI Service** (`state-machine-rest`) — port 8081, the Flink Web UI and REST API
- **Internal Service** (`state-machine`) — cluster-internal communication between JM and TMs

Karpenter provisions 1 or 2 On-Demand instances — typically `m5a.2xlarge` or `r5a.2xlarge` under the shipped requirements — and bin-packs the three Flink pods onto them. First deploy takes ~3-6 minutes end-to-end while the JVM warms up. When the CR reports `LIFECYCLE STATE=STABLE`:

```sh
kubectl get flinkdeployment -n flink
kubectl get pods -n flink -o wide
kubectl get nodeclaims -l karpenter.sh/nodepool=flink
```

Endpoints (in-cluster):

- **REST + Web UI:** `http://state-machine-rest.flink.svc:8081`
- **JobManager RPC:** `state-machine.flink.svc:6123` (internal)

## Verify

Unlike a Spark job that reads a file and finishes, a Flink job is **long-running**. The proof-of-running lives in three places: the CR status, the JobManager REST API, and the checkpoint counter.

**1. FlinkDeployment status:**

```sh
kubectl -n flink get flinkdeployment state-machine
```

Expected:

```
NAME            JOB STATUS   LIFECYCLE STATE
state-machine   RUNNING      STABLE
```

**2. Port-forward the JobManager REST / Web UI:**

```sh
kubectl -n flink port-forward svc/state-machine-rest 8081:8081
```

Open [http://localhost:8081](http://localhost:8081) — Flink Web UI with jobs, tasks, backpressure, thread dumps, and checkpoint history.

**3. Query the REST API for job overview and checkpoint counts:**

```sh
curl -s http://localhost:8081/jobs/overview | python3 -m json.tool

JID=$(kubectl -n flink get flinkdeployment state-machine -o jsonpath='{.status.jobStatus.jobId}')
curl -s "http://localhost:8081/jobs/$JID/checkpoints" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('completed:', d['counts']['completed'])
print('failed:',    d['counts']['failed'])
"
```

Expected — all 4 tasks (Source Generator × 2 parallelism + Flat Map + Print Sink × 2) running, and the completed checkpoint count climbing every few seconds.

**Do not** grep TaskManager stdout for state-machine events — the sample only prints on invalid transitions, which are probability-driven and sparse. Job health lives in the status + REST API, not in tail logs. That's the streaming pattern, not the batch pattern.

## Cleanup

```sh
./cleanup.sh
```

Removes the FlinkDeployment and any PVCs it created. The Flink operator, the `flink-gp3` StorageClass, the dedicated Flink NodePool, and the `flink` namespace stay in place — those are owned by Terraform and go away with `terraform destroy` when you tear down the workshop.

Because the StorageClass uses `reclaimPolicy: Retain`, deleting the PVCs does not delete the underlying EBS volumes on its own — the volumes stay in an available state until Terraform cleans them up during `destroy`. That's the trade-off for making the workshop safe against accidental `kubectl delete pvc`.

## Storage tiers

**Flink's I/O demand is bursty.** Every checkpoint interval each TaskManager writes a state delta; when RocksDB is enabled, background compaction competes for the same volume. The sample job uses filesystem checkpoints on ephemeral `/tmp` (so a JM restart loses recent progress) — good enough for a demo, wrong for production. Two upgrade paths:

| Approach | When it makes sense | Trade-off |
|---|---|---|
| **`file:///tmp/flink-checkpoints`** (default) | Workshop demos, functional testing | Data lost on JobManager pod restart — not fault-tolerant |
| **PVC on `flink-gp3` + RocksDB local dir** | Local state larger than heap; single-node fault tolerance if pod is rescheduled to the same node | AZ-scoped EBS ties pod placement |
| **`s3://<bucket>/flink-checkpoints`** | Real fault tolerance — any pod can recover from any other pod's checkpoint | Adds S3 latency to the checkpoint path |

To switch the sample to S3, edit `flink-cluster.yaml`:

```yaml
flinkConfiguration:
  state.backend.type: rocksdb
  state.checkpoints.dir: s3://<your-bucket>/flink-checkpoints
  state.savepoints.dir:  s3://<your-bucket>/flink-savepoints
```

Then give the JM + TM pods an S3 IRSA role so they can write, and re-apply.

## Sizing

Rules of thumb for a Flink cluster, worth knowing before you touch the resource requests in `flink-cluster.yaml`:

- **Heap sizing is a Flink concern, not just a JVM one.** Flink's memory model splits the container into JVM heap, JVM off-heap, managed memory (for RocksDB), network buffers, and framework overhead. Bumping the container size without thinking about the split usually just grows framework overhead. Read the Flink [memory tuning guide](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup/) before scaling up.
- **CPU per TaskManager × slots per TaskManager = parallelism budget.** Setting `taskmanager.numberOfTaskSlots: 4` on a 4 CPU pod means 4 parallel tasks per TM. Balance is workload-dependent: shuffle-heavy jobs like 1 slot per pod (fewer noisy-neighbour effects); CPU-light jobs pack more slots.
- **RocksDB needs disk, not just memory.** When you swap to RocksDB, attach a PVC on `flink-gp3` and set `state.backend.rocksdb.localdir` to the mount path. Local state grows with keyspace × timers.
- **Checkpoint interval is a knob, not a constant.** Default is a few seconds. Shorter = smaller recovery gap but higher I/O overhead. Longer = less overhead but more work to redo on restart.
- **Instance families:** the shipped `m5a`/`r5a` are the workshop starting point. `r7iz` (Sapphire Rapids, high sustained clock) for latency-critical stream processing. `m7g` (Graviton3) for the best price-performance if all your operators and connectors are ARM-compatible.

## Extending

- **Kafka source.** The obvious follow-up: swap `StateMachineExample.jar` for a Flink job that reads from `cluster-kafka-bootstrap.kafka.svc:9092`. The Flink Kafka connector is a bundled dependency in the `flink:1.20` image.
- **ClickHouse sink.** Once your Flink job produces aggregated records, sink them into ClickHouse via the [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/jdbc/) using the aggregated HTTP endpoint at `http://clickhouse-cluster.clickhouse.svc:8123`. That's the canonical Kafka → Flink → ClickHouse streaming pipeline.
- **HA JobManager.** Set `spec.jobManager.replicas: 2` and add a `kubernetesHAOptions` block so JobManager metadata is persisted in a ConfigMap and TaskManagers can reconnect if the leader dies. Requires a persistent checkpoint store (S3, not local).
- **Autoscaling.** The Flink operator 1.15+ supports the [Autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/) — scale TaskManagers based on backpressure and lag rather than by hand. Enable via `spec.flinkConfiguration.job.autoscaler.enabled: "true"`.
- **Blue/Green deployments.** Flink operator 1.15's headline feature: deploy a new version of your streaming app in parallel with the running one, switch over on savepoint. See the [Blue/Green docs](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/blue-green/) for the operator-native pattern.
- **Disable the operator install.** Set `enable_flink_lab = false` in Terraform. The Flink operator and `flink-gp3` StorageClass are no longer created; the Flink Karpenter NodePool remains (harmless — nothing tolerates its taint).
