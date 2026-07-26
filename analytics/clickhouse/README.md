# ClickHouse on EKS Auto Mode

An in-workshop [ClickHouse](https://clickhouse.com/) cluster that sits alongside the Spark labs — and, when deployed together, the Kafka lab — on the **same** EKS Auto Mode cluster created by `analytics/terraform/spark-k8s-operator/`. No extra VPC, no separate cluster, no side install.

Under the hood: the [Altinity Kubernetes Operator for ClickHouse](https://github.com/Altinity/clickhouse-operator) manages a `ClickHouseInstallation` (1 shard × 3 replicas) coordinated by a `ClickHouseKeeperInstallation` (3 members) — no ZooKeeper anywhere in the stack. Replicas and Keeper members are spread across the workshop's three availability zones with hard pod anti-affinity.

## Architecture at a glance

Four design decisions shape this lab. Understanding them up-front makes the rest of the manifests obvious.

### 1. ClickHouse runs on a dedicated NodePool, kept away from Spark and Kafka

ClickHouse replicas are stateful — each one owns a `ReplicatedMergeTree` shard's local part files on its own EBS volume, plus a running merge schedule and page cache. Spark executors are stateless and typically run on Spot; a Spot reclamation event would take a replica with it. Kafka brokers are similarly stateful and get their own pool for the same reason.

The workshop's Terraform creates a **dedicated Karpenter `NodePool`** for ClickHouse (see `analytics/terraform/spark-k8s-operator/manifests/automode/nodepool-clickhouse.yaml`) that runs **On-Demand only** and carries a `workload=clickhouse:NoSchedule` taint. The CHI and Keeper CRs in this folder carry the matching toleration and nodeSelector. Nothing else in the workshop cluster tolerates that taint, so nothing else lands on those nodes.

The routing uses **three complementary pieces** on top of Karpenter's usual scheduling:

| Direction | Mechanism | Effect |
|---|---|---|
| Non-ClickHouse pods **off** the ClickHouse pool | `workload=clickhouse:NoSchedule` taint on the NodePool | Scheduler refuses to place them |
| ClickHouse + Keeper pods **allowed on** the pool | Matching toleration on the pod template | Scheduler accepts placement |
| ClickHouse + Keeper pods **routed to** the pool | `workload: clickhouse` label on the pool + matching `nodeSelector` on the pod | Karpenter provisions from this pool, not from a higher-weighted general-purpose pool |

The nodeSelector matters as much as the taint. Without it, when a ClickHouse replica becomes pending, Karpenter picks the highest-weight *feasible* NodePool — since `general-purpose` weight=50 and our ClickHouse pool has no weight, ClickHouse would land on general-purpose and the dedicated pool would sit empty. Taint + toleration alone doesn't route.

### 2. Replicas are marked `karpenter.sh/do-not-disrupt`

Voluntary consolidation is a good thing for stateless workloads. For a ClickHouse replica in the middle of a merge, or a Keeper member in the middle of a raft term, it isn't.

If Karpenter decides a ClickHouse node is underutilised and drains it, an in-flight merge is killed and the replica restarts on a new node with a fresh PVC attach cycle. During that gap, `ReplicatedMergeTree` writes on the other replicas queue in Keeper until this replica catches up. On the Keeper side, disrupting a member forces a raft leader re-election that briefly pauses coordination for every replica.

The pod annotation `karpenter.sh/do-not-disrupt: "true"` on both the ClickHouse and Keeper pod templates tells Karpenter to leave those nodes alone until they're truly empty. You still get expiry, drift, and forced-disruption paths; you just don't get voluntary bin-packing.

### 3. Rack awareness needs `az_count >= replicasCount`

This cluster deploys with `replicasCount: 3` for ClickHouse and `replicasCount: 3` for Keeper. Hard pod anti-affinity keyed on `topology.kubernetes.io/zone` requires **at least as many failure domains (AZs) as replicas**.

The workshop's Terraform ships `az_count = 3` for exactly this reason. If you drop to 2 AZs, the third replica of both the CH cluster and Keeper will stay `Pending` forever — Karpenter cannot provision a node in a third zone that doesn't exist. Either bump `az_count` back to 3, or relax the anti-affinity in the pod templates from `requiredDuringSchedulingIgnoredDuringExecution` to `preferredDuringSchedulingIgnoredDuringExecution` and accept a weaker HA story.

### 4. ClickHouse still needs a Keeper — just not ZooKeeper

`ReplicatedMergeTree` is coordinated through a Zookeeper-wire-protocol service. Historically that had to be an actual ZooKeeper ensemble (JVM, external, operationally distinct). Since ClickHouse 21.3 there's a first-party replacement called **ClickHouse Keeper** — same protocol on port 2181, written in C++, no JVM. Deploying Keeper alongside ClickHouse pods gives you the same coordination guarantees without a separate JVM stack to run.

The CHI in `cluster.yaml` still uses a `zookeeper.nodes` config field for backwards compatibility with older configs, but the daemon it points at is Keeper. Same shape, different implementation — analogous to Kafka's move from ZooKeeper to KRaft.

**Watch out for the naming.** The Altinity operator names the Service that fronts each Keeper installation as `<CR-name>-<cluster-name>`. Both our CR name and inner cluster name are `keeper`, so the actual Service is `keeper-keeper.clickhouse.svc.cluster.local`, not `keeper.clickhouse.svc.cluster.local`. The CHI's `zookeeper.nodes[0].host` reflects that.

## Prerequisites

The Spark-on-EKS workshop cluster is up (`analytics/terraform/spark-k8s-operator/` deployed) and `kubectl` targets it:

```sh
kubectl get nodes -o wide | head
kubectl get storageclass       # expect: gp3 (default), clickhouse-gp3, kafka-gp3 (if the Kafka lab is enabled)
```

## Files

```
analytics/clickhouse/
├── README.md
├── deploy-clickhouse.sh   # applies keeper.yaml + cluster.yaml, waits for both CRs to reach status=Completed
├── cleanup.sh             # removes the CHI + Keeper CRs and their PVCs (Terraform owns the operator)
├── cluster.yaml           # ClickHouseInstallation (1 shard x 3 replicas)
└── keeper.yaml            # ClickHouseKeeperInstallation (3 members)
```

The Altinity operator itself is installed exclusively by the workshop's Terraform (see `analytics/terraform/spark-k8s-operator/clickhouse-operator.tf`). There is no bash-wrapper install script — helm is invoked from Terraform's `helm_release` resource, so operator version and lifecycle are managed alongside the rest of the workshop infrastructure.

## Deploy

### Confirm the operator is running

The Altinity ClickHouse Operator is installed by the workshop's Terraform when `enable_clickhouse_lab = true` (the default). Confirm it's ready before you apply the CRs:

```sh
kubectl -n clickhouse rollout status deploy/clickhouse-operator-altinity-clickhouse-operator
kubectl get storageclass clickhouse-gp3
```

Both should be present. If either is missing, re-apply Terraform with `enable_clickhouse_lab = true` (the default) — the operator, StorageClass, and NodePool all come from that single toggle.

### Apply the ClickHouse + Keeper resources

```sh
./deploy-clickhouse.sh
```

The Altinity operator turns `keeper.yaml` and `cluster.yaml` into:

- **3× Keeper pods** (`chk-keeper-keeper-0-{0,1,2}-0`), each `500m CPU / 1 GiB` request (`1 / 2 GiB` limits), `10 GiB` gp3
- **3× ClickHouse replica pods** (`chi-cluster-replicated-0-{0,1,2}-0`), each `1 CPU / 4 GiB` request (`4 / 16 GiB` limits), `50 GiB` gp3
- **1× aggregated headless Service** per CR (`keeper-keeper`, `clickhouse-cluster`) plus one per replica for stable direct-addressing

Karpenter provisions 3 On-Demand instances spread across the three workshop AZs — typically `m5a.2xlarge` or `r5a.2xlarge` under the shipped requirements. First deploy takes ~3-6 minutes end-to-end. When both CRs reach `status=Completed`:

```sh
kubectl get chi,chk -n clickhouse
kubectl get pods -n clickhouse -o wide
kubectl get nodeclaims -l karpenter.sh/nodepool=clickhouse
```

In-cluster endpoints:

- **HTTP (aggregated across replicas):** `http://clickhouse-cluster.clickhouse.svc:8123`
- **Native TCP (aggregated):** `clickhouse-cluster.clickhouse.svc:9000`
- **Per-replica direct:** `chi-cluster-replicated-0-<n>.clickhouse.svc:9000`

## Verify

Create a `ReplicatedMergeTree` table across the cluster and prove replication:

```sh
kubectl -n clickhouse exec -i chi-cluster-replicated-0-0-0 -c clickhouse -- \
  clickhouse-client --multiline --multiquery <<'SQL'
CREATE TABLE IF NOT EXISTS events ON CLUSTER 'replicated' (
    ts       DateTime DEFAULT now(),
    user_id  UInt64,
    action   String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
ORDER BY (ts, user_id);
SQL
```

The `{shard}` and `{replica}` macros are substituted per pod so each replica writes to the correct z-node in Keeper. Insert on one replica:

```sh
kubectl -n clickhouse exec -i chi-cluster-replicated-0-0-0 -c clickhouse -- \
  clickhouse-client --query "INSERT INTO events(user_id, action) VALUES (1,'login'),(2,'checkout'),(3,'view');"
```

Read from a different replica — Keeper propagates within seconds:

```sh
for i in 0 1 2; do
  kubectl -n clickhouse exec chi-cluster-replicated-0-${i}-0 -c clickhouse -- \
    clickhouse-client --query "SELECT hostName() AS replica, count() AS rows FROM events;"
done
```

Expected:

```
chi-cluster-replicated-0-0-0	3
chi-cluster-replicated-0-1-0	3
chi-cluster-replicated-0-2-0	3
```

## Cleanup

```sh
./cleanup.sh
```

Removes the CHI, Keeper CR, and PVCs. The Altinity operator, the `clickhouse-gp3` StorageClass, the dedicated ClickHouse NodePool, and the `clickhouse` namespace stay in place — those are owned by Terraform and go away with `terraform destroy` when you tear down the workshop.

Because the StorageClass uses `reclaimPolicy: Retain`, deleting the PVCs does not delete the underlying EBS volumes on its own — the volumes stay in an available state until Terraform cleans them up during `destroy`. That's the trade-off for making the workshop safe against accidental `kubectl delete pvc`.

## Storage tiers

**ClickHouse mixes throughput-bound merges with IOPS-bound query reads.** Background compactions rewrite gigabytes of parts (throughput-heavy). Selective queries read random small slices of parts and index files (IOPS-heavy). Provisioning a single volume to satisfy both is the right call for the workshop shape.

ClickHouse and Keeper PVs bind to the `clickhouse-gp3` StorageClass created by Terraform. That's a **gp3 volume with 16 000 IOPS and 1000 MiB/s throughput per volume** — the gp3 ceiling for both. `reclaimPolicy: Retain` and `allowVolumeExpansion: true` are set for the reasons above.

Two ways to change the storage tier for a heavier workload:

| Tier | Latency | Peak / volume | Best for | Trade-off |
|---|---|---|---|---|
| **`clickhouse-gp3` (default)** | ~1 ms | 16 000 IOPS / 1000 MiB/s | Workshop, dev, small–mid prod | Hits gp3 ceiling — can't push further per volume |
| **`io2` Block Express** | sub-ms | 256 000 IOPS / 4000 MiB/s | Latency-sensitive prod, heavy concurrent workloads | ~10× the cost of gp3 — benchmark against gp3-at-ceiling first |
| **Local NVMe** (`i8g`, `i4i`) | sub-ms | ~1M+ IOPS | Absolute performance ceiling for hot working sets | Node loss = data loss on that replica — safe with 3 replicas + Keeper but adds ops complexity around Karpenter drift replacement |

To switch to `io2 Block Express`, create an alternate StorageClass and update the `storageClassName` field in `cluster.yaml` and `keeper.yaml`:

```yaml
# analytics/terraform/spark-k8s-operator/manifests/automode/storageclass-clickhouse-io2.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: { name: clickhouse-io2 }
provisioner: ebs.csi.eks.amazonaws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: Retain
parameters:
  type: io2
  iops: "50000"
  fsType: xfs
  encrypted: "true"
```

## Sizing

Rules of thumb for a production-shape ClickHouse cluster, worth knowing before you touch the resource requests in `cluster.yaml`:

- **Memory is the primary lever for query performance.** ClickHouse holds hot indexes and page cache in RAM. `r` (memory-optimised) is the natural family; the workshop keeps `m` (general-purpose) in the mix as a fallback for cost and availability.
- **CPU headroom during merges.** Compaction is CPU-bound in bursts. Sizing for `4 CPU / 16 GiB` limits per replica works for the workshop; production usually goes 16-64 vCPU / 128-512 GiB per replica.
- **Local NVMe (`i8g` / `i4i`) is a real step-up.** AWS lists ClickHouse as a target workload for `i8g`. The trade-off: instance-store data dies with the node, so you *must* have 2+ replicas per shard on separate nodes (which this lab already does). Karpenter drift replacements become full replica resyncs from peers.
- **Scale out for shards, scale up for replicas.** Adding shards is horizontal — more data + parallel query fan-out. Adding replicas is vertical HA + read concurrency. Match the topology to whether your bottleneck is data volume or query concurrency.
- **Instance-EBS bandwidth is the ceiling on gp3.** `m5a.2xlarge → m5a.4xlarge` gives *no* per-volume throughput gain at default gp3 settings because the volume caps out at 1000 MiB/s. That's why this lab ships gp3 provisioned at 1000 MiB/s — matches the instance's EBS baseline on 4xlarge and up.

CloudWatch metrics worth alerting on for a production-shape replica:

- `VolumeReadOps` / `VolumeWriteOps` and `VolumeThroughputPercentage` (gp3 headroom)
- EC2 `EBSIOBalance%` and `EBSByteBalance%` — instance-level EBS credits
- ClickHouse system tables: `system.merges` (in-flight compactions), `system.replication_queue` (replication lag), `system.parts` (part count per table — too many is a sign of merge starvation)

## Extending

- **Shard count** — raise `shardsCount` in `cluster.yaml`. Each shard is an independent ReplicatedMergeTree instance; queries fan out across shards and results merge on the coordinator. Combine with a `Distributed` engine table for cluster-wide reads.
- **Replica count** — raise `replicasCount`. Requires `az_count >= replicasCount` because of the AZ-level hard anti-affinity — bump both together for larger HA topologies, or relax the anti-affinity to `preferredDuringScheduling`.
- **Instance family** — the ClickHouse NodePool restricts to `m`/`r`, Gen 5+, `2xlarge..8xlarge`. If you need lowest-latency storage, look at `i8g` (Graviton4 + NVMe) or `i4i` (x86 + NVMe) — but note the instance-store trade-off flagged in the storage tiers section.
- **ClickHouse version** — bump the `image:` in `cluster.yaml`. The Altinity operator supports rolling upgrades of the CH image without touching the CR schema. Coordinate with the Keeper image version — both are pinned in this lab so a bump changes only the ClickHouse side.
- **Add a natural-language query layer.** The aggregated HTTP endpoint at `http://clickhouse-cluster.clickhouse.svc:8123` accepts SELECT queries over plain HTTP. Point an LLM (Bedrock, OpenAI, Anthropic) at it with `system.tables` and `system.columns` as tool context, and you have an agentic query interface over the workshop's analytical data.
- **Disable the operator install** — set `enable_clickhouse_lab = false` in Terraform. The Altinity operator and `clickhouse-gp3` StorageClass are no longer created; the ClickHouse Karpenter NodePool remains (harmless — nothing tolerates its taint).
