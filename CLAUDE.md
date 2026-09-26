# CLAUDE.md — datahub cluster

Working notes for whoever (human or Claude) touches this cluster next. This
repo (`/root/datahub`) holds the audit-logging pipeline added on 2026-09-23;
see `audit-logging/README.md` and `audit-logging/ARCHITECTURE.md` for that.
This file is about the cluster itself — facts that aren't obvious from the
code and will bite you if you don't know them going in.

## The cluster

- Single-node RKE2 (`rag`), v1.27.13. 24 vCPU / 32Gi RAM. This is the box
  you're on — `kubectl` talks to `127.0.0.1:6443` locally, no separate
  control plane.
- Root filesystem is at **~93% used, ~11GB free** on a 146GB volume, and
  everything (NFS-backed PVs, container images, both containerd's store and
  a separate Docker daemon's store) shares it. This is tight. Building or
  importing large images (anything gigabyte-scale) can push it into
  kubelet's flood-stage disk watermark, which auto-blocks writes on
  anything with data on `nfs-client` storage (that's most PVs in this
  cluster, including OpenSearch's). Check `df -h /` before doing anything
  disk-heavy; `docker builder prune -f` and `docker image prune -f` are
  cheap, safe wins if you need headroom (already run once, freed ~7GB).
- Two container runtimes exist on this host: RKE2's own containerd
  (`/run/k3s/containerd/containerd.sock`, what kubelet actually uses) and a
  separate, independent Docker daemon. Images built with `docker build`
  live ONLY in Docker's store until explicitly imported into containerd.

## Node IP: two addresses, both needed

`ens160` carries `192.168.86.100/24` (the real, current, netplan-configured
address) AND `192.168.2.100/32` (added as a secondary address, persisted in
`/etc/netplan/50-cloud-init.yaml`). Both are required:

- `192.168.2.100` is hardcoded all over this deployment from an earlier
  network configuration: the `nfs-provisioner` chart's `NFS_SERVER` env var,
  `hostAliases` entries in the ranger/superset/trino helm values (for
  `trino.local`/an LDAP server), and originally etcd's own bootstrap record.
- `192.168.86.100` is what's actually configured on the NIC and what DHCP/
  routing on the real LAN expects.

Since the NFS server, ingress controller, LDAP, and kubelet are all on this
same single node, adding `.2.100` as a secondary local address satisfies
every hardcoded reference via local delivery — it doesn't need to be a real
routable subnet. **Do not remove this secondary address** unless you've
first hunted down and fixed every one of those hardcoded references, or
things silently break again (NFS mounts time out, LDAP hostAlias resolution
fails).

If this node's IP ever changes again and things break in the exact pattern
below, this is why:

## Symptom: mass pod failure across every namespace, API server flaky

This happened once already (root cause: the IP mismatch above reached
etcd). etcd stores its own member's peer-url separately from "what IP is
actually on the NIC right now" — if they diverge, `rke2-server` refuses to
start with `this server is a not a member of the etcd cluster. Found
[...], expect: [...]`. Fix (metadata-only, no data touched, safe for a
single-member cluster):

```bash
# etcdctl isn't bundled with RKE2; run it via containerd directly
/var/lib/rancher/rke2/bin/ctr -a /run/k3s/containerd/containerd.sock \
  images pull quay.io/coreos/etcd:v3.5.9

/var/lib/rancher/rke2/bin/ctr -a /run/k3s/containerd/containerd.sock \
  run --rm --net-host \
  --mount type=bind,src=/var/lib/rancher/rke2/server/tls/etcd,dst=/etcd-certs,options=rbind:ro \
  quay.io/coreos/etcd:v3.5.9 etcd-fix \
  etcdctl --endpoints=https://127.0.0.1:2379 \
    --cacert=/etcd-certs/server-ca.crt --cert=/etcd-certs/server-client.crt \
    --key=/etcd-certs/server-client.key \
    member update <member-id> --peer-urls=https://<current-real-ip>:2380
# find <member-id> via `member list` with the same flags first

systemctl restart rke2-server
```

After that, everything in `default`/`ranger`/`nfs-provisioner` namespaces
may still be sitting at `replicas: 0` (see next section) — that's a
separate problem from the etcd fix, don't assume one implies the other.

## Symptom: MinIO/Trino/Superset/Ranger/Nessie all down, `0/0` replicas

Also happened once already, likely during the same outage (possibly a
manual or scripted scale-down to relieve the disk/etcd crisis that never
got reversed). **Every Deployment/StatefulSet in `default`, `ranger`,
`nfs-provisioner`, and `spark-operator` namespaces had its replica count
set to 0** — not crashed, just scaled down. This included `loki` in
`monitoring` too (found later, while wiring up the audit-logging pipeline
-- fluent-bit showed `0/1` ready with no obvious cause until this was
spotted). `helm get manifest <release>` confirms the chart-intended count
is 1 for essentially everything (2 for `minio-operator`). Fix is just
scaling back up, in dependency order:

```bash
kubectl scale deploy minio-operator -n default --replicas=2
kubectl scale statefulset ranger-postgresql -n ranger --replicas=1
kubectl scale statefulset superset-postgresql -n default --replicas=1
kubectl scale statefulset superset-redis-master -n default --replicas=1
kubectl scale deploy nfs-provisioner-nfs-subdir-external-provisioner -n nfs-provisioner --replicas=1
kubectl scale statefulset loki -n monitoring --replicas=1
# wait for those, then:
kubectl scale statefulset myminio-pool-test -n default --replicas=1  # minio-operator may not auto-reconcile this; check and scale manually if still 0/0
kubectl scale deploy ranger-apache-ranger -n ranger --replicas=1
kubectl scale deploy nessie -n default --replicas=1
kubectl scale deploy trino-coordinator trino-worker -n default --replicas=1
kubectl scale deploy superset superset-worker -n default --replicas=1
kubectl scale deploy spark-operator-controller spark-operator-webhook -n spark-operator --replicas=1
```

Given the pattern keeps recurring across namespaces discovered at
different times, if something in this cluster is unexpectedly down with no
crash/error, check `kubectl get deploy,statefulset -A -o
custom-columns=NS:.metadata.namespace,NAME:.metadata.name,DESIRED:.spec.replicas
| grep ' 0$'` before assuming it's actually broken.

The cluster is also littered with ~250 leftover `ContainerStatusUnknown` /
`Init:ContainerStatusUnknown` / `Error` pod records from whenever this
outage happened (stale API objects, not real running containers — they
don't hold real resource allocations). Harmless but noisy; nobody's
cleaned them up. `kubectl delete pod --field-selector=status.phase=Failed
-A` (or similar, per-namespace) would tidy them up if asked, but that's a
few hundred deletes across namespaces this session didn't own — do it
deliberately, not as a drive-by.

## Superset's custom image only exists on this host

`docker.io/library/superset-ldap:6.0.0` (the LDAP-patched Superset image
Superset's helm release uses) was built locally at some point and never
pushed to any registry — the `docker.io/library/` prefix is misleading,
it's purely a local tag. If it's ever evicted from containerd's image
store again (e.g. by disk-pressure image GC, which is what happened this
session), it can be recovered from the separate Docker daemon's store,
which isn't subject to the same GC:

```bash
docker save superset-ldap:6.0.0 -o /tmp/superset-ldap.tar
/var/lib/rancher/rke2/bin/ctr -a /run/k3s/containerd/containerd.sock \
  -n k8s.io images import /tmp/superset-ldap.tar   # -n k8s.io matters, see below
```

If Docker's own copy is ever gone too, this image needs to be rebuilt from
whatever Dockerfile/build context originally produced it (not present in
this repo — go find it, likely `/root/Dockerfile` or `/root/superset/` per
what existed on this host as of 2026-09-23).

## `ctr images import` needs `-n k8s.io`

`ctr` defaults to the `default` containerd namespace. kubelet's CRI plugin
reads from the `k8s.io` namespace. An import without `-n k8s.io` succeeds
silently but kubelet will still report `ImagePullBackOff` / "pull access
denied" as if the image weren't there at all. Always:

```bash
ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images import <tar>
```

## Docker Hub image pulls sometimes need a different registry

`docker.io/minio/mc:latest` returns `pull access denied /
insufficient_scope` (MinIO restricted anonymous pulls at some point).
`quay.io/minio/mc:latest` is the working public mirror. If another
`docker.io/<org>/...` pull fails the same way, check for a `quay.io`
mirror before assuming the image is actually gone.

## Reserved OpenSearch field names

Never name a real document field `_source` (or other leading-underscore ES
metadata names like `_index`, `_id`, `_type`) — bulk indexing fails with
`mapper_parsing_exception` for every such document, silently retried
forever by Fluent Bit with no obvious error unless you go looking at
`Trace_Error On` output.

## Disk pressure is a recurring, real risk -- not a one-off

Root disk usage on this host swings between ~80% and ~95% depending on
what's being pulled/built at the moment, and kubelet **will** taint the
node `node.kubernetes.io/disk-pressure:NoSchedule` when it crosses the
threshold -- this happened twice during the audit-logging work (once
during initial setup, once while testing the Spark archival job). When it
happens:
- All new pod scheduling stops cluster-wide until usage drops back down
  and the condition self-clears (kubelet re-evaluates periodically, no
  manual intervention needed for the taint itself).
- Kubelet's image garbage collection may evict container images to free
  space -- including custom, never-pushed-anywhere images like
  `opensearch-with-s3:2.19.1` and `superset-ldap:6.0.0`. If pods relying on
  those start `ImagePullBackOff`'ing right after a disk-pressure episode,
  that's why -- re-import them from the separate Docker daemon's store
  (see below), they're not actually gone, just evicted from containerd.

`df -h /` before anything disk-heavy (image builds/pulls, Spark jobs
downloading dependency jars). Safe, non-destructive relief valves that
have worked so far: `docker builder prune -f`, `docker image prune -f`
(only removes dangling/unused images, never touches running containers).
This host also runs a large number of **unrelated** Docker Compose
services directly (OpenMetadata, Airflow, lldap, neo4j, redis-stack,
confluent schema-registry, and more) with multi-gigabyte images — never
delete or touch these, they aren't part of the k8s cluster and aren't
yours to manage.

## spark-operator: stuck `pending-install` release

The `spark-operator` helm release was found stuck at `STATUS:
pending-install` (revision 1, from the original 2026-08-12 install that
never completed) even though its actual Deployments/Service already
existed in the cluster (just scaled to 0, see the mass-scale-down section
above). A `pending-install`/`pending-upgrade` helm release blocks all
future `helm upgrade`/`install` on it with `Error: UPGRADE FAILED: another
operation (install/upgrade/rollback) is in progress` -- patching the
release secret's `status` label does NOT fix this (Helm's real lock state
lives inside the encoded release data, not the label). What worked:
delete the stuck release secret outright and reinstall fresh --
non-destructive here since a `pending-install` release, by definition,
never finished creating anything Helm itself considers "owned":

```bash
kubectl delete secret sh.helm.release.v1.spark-operator.v1 -n spark-operator
helm install spark-operator spark-operator/spark-operator -n spark-operator -f <its values>
```

spark-operator only watches `default` namespace for
SparkApplication/ScheduledSparkApplication CRs on this cluster
(`spark.jobNamespaces` in its helm values) -- CRs created in any other
namespace are silently ignored.

## Running Spark jobs (via spark-operator) against the Nessie/Iceberg catalog

Two non-obvious fixes were needed to get a `SparkApplication` using
`spec.deps.packages` (Maven coordinates, Ivy-resolved) to submit and run
against this cluster's Nessie-backed Iceberg catalog at all:

1. **`spark.jars.ivy` must point at a writable path** (e.g. `/tmp/.ivy2`).
   Package resolution for `deps.packages` runs inside the
   **spark-operator's own controller pod** (to validate before ever
   creating a driver pod), and that pod's `$HOME` isn't writable, so Ivy's
   default cache location fails immediately with a bare
   `FileNotFoundException` -- no driver pod is ever created, and the
   `SparkApplication` just shows `SUBMISSION_FAILED`.
2. **The Nessie REST catalog dictates `org.apache.iceberg.aws.s3.S3FileIO`**
   as the FileIO implementation for its warehouse, regardless of any
   client-side `spark.sql.catalog.<name>.io-impl` override -- tried
   `HadoopFileIO` first (to reuse the same `hadoop-aws` jar already used
   elsewhere in this cluster for `s3a://` access) and it failed with
   `Cannot find constructor for interface org.apache.iceberg.io.FileIO`.
   Needed `org.apache.iceberg:iceberg-aws-bundle` (AWS SDK v2) on the
   classpath plus `spark.sql.catalog.<name>.s3.endpoint` /
   `.s3.path-style-access` client settings -- separate from the
   `fs.s3a.*` settings still needed for reading raw files via
   `spark.read.json("s3a://...")`, which is an unrelated Hadoop-S3A code
   path, not Iceberg's FileIO.

Also: `spark.read.json(path)` does not descend into subdirectories by
default -- pass `.option("recursiveFileLookup", "true")` if the source
data is partitioned into nested folders (e.g. by fluent-bit's
hour-partitioned `s3_key_format`), or you'll get a confusing "Unable to
infer schema for JSON" even though matching files genuinely exist under
the path.

See `audit-logging/spark/` for a working example of all of this wired up
end-to-end (verified 2026-09-23: real Iceberg tables with Parquet data
files, registered in Nessie, confirmed via direct REST API + MinIO
inspection).

## `ScheduledSparkApplication` can silently wedge if the operator restarts at the wrong moment

Found 2026-09-26, testing the audit-logging cold tier: a
`ScheduledSparkApplication`'s run can complete successfully (driver pod
`Completed`, logs show real success) but the operator crashing/restarting
right at that moment can leave the corresponding `SparkApplication` object
stuck in `PENDING_RERUN` forever. This silently blocks all future
scheduled runs -- the schedule object itself still reports
`"scheduleState": "Scheduled"` and looks completely healthy, and
`kubectl get pods` shows nothing wrong either (the stuck job's driver pod
is just sitting there `Completed`, same as every other successful run).
The only way to notice is to check whether the *data* is actually still
growing (e.g. an Iceberg row count), not the Kubernetes objects.

Diagnose:
```bash
kubectl get scheduledsparkapplication <name> -n default -o jsonpath='{.status}' | python3 -m json.tool
# a `lastRun`/`nextRun` that's days old, with the schedule cron implying
# it should have fired since, is the tell
```

Fix -- delete the wedged run, then force a reconcile (deleting the child
object alone isn't enough; the `ScheduledSparkApplication` controller only
evaluates whether to submit a new run at each cron tick or on a watch
event against the *schedule* object itself, not on the child's deletion):
```bash
kubectl delete sparkapplication <stuck-run-name> -n default
kubectl annotate scheduledsparkapplication <name> -n default force-reconcile="$(date +%s)" --overwrite
```
A new run should submit within ~15 seconds. This is a genuinely
destructive-adjacent action (deleting a workload object) -- confirm the
driver's logs show real prior success first (don't delete a run that
might still be legitimately in-flight), and get confirmation before
running it if you're not sure.

## This cluster's fluent-bit build has a real bug: avoid the `http` input

`cr.fluentbit.io/fluent/fluent-bit:5.0.9` (and confirmed also `3.1.9`) never
routes records from an `http`-type `[INPUT]` to any network-based
`[OUTPUT]` (`opensearch`, `es`, `loki` all confirmed) — the request is
accepted (200/201, input counters increment) but the engine logs `task ...
without routes, dropping` and the record vanishes, no errors anywhere.
Reproduces in the simplest possible config (one http input, one opensearch
output, no filters). Local outputs (`stdout`, `null`) are unaffected.

Full writeup and the workaround (route everything through the existing
`tail` input instead) is in `audit-logging/fluent-bit/values.yaml`'s header
comment. **Don't add a new `http` input to fluent-bit for anything without
re-testing this first** — it may get fixed in a future fluent-bit version,
but don't assume it has been.

## MinIO Object Lock is a one-way door -- decide retention *before* creating the bucket

Object Lock (`mc mb --with-lock`, or `create_bucket` with
`ObjectLockEnabledForBucket=True`) can only be set **at bucket creation**.
It cannot be added to an existing bucket, and — this is the part that
actually bit us — **it cannot be removed either**, ever, for the life of
that bucket. Confirmed live 2026-09-26 via boto3's
`get_object_lock_configuration` and a real object's `head_object`
(`ObjectLockMode: GOVERNANCE`, a genuine `ObjectLockRetainUntilDate` a year
out) when a design that had assumed "we can just stop enforcing this
later" turned out to be structurally impossible — GOVERNANCE-mode
retention rejects a plain delete outright unless the caller has
`s3:BypassGovernanceRetention`, and there's no config change, bucket
policy, or admin override that lifts the lock itself.

If a bucket is ever going to need app-managed deletion (a retention job,
a cleanup script, anything other than "this data lives here forever") —
**do not create it with Object Lock**, even if that seems like the safer
default at the time. The fix when this goes wrong isn't a settings change,
it's a new bucket and a migration (see `audit-logging/ARCHITECTURE.md`'s
"Raw landing retention" section for exactly this happening to
`audit-logs-cold` -> `audit-logs-raw`, and
`audit-logging/minio/create-audit-raw-bucket-job.yaml` for the
lock-free replacement).
