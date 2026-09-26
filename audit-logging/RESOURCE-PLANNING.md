# Resource planning: CPU, RAM, disk for 1 year

## Read this first: every disk number in this doc shares ONE physical disk

Checked directly on this cluster (2026-09-26): **neither storage class in
use actually enforces its declared PVC size.**

| PVC | Declared size | StorageClass | What `df` shows inside the pod | Actual data (`du`) |
|---|---|---|---|---|
| OpenSearch (`nfs-client`) | 100Gi | `nfs-client` (NFS, provisioner `nfs-subdir-external-provisioner`) | `146G size, 125-126G used, 13-14G avail` | **19 MB** |
| MinIO (`local-minio`) | 500Gi | `local-minio` (`no-provisioner`, a local hostPath-style PV) | `146G size, 125G used, 14G avail` | audit-logs-raw: 2.0 MB, lakehouse: 170 MB, audit-logs-cold (legacy): 5.1 MB |

Both `df` outputs are identical and match this single-node host's actual
root filesystem (`/dev/mapper/ubuntu--vg-ubuntu--lv`, 146G) exactly — because
that's literally what they are. The NFS server is this same host (see
`CLAUDE.md`'s Node IP section), and the `local-minio` PV is a hostPath
into the same disk. **Every PVC "size:" value on this cluster (100Gi,
500Gi, whatever) is a planning label, not an enforced quota** — the real,
only constraint is the one shared physical disk, and disk pressure on it
(see `CLAUDE.md`) is a whole-cluster event, not something isolated to
whichever component happens to be writing a lot at the time.

**Practical consequence for this doc:** every disk figure below should be
read as "additional bytes that need to exist somewhere on the shared
host disk," not as a dedicated allocation. If this pipeline ever moves to
real infrastructure with actual per-volume quotas (separate cloud disks,
a real multi-node NFS/Ceph cluster, etc.), the PVC `size:` fields should
be set to match the numbers here for real, since they currently do
nothing.

## Scope: what's "the stack"

**Dedicated to this pipeline** (fully budgeted below): OpenSearch (+
optional Dashboards), Fluent Bit, `trino-audit-shim`, the three
`ScheduledSparkApplication` jobs, and the MinIO buckets
(`audit-logs-raw`, plus the `audit` schema inside `lakehouse`).

**Pre-existing shared infrastructure** (Trino, Superset, Ranger, Nessie,
MinIO's own tenant, spark-operator itself): this pipeline adds marginal
load to these — a few more Ranger policy checks per query, one more
event-listener HTTP POST per Trino query, a handful of extra rows in
Superset's own DB — but they'd all be running at roughly the same cost
regardless of whether this audit pipeline existed, since they're the
actual lakehouse platform, not part of the logging stack itself. Not
separately budgeted here.

## 1. Baseline CPU/RAM (current live configuration)

| Component | CPU request | CPU limit | RAM request | RAM limit | Live measured (2026-09-26, test volume) |
|---|---|---|---|---|---|
| OpenSearch (`opensearch-cluster-master-0`) | 1 core | 2 cores | 3Gi | 4Gi (1.5GB JVM heap) | 12m CPU, 2.53Gi RAM |
| Fluent Bit (per node, DaemonSet — 1 node here) | 50m | *(none set)* | 64Mi | *(none set)* | 12m CPU, 13Mi RAM |
| `trino-audit-shim` | 10m | 100m | 32Mi | 64Mi | negligible |
| Spark driver (per run, ×3 sources) | 1 core | — | 1Gi | — | ~1.5-2 min runtime |
| Spark executor (per run, ×3 sources, 1 each) | 1 core | — | 1Gi | — | same window as driver |
| OpenSearch Dashboards (optional, **not installed**) | 250m | 1 core | 512Mi | 1Gi | n/a |

Two things worth calling out:

- **Fluent Bit has no CPU/memory limit set** — only a request. On a
  single-node cluster already prone to memory/disk pressure (`CLAUDE.md`),
  an unbounded DaemonSet pod is a real (if currently theoretical, given
  13Mi actual usage) risk if throughput ever spikes hard. Worth adding a
  limit once real production volume is known, rather than leaving it open.
- **Spark's per-run cost is transient, not resident** — 2 driver+executor
  cores and 2Gi RAM exist only for ~1.5-2 minutes, three times an hour
  (roughly 5, 10, and 15 minutes past), not continuously. Peak concurrent
  demand if all three overlap (they're staggered 5 minutes apart
  specifically to avoid this) would be 6 cores / 6Gi for a couple of
  minutes — worth keeping the stagger if this cluster ever gets busier
  with something else needing that headroom.

## 2. How CPU/RAM need to scale with real volume

Using the same event-volume and user-count scenarios as `CAPACITY.md`,
since the two documents should stay consistent — reuse its per-record
sizes and assumptions rather than re-deriving them here.

### OpenSearch: RAM/heap is the dimension that scales, not CPU

Indexing CPU scales with **events per second**, not total data held. Even
the top of this doc's realistic range (10,000,000 events/day ≈ 116
events/sec average) is modest for OpenSearch — general operational
guidance for small, low-cardinality documents like these puts
comfortable single-core indexing throughput in the hundreds-to-low-
thousands of events/sec range. **CPU is not the constraint here across
the ranges in this doc** — the current 1-2 core allocation has
substantial headroom even at the high end, and isn't worth pre-emptively
increasing without an actual measured bottleneck.

RAM/heap needs to track the amount of hot-tier data actually held (7-day
window, per `CAPACITY.md`), using the general Elasticsearch/OpenSearch
rule of thumb of keeping total held data within roughly 10-20x of heap
size for a node also serving queries (not just indexing) — this is
general community operational guidance, not something benchmarked on
this specific cluster:

| Combined events/day | Hot-tier data (7d, from CAPACITY.md) | Heap needed (data÷15, rule of thumb) | Recommendation |
|---|---|---|---|
| 100 – 100,000 | 1.9 MB – 1.9 GB | negligible – ~127 MB | **No change** — current 1.5GB heap / 3-4Gi container has large headroom |
| 1,000,000 | 19.1 GB | ~1.27 GB | Current 1.5GB heap is now close to the rule-of-thumb floor — bump to ~2GB heap / 4-6Gi container |
| 10,000,000 | 191 GB | ~12.7 GB | **Needs a real multi-node OpenSearch cluster.** A single pod on this single-node host cannot host a 12.7GB heap alongside everything else this host runs (see `CLAUDE.md`'s chronic disk/memory constraints) — this volume is genuinely beyond what this cluster's architecture supports as-is. |

**This ignores `CAPACITY.md`'s field-mapping-explosion finding** (606
mapped fields on `trino_query_audit` from unfiltered Kubernetes
annotations) — if that's not fixed, real heap pressure will be
meaningfully worse than this table implies at every scale, since mapped
field count (not just document count) drives a real share of OpenSearch's
memory overhead. Fix that first before using this table to justify a
bigger allocation.

### Fluent Bit: stays lightweight across the whole range

Fluent Bit is built for high-throughput log shipping; even 10,000,000
events/day (116 eps average, generously say 500-1000 eps at peak) is
well within what the current 50m/64Mi request handles. **Recommendation:
add explicit limits** (e.g. 200m/256Mi) once real volume is known, mainly
as a safety net against runaway resource use, not because more is
expected to be needed.

### Spark jobs: fixed cost per run, until a real volume threshold

Each run's 1 driver + 1 executor (2 cores / 2Gi total, ~1.5-2 min) is
sized for this cluster's current near-zero real volume, not for a full
hour's backlog at serious scale. At the high end of this doc's range
(10,000,000 events/day ÷ 24 runs/day ÷ 3 sources ≈ 139,000 events/run for
the busiest source), a single 1-core executor processing that much JSON
in one pass would very likely run meaningfully longer and use
meaningfully more memory than the current 2-3 minute runs — **at that
scale, add executor instances (`executor.instances`) rather than assume
the current single-executor config keeps up.** No specific number is
given here since this hasn't been measured at real volume; treat it as
"revisit once actual run duration climbs past a few minutes," not a fixed
threshold.

## 3. Disk over 1 year

**`CAPACITY.md` is the source of truth for data volume** — this section
adds the pieces that aren't pure audit-record data.

### Recap: bounded vs. growing (from CAPACITY.md)

- Hot tier (OpenSearch): 7 days, constant, does not grow year over year.
- Raw landing (MinIO `audit-logs-raw`): 30 days, constant, does not grow
  year over year.
- Cold, queryable (Iceberg in `lakehouse`): unbounded, this is the one
  number that genuinely means "after 1 year of accumulation."

### Additional disk costs `CAPACITY.md` doesn't cover

- **OpenSearch operational headroom.** Translog and segment-merge
  temporary space typically need real headroom beyond the raw indexed
  data size — a common rule of thumb is provisioning at **2-3x** the pure
  data estimate for a node also handling merges comfortably. Combined
  with the field-mapping-explosion risk (above), **provision hot-tier
  disk at ~3x `CAPACITY.md`'s 7-day hot-tier number**, not 1x.
- **Spark's per-run dependency re-download.** `ARCHITECTURE.md`'s
  "Resource footprint" section already flags this: every run re-downloads
  ~300MB of Maven/Ivy dependency jars (no shared cache between ephemeral
  runs). Over a year: `300MB × 3 sources × 24 runs/day × 365 days ≈
  7.9 TB/year` of repeated download + unpack traffic. **This is not disk
  that accumulates** (ephemeral pod filesystem, discarded each run) — it's
  real, repeated network egress and disk I/O *churn*, worth knowing about
  on a host that's already disk-I/O sensitive. The fix, if this becomes a
  real problem (network cost, or I/O contention during the 3 runs that
  land within 10 minutes of each other), is exactly what
  `ARCHITECTURE.md` already suggests: bake the jars into a custom image
  (`spark-history-server`'s chart already does this — same pattern, see
  `install-guide/README.md`'s offline-install section), trading a one-time
  image size cost for eliminating ~7.9TB/year of repeated download.
- **Custom container images.** `opensearch-with-s3` and `superset-ldap`
  (~1.17GB per `docker images` — see `CLAUDE.md`'s recurring eviction
  gotcha) — fixed, one-time, not growing with time, but real disk that
  has to exist on this host and gets **evicted under disk pressure and
  needs re-importing**, which happened twice during this project already.

### Combined 1-year disk estimate, by scenario

Using `CAPACITY.md`'s Method B (by users/day) numbers, with the ~3x
OpenSearch headroom multiplier and Spark's ephemeral churn noted
separately (not summed into the disk total, since it isn't retained):

| Users/day | Hot tier ×3 headroom | Raw landing (30d) | Iceberg (1yr) | **Total resident disk** | Spark churn/year (not retained) |
|---|---|---|---|---|---|
| 10 | 52.5 MB | 57.6 MB | 105 MB | **~215 MB** | 7.9 TB |
| 100 | 525 MB | 576 MB | 1.05 GB | **~2.15 GB** | 7.9 TB |
| 1,000 | 5.25 GB | 5.76 GB | 10.5 GB | **~21.5 GB** | 7.9 TB |
| 10,000 | 52.5 GB | 57.6 GB | 105 GB | **~215 GB** | 7.9 TB |
| 100,000 | 525 GB | 576 GB | 1.05 TB | **~2.15 TB** | 7.9 TB |

Notice the Spark churn number **doesn't change with event volume** — it's
driven by run *frequency* (hourly × 3 sources), not data volume, so it's
a bigger relative cost at low volume than high volume. At the 10-100
users/day end of this table, the repeated dependency download is
actually a bigger yearly disk-I/O cost than all the retained audit data
combined — worth fixing (the custom-image approach) regardless of how
much real traffic this ends up serving.

## 4. Putting it together: sizing reference at three scales

Total CPU/RAM/disk if provisioning a cluster specifically for this stack
at a given scale (dedicated components only, per Scope above):

| | 1,000 users/day | 10,000 users/day | 100,000 users/day |
|---|---|---|---|
| OpenSearch CPU | 1-2 cores (unchanged) | 1-2 cores (unchanged) | 1-2 cores (unchanged) |
| OpenSearch RAM | 3-4Gi (unchanged) | 3-4Gi (unchanged) | 3-4Gi (unchanged) |
| Fluent Bit | 50-200m / 64-256Mi | 50-200m / 64-256Mi | 100-200m / 128-256Mi |
| Spark (peak, 3 runs overlap) | 6 cores / 6Gi, ~2min, 3x/hour | 6 cores / 6Gi, longer runs likely | 6+ cores / 6+Gi, executor scale-out likely needed |
| Disk, resident | ~21.5 GB | ~215 GB | ~2.15 TB |
| Disk, yearly churn (Spark deps) | 7.9 TB | 7.9 TB | 7.9 TB |

At 1,000-10,000 users/day, this comfortably fits the current single-node
host's spare capacity (24 vCPU / 32Gi RAM total) alongside everything
else already running on it, disk permitting (see the shared-disk caveat
at the top of this doc). At 100,000 users/day, OpenSearch's RAM stays
fine but Spark's per-run duration and the ~2.15TB resident disk start
being real planning constraints on a host this size; at the
10,000,000-events/day tier from Section 2, OpenSearch genuinely needs a
real multi-node deployment, not just a bigger single pod.

## Caveats

- Every scaling number above (OpenSearch heap ratios, Fluent Bit
  headroom, "when to add Spark executors") is engineering judgment /
  general operational rules of thumb, clearly labeled as such — **not**
  something measured on this cluster at real volume, unlike the baseline
  configuration table in Section 1 and the disk measurements at the top,
  which are. Re-measure and revise once this pipeline carries real
  production traffic.
- This doc assumes `CAPACITY.md`'s BI-workload assumptions (15
  queries/user/day, etc.) — if those don't match your real usage, redo
  both docs' numbers together, they're meant to stay consistent.
- Fixing the OpenSearch field-mapping explosion (drop/disable dynamic
  mapping on `kubernetes.annotations`) matters more to real RAM headroom
  than anything in this doc's scaling tables — do that before relying on
  the heap-sizing guidance above.
