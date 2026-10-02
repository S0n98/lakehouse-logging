# Install Guide

Self-contained bundle for reproducing the audit-logging pipeline's Helm
releases: pulled chart archives plus the exact values used on this
cluster. Read **"Installing in an offline / air-gapped environment"**
below before using this anywhere without internet access -- several
pieces of this pipeline reach out to the internet at points that aren't
obvious from the Helm charts alone.

```
charts/    Pulled chart archives (.tgz), pinned to the exact version
           deployed on this cluster.
values/    The exact values used for each release. Secrets have been
           REDACTED (see "Redacted secrets" below) -- fill in your own
           before installing.
```

**`download-charts.sh` automates (re-)populating both directories** --
pulls all 6 chart archives at the pinned versions above, and refreshes
`values/` (copying from this repo's own tracked source for
opensearch/opensearch-dashboards/fluent-bit, live-dumping spark-operator
which has no secrets). **It deliberately does NOT touch
`values/trino-values.yaml` or `values/superset-values.yaml`** -- those
carry real live secrets when dumped fresh (see "Redacted secrets" below),
so a live re-dump lands in a separate, gitignored
`values-live-unredacted/` instead, with instructions printed for manually
carrying forward real changes while keeping every `<REDACTED-...>`
placeholder intact. Run it on the box these releases are actually
deployed on (needs both internet, for the chart repos, and a working
`kubectl`/`helm` context against this cluster, for the live dumps).

## What's here and where it came from

| Release | Chart | Namespace | Why it's part of this pipeline |
|---|---|---|---|
| `opensearch` | `opensearch-3.8.0` | `logging` | Hot, searchable tier -- new install |
| `opensearch-dashboards` | `opensearch-dashboards-3.8.0` | `logging` | Optional UI for the above -- **installed 2026-09-29**; its values here fix a real chart bug (see below) |
| `spark-operator` | `spark-operator-2.5.2` | `spark-operator` | Runs the periodic Iceberg-loading jobs -- pre-existing on this cluster, its stuck install was repaired as part of this work (see `/root/datahub/CLAUDE.md`) |
| `fluent-bit` | `fluent-bit-0.57.9` | `monitoring` | Collector -- pre-existing release, values here are the FULL merged config including the audit routing added by this work |
| `trino` | `trino-1.42.2` | `default` | Audit source (query events) + Ranger plugin -- pre-existing release, values here are the full live config including this work's Ranger-audit fix and event-listener addition |
| `superset` | `superset-0.22.4` | `default` | Audit source (user actions) -- pre-existing release, values here are the full live config including this work's custom event logger |

`trino` and `superset` values are the cluster's **entire** live
configuration (pulled via `helm get values`), not just the audit-related
pieces -- reinstalling from these reproduces the whole release, not only
the audit logging parts. `opensearch`, `opensearch-dashboards`, and
`fluent-bit` values come from `../opensearch/`, `../fluent-bit/` in this
repo (already documented there).

## Redacted secrets

`values/trino-values.yaml` and `values/superset-values.yaml` had real,
live credentials in them when pulled from the cluster (LDAP bind
password, Trino's internal-communication shared secret, bcrypt password
hashes, MinIO access keys, Superset's Flask secret key, Postgres
passwords). Every one of those has been replaced with a
`<REDACTED-...>` placeholder and a comment saying what to put there
instead. **Search for `REDACTED` in both files and fill in real values
before installing** -- these charts will deploy with broken auth
otherwise (or, worse, install fine with a value literally named
`<REDACTED-...>`, which is obviously wrong but won't necessarily fail
loudly).

## Standard install (cluster has internet access)

Same order as `../README.md`, just pointing `helm install`/`upgrade` at
the local `.tgz` files instead of a chart repo:

```bash
kubectl create namespace logging   # if not already created

helm install opensearch charts/opensearch-3.8.0.tgz \
  -n logging -f values/opensearch-values.yaml

helm install opensearch-dashboards charts/opensearch-dashboards-3.8.0.tgz \
  -n logging -f values/opensearch-dashboards-values.yaml   # optional but recommended

helm upgrade --install spark-operator charts/spark-operator-2.5.2.tgz \
  -n spark-operator -f values/spark-operator-values.yaml

helm upgrade fluent-bit charts/fluent-bit-0.57.9.tgz \
  -n monitoring -f values/fluent-bit-values.yaml

helm upgrade trino charts/trino-1.42.2.tgz \
  -n default -f values/trino-values.yaml

helm upgrade superset charts/superset-0.22.4.tgz \
  -n default -f values/superset-values.yaml
```

This step alone does **not** need the internet **for Helm's part** --
the charts are local. It still needs the internet (or a private registry
+ mirrored images, see below) for every container image these charts
reference, and Trino/Superset assume `default`/`logging`/`monitoring`
namespaces and several other pre-existing pieces (MinIO tenant, Nessie,
Ranger, the `trino-audit-shim` service, secrets) already exist -- see
`../README.md` for the full dependency order, this table only covers the
Helm layer.

### Two real bugs hit installing `opensearch-dashboards`, already fixed in `values/opensearch-dashboards-values.yaml`

Worth knowing if you're installing this fresh anywhere, not just this
cluster:

1. **The chart's ingress template needs the backend spelled out
   explicitly.** `paths[].backend.serviceName`/`servicePort` (old-style,
   not the `service.name`/`service.port.number` shape newer ingress specs
   use) -- omitting it fails the whole `helm install` with a nil-pointer
   error on `.backend.serviceName`, since this chart's ingress template
   doesn't default to the release's own service the way some others do.
   Already set correctly in the values file here (`opensearch-dashboards`
   / `5601`, matching the chart's own `service.port` default).
2. **A stray `cluster.blocks.create_index: true` OpenSearch cluster
   setting can block Dashboards from ever creating its `.kibana`
   saved-objects index on first connect**, with no error in Helm or the
   pod's readiness -- only visible in the Dashboards pod's own logs
   (`index_create_block_exception`). Not something either chart sets; if
   you hit this, check `GET _cluster/settings?flat_settings=true` on the
   OpenSearch cluster and clear it:
   `PUT _cluster/settings {"persistent": {"cluster.blocks.create_index": null}}`.

## Installing in an offline / air-gapped environment

This is the part that needs real attention -- several pieces of this
pipeline reach out to the internet **at runtime**, not just at initial
install, which is easy to miss if you only check the Helm charts.

### 1. Container images

**`download-images.sh` automates pulling and saving every image below**
(as `.tar` files, ready to transfer) -- run it on a machine with internet
access, then transfer its output directory and `ctr ... images import`
each file on the air-gapped cluster (exact command in the script's own
header). It can't fetch the two custom, local-only images for you
(`opensearch-with-s3`, `superset-ldap`) -- it saves them if they already
exist in this host's Docker store, and tells you clearly if not, per the
table below.

**Every image below is needed for the full working system, not just the
audit-logging additions** -- the audit pipeline doesn't run standalone,
it sits on top of an existing Trino/Superset/Ranger/Nessie/MinIO
lakehouse, and none of those platform pieces' images are covered by any
other list in this repo. Confirmed against what's **actually running**
on this cluster (`kubectl get pods -o jsonpath='{...image}'`) on
2026-09-29, not just read out of chart defaults -- chart defaults can
lag what's really deployed.

Every image below must be reachable from the cluster -- either by
mirroring it into a private registry the cluster can reach, or by
`docker save`/`ctr export`-ing it on a machine with internet access and
`ctr ... images import`-ing it directly into each node's containerd (the
approach used throughout this work; see `/root/datahub/CLAUDE.md` for the
exact commands and the `-n k8s.io` namespace gotcha).

**A. Audit-logging pipeline's own additions** (charts bundled in
`charts/`, values in `values/`):

| Image | Public? | Notes |
|---|---|---|
| `opensearch-with-s3:2.19.1` | **No -- custom, local-only** | Built from `../opensearch/Dockerfile.opensearch-s3`. Building it requires internet (downloads the `repository-s3` plugin). For offline use: build it once where there IS internet, then transfer the built image (`docker save` / `ctr export`) -- do not try to build the Dockerfile inside the air-gapped network. |
| `docker.io/library/busybox@sha256:cac8f90bbee42dc962a6b38bb1a235948d070385bb9d996bba15a6db8d364008` | Yes | OpenSearch init container (`fsgroup-volume`). This is the **exact digest actually running** on this cluster, not `busybox:latest` -- pin to this (or re-resolve and re-pin deliberately) for a real offline bundle; `latest` can silently drift between when you mirror it and when you install. |
| `opensearchproject/opensearch-dashboards:3.8.0` | Yes | Now actually installed on this cluster (2026-09-29) -- see the ingress/cluster-block gotchas above before assuming a clean install elsewhere. |
| `ghcr.io/kubeflow/spark-operator/controller:2.5.2` | Yes | Used for both the spark-operator controller and webhook deployments. |
| `apache/spark:3.5.3` | Yes | Driver/executor image for the archival jobs (`../spark/`). |
| `cr.fluentbit.io/fluent/fluent-bit:5.0.9` | Yes | |
| `quay.io/minio/mc:latest` | Yes, **but flaky** | Used by the one-shot bucket-setup jobs (`../minio/create-audit-raw-bucket-job.yaml`, and the legacy `create-audit-bucket-job.yaml`). `docker.io/minio/mc` requires auth for anonymous pulls (`insufficient_scope`); `quay.io/minio/mc:latest` has also returned HTTP 401 intermittently during this project (rate-limiting, apparently) -- if either fails, create the bucket directly with any S3 SDK instead (`boto3.client("s3").create_bucket(...)`, no image needed at all). Pin an exact tag if you rely on the image, not `latest`. |
| `python:3.12-alpine` | Yes | `trino-audit-shim` (`../trino/audit-shim.yaml`). |

**B. Pre-existing lakehouse platform** (this pipeline depends on these
already running -- their charts aren't bundled here since they're not
part of this pipeline's own install, but their images are still required
for the full system to work in an air-gapped environment):

| Image | Public? | Notes |
|---|---|---|
| `docker.io/library/superset-ldap:6.0.0` | **No -- custom, local-only** | Pre-existing on this cluster before this work; no Dockerfile for it in this repo. Must be exported from wherever it was originally built, or rebuilt from its original source and transferred the same way. Gets evicted under disk pressure more than any other image on this cluster (see `/root/datahub/CLAUDE.md`) -- keep a copy in Docker's separate store as a recovery source, not just containerd. |
| `trinodb/trino:480` | Yes | Trino coordinator + worker. |
| `apache/ranger:2.8.0` | Yes | Ranger admin (`ranger-apache-ranger`). |
| `ghcr.io/projectnessie/nessie:0.107.9` | Yes | Iceberg REST catalog. |
| `quay.io/minio/operator:v5.0.18` | Yes | MinIO operator + console. (The running tenant pod's operator-image sidecar is pinned at `v5.0.17`, one patch behind the installed operator chart's `v5.0.18` -- both are needed if reproducing this exactly.) |
| `quay.io/minio/minio:RELEASE.2024-08-03T04-33-23Z` | Yes | The actual MinIO server image (tenant pods). |
| `docker.io/bitnamilegacy/postgresql:14.17.0-debian-12-r3` | Yes | Backs **both** Superset's and Ranger's Postgres (two separate StatefulSets, same image) -- note the `bitnamilegacy` org, not `bitnami`: Bitnami restricted anonymous pulls on their main images, `bitnamilegacy` is the mirror actually in use. |
| `docker.io/bitnamilegacy/redis:7.0.10-debian-11-r4` | Yes | Superset's Redis (Celery broker). Same `bitnamilegacy` note. |

**Not included:** the `lldap` LDAP server and every other Docker Compose
service on this host (OpenMetadata, Airflow, Flink, neo4j, etc.) --
those aren't part of the Kubernetes cluster or this lakehouse stack at
all, see `/root/datahub/CLAUDE.md`. If your offline environment also
needs LDAP-backed auth for Trino/Superset, that's a separate system to
inventory on its own terms, not part of this list.

### 2. Spark's Maven dependency resolution -- the biggest offline blocker

`../spark/scheduled-spark-application.yaml` uses `spec.deps.packages` with
Maven coordinates:

```
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262
org.apache.iceberg:iceberg-aws-bundle:1.6.1
```

**This resolves against Maven Central over the internet on every single
job run**, not once at install time -- spark-submit's Ivy resolver
downloads these fresh into an ephemeral pod filesystem each time (see
`/root/datahub/CLAUDE.md` for why there's no shared cache between runs).
An air-gapped cluster running the archival jobs as-is will have every
hourly run fail at submission with exactly the kind of Ivy resolution
error documented in `../ARCHITECTURE.md`, just for "can't reach
repo1.maven.org" instead of the writable-cache issue that showed up
during development.

Two ways to fix this for offline use, in order of how much they change
the design:

**Option A -- bake the jars into a custom Spark image (recommended).**
While you still have internet access, download the four jars above (plus
their transitive dependency, `org.wildfly.openssl:wildfly-openssl:1.0.7.Final`
-- pulled in automatically by `hadoop-aws`) from Maven Central, `COPY`
them into `/opt/spark/jars/` in a Dockerfile `FROM apache/spark:3.5.3`,
build and transfer that image the same way as `opensearch-with-s3`, and:
- point `image:` at the new custom image instead of `apache/spark:3.5.3`
  in all three `scheduled-spark-application.yaml` templates,
- delete the `deps.packages` block entirely (the jars are already on the
  classpath via `SPARK_DIST_CLASSPATH`/`/opt/spark/jars/`, matching the
  pattern this cluster's own `spark-history-server` chart already uses
  for `hadoop-aws`/`aws-java-sdk-bundle` -- see
  `/root/datahub/CLAUDE.md` for that precedent).
- keep `spark.jars.ivy` removed too, since nothing needs Ivy resolution
  anymore.

**Option B -- host a local Maven-compatible mirror** (e.g. Nexus,
Artifactory, or even a plain HTTP server serving the right directory
layout) reachable from inside the air-gapped network, and add it via
`deps.repositories` in the SparkApplication spec alongside
`deps.packages`, pointing Ivy there instead of Maven Central. More
infrastructure to stand up, but keeps the "fetch by coordinate" model
instead of transitioning to Option A's pre-baked jars.

Either way: **verify this before relying on the hourly schedule** -- run
one manual `SparkApplication` (see `../README.md` "Querying the cold
archive" for the one-off-run pattern) and confirm it reaches
`RUNNING`/`COMPLETED`, not `SUBMISSION_FAILED`.

### 3. Everything else that talks over a network at runtime

Once the images and jars above are handled, the rest of the pipeline only
talks to other services already inside the same cluster (OpenSearch,
MinIO, Nessie, Trino, Fluent Bit) -- no other internet dependency. The
`opensearch-with-s3` image's `repository-s3` plugin, in particular, is
already installed into the image at build time (step 1 above); it does
**not** reach out to the internet again at container startup.
