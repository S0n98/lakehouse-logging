# Audit Logging Pipeline

Hot (searchable) + cold (queryable Iceberg archive) audit logging for
Ranger, Trino, and Superset on this cluster. Hot tier is OpenSearch; cold
tier is real Iceberg tables in the existing `lakehouse` warehouse, loaded
by a periodic Spark job -- queryable from Trino with plain SQL, not just
restorable like an OpenSearch snapshot. See `ARCHITECTURE.md` for the full
design and the reasoning behind each choice; this file is the
install/operate guide.

**Status as of 2026-09-23: fully deployed and verified end-to-end on this
cluster with real, impersonated queries** (not just synthetic markers) --
confirmed flowing correctly through every stage: OpenSearch indices (with
the real end user, not a shared service account -- see "Does it show the
real user" below), MinIO raw landing files, and Iceberg tables in the
`lakehouse` warehouse (schema, Parquet data files, and Nessie catalog
registration all inspected directly), plus 12+ consecutive successful
hourly Spark archival runs across all three sources. Three real bugs were
found and fixed along the way (not just theoretical caveats) -- see
`ARCHITECTURE.md` "Bugs found and fixed during rollout" for the full
writeup: Trino's query-audit field is nested (`metadata.queryId`, not
top-level), Ranger's async audit queue needed an explicit flush interval,
and Ranger's audit JSON is embedded in Trino's tab-separated log format
rather than being a bare JSON line.

**Updated 2026-09-26: retention model redesigned after an architecture
review.** Hot tier is now 7 days (was 30). Raw landing moved to a new
bucket (`audit-logs-raw`, no Object Lock) with a 30-day retention gated on
confirmed Iceberg archival, replacing an earlier "never delete" design
whose bucket turned out to structurally block early deletion via Object
Lock regardless of application logic. The Spark job switched from a
dynamic-partition-overwrite write to an idempotent `MERGE INTO` keyed on
`record_id = sha256(raw_json)`, which is what makes safe-to-delete
possible at all -- see `ARCHITECTURE.md`'s "Raw landing retention" section
for the full reasoning.

**Updated 2026-09-29: fixed an OpenSearch field-mapping explosion**
(`trino_query_audit` had reached 620 mapped fields from unfiltered
Kubernetes pod annotations, found while writing `CAPACITY.md`/
`RESOURCE-PLANNING.md`) -- a `lua` filter now strips
`kubernetes.annotations` before indexing, verified live (a fresh index
dropped to 75 fields). See `ARCHITECTURE.md`'s "Bugs found and fixed
during rollout" (#4) for the full writeup.

## What's here

```
opensearch/    Hot tier: OpenSearch (stock image), ISM policy setup job.
fluent-bit/    The collector: routes Ranger/Trino/Superset audit lines to
               both OpenSearch (hot) and MinIO raw landing (cold source).
               Read the header comment in values.yaml before touching this
               file -- there's a real fluent-bit bug its design works around.
minio/         The audit-logs-raw bucket (no Object Lock; retention is
               app-managed by the Spark job, see below) -- raw JSON
               landing zone, the input to the Spark->Iceberg load. The
               old, Object-Lock'd bucket this replaced is gone from this
               repo now (nothing reads/writes it going forward -- its
               history is in ARCHITECTURE.md's "Raw landing retention").
spark/         The periodic Spark job that loads raw JSON into Iceberg
               tables under the `lakehouse` warehouse. This is the actual
               cold, queryable archive.
ism-policies/  Per-source hot(7d)->delete lifecycle policies for OpenSearch.
ranger/        Fixes Ranger's audit destination (was silently broken) and
               grants the real-user-impersonation policy Superset needs.
trino/         Query-audit event listener + the shim it needs (see below).
superset/      Action-audit event logger (pastes into superset_config.py).
install-guide/ Pulled Helm chart archives + the exact values used, for
               reproducing these releases without a chart-repo lookup --
               includes offline/air-gapped install notes.
```

Also see **[TESTING.md](TESTING.md)** (how to actually verify each stage
is working, not just that pods are running -- includes a worked example
report), **[VIEWING-LOGS.md](VIEWING-LOGS.md)** (how to browse hot-tier
data in OpenSearch Dashboards and cold-tier data in Superset SQL Lab), and
**[CAPACITY.md](CAPACITY.md)** (1-year storage estimates by raw event
volume and by active user count, from measured per-record sizes), and
**[RESOURCE-PLANNING.md](RESOURCE-PLANNING.md)** (CPU/RAM/disk for the
whole stack over 1 year -- includes a finding worth knowing regardless of
this pipeline: this cluster's PVCs don't actually enforce their declared
size, every one of them shares the same physical disk).

## Architecture in one paragraph

Every audit source ends up as a JSON line on some pod's stdout, which
Fluent Bit's `tail` input already reads (it's the same input already
shipping every pod's logs to Loki). A `rewrite_tag` filter per source
picks these lines back out by a field unique to that source's JSON shape,
and re-emits them to **two** outputs: an `opensearch` output (hot,
searchable, 7-day retention) and an `s3` output that lands the same raw
JSON in MinIO, partitioned by hour. Once an hour, a
`ScheduledSparkApplication` per source reads that source's raw JSON and
merges it into a real Iceberg table under the cluster's existing
`lakehouse` warehouse (same Nessie catalog Trino's `iceberg` catalog
already uses) -- queryable forever with plain SQL, independent of
whatever OpenSearch's hot-tier retention does. Once a raw file's data is
confirmed merged and the file is 30+ days old, it's deleted -- see
`ARCHITECTURE.md`'s "Raw landing retention" for the full reasoning
(including why that wasn't always possible).

## History: the `opensearch-with-s3` custom image is gone (removed 2026-10-02)

OpenSearch used to run a custom image (stock
`opensearchproject/opensearch:2.19.1` + the `repository-s3` plugin, for
snapshotting indices straight to a MinIO bucket) instead of the plain
upstream image. **That feature was never actually used by this pipeline's
design** -- cold storage went with the Spark/Iceberg archival pipeline
described below instead of OpenSearch snapshots -- and the custom image
was a recurring disk-pressure-eviction pain point (local-only, never in
any registry, so kubelet GC'ing it meant a manual re-import every time;
one such eviction went unnoticed for 14 hours before anyone caught it).

Removed for real: `opensearch/values.yaml` now points at the stock image,
the now-pointless `keystore`/`s3.client.default.*` config was dropped
alongside it, the orphaned `minio-s3-keystore-creds` secret was deleted,
and `Dockerfile.opensearch-s3`/`build-and-import-image.sh` no longer exist
in this repo (see git history before this commit if snapshot-based cold
storage is ever revisited). Live-verified: `helm upgrade`d the running
release, rollout completed clean, `opensearch-cluster-master-0` running
`opensearchproject/opensearch:2.19.1` with no `keystore` init container.

## Install order

Namespaces/secrets assumed: `logging` (new, for OpenSearch), `monitoring`
(existing, has Loki + fluent-bit), `default` (existing, has MinIO/Trino/
Superset/Nessie/spark-operator), `ranger` (existing).

### 1. OpenSearch (hot tier)

```bash
helm repo add opensearch https://opensearch-project.github.io/helm-charts
helm repo update opensearch
kubectl create namespace logging

# Strong admin password, generated and stored directly -- never typed/echoed
kubectl create secret generic opensearch-admin-password -n logging \
  --from-literal=OPENSEARCH_INITIAL_ADMIN_PASSWORD="$(openssl rand -base64 24 | tr -d '=+/' | cut -c1-20)Aa1!"

# Also copy it into monitoring ns -- fluent-bit needs it to auth to OpenSearch
kubectl get secret opensearch-admin-password -n logging \
  -o jsonpath='{.data.OPENSEARCH_INITIAL_ADMIN_PASSWORD}' | base64 -d \
  > /tmp/ospw
kubectl create secret generic opensearch-admin-password -n monitoring \
  --from-file=OPENSEARCH_ADMIN_PASSWORD=/tmp/ospw
shred -u /tmp/ospw

helm install opensearch opensearch/opensearch -n logging -f opensearch/values.yaml
kubectl wait --for=condition=ready pod/opensearch-cluster-master-0 -n logging --timeout=180s

# Optional: dashboards UI
helm install opensearch-dashboards opensearch/opensearch-dashboards \
  -n logging -f opensearch/dashboards-values.yaml
```

### 2. MinIO raw landing bucket

```bash
kubectl apply -f minio/create-audit-raw-bucket-job.yaml
kubectl logs -n default job/create-audit-raw-bucket   # confirm success
```

(The old `audit-logs-cold` bucket's provisioning job, Object-Lock'd and
superseded by the above, has been removed from this repo -- see
`ARCHITECTURE.md`'s "Raw landing retention" section for why it existed
and why it's gone.)

### 3. ISM policies (needs step 1 done first)

```bash
kubectl apply -f opensearch/post-install-setup-job.yaml
kubectl logs -n logging job/opensearch-post-install-setup
```

### 4. Fluent Bit (the collector)

```bash
# Copy MinIO credentials into monitoring ns -- fluent-bit's s3 output needs
# them (secrets don't cross namespaces)
kubectl create secret generic minio-credentials -n monitoring \
  --from-literal=awsAccessKeyId="$(kubectl get secret minio-credentials -n default -o jsonpath='{.data.awsAccessKeyId}' | base64 -d)" \
  --from-literal=awsSecretAccessKey="$(kubectl get secret minio-credentials -n default -o jsonpath='{.data.awsSecretAccessKey}' | base64 -d)"

helm upgrade fluent-bit fluent/fluent-bit -n monitoring -f fluent-bit/values.yaml
kubectl rollout status daemonset/fluent-bit -n monitoring
```

This is the FULL values for the release (everything already in production
plus the audit additions) -- it's additive, the existing Loki pipeline is
untouched.

### 5. Trino audit shim + event listener

Trino's `http-event-listener` plugin can only POST over HTTP; because of
the fluent-bit bug (see `fluent-bit/values.yaml`), it can't POST directly
to fluent-bit. It posts to this tiny shim instead, which just prints to
its own stdout:

```bash
kubectl apply -f trino/audit-shim.yaml
kubectl wait --for=condition=ready pod -l app=trino-audit-shim -n monitoring --timeout=60s
```

### 6. Applying the Trino/Ranger/Superset changes

These edit live, already-deployed helm releases. Pull current values,
merge in the audit pieces, then upgrade -- don't overwrite the whole
release with only these files, you'll lose unrelated existing config.

**Trino** (adds `ranger/ranger-trino-audit.xml` to both
`coordinator.additionalConfigFiles` and `worker.additionalConfigFiles`,
and `trino/event-listener.properties` to
`coordinator.additionalConfigFiles` only):

```bash
helm get values trino -n default -o yaml > /tmp/trino-values.yaml
# edit /tmp/trino-values.yaml: paste ranger/ranger-trino-audit.xml over the
# existing coordinator.additionalConfigFiles["ranger-trino-audit.xml"] and
# worker.additionalConfigFiles["ranger-trino-audit.xml"] entries; add
# trino/event-listener.properties as a new
# coordinator.additionalConfigFiles["event-listener.properties"] entry
helm upgrade trino trino/trino -n default -f /tmp/trino-values.yaml

# IMPORTANT: this chart does not checksum config into the pod template, so
# changing ConfigMap-sourced file content does NOT trigger a rollout on its
# own. Force one:
kubectl rollout restart deployment/trino-coordinator deployment/trino-worker -n default
```

**Superset** (adds `superset/event-logger.py` as a new
`configOverrides` entry, e.g. key `audit_event_logger`):

```bash
helm get values superset -n default -o yaml > /tmp/superset-values.yaml
# edit: add configOverrides.audit_event_logger: <contents of event-logger.py>
helm upgrade superset superset/superset -n default -f /tmp/superset-values.yaml
kubectl rollout restart deployment/superset deployment/superset-worker -n default
```

**If Superset's Trino connection has "Impersonate the logged in user"
enabled** (check: Data > Databases > Trino > Edit > Advanced > Security in
the Superset UI, or `d.impersonate_user` on the `Database` row via
`superset shell`) -- it does on this cluster -- Ranger needs to explicitly
authorize whichever principal Superset actually connects as to impersonate
other users, or every impersonated query fails outright:

```bash
RANGER_ADMIN_USER=admin RANGER_ADMIN_PASSWORD=<...> \
IMPERSONATOR=<the base user in Superset's Trino connection string> \
  ./ranger/setup-impersonation.sh
```

See "Does it show the real user, or the shared service account?" below for
why this specific step is needed and how it was verified.

### 7. Spark archival job (cold tier -> Iceberg)

Requires `spark-operator` (already installed on this cluster; if its
release is ever stuck in `pending-install`, see `/root/datahub/CLAUDE.md`
for the recovery steps used the first time).

```bash
kubectl create configmap audit-archive-script -n default \
  --from-file=iceberg_archive_job.py=spark/iceberg_archive_job.py
kubectl apply -f spark/scheduled-spark-application.yaml
kubectl get scheduledsparkapplication -n default
```

Runs hourly (5/10/15 minutes past, one source each, staggered so
fluent-bit's upload buffer has flushed and so the three don't compete for
the node's resources at the same instant).

## Querying the cold archive

Once at least one hourly run has happened (or after a manual one-off run,
see below), the data is plain SQL-queryable from Trino:

```sql
SELECT * FROM iceberg.audit.ranger_audit ORDER BY event_time DESC LIMIT 20;
SELECT * FROM iceberg.audit.trino_query_audit ORDER BY event_time DESC LIMIT 20;
SELECT * FROM iceberg.audit.superset_audit ORDER BY event_time DESC LIMIT 20;

-- promoted columns are queryable directly, no JSON functions needed --
-- see "Query cookbook" below for more, and ARCHITECTURE.md's "Table
-- structure" section for why (raw_json is still there as a forensic
-- catch-all, just not the normal way to query anymore):
SELECT event_time, user_name, query_text
FROM iceberg.audit.trino_query_audit
WHERE event_date = current_date;
```

To trigger a one-off run instead of waiting for the schedule (useful right
after setup, or to backfill), copy one `template:` block out of
`spark/scheduled-spark-application.yaml` into a bare `SparkApplication`:

```bash
kubectl get scheduledsparkapplication audit-archive-ranger -n default -o jsonpath='{.spec.template}' > /tmp/tmpl.json
# wrap /tmp/tmpl.json as {"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"audit-archive-ranger-manual","namespace":"default"},"spec": <paste tmpl.json here>}
kubectl apply -f /tmp/manual-run.json
kubectl get pod audit-archive-ranger-manual-driver -n default -w
```

## Verifying it's working

```bash
OS_PASS=$(kubectl get secret opensearch-admin-password -n logging -o jsonpath='{.data.OPENSEARCH_INITIAL_ADMIN_PASSWORD}' | base64 -d)

# Simulate a Trino event through the real shim:
kubectl run t --restart=Never -n monitoring --image=curlimages/curl:latest --command -- \
  sh -c 'curl -s -X POST http://trino-audit-shim.monitoring.svc.cluster.local:8080/ -d "{\"queryId\":\"verify1\"}"'

# Simulate a Superset event (what event-logger.py prints):
kubectl run s --restart=Never -n monitoring --image=busybox --command -- \
  sh -c 'echo "{\"audit_source\":\"superset\",\"marker\":\"verify2\"}"; sleep 30'

# Wait ~15-20s for the tail input + 5s flush interval, then check OpenSearch:
kubectl run v --restart=Never -n logging --image=curlimages/curl:latest --command -- \
  sh -c "curl -sk -u admin:${OS_PASS} 'https://opensearch-cluster-master:9200/trino_query_audit-*/_search?q=verify1'"
kubectl logs v -n logging

# And check the raw landing file made it to MinIO (upload_timeout is 1m,
# so give it up to ~70s):
AK=$(kubectl get secret minio-credentials -n default -o jsonpath='{.data.awsAccessKeyId}' | base64 -d)
SK=$(kubectl get secret minio-credentials -n default -o jsonpath='{.data.awsSecretAccessKey}' | base64 -d)
kubectl run mc --restart=Never -n default --image=quay.io/minio/mc:latest --command -- \
  sh -c "mc alias set m http://myminio-hl.default.svc.cluster.local:9000 $AK $SK && mc ls -r m/audit-logs-raw/raw/trino/"
```

For Ranger, provoke any Trino query as a Ranger-authenticated user and
check `ranger_audits-*` the same way.

## Does it show the real user, or the shared service account?

**Yes, for both Ranger and Trino query audit** -- verified end-to-end with
real impersonated queries on 2026-09-23, not just synthetic tests. Superset
connects to Trino with one fixed base credential (`admin`), but its Trino
connection has `impersonate_user = True` set, and Trino's Ranger plugin
genuinely enforces per-user impersonation via a real policy check (not a
hardcoded allow/deny) -- confirmed by decompiling the plugin's
`checkCanImpersonateUser`. That policy (Ranger's default `all - trinouser`
policy, resource type `trinouser`, access type `impersonate`) only granted
`ranger` and `{USER}` (self-impersonation) by default; `superset` needed to
be added, which required first registering `superset` as a Ranger user
(Ranger has no user until you've done that explicitly). Once granted,
`SELECT current_user` through a Superset-impersonated session correctly
returns the real end user, and that same real user shows up correctly in
both `ranger_audits-*` (`reqUser`) and `trino_query_audit-*`
(`context.user`) in OpenSearch, with real per-column authorization detail.

If you rotate/replace Superset's Trino connection or its base credential,
re-check that the new principal has this same `impersonate` grant, or
audit trails will silently revert to showing the shared account for every
dashboard-driven query.

## Retention

- **Hot (OpenSearch)**: 7 days per index, then deleted. Change
  `min_index_age` in `ism-policies/*.json` to adjust.
- **Raw landing (MinIO, `audit-logs-raw`)**: 30 days, but only once a file
  is *also* confirmed merged into Iceberg in that same job run -- deletion
  is app-level (`spark/iceberg_archive_job.py`), not a bucket lifecycle
  rule, and there's deliberately no Object Lock on this bucket (see
  `ARCHITECTURE.md`'s "Raw landing retention" section for why an earlier
  Object-Lock'd bucket, `audit-logs-cold`, had to be replaced rather than
  reconfigured -- its files are legacy, draining on their own 365-day
  locks, untouched by anything now). Change `MIN_AGE_SECONDS_BEFORE_DELETE`
  in `iceberg_archive_job.py` to adjust the 30-day floor.
- **Cold, queryable (Iceberg)**: data itself is never expired by anything
  in this pipeline -- add a retention job of your own if it needs to age
  out eventually. File-level compaction (not data expiration) runs daily
  via `audit-iceberg-maintenance` -- see "Cold tier maintenance" below.

## Cold tier maintenance

A daily `ScheduledSparkApplication` (`audit-iceberg-maintenance`, 02:30)
compacts each table's small files (`CALL iceberg.system.
rewrite_data_files(...)`) -- this pipeline's hourly, low-volume writes
naturally produce a lot of small files otherwise (see `CAPACITY.md`'s
file-count-overhead finding).

**Orphan-file cleanup is NOT automated** -- run manually, per table, when
storage actually warrants it (this cluster's current data volumes don't):
```sql
ALTER TABLE iceberg.audit.ranger_audit        EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE iceberg.audit.trino_query_audit   EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE iceberg.audit.superset_audit      EXECUTE remove_orphan_files(retention_threshold => '7d');
```
Must be run via **Trino**, not Spark -- Spark's Iceberg integration
blocks this (a Nessie GC safety guard, also blocks `expire_snapshots`
entirely, everywhere); Trino's connector doesn't enforce that guard for
this specific procedure. See `ARCHITECTURE.md`'s "Cold tier maintenance"
section and `spark/iceberg_maintenance_job.py`'s module docstring for
the full reasoning -- this is a deliberate scope decision, not a TODO.

## Query cookbook

Common forensic questions, using the promoted columns from the 2026-10-01
table redesign (see `ARCHITECTURE.md`'s "Table structure" section) --
plain SQL via Superset SQL Lab or any Trino client, database `Trino`:

**Which queries did user A run on a given date?**
```sql
SELECT event_time, query_text, query_state
FROM iceberg.audit.trino_query_audit
WHERE event_date = DATE '2026-10-01' AND user_name = 'alice'
ORDER BY event_time;
```

**What queries ran on a given date (all users)?**
```sql
SELECT event_time, user_name, query_text
FROM iceberg.audit.trino_query_audit
WHERE event_date = DATE '2026-10-01'
ORDER BY event_time;
```

**When was a specific table queried?** (`tables` is a native array
column -- plain `UNNEST`, no JSON functions needed)
```sql
SELECT a.event_time, a.user_name, t.catalog, t.schema, t."table"
FROM iceberg.audit.trino_query_audit a
CROSS JOIN UNNEST(a.tables) AS t(catalog, schema, "table")
WHERE t."table" = 'nation' AND t.schema = 'tiny';
```
(`table` is a reserved word in Trino SQL -- always quote it, both as a
column name and in the `UNNEST` alias list, as above.)

**Was access to a table granted or denied?** (Ranger's own decision,
not just "was it queried")
```sql
SELECT event_time, req_user, access_type, resource, result  -- 1 = allowed, 0 = denied
FROM iceberg.audit.ranger_audit
WHERE resource = 'tpch/tiny/nation'
ORDER BY event_time DESC;
```

## Known gaps / follow-ups

- OpenSearch uses the image's auto-generated demo TLS certs (self-signed,
  cluster-internal only). Fine for this single-node, internal-only setup;
  replace before ever exposing OpenSearch outside the cluster.
- The `opensearch-admin-password` and `minio-credentials` secrets are each
  duplicated across namespaces (Kubernetes secrets aren't cross-namespace).
  If you rotate either, update every copy.
- The Spark archival job re-reads each source's *entire* raw prefix on
  every run (see `spark/iceberg_archive_job.py`'s docstring) -- fine at
  this log volume, but if raw data grows enough that this gets slow,
  switch to a real watermark (track last-processed file/timestamp) instead
  of full reprocessing.
- Nessie's `versionStoreType` is `IN_MEMORY` (pre-existing, unrelated to
  this work) -- all catalog metadata, including these new Iceberg tables'
  registration, is lost if the Nessie pod restarts. The underlying Parquet
  data would still be in MinIO but would need to be re-registered. Not
  something this work fixes; flagging because it directly affects the
  durability of the cold archive this pipeline builds.
- See `/root/datahub/CLAUDE.md` for cluster-level operational gotchas
  (disk space, node IP, the fluent-bit bug in more detail, image registry
  quirks, spark-operator recovery) uncovered while building this.
- `install-guide/values/trino-values.yaml` and `superset-values.yaml` are
  now stale against the live cluster (confirmed via `helm get values`,
  2026-10-02) -- live Trino has a fix (the
  `xasecure.audit.log4j.async.max.flush.interval.ms` property from "Bugs
  found and fixed during rollout" below) that the tracked file is missing
  entirely, and live Superset's LDAP group mapping has moved on from what
  the tracked file shows. Refreshing them requires dumping real
  credentials (LDAP bind password, Trino's shared secret, bcrypt hashes,
  MinIO keys, Superset's secret key, Postgres passwords) to do the diff,
  which is deliberately not something this pass did automatically -- see
  `install-guide/download-charts.sh`'s `dump_values_unredacted` path
  (writes to a gitignored `values-live-unredacted/`, never over the
  tracked files) and redact by hand per "Redacted secrets" in
  `install-guide/README.md` before replacing the tracked copies.
