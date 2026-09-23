# Audit Logging Pipeline

Hot (searchable) + cold (queryable Iceberg archive) audit logging for
Ranger, Trino, and Superset on this cluster. Hot tier is OpenSearch; cold
tier is real Iceberg tables in the existing `lakehouse` warehouse, loaded
by a periodic Spark job -- queryable from Trino with plain SQL, not just
restorable like an OpenSearch snapshot. See `ARCHITECTURE.md` for the full
design and the reasoning behind each choice; this file is the
install/operate guide.

**Status as of 2026-09-23: fully deployed and verified end-to-end on this
cluster** -- real events (not just synthetic markers) for all three
sources confirmed flowing all the way through: OpenSearch indices, MinIO
raw landing files, and Iceberg tables in the `lakehouse` warehouse (schema,
Parquet data files, and Nessie catalog registration all inspected
directly). One thing is *not* fully verified: the exact shape of Ranger's
real audit-log lines in production (only tested against a synthetic
marker, since getting a real query through Trino's auth wasn't done during
this rollout) -- see "Verifying the Ranger audit filter" below.

## What's here

```
opensearch/    Hot tier: OpenSearch + the custom image with repository-s3
               (kept for possible future use), ISM policy setup job.
fluent-bit/    The collector: routes Ranger/Trino/Superset audit lines to
               both OpenSearch (hot) and MinIO raw landing (cold source).
               Read the header comment in values.yaml before touching this
               file -- there's a real fluent-bit bug its design works around.
minio/         The audit-logs-cold bucket (Object Lock, ILM) -- raw JSON
               landing zone, the input to the Spark->Iceberg load.
spark/         The periodic Spark job that loads raw JSON into Iceberg
               tables under the `lakehouse` warehouse. This is the actual
               cold, queryable archive.
ism-policies/  Per-source hot(30d)->delete lifecycle policies for OpenSearch.
ranger/        Fixes Ranger's audit destination (was silently broken).
trino/         Query-audit event listener + the shim it needs (see below).
superset/      Action-audit event logger (pastes into superset_config.py).
```

## Architecture in one paragraph

Every audit source ends up as a JSON line on some pod's stdout, which
Fluent Bit's `tail` input already reads (it's the same input already
shipping every pod's logs to Loki). A `rewrite_tag` filter per source
picks these lines back out by a field unique to that source's JSON shape,
and re-emits them to **two** outputs: an `opensearch` output (hot,
searchable, 30-day retention) and an `s3` output that lands the same raw
JSON in MinIO, partitioned by hour. Once an hour, a
`ScheduledSparkApplication` per source reads that source's raw JSON and
loads it into a real Iceberg table under the cluster's existing
`lakehouse` warehouse (same Nessie catalog Trino's `iceberg` catalog
already uses) -- queryable forever with plain SQL, independent of
whatever OpenSearch's hot-tier retention does. Full reasoning in
`ARCHITECTURE.md`.

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

# Build the custom image (repository-s3 plugin -- not required by the
# current design since cold storage no longer uses OpenSearch snapshots,
# kept in case that changes)
cd opensearch/
./build-and-import-image.sh   # requires the local Docker daemon

helm install opensearch opensearch/opensearch -n logging -f values.yaml
kubectl wait --for=condition=ready pod/opensearch-cluster-master-0 -n logging --timeout=180s

# Optional: dashboards UI
helm install opensearch-dashboards opensearch/opensearch-dashboards \
  -n logging -f dashboards-values.yaml
```

### 2. MinIO raw landing bucket

```bash
kubectl apply -f minio/create-audit-bucket-job.yaml
kubectl logs -n default job/create-audit-bucket   # confirm success
```

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

-- full audit payload is in raw_json; pull specific fields out with Trino's
-- JSON functions, e.g.:
SELECT event_time, json_extract_scalar(raw_json, '$.user') AS user
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
  sh -c "mc alias set m http://myminio-hl.default.svc.cluster.local:9000 $AK $SK && mc ls -r m/audit-logs-cold/raw/trino/"
```

For Ranger, provoke any Trino query as a Ranger-authenticated user and
check `ranger_audits-*` the same way.

## Verifying the Ranger audit filter

The `rewrite_tag` rule that picks Ranger's audit lines out of Trino's
stdout (`fluent-bit/values.yaml`, matches on a `repoType` field) was tested
against a *synthetic* JSON line, not Ranger's real log4j output -- I did
not get a real query through Trino's auth during this rollout. Ranger's
audit schema has used `repoType` as a stable top-level field for years, so
this is a reasonable default, but before relying on it:

```bash
kubectl logs -l app.kubernetes.io/component=coordinator -n default | grep -i xaaudit
```

Look at an actual audit line's shape. If Ranger's log4j appender prefixes
the JSON with a timestamp/log-level (common log4j behavior), `Merge_Log`
in the `kubernetes` filter will fail to parse it as JSON, and `repoType`
won't exist as a top-level field (Merge_Log only merges JSON that occupies
the *entire* log line, and drops the raw `log` field either way per
`Keep_Log Off`). If that's the case, either:
- adjust `xasecure.audit.log4j` settings / the underlying logger's pattern
  to emit bare JSON with no prefix, or
- change the rewrite_tag rule to match against whatever raw text pattern
  the real line actually has, keeping `Keep_Log On` in the kubernetes
  filter so the raw `$log` field survives for regex matching.

## Retention

- **Hot (OpenSearch)**: 30 days per index, then deleted. Change
  `min_index_age` in `ism-policies/*.json` to adjust.
- **Raw landing (MinIO)**: Object Lock GOVERNANCE mode, 365-day default
  retention; lifecycle rule expires objects after 2555 days (~7 years).
  Adjust in `minio/create-audit-bucket-job.yaml` to match your actual
  compliance requirement -- these are placeholder defaults, not derived
  from any stated policy.
- **Cold, queryable (Iceberg)**: never expired by anything in this
  pipeline. Add a retention job of your own (e.g. an Iceberg `expire
  snapshots` / delete-old-partitions call) if data needs to age out of the
  queryable archive eventually.

## Known gaps / follow-ups

- Ranger audit filter needs verification against real output (above).
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
