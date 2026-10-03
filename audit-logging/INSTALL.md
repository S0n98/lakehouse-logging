# Install Guide: Audit Logging Pipeline (step by step)

This is the detailed, run-it-top-to-bottom version of `README.md`'s
"Install order" section -- every command, every secret it creates (name,
namespace, exact keys, and why), and a verification step after each
stage. If you just want the command blocks without the explanation,
`README.md`'s "Install order" is the condensed version of this same
sequence.

**Scope: this pipeline only.** It assumes Trino, Superset, Ranger,
Nessie, the MinIO tenant, and spark-operator are **already installed and
running** -- this repo was built on top of a pre-existing lakehouse
platform, not from zero. Nothing here walks through installing those
(there's no "from scratch" doc for them in this repo, since they
pre-existed this work). If any of those five aren't already up, stop
here and get them running first; everything below assumes they are.

For an **offline/air-gapped install**, use `install-guide/` instead (pulls
chart archives + values into local files) -- this doc assumes normal
internet access to Helm chart repos and container registries.

## Before you start

Check these five are actually running -- if any aren't, this pipeline has
nothing to attach to:

```bash
kubectl get deploy trino-coordinator trino-worker -n default
kubectl get deploy superset superset-worker -n default
kubectl get deploy ranger-apache-ranger -n ranger
kubectl get deploy nessie -n default
kubectl get pods -n default -l v1.min.io/tenant=myminio
kubectl get deploy spark-operator-controller -n spark-operator
```

Also confirm the `default` namespace already has a `minio-credentials`
secret (created by the pre-existing MinIO tenant setup, not by this
pipeline) -- step 2, step 4, and step 7 below all read from it:

```bash
kubectl get secret minio-credentials -n default -o jsonpath='{.data}' | python3 -c "import json,sys; print(list(json.load(sys.stdin).keys()))"
# expect: ['awsAccessKeyId', 'awsSecretAccessKey']
```

Namespaces this install touches: `logging` (new -- created in step 1),
`monitoring` (existing, already has Loki + fluent-bit), `default`
(existing), `ranger` (existing).

Run everything below from `audit-logging/` (paths in the commands are
relative to that directory).

---

## Step 1: OpenSearch (hot tier)

**Secrets created in this step:**

| Secret | Namespace | Key | Why |
|---|---|---|---|
| `opensearch-admin-password` | `logging` | `OPENSEARCH_INITIAL_ADMIN_PASSWORD` | OpenSearch >=2.12 requires this at startup (no more default `admin`/`admin`) -- the chart reads this exact key name. |
| `opensearch-admin-password` | `monitoring` | `OPENSEARCH_ADMIN_PASSWORD` | A **copy** of the same password for Fluent Bit (step 4) to authenticate to OpenSearch. **Different key name than the `logging` copy** -- that's not a typo, the two consumers (OpenSearch chart vs. fluent-bit's values.yaml) each expect their own key name; same secret, same password, two different key names across the two namespace copies. |

Kubernetes secrets don't cross namespaces, which is why this is a *copy*,
not a shared reference -- same pattern repeats in steps 2/4/7 for
`minio-credentials`.

```bash
helm repo add opensearch https://opensearch-project.github.io/helm-charts
helm repo update opensearch
kubectl create namespace logging

# Strong admin password, generated and stored directly -- never typed,
# echoed, or saved to a file outside this one-shot pipe
kubectl create secret generic opensearch-admin-password -n logging \
  --from-literal=OPENSEARCH_INITIAL_ADMIN_PASSWORD="$(openssl rand -base64 24 | tr -d '=+/' | cut -c1-20)Aa1!"

# Copy into monitoring ns under fluent-bit's expected key name
kubectl get secret opensearch-admin-password -n logging \
  -o jsonpath='{.data.OPENSEARCH_INITIAL_ADMIN_PASSWORD}' | base64 -d \
  > /tmp/ospw
kubectl create secret generic opensearch-admin-password -n monitoring \
  --from-file=OPENSEARCH_ADMIN_PASSWORD=/tmp/ospw
shred -u /tmp/ospw

helm install opensearch opensearch/opensearch -n logging -f opensearch/values.yaml
kubectl wait --for=condition=ready pod/opensearch-cluster-master-0 -n logging --timeout=180s

# Optional: Dashboards UI (browse the hot tier) -- no secrets of its own,
# reuses the same OpenSearch admin password at login time
helm install opensearch-dashboards opensearch/opensearch-dashboards \
  -n logging -f opensearch/dashboards-values.yaml
```

**Verify:**
```bash
kubectl get pod opensearch-cluster-master-0 -n logging
# expect: 1/1 Running
kubectl get secret opensearch-admin-password -n logging -n monitoring
# expect: both exist (run the two `kubectl get secret` commands separately,
# one per namespace, if your kubectl version doesn't accept -n twice)
```

---

## Step 2: MinIO raw landing bucket

**No new secret created** -- `create-audit-raw-bucket-job.yaml` reads the
pre-existing `minio-credentials` secret directly from `default` (same
namespace the job runs in, no copy needed here).

```bash
kubectl apply -f minio/create-audit-raw-bucket-job.yaml
kubectl logs -n default job/create-audit-raw-bucket   # confirm success
```

Creates the `audit-logs-raw` bucket, **deliberately without Object
Lock** -- see `ARCHITECTURE.md`'s "Raw landing retention" if you're
wondering why that matters (short version: Object Lock can't be removed
later, and this pipeline needs to delete files after 30 days).

**Verify:**
```bash
kubectl logs -n default job/create-audit-raw-bucket | tail -5
# expect a line confirming the bucket exists/was created, no errors
```

---

## Step 3: ISM policies (hot-tier retention)

**No new secret created** -- reads the `opensearch-admin-password` secret
already created in `logging` (step 1), same namespace this job runs in.

```bash
kubectl apply -f opensearch/post-install-setup-job.yaml
kubectl logs -n logging job/opensearch-post-install-setup
```

Registers the 7-day hot-tier delete policies (`ism-policies/*.json`) for
all three sources. Requires step 1's OpenSearch pod to already be
`Running` -- this is a hard dependency, not just an ordering suggestion.

**Verify:**
```bash
kubectl logs -n logging job/opensearch-post-install-setup | tail -10
# expect each policy creation to report success, no errors
```

---

## Step 4: Fluent Bit (the collector)

**Secret created in this step:**

| Secret | Namespace | Keys | Why |
|---|---|---|---|
| `minio-credentials` | `monitoring` | `awsAccessKeyId`, `awsSecretAccessKey` | A **copy** of the pre-existing `default`-namespace secret of the same name -- fluent-bit's `s3` output (writes raw JSON to the `audit-logs-raw` bucket) needs these in its own namespace. |

```bash
kubectl create secret generic minio-credentials -n monitoring \
  --from-literal=awsAccessKeyId="$(kubectl get secret minio-credentials -n default -o jsonpath='{.data.awsAccessKeyId}' | base64 -d)" \
  --from-literal=awsSecretAccessKey="$(kubectl get secret minio-credentials -n default -o jsonpath='{.data.awsSecretAccessKey}' | base64 -d)"

helm upgrade fluent-bit fluent/fluent-bit -n monitoring -f fluent-bit/values.yaml
kubectl rollout status daemonset/fluent-bit -n monitoring
```

`fluent-bit/values.yaml` is the **FULL** live config for this release
(everything already in production plus the audit additions) -- this is
additive, the existing Loki pipeline is untouched. Read the header
comment in that file before touching it: there's a real fluent-bit
version bug (`http`-type inputs silently drop records) the design works
around.

**Verify:**
```bash
kubectl rollout status daemonset/fluent-bit -n monitoring
# expect: daemon set rolled out, all pods ready
```

---

## Step 5: Trino audit shim

**No secret needed** -- this is a stateless HTTP-to-stdout relay (Trino's
`http-event-listener` plugin can only POST over HTTP, and the fluent-bit
bug above means it can't POST directly to fluent-bit; this shim is the
workaround, see `README.md`'s step 5 for the full why).

```bash
kubectl apply -f trino/audit-shim.yaml
kubectl wait --for=condition=ready pod -l app=trino-audit-shim -n monitoring --timeout=60s
```

**Verify:**
```bash
kubectl get pod -n monitoring -l app=trino-audit-shim
# expect: 1/1 Running
```

---

## Step 6: Applying the Trino/Ranger/Superset changes

**No new secrets created** -- this step edits **live, already-deployed**
Helm releases that already carry their own real secrets (LDAP bind
password, Trino's internal shared secret, Postgres passwords, etc.).
**The entire risk in this step is accidentally dropping or overwriting
one of those while editing** -- always pull current values first and
merge in only the audit pieces, never start from a blank/template values
file.

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
shred -u /tmp/trino-values.yaml   # it contains real live secrets, don't leave it lying around

# This chart does NOT checksum config into the pod template, so changing
# ConfigMap-sourced file content does NOT trigger a rollout on its own --
# force one explicitly:
kubectl rollout restart deployment/trino-coordinator deployment/trino-worker -n default
```

**Superset** (adds `superset/event-logger.py` as a new
`configOverrides` entry, e.g. key `audit_event_logger`):

```bash
helm get values superset -n default -o yaml > /tmp/superset-values.yaml
# edit: add configOverrides.audit_event_logger: <contents of event-logger.py>
helm upgrade superset superset/superset -n default -f /tmp/superset-values.yaml
shred -u /tmp/superset-values.yaml   # same reason as above
kubectl rollout restart deployment/superset deployment/superset-worker -n default
```

**If Superset's Trino connection has "Impersonate the logged in user"
enabled** (check: Data > Databases > Trino > Edit > Advanced > Security
in the Superset UI, or `d.impersonate_user` on the `Database` row via
`superset shell`) -- Ranger needs to explicitly authorize whichever
principal Superset actually connects as to impersonate other users, or
every impersonated query fails outright:

```bash
RANGER_ADMIN_USER=admin RANGER_ADMIN_PASSWORD="$(read -rsp 'Ranger admin password: ' p && echo "$p")" \
IMPERSONATOR=<the base user in Superset's Trino connection string> \
  ./ranger/setup-impersonation.sh
```

(Using `read -rsp` instead of typing the password as a literal argument
keeps it out of shell history -- the script itself still passes it to
`curl` on the command line, which is visible to anything that can read
this host's process list for the few seconds the script runs; acceptable
on a single-user cluster, worth hardening with a mounted secret file
instead of an env var if this ever runs somewhere less trusted.)

See `README.md`'s "Does it show the real user, or the shared service
account?" section for why this specific step is needed and how it was
verified.

**Verify:**
```bash
kubectl rollout status deployment/trino-coordinator deployment/trino-worker -n default
kubectl rollout status deployment/superset deployment/superset-worker -n default
# both: expect "successfully rolled out"
```

---

## Step 7: Spark archival + maintenance jobs (cold tier -> Iceberg)

**No new secret created** -- both jobs read `AWS_ACCESS_KEY_ID`/
`AWS_SECRET_ACCESS_KEY` from the pre-existing `minio-credentials` secret
in `default` (same namespace the Spark driver/executor pods run in, no
copy needed -- see `spark/scheduled-spark-application.yaml`'s `env:`
blocks).

Requires `spark-operator` already installed (per "Before you start"
above; if its release is ever stuck in `pending-install`, see
`/root/datahub/CLAUDE.md` for the recovery steps used the first time).

**Two ConfigMaps needed, not one** -- easy to miss since there are four
`ScheduledSparkApplication` resources in the one YAML file (one hourly
per source, plus one daily maintenance job), but only two scripts back
them:

```bash
kubectl create configmap audit-archive-script -n default \
  --from-file=iceberg_archive_job.py=spark/iceberg_archive_job.py
kubectl create configmap audit-maintenance-script -n default \
  --from-file=iceberg_maintenance_job.py=spark/iceberg_maintenance_job.py
kubectl apply -f spark/scheduled-spark-application.yaml
kubectl get scheduledsparkapplication -n default
```

The three archive jobs run hourly (5/10/15 minutes past, one source
each, staggered so fluent-bit's upload buffer has flushed and so they
don't compete for the node's resources at the same instant). The
maintenance job runs daily (`30 2 * * *`), compacting small Iceberg data
files -- see `ARCHITECTURE.md`'s "Cold tier maintenance" section for why
orphan-file cleanup is deliberately *not* automated alongside it.

**Verify:**
```bash
kubectl get scheduledsparkapplication -n default
# expect 4 resources, each scheduleState: Scheduled, nextRun a sensible
# near-future time (not stale -- see CLAUDE.md's ScheduledSparkApplication
# gotchas if nextRun ever looks frozen days in the past)
```

---

## Final end-to-end verification

Once all 7 steps are done, confirm real events actually flow through
both tiers -- `README.md`'s "Verifying it's working" section has the
full simulate-an-event-and-check commands for both OpenSearch (hot) and,
after the first hourly Spark run, Iceberg (cold). Don't consider the
install complete until at least one real or simulated event from each of
the three sources (Ranger, Trino, Superset) has been confirmed in
OpenSearch; the cold tier can be verified up to an hour later once the
first scheduled Spark run has fired.

---

## Secrets reference (everything created by this install, in one table)

| Secret | Namespace | Key(s) | Created in | Consumed by |
|---|---|---|---|---|
| `opensearch-admin-password` | `logging` | `OPENSEARCH_INITIAL_ADMIN_PASSWORD` | Step 1 | OpenSearch chart, ISM policy job (step 3), Dashboards login |
| `opensearch-admin-password` | `monitoring` | `OPENSEARCH_ADMIN_PASSWORD` | Step 1 | Fluent Bit (step 4) |
| `minio-credentials` | `monitoring` | `awsAccessKeyId`, `awsSecretAccessKey` | Step 4 | Fluent Bit's `s3` output |

Everything else this pipeline's jobs read (`minio-credentials` in
`default` for steps 2/7; Trino's/Superset's own internal secrets touched
in step 6) is **pre-existing**, created by the platform this pipeline
sits on top of, not by this install.
