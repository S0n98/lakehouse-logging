# Testing this pipeline

## Philosophy

Every test here follows the same rule established during the original
rollout (see `ARCHITECTURE.md`'s closing note): **verification means
generating a real, successful query and checking whether it actually shows
up end-to-end -- not re-reading config and assuming it's fine.** Every one
of the three bugs documented in `ARCHITECTURE.md`'s "Bugs found and fixed
during rollout" section passed a synthetic/config-level check and only
showed up once a real query was run and chased through the pipeline. The
stuck-Spark-job bug in this doc's example report below was found the same
way -- the driver pod said `Completed` and the schedule object said
`Scheduled`, both of which look healthy at a glance; only actually
querying the Iceberg table's row count revealed it had been frozen for two
days.

Don't skip straight to "looks fine" from a `kubectl get` -- follow every
test below through to an actual data check.

## Prerequisites

- `kubectl` access to the cluster.
- The Ranger admin password (see `helm get values ranger -n ranger`, or
  whatever secret store your org uses -- do not hardcode it in scripts).
- Enough disk headroom to not be fighting disk pressure while testing (see
  `../CLAUDE.md`'s disk-pressure section) -- `df -h /` first.

## Test suite

### 1. Cluster / infra health

```bash
kubectl get nodes -o json | python3 -c "import json,sys; d=json.load(sys.stdin); [print(n['metadata']['name'], n['spec'].get('taints',[])) for n in d['items']]"
df -h /
kubectl get pods -n default   | grep -E "trino|superset|myminio|nessie"
kubectl get pods -n monitoring | grep -E "fluent-bit|loki"
kubectl get pods -n logging
kubectl get pods -n ranger
kubectl get pods -n spark-operator
```

**Pass:** no `disk-pressure` taint on any node, all listed pods
`Running`/`Ready`. A handful of `spark-operator-controller` pods in
`ContainerStatusUnknown`/`Completed` alongside one `Running` one is normal
churn, not a failure (see gotcha in Test 4 below) -- what matters is
exactly one is `Running`.

### 2. Hot tier -- Ranger audit (real query, not synthetic)

Run a real query against a Ranger-protected table as an impersonated
user, then confirm it lands in `ranger_audits-*` correctly attributed.
Easiest way to generate the query is through Superset's already-configured
Trino connection with a fake `g.user` (this is the same technique used to
originally verify impersonation -- see `ARCHITECTURE.md`):

```bash
POD=$(kubectl get pods -n default -l app.kubernetes.io/name=superset,app.kubernetes.io/component=web -o jsonpath='{.items[0].metadata.name}')

cat > /tmp/test_query.py << 'EOF'
from superset import db
from superset.models.core import Database
from flask import g
import time

database = db.session.query(Database).filter_by(id=1).first()
class FakeUser:
    username = "pipeline_test_" + str(int(time.time()))
g.user = FakeUser()
print("TEST_USERNAME:", FakeUser.username)

with database.get_sqla_engine() as engine:
    with engine.connect() as conn:
        result = conn.execute("SELECT count(*) as cnt FROM tpch.tiny.nation")
        print("QUERY_RESULT:", dict(result.fetchone()._mapping))
EOF
kubectl cp /tmp/test_query.py default/$POD:/tmp/test_query.py
echo "exec(open('/tmp/test_query.py').read())" | kubectl exec -i -n default $POD -- superset shell 2>&1 | grep -A2 "TEST_USERNAME\|QUERY_RESULT"
```

Note the printed `TEST_USERNAME`, wait ~15s for Fluent Bit to pick up the
log line, then check OpenSearch:

```bash
OS_PASS=$(kubectl get secret opensearch-admin-password -n logging -o jsonpath='{.data.OPENSEARCH_INITIAL_ADMIN_PASSWORD}' | base64 -d)
kubectl exec -n logging opensearch-cluster-master-0 -c opensearch -- \
  curl -sk -u "admin:${OS_PASS}" \
  "https://localhost:9200/ranger_audits-*/_search?q=reqUser:<TEST_USERNAME>&pretty"
```

**Pass:** at least one hit, with `reqUser` equal to the test username and
`access`/`resource` matching the query (e.g. `SelectFromColumns` on
`tpch/tiny/nation`).

### 3. Hot tier -- Trino query audit

Same test query as above also produces a `trino_query_audit-*` document:

```bash
kubectl exec -n logging opensearch-cluster-master-0 -c opensearch -- \
  curl -sk -u "admin:${OS_PASS}" \
  "https://localhost:9200/trino_query_audit-*/_search?q=context.user:<TEST_USERNAME>&pretty"
```

**Pass:** at least one hit, `context.user` equal to the test username
(this is the field that was silently broken by the nested-`queryId` bug --
see `ARCHITECTURE.md` -- so this test alone would have caught it).

### 4. Cold tier -- scheduled Spark archival jobs

```bash
kubectl get scheduledsparkapplication -n default
kubectl get sparkapplication -n default -o custom-columns=NAME:.metadata.name,STATE:.status.applicationState.state,SUBMIT:.status.lastSubmissionAttemptTime --sort-by=.status.lastSubmissionAttemptTime
```

**Pass:** each of the three schedules (`audit-archive-ranger/trino/
superset`) has a recent `lastRun` (within the last ~2 schedule intervals)
and its most recent `SparkApplication` is `COMPLETED`, not stuck.

**Known gotcha, found 2026-09-26:** a `SparkApplication`'s driver pod can
show `Completed` (and its logs can show it genuinely succeeded, e.g.
`wrote N records to iceberg...exitCode 0`) while the `SparkApplication`
object itself stays wedged in `PENDING_RERUN` forever -- this happens if
the spark-operator controller pod crashes/restarts at exactly the moment
it would have finalized that object's status. A wedged run silently blocks
the `ScheduledSparkApplication` from ever submitting a new one, with no
error anywhere -- `kubectl get pods` and `kubectl get scheduledsparkapplication`
both look completely normal. **This is why step 6 (below) matters more
than checking pod status.** See `../CLAUDE.md`'s spark-operator section
for the fix (delete the wedged `SparkApplication`, then force a reconcile
by annotating the `ScheduledSparkApplication`).

### 5. Ranger impersonation policy still granted

```bash
RANGER_PASS=$(helm get values ranger -n ranger -o yaml | grep adminPassword | awk '{print $2}')
kubectl exec -n ranger deploy/ranger-apache-ranger -- \
  curl -sk -u "admin:${RANGER_PASS}" \
  "http://localhost:6080/service/public/v2/api/service/trino/policy/all%20-%20trinouser" \
  | python3 -c "
import json,sys
d = json.load(sys.stdin)
for item in d.get('policyItems', []):
    if any(a['type']=='impersonate' and a['isAllowed'] for a in item.get('accesses',[])):
        print('impersonate users:', item.get('users'))
"
```

**Pass:** the principal Superset (or whichever service account) uses is
in the printed list, alongside the defaults `ranger` and `{USER}`.

### 6. Cold tier -- Iceberg is actually queryable and growing

This is the test that catches Test 4's gotcha -- a healthy-looking
schedule with a frozen table is only visible here:

```bash
kubectl exec -n default deploy/trino-coordinator -- trino --execute "
  SELECT 'ranger' AS src, count(*) FROM iceberg.audit.ranger_audit
  UNION ALL SELECT 'trino', count(*) FROM iceberg.audit.trino_query_audit
  UNION ALL SELECT 'superset', count(*) FROM iceberg.audit.superset_audit
"
```

**Pass:** re-run this an hour apart (or across your schedule interval) and
confirm every count increased. A count that hasn't moved despite the
schedule "running" is exactly Test 4's gotcha.

### 7. install-guide integrity

```bash
for f in ../audit-logging/install-guide/charts/*.tgz; do
  tar -tzf "$f" > /dev/null 2>&1 && echo "OK: $f" || echo "CORRUPT: $f"
done
grep -L "REDACTED" ../audit-logging/install-guide/values/*.yaml
```

**Pass:** every chart prints `OK`; the `grep -L` (files that do *not*
contain `REDACTED`) prints nothing for `trino-values.yaml` and
`superset-values.yaml` specifically (the two files with real secrets to
redact -- the others legitimately have none).

## Example test report

A real run of this suite, 2026-09-26, included for reference on what a
report from this suite should look like -- pass/fail per test, plus a bug
that was actually found and fixed rather than glossed over:

| # | Test | Result |
|---|---|---|
| 1 | Cluster health | ✅ PASS -- no taints, all core pods `Running` |
| 2 | Hot tier -- Ranger audit | ✅ PASS -- real impersonated query showed up with correct `reqUser`, `access: SelectFromColumns`, `resource: tpch/tiny/nation` |
| 3 | Hot tier -- Trino query audit | ✅ PASS -- correct `context.user` |
| 4 | Cold tier -- schedule status | ❌ FAIL (see below) |
| 5 | Ranger impersonation policy | ✅ PASS -- `["ranger", "{USER}", "superset"]` |
| 6 | Cold tier -- Iceberg queryable/growing | ⚠️ Ranger/superset tables growing normally (30, 25 rows); **Trino table frozen at 18 rows since 2026-09-24** |
| 7 | install-guide integrity | ✅ PASS -- all 6 charts valid, redaction markers present |

**Bug found:** `audit-archive-trino`'s last successful run had actually
completed successfully two days earlier (driver log: `wrote 18 records...
exitCode 0`), but a spark-operator controller crash right after left the
`SparkApplication` object wedged in `PENDING_RERUN`, silently blocking new
runs the whole time. Neither `kubectl get pods` nor
`kubectl get scheduledsparkapplication` showed anything wrong -- only Test
6's actual row-count check surfaced it.

**Fix applied:**
```bash
kubectl delete sparkapplication audit-archive-trino-<stuck-run-name> -n default
kubectl annotate scheduledsparkapplication audit-archive-trino -n default force-reconcile="$(date +%s)" --overwrite
```
New run submitted within 15 seconds, completed successfully, table grew
from 18 to 32 rows (picking up the ~2 days of backlogged raw files in one
pass, confirming no data was actually lost -- just delayed). Re-ran Test 4
and 6 afterward to confirm the fix: both passed.
