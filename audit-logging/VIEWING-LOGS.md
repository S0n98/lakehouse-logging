# Viewing audit logs in a UI

Two tiers, two different UIs -- neither one shows both. Hot tier lives in
OpenSearch (browse with OpenSearch Dashboards); cold tier lives in Iceberg
tables (browse with Superset SQL Lab, since Iceberg has no browsing UI of
its own).

## Before you start: hostnames

This cluster's ingress (`nginx`) routes by hostname on the node's LAN IP
(`192.168.86.100`), not by path -- so your **browser's** machine needs
these names resolvable, typically by adding them to your local
`/etc/hosts` (or `C:\Windows\System32\drivers\etc\hosts` on Windows)
pointing at that IP:

```
192.168.86.100  superset.local
192.168.86.100  ranger.local
192.168.86.100  audit-logs.local
```

(This is separate from, and doesn't conflict with, the
`192.168.2.100 trino.local` entry already on the cluster node itself --
that one is for pod-to-pod traffic on this specific host, not for your
browser. See `../CLAUDE.md`'s "Node IP" section if that distinction
matters for what you're doing.)

## Hot tier: OpenSearch Dashboards

**Status on this cluster: not currently installed** -- it's an optional
step in `README.md` (`helm install opensearch-dashboards ...`) that
hasn't been run here. Everything below assumes you've run that step
(ingress host `audit-logs.local`, matching `opensearch/dashboards-values.yaml`).
If you haven't installed it yet and just need a quick answer right now,
skip to "No Dashboards? Query OpenSearch directly" below.

### One-time setup: index patterns

Each audit source writes to a **daily** index (`ranger_audits-2026.09.26`,
etc. -- see `Logstash_DateFormat` in `fluent-bit/values.yaml`), so use a
wildcard pattern to see all of a source's history:

1. Open `http://audit-logs.local`, log in (default `admin` / the same
   password as the OpenSearch cluster -- see `opensearch/values.yaml`'s
   secret reference).
2. **Stack Management → Index Patterns → Create index pattern.**
3. Create three patterns: `ranger_audits-*`, `trino_query_audit-*`,
   `superset_audit-*`. When asked for the time field, pick `event_time`
   (all three sources have one) so the time-range picker in Discover
   works correctly.

### Browsing: the Discover tab

**Discover → pick an index pattern** (top-left dropdown) **→ set the time
range** (top-right -- this defaults to "Last 15 minutes", which will show
nothing if you're looking at older data; widen it first, this is the most
common "why is it empty" mistake).

Useful searches (typed into the search bar, KQL syntax):

| Index pattern | Example search | Finds |
|---|---|---|
| `ranger_audits-*` | `reqUser: "alice"` | Everything a specific user did |
| `ranger_audits-*` | `access: "SelectFromColumns" and resource: "tpch/tiny/nation"` | Access to a specific table |
| `ranger_audits-*` | `result: 0` | **Denied** access attempts (result `1` = allowed, `0` = denied) |
| `trino_query_audit-*` | `context.user: "alice"` | All of a user's queries, including query text (`metadata.query`) and query id (`metadata.queryId`) |
| `superset_audit-*` | `audit_source: "superset" and action: "dashboard_load"` | Superset UI actions (adjust `action` to whatever's actually logged -- check a raw document first, Superset's event names vary by version) |

Add columns (hover a field in the left sidebar → "+") for `reqUser`,
`access`, `resource`, `result` (ranger) or `context.user`,
`metadata.query` (trino) to turn the default single-line view into a
readable table.

### No Dashboards? Query OpenSearch directly

Without Dashboards installed, you can still inspect hot-tier data via
`curl` from inside the cluster (this is what `TESTING.md` uses):

```bash
OS_PASS=$(kubectl get secret opensearch-admin-password -n logging -o jsonpath='{.data.OPENSEARCH_INITIAL_ADMIN_PASSWORD}' | base64 -d)

# recent counts per source
kubectl exec -n logging opensearch-cluster-master-0 -c opensearch -- \
  curl -sk -u "admin:${OS_PASS}" "https://localhost:9200/_cat/indices/*audit*?v&h=index,docs.count"

# search ranger_audits for a specific user
kubectl exec -n logging opensearch-cluster-master-0 -c opensearch -- \
  curl -sk -u "admin:${OS_PASS}" "https://localhost:9200/ranger_audits-*/_search?q=reqUser:alice&pretty"
```

Not a UI, but functionally equivalent for a quick check.

## Cold tier: Superset SQL Lab

This one's already deployed and working right now -- Superset's existing
"Trino" database connection (the same one used for impersonation, id `1`)
can query the `iceberg` catalog directly, no extra setup needed.

1. Open `http://superset.local`, log in with your (LDAP) credentials.
2. **SQL → SQL Lab.**
3. **Database:** `Trino`. **Schema:** you can leave this blank and fully
   qualify table names instead (`iceberg.audit.<table>`), since the
   connection isn't pinned to one catalog/schema.

### What the tables actually look like

The Spark archival job (`spark/iceberg_archive_job.py`) intentionally
keeps each row as `event_date` (partition), `event_time`, and a single
`raw_json` string column holding the *entire* original audit record --
it doesn't flatten fields into their own columns. Browsing means
extracting fields from `raw_json` with Trino's JSON functions.

**Ranger audit** (`iceberg.audit.ranger_audit`):
```sql
SELECT
  event_time,
  json_extract_scalar(raw_json, '$.reqUser')  AS req_user,
  json_extract_scalar(raw_json, '$.access')   AS access,
  json_extract_scalar(raw_json, '$.resource') AS resource,
  json_extract_scalar(raw_json, '$.result')   AS result,   -- '1' = allowed, '0' = denied
  json_extract_scalar(raw_json, '$.reqData')  AS query_text
FROM iceberg.audit.ranger_audit
ORDER BY event_time DESC
LIMIT 50;
```

**Trino query audit** (`iceberg.audit.trino_query_audit`):
```sql
SELECT
  event_time,
  json_extract_scalar(raw_json, '$.context.user')     AS user,
  json_extract_scalar(raw_json, '$.metadata.queryId')  AS query_id,
  json_extract_scalar(raw_json, '$.metadata.query')    AS query_text,
  json_extract_scalar(raw_json, '$.metadata.queryState') AS query_state
FROM iceberg.audit.trino_query_audit
ORDER BY event_time DESC
LIMIT 50;
```

**Superset action audit** (`iceberg.audit.superset_audit`):
```sql
SELECT
  event_time,
  json_extract_scalar(raw_json, '$.action')        AS action,
  json_extract_scalar(raw_json, '$.user_id')       AS user_id,
  json_extract_scalar(raw_json, '$.dashboard_id')  AS dashboard_id,
  json_extract_scalar(raw_json, '$.duration_ms')   AS duration_ms
FROM iceberg.audit.superset_audit
ORDER BY event_time DESC
LIMIT 50;
```

Save any of these as a SQL Lab query (or a dataset, then a dashboard) if
you want a persistent view rather than re-running it each time.

### Bonus: Trino's own web UI for query monitoring

`http://trino.local` (needs the same `trino.local → 192.168.86.100`
hosts-file entry, or use `https://trino.local` per the ingress's TLS
listener) shows Trino's live and historical query execution -- useful for
watching the archival job's own queries run, or diagnosing a slow query,
but it doesn't browse table *contents* the way SQL Lab does. Use SQL Lab
for "what's in the audit trail", Trino's UI for "is/was a query running
and how did it perform".
