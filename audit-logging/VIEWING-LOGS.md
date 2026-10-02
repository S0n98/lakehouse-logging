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

**Status on this cluster: installed 2026-09-29** (ingress host
`audit-logs.local`, matching `opensearch/dashboards-values.yaml`) -- the
three index patterns below are already created, so you can skip straight
to "Browsing: the Discover tab" after logging in.

One thing worth knowing if you ever reinstall this: the chart's
`ingress.hosts[].paths[].backend` needs `serviceName`/`servicePort`
spelled out explicitly (`opensearch-dashboards` / `5601`) -- this chart's
ingress template doesn't default to the release's own service the way
some others do, and omitting it fails with a nil-pointer error on
`.backend.serviceName`. Already fixed in `dashboards-values.yaml`.

Also hit, and fixed, a persistent `cluster.blocks.create_index: true`
setting on the OpenSearch cluster that blocked Dashboards from creating
its own `.kibana` saved-objects index on first connect -- not something
this repo's own provisioning ever sets, so it was very likely a leftover
from earlier ad-hoc disk-pressure firefighting that never got reverted.
Cleared via `PUT _cluster/settings {"persistent": {"cluster.blocks.create_index": null}}`.
If a future Dashboards install (or anything else needing to create an
index) fails the same way, check `GET _cluster/settings?flat_settings=true`
for this before assuming it's a fresh problem.

### One-time setup: index patterns (already done on this cluster)

Each audit source writes to a **daily** index (`ranger_audits-2026.09.26`,
etc. -- see `Logstash_DateFormat` in `fluent-bit/values.yaml`), so use a
wildcard pattern to see all of a source's history. Already created via the
saved objects API for `ranger_audits-*`, `trino_query_audit-*`, and
`superset_audit-*` (time field `event_time` on all three) -- steps below
are for reference / reinstalling elsewhere:

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

**Updated 2026-10-01** -- this used to describe an opaque-blob design
(every field behind `json_extract_scalar(raw_json, ...)`); that was
redesigned for the long term (see `ARCHITECTURE.md`'s "Table structure:
promoted columns, not one opaque JSON blob"). Each source now has its own
real, typed columns -- no JSON functions needed for normal browsing.
`raw_json` is still kept on every table as a forensic catch-all for
anything not promoted, so nothing from before is actually lost, it's just
not the primary way to query anymore.

**Ranger audit** (`iceberg.audit.ranger_audit`):
```sql
SELECT event_time, req_user, access_type, resource, resource_type, repo,
       result,   -- 1 = allowed, 0 = denied
       req_data AS query_text
FROM iceberg.audit.ranger_audit
ORDER BY event_time DESC
LIMIT 50;
```

**Trino query audit** (`iceberg.audit.trino_query_audit`) -- `tables` is a
native `ARRAY<STRUCT<catalog,schema,table>>` column; `UNNEST` it rather
than parsing JSON if you need per-table rows (see README.md's "Query
cookbook" for a worked example):
```sql
SELECT event_time, user_name, source, remote_address,
       query_id, query_state, query_text
FROM iceberg.audit.trino_query_audit
ORDER BY event_time DESC
LIMIT 50;
```

**Superset action audit** (`iceberg.audit.superset_audit`):
```sql
SELECT event_time, user_id, action, dashboard_id, slice_id, duration_ms
FROM iceberg.audit.superset_audit
ORDER BY event_time DESC
LIMIT 50;
```

Save any of these as a SQL Lab query (or a dataset, then a dashboard) if
you want a persistent view rather than re-running it each time. See
README.md's "Query cookbook" for task-oriented queries (by user, by
date, by table, Ranger allow/deny) built on this same schema.

### Bonus: Trino's own web UI for query monitoring

`http://trino.local` (needs the same `trino.local → 192.168.86.100`
hosts-file entry, or use `https://trino.local` per the ingress's TLS
listener) shows Trino's live and historical query execution -- useful for
watching the archival job's own queries run, or diagnosing a slow query,
but it doesn't browse table *contents* the way SQL Lab does. Use SQL Lab
for "what's in the audit trail", Trino's UI for "is/was a query running
and how did it perform".
