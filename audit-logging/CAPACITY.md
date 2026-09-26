# Capacity planning: 1 year of audit logs

Two ways to estimate, depending on what number you actually know: raw
event volume, or active user count. Both use the same measured baseline
and retention model below — pick whichever matches the number you have,
or use both as a cross-check.

## Retention model (read this first — it changes the shape of the answer)

- **Hot tier (OpenSearch) is bounded, not growing.** All three ISM
  policies (`ism-policies/*.json`) delete at `min_index_age: 30d`. A year
  from now, hot tier still only holds **~30 days of rolling data** — it
  never exceeds that, regardless of how long the system has been running.
- **Cold tier keeps growing forever**, and is stored **twice**: the raw
  NDJSON landing files in MinIO are never deleted by design (see
  `fluent-bit/values.yaml`'s comment on this), plus the compressed Iceberg
  copy. Neither has any expiration or compaction configured today.

This is why "capacity in 1 year" has two different answers per tier: hot
tier's answer is really "capacity at any given moment, forever" (a
constant), while cold tier's answer genuinely means "after accumulating
for 365 days" (and keeps climbing every year after that).

## Measured baseline (real documents from this cluster, not guessed)

| Source | Raw JSON payload/record | Iceberg (compressed) — measured via `iceberg.audit."<table>$files"` |
|---|---|---|
| Ranger audit | 2.1 KB | ~285 bytes (13.6% of raw) |
| Trino query audit | 1.9 KB | ~9.4 KB — **not used below, see caveat** |
| Superset action audit | 2.3 KB | ~321 bytes (14.0% of raw) |

**Caveat on Trino's compressed number:** measured directly via Trino's
`$files` metadata table, but on only 32 records split across 3 Parquet
files — fixed per-file overhead (footer, schema, dictionary pages)
dominates at that scale, making the real record data look *larger* than
raw, which won't hold at production volume. The tables below substitute
Ranger/Superset's measured ~14% compression ratio for Trino too, as the
more representative estimate — re-measure once real production-scale
files exist and update this doc.

**Formulas used:**
```
Hot tier (GB)          = events/day × avg_record_KB × 30 × 1.3(*) / 1,000,000
Cold tier after N years = events/day × avg_record_KB × 365 × N × 1.15(**) / 1,000,000
```
`(*)` 1.3x = normal OpenSearch indexing overhead for a *clean* mapping —
see the caveat below, this cluster's actual overhead is currently worse.
`(**)` 1.15x = raw copy (1.0x) + compressed Iceberg copy (~0.15x), using
the measured Ranger/Superset ratio.

---

## Method A: by raw event volume

Assumes an even split across the three sources (reweight if your real mix
differs — e.g. if Superset UI actions dominate, shift the blended
per-record size toward 2.3 KB).

Blended average: **2.1 KB/record** (the three sources happen to average
out almost exactly to Ranger's own number).

| Combined events/day | Hot tier (30d, constant) | Cold tier after 1 year | Cold tier after 5 years |
|---|---|---|---|
| 100 | 8.2 MB | 88 MB | 442 MB |
| 1,000 | 82 MB | 882 MB | 4.4 GB |
| 10,000 | 819 MB | 8.8 GB | 44 GB |
| 100,000 | 8.2 GB | 88 GB | 442 GB |
| 1,000,000 | 82 GB | 882 GB (~0.9 TB) | 4.4 TB |
| 10,000,000 | 819 GB (~0.8 TB) | 8.8 TB | 44 TB |

---

## Method B: by active user count

Turns "N users/day" into events using BI-workload assumptions — **these
are the two biggest levers if your real usage differs, adjust first**:

| Assumption | Value | Basis |
|---|---|---|
| Trino queries per user/day | 15 | Moderate dashboard + ad-hoc SQL Lab usage |
| Ranger audit events per Trino query | 3 | Measured 2 events (`AccessCatalog` + `SelectFromColumns`) for a single-table query in live testing; bumped to 3 for queries joining multiple tables |
| Superset action-audit events per user/day | 30 | UI actions (dashboard loads, chart renders, filters) fire more often than raw queries — ~2x the query count |

Per-user-per-day event generation this implies: 45 Ranger events, 15
Trino events, 30 Superset events → **90 events/user/day** total, weighted
2.1/1.9/2.3 KB per the measured baseline.

**Worked example at 1,000 users/day** (shows the per-source breakdown the
broad table below rolls up):

| Source | Events/day | Hot (30d) | Cold (1yr) |
|---|---|---|---|
| Ranger | 45,000 | ~3.7 GB | ~39.7 GB |
| Trino | 15,000 | ~1.1 GB | ~12.0 GB |
| Superset | 30,000 | ~2.7 GB | ~29.0 GB |
| **Total** | 90,000 | **~7.5 GB** | **~80.6 GB** |

**Broad sample, totals only:**

| Active users/day | Hot tier (30d, constant) | Cold tier after 1 year | Cold tier after 5 years |
|---|---|---|---|
| 10 | 75 MB | 806 MB | 4.0 GB |
| 100 | 749 MB | 8.1 GB | 40.3 GB |
| 1,000 | 7.5 GB | 80.6 GB | 403 GB |
| 10,000 | 74.9 GB | 806 GB (~0.8 TB) | 4.0 TB |
| 100,000 | 749 GB (~0.75 TB) | 8.1 TB | 40.3 TB |

---

## Two things to fix that affect these numbers

1. **OpenSearch field-mapping explosion.** The `trino_query_audit` index
   alone has **606 distinct mapped fields**, almost entirely from
   Kubernetes pod annotation keys (`checksum/catalog-config`,
   `cni.projectcalico.org/podIP`, etc.) that the `kubernetes` filter
   merges in — dynamic mapping then creates both a `text` and `.keyword`
   sub-field for each, with zero audit value. The 1.3x overhead multiplier
   above assumes this gets fixed (an index template with `"dynamic":
   false` on the `kubernetes.annotations` subtree, or dropping that
   subtree in Fluent Bit before indexing) — **without that fix, hot tier
   could run meaningfully higher than these numbers**, and there's a real
   risk of hitting OpenSearch's default 1000-field-per-index limit as more
   distinct pods/checksums appear over time.
2. **`security-auditlog-*` has no ISM policy at all** (confirmed via
   `_plugins/_ism/explain/security-auditlog-*`) — OpenSearch's own
   internal security-plugin audit log, unrelated to the 3-source design
   here, growing completely unbounded on the same disk budget. Not
   included in any number above. Either add a 4th ISM policy for it, or
   explicitly decide it's out of scope and size for it separately.

## Recomputing with your own numbers

Both formulas above are the whole model — plug in your own
`events/day` (Method A) or your own assumptions for queries/user,
Ranger-events/query, and Superset-events/user (Method B) and recompute.
The measured baseline (2.1 / 1.9 / 2.3 KB per source) is the one number
in this doc that came from direct measurement rather than judgment calls
— revisit it periodically (`TESTING.md` has the commands) since it'll
shift as query patterns and payload shapes change.
