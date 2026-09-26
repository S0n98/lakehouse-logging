# Capacity planning: 1 year of audit logs

Two ways to estimate, depending on what number you actually know: raw
event volume, or active user count. Both use the same measured baseline
and retention model below — pick whichever matches the number you have,
or use both as a cross-check.

**Updated 2026-09-26** after a retention-model architecture review changed
two of the three tiers — see the retention model section immediately
below before reading any table, the shape of the answer changed, not just
the numbers.

## Retention model (read this first — it changes the shape of the answer)

All three tiers are now **bounded**, not just hot tier — only Iceberg
still grows forever:

- **Hot tier (OpenSearch): 7 days**, down from 30. ISM policies
  (`ism-policies/*.json`) delete at `min_index_age: 7d`.
- **Raw landing (MinIO, `audit-logs-raw`): 30 days, constant.** This
  changed from "never delete" — see `ARCHITECTURE.md`'s "Raw landing
  retention" section for the full story, but in short: the original
  design relied on the bucket's Object Lock for immutability and never
  deleted anything; that bucket's Object Lock turned out to structurally
  block early deletion regardless of application logic, so raw landing
  moved to a new, lock-free bucket where the Spark job now deletes a file
  once it's confirmed merged into Iceberg **and** at least 30 days old.
  Like hot tier, this is now a rolling window, not an accumulating total —
  a year from now it still only holds ~30 days of data.
- **Cold, queryable (Iceberg) still grows forever.** Nothing expires it.
  This is now the *only* tier where "after 1 year" means "1 year of
  accumulation" rather than "a constant, at any point in time."

This is a meaningfully different shape than the previous version of this
doc: previously, "cold tier" (raw + Iceberg combined) was the only
unbounded quantity and dominated the 1-year numbers. Now only Iceberg's
compressed copy accumulates — raw landing's disk footprint is capped at
30 days' worth, same as hot tier is capped at 7. The tables below reflect
this by giving hot, raw-landing, and Iceberg-after-N-years as three
separate figures, not two.

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
Hot tier (GB, constant)         = events/day × avg_record_KB × 7  × 1.3(*)  / 1,000,000
Raw landing (GB, constant)      = events/day × avg_record_KB × 30 × 1.0(**) / 1,000,000
Iceberg after N years (GB)      = events/day × avg_record_KB × 365 × N × 0.15(***) / 1,000,000
```
`(*)` 1.3x = normal OpenSearch indexing overhead for a *clean* mapping —
see the caveat below, this cluster's actual overhead is currently worse.
`(**)` 1.0x = plain NDJSON files, no indexing/compression overhead to
account for.
`(***)` 0.15x = measured Ranger/Superset compression ratio (see caveat
above) — this is the *only* growing number; hot and raw landing are both
constants regardless of how many years pass.

---

## Method A: by raw event volume

Assumes an even split across the three sources (reweight if your real mix
differs — e.g. if Superset UI actions dominate, shift the blended
per-record size toward 2.3 KB).

Blended average: **2.1 KB/record** (the three sources happen to average
out almost exactly to Ranger's own number).

| Combined events/day | Hot tier (7d, constant) | Raw landing (30d, constant) | Iceberg after 1 year | Iceberg after 5 years |
|---|---|---|---|---|
| 100 | 1.9 MB | 6.3 MB | 11.5 MB | 57.5 MB |
| 1,000 | 19.1 MB | 63 MB | 115 MB | 575 MB |
| 10,000 | 191 MB | 630 MB | 1.15 GB | 5.75 GB |
| 100,000 | 1.9 GB | 6.3 GB | 11.5 GB | 57.5 GB |
| 1,000,000 | 19.1 GB | 63 GB | 115 GB | 575 GB |
| 10,000,000 | 191 GB | 630 GB | 1.15 TB | 5.75 TB |

Total disk footprint at any point in time = Hot + Raw landing + however
many years of Iceberg accumulation you're planning for (e.g. at 100,000
events/day, after 1 year: 1.9 + 6.3 + 11.5 ≈ **19.7 GB total**, versus the
previous design's ~88 GB for the same volume — raw landing no longer
accumulating for the full year is the entire difference).

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

| Source | Events/day | Hot (7d) | Raw landing (30d) | Iceberg (1yr) |
|---|---|---|---|---|
| Ranger | 45,000 | ~0.86 GB | ~2.84 GB | ~5.17 GB |
| Trino | 15,000 | ~0.26 GB | ~0.86 GB | ~1.56 GB |
| Superset | 30,000 | ~0.63 GB | ~2.07 GB | ~3.78 GB |
| **Total** | 90,000 | **~1.75 GB** | **~5.76 GB** | **~10.5 GB** |

Total disk footprint after 1 year at 1,000 users/day: **~18 GB**
(1.75 + 5.76 + 10.5) — versus ~80.6 GB under the previous "raw landing
grows forever" design. The reduction is entirely from raw landing no
longer accumulating past 30 days.

**Broad sample, totals only:**

| Active users/day | Hot tier (7d, constant) | Raw landing (30d, constant) | Iceberg after 1 year | Iceberg after 5 years |
|---|---|---|---|---|
| 10 | 17.5 MB | 57.6 MB | 105 MB | 526 MB |
| 100 | 175 MB | 576 MB | 1.05 GB | 5.26 GB |
| 1,000 | 1.75 GB | 5.76 GB | 10.5 GB | 52.6 GB |
| 10,000 | 17.5 GB | 57.6 GB | 105 GB | 526 GB |
| 100,000 | 175 GB | 576 GB | 1.05 TB | 5.26 TB |

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
3. **Iceberg itself still has no expiration or snapshot-cleanup job.**
   It's the one tier that's supposed to grow forever by design, but even
   an intentionally-unbounded table needs periodic `expire_snapshots` /
   orphan-file cleanup as routine maintenance (old snapshots and their
   superseded data files otherwise pile up as dead weight on top of the
   live data) — not addressed by anything in this doc or the recent
   retention redesign.

## Recomputing with your own numbers

The formulas above are the whole model — plug in your own `events/day`
(Method A) or your own assumptions for queries/user, Ranger-events/query,
and Superset-events/user (Method B) and recompute. The measured baseline
(2.1 / 1.9 / 2.3 KB per source) is the one number in this doc that came
from direct measurement rather than judgment calls — revisit it
periodically (`TESTING.md` has the commands) since it'll shift as query
patterns and payload shapes change.
