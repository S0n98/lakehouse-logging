"""
Daily compaction for the three audit Iceberg tables. Deliberately
separate from iceberg_archive_job.py (which runs hourly) -- compaction is
expensive relative to an incremental hourly load and doesn't need to run
that often; bundling it into every hourly run would make every run pay
compaction's cost for no benefit.

Reuses the SAME ScheduledSparkApplication image/deps/catalog config as
the archival jobs (see ../spark/scheduled-spark-application.yaml) --
`rewrite_data_files` is one of Iceberg's own stored procedures, available
via Spark SQL's `CALL` syntax through the `iceberg-spark-runtime`
dependency already required for MERGE INTO, no new auth or dependency
needed.

WHY ONLY rewrite_data_files -- NOT remove_orphan_files, NOT
expire_snapshots -- IS HERE, DELIBERATELY:

All three were tried live 2026-10-01. `rewrite_data_files` works fine via
Spark. The other two both fail via Spark with "GC is disabled (deleting
files may corrupt other tables)" -- this REST catalog (Nessie)'s own
deliberate safety guard, not a bug: Nessie maintains its own commit/
branch/tag history independent of Iceberg's native snapshot list, and
blindly deleting files out from under it risks breaking a Nessie
reference that still points to one.

Interestingly, Trino's Iceberg connector does NOT enforce this guard for
`remove_orphan_files` specifically (only for `expire_snapshots`) --
confirmed live, `ALTER TABLE ... EXECUTE remove_orphan_files(...)` via
Trino found and removed 120 real orphan files with no error. Arguably
Trino has the more correct read here: a genuinely orphaned file (one no
current manifest references at all) can't belong to a Nessie reference
either, so the Nessie-safety concern doesn't really apply to it -- but
Spark's integration enforces the guard as a blanket rule regardless.

Rather than add Trino credentials to this automated job just to call one
procedure Spark won't allow, orphan-file cleanup is left as a periodic
MANUAL operation via Trino, run by a human when actually needed (e.g.
after noticing excess storage, or on whatever cadence seems warranted --
this cluster's data volumes don't currently justify automating it). Run
per table:

    ALTER TABLE iceberg.audit.<table> EXECUTE remove_orphan_files(retention_threshold => '7d');

See ../README.md's "Cold tier maintenance" section for the full command
set, including when to actually use this.
"""
from pyspark.sql import SparkSession

TABLES = ["ranger_audit", "trino_query_audit", "superset_audit"]


def main() -> None:
    spark = SparkSession.builder.appName("iceberg-audit-maintenance").getOrCreate()

    for table_name in TABLES:
        table = f"audit.{table_name}"
        try:
            result = spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{table}')").collect()
            print(f"[{table_name}] rewrite_data_files: {result}")
        except Exception as exc:
            print(f"[{table_name}] rewrite_data_files FAILED: {exc}")

    spark.stop()


if __name__ == "__main__":
    main()
