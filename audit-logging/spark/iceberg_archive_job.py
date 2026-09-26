"""
Cold-tier audit archiver: reads the raw NDJSON audit files Fluent Bit lands
in MinIO (see ../fluent-bit/values.yaml -- the "audit_source" s3 outputs)
and loads them into a real Iceberg table under the existing `lakehouse`
warehouse / Nessie catalog, the same catalog Trino already queries. Once
this runs, the data is queryable from Trino with plain SQL:

    SELECT * FROM iceberg.audit.ranger_audit ORDER BY event_time DESC LIMIT 20;

Run once per source (ranger / trino / superset) via the source arg -- see
scheduled-spark-application.yaml, one ScheduledSparkApplication per source.

RAW LANDING ZONE RETENTION -- read this before changing the delete logic:
raw files are kept for at least 30 days, then deleted, but ONLY once BOTH
of these hold:
  1. The file has been successfully merged into Iceberg (confirmed within
     THIS SAME run, immediately before deletion -- never assumed from a
     past run).
  2. The file is at least 30 days old (Hadoop FileStatus modification
     time), giving a real recovery window if a bug in this job's own
     transform logic ever silently corrupts/loses data despite the write
     technically succeeding.

This design replaced an earlier one (see git history) that never deleted
raw files at all, relying on the `audit-logs-cold` bucket's Object Lock
(WORM, GOVERNANCE, 365d) for immutability. That bucket's Object Lock
config turned out to structurally block ANY early deletion regardless of
what this job does (GOVERNANCE-mode retention rejects plain deletes, and
Object Lock cannot be removed from a bucket once enabled at creation) --
confirmed live via `get_object_lock_configuration` /
`head_object`'s `ObjectLockRetainUntilDate` before this rewrite. Raw
landing now lands in a NEW bucket, `audit-logs-raw`, created without
Object Lock specifically so this job can manage its own retention. The
old `audit-logs-cold` bucket's files are untouched legacy data, draining
naturally on their original 365-day locks -- see ../ARCHITECTURE.md.

IDEMPOTENCY -- because raw files now stick around for up to 30 days (not
deleted immediately after their first successful merge), and because a
retry after a partial failure must not double-insert, every record is
identified by `record_id` = sha256(raw_json). Writes go through Iceberg's
MERGE INTO (WHEN NOT MATCHED THEN INSERT), so re-merging an
already-present record -- whether it's the same file re-read on a later
run because it's not 30 days old yet, or a retry after a crash between a
successful commit and the delete step -- is always a safe no-op, never a
duplicate.

Every run reads the FULL raw prefix for the source, same as before (no
watermark) -- with a real backlog of up to 30 days now living there
between deletions, this reprocesses more data per run than the old
delete-immediately design would have, which is the deliberate cost of the
30-day recovery window. At this cluster's data volumes that's not a
concern; revisit (a real incremental/watermarked read) if it ever
becomes one -- see ../README.md "Known gaps".
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_TABLES = {
    "ranger": "ranger_audit",
    "trino": "trino_query_audit",
    "superset": "superset_audit",
}

MIN_AGE_SECONDS_BEFORE_DELETE = 30 * 24 * 60 * 60  # 30 days


def main(source: str) -> None:
    if source not in SOURCE_TABLES:
        raise SystemExit(f"unknown source {source!r}, expected one of {list(SOURCE_TABLES)}")

    table_name = SOURCE_TABLES[source]
    raw_path = f"s3a://audit-logs-raw/raw/{source}/"
    table = f"iceberg.audit.{table_name}"

    spark = SparkSession.builder.appName(f"audit-archive-{source}").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.audit")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            record_id  STRING,
            event_date DATE,
            event_time TIMESTAMP,
            raw_json   STRING
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    # Schema migration for tables created before record_id existed (this
    # cluster's tables were -- see git history). Safe to run every time:
    # Iceberg errors if the column already exists, deliberately ignored.
    try:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (record_id STRING)")
        print(f"[{source}] migrated {table}: added record_id column")
    except Exception:
        pass  # already has the column

    # Idempotent/cheap to run every time -- backfills any pre-migration
    # rows exactly once, a no-op afterward since the WHERE excludes them.
    spark.sql(f"UPDATE {table} SET record_id = sha2(raw_json, 256) WHERE record_id IS NULL")

    try:
        # recursiveFileLookup: fluent-bit's s3 output lands files under
        # nested raw/<source>/%Y/%m/%d/%H/ folders -- Spark's file source
        # does not descend into subdirectories by default.
        df = spark.read.option("recursiveFileLookup", "true").json(raw_path)
    except Exception as exc:  # noqa: BLE001 - genuinely want to swallow "no files yet"
        print(f"[{source}] nothing to read yet at {raw_path}: {exc}")
        spark.stop()
        return

    if df.rdd.isEmpty():
        print(f"[{source}] no records found under {raw_path}")
        spark.stop()
        return

    original_columns = df.columns  # before adding _input_file below

    df = (
        df.withColumn("_input_file", F.input_file_name())
          .withColumn("event_time", F.to_timestamp("time"))
          .withColumn("event_date", F.to_date("event_time"))
          .withColumn("raw_json", F.to_json(F.struct([c for c in original_columns if c != "time"])))
          .withColumn("record_id", F.sha2(F.col("raw_json"), 256))
          .cache()
    )

    input_files = sorted(row["_input_file"] for row in df.select("_input_file").distinct().collect())

    out = df.select("record_id", "event_date", "event_time", "raw_json")
    out.createOrReplaceTempView("new_records")

    try:
        spark.sql(f"""
            MERGE INTO {table} t
            USING new_records s
            ON t.record_id = s.record_id
            WHEN NOT MATCHED THEN INSERT (record_id, event_date, event_time, raw_json)
            VALUES (s.record_id, s.event_date, s.event_time, s.raw_json)
        """)
    except Exception:
        # Nothing committed (or partially committed -- Iceberg's MERGE is a
        # single atomic commit, so this is all-or-nothing). Leave every raw
        # file exactly as it is; the next run re-reads and re-merges them,
        # a safe no-op for anything that actually did make it in, a real
        # retry for anything that didn't. Never touch raw files on failure.
        print(f"[{source}] MERGE into {table} failed, raw files left in place for retry")
        spark.stop()
        raise

    print(f"[{source}] merged {len(input_files)} raw file(s) into {table}")

    # Delete only files BOTH just confirmed merged (above, this run) AND
    # already at least 30 days old -- files younger than that stay, and
    # get safely re-merged (idempotent no-op) on every run until they age
    # out. See the module docstring for why age is a hard requirement here,
    # not just "merged", and why 30 days.
    hadoop_conf = spark._jsc.hadoopConfiguration()
    JPath = spark._jvm.org.apache.hadoop.fs.Path
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (MIN_AGE_SECONDS_BEFORE_DELETE * 1000)

    deleted, kept_too_young, failed = 0, 0, 0
    for f in input_files:
        path = JPath(f)
        try:
            fs = path.getFileSystem(hadoop_conf)
            mtime_ms = fs.getFileStatus(path).getModificationTime()
            if mtime_ms > cutoff_ms:
                kept_too_young += 1
                continue
            if fs.delete(path, False):
                deleted += 1
            else:
                failed += 1
                print(f"[{source}] WARNING: delete returned false for {f}")
        except Exception as exc:
            failed += 1
            print(f"[{source}] WARNING: failed to delete {f}: {exc}")

    print(f"[{source}] raw file cleanup: {deleted} deleted (merged + >=30d old), "
          f"{kept_too_young} kept (merged but <30d old), {failed} delete failure(s)")

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: iceberg_archive_job.py <ranger|trino|superset>")
    main(sys.argv[1])
