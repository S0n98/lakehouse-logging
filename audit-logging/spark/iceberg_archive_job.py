"""
Cold-tier audit archiver: reads the raw NDJSON audit files Fluent Bit lands
in MinIO (see ../fluent-bit/values.yaml -- the "audit_source" s3 outputs)
and loads them into a real Iceberg table under the existing `lakehouse`
warehouse / Nessie catalog, the same catalog Trino already queries. Once
this runs, the data is queryable from Trino with plain SQL:

    SELECT * FROM iceberg.audit.ranger_audit ORDER BY event_time DESC LIMIT 20;

Run once per source (ranger / trino / superset) via the source arg -- see
scheduled-spark-application.yaml, one ScheduledSparkApplication per source.

Idempotent by design: rather than tracking a processed-files watermark,
every run re-reads the FULL raw prefix for that source and does a dynamic
partition overwrite keyed on event_date. Re-running for the same time
range recomputes those partitions instead of duplicating rows. This is
intentionally simple for the log volume this cluster has -- see
../README.md "Known gaps" for when this stops scaling and what to do
instead (a real watermark / incremental read).

The raw files are never deleted or moved by this job: they sit in the
Object-Lock-protected `audit-logs-cold` bucket as the immutable original
copy, independent of whatever this job (or a future rewrite of it) does.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_TABLES = {
    "ranger": "ranger_audit",
    "trino": "trino_query_audit",
    "superset": "superset_audit",
}


def main(source: str) -> None:
    if source not in SOURCE_TABLES:
        raise SystemExit(f"unknown source {source!r}, expected one of {list(SOURCE_TABLES)}")

    table_name = SOURCE_TABLES[source]
    raw_path = f"s3a://audit-logs-cold/raw/{source}/"
    table = f"iceberg.audit.{table_name}"

    spark = SparkSession.builder.appName(f"audit-archive-{source}").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.audit")

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

    # Fluent Bit stamps every record with its own "time" field (RFC3339,
    # nanosecond precision) regardless of source -- use it as the canonical
    # event_time / partition key rather than trusting each source's own
    # (differently-named, differently-shaped) timestamp field.
    df = df.withColumn("event_time", F.to_timestamp("time")) \
           .withColumn("event_date", F.to_date("event_time")) \
           .withColumn("raw_json", F.to_json(F.struct([c for c in df.columns if c != "time"])))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            event_date DATE,
            event_time TIMESTAMP,
            raw_json   STRING
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    out = df.select("event_date", "event_time", "raw_json")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out.writeTo(table).overwritePartitions()

    count = out.count()
    print(f"[{source}] wrote {count} records to {table} "
          f"across {out.select('event_date').distinct().count()} partition(s)")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: iceberg_archive_job.py <ranger|trino|superset>")
    main(sys.argv[1])
