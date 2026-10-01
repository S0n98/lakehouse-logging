"""
Cold-tier audit archiver: reads the raw NDJSON audit files Fluent Bit lands
in MinIO (see ../fluent-bit/values.yaml -- the "audit_source" s3 outputs)
and loads them into real Iceberg tables under the existing `lakehouse`
warehouse / Nessie catalog, the same catalog Trino already queries. Once
this runs, the data is queryable from Trino with plain SQL:

    SELECT * FROM iceberg.audit.ranger_audit ORDER BY event_time DESC LIMIT 20;

Run once per source (ranger / trino / superset) via the source arg -- see
scheduled-spark-application.yaml, one ScheduledSparkApplication per source.

TABLE DESIGN -- redesigned 2026-10-01 for real partition/file pruning:
each source's commonly-filtered fields (see SOURCE_CONFIG below -- the
user, the SQL text, which tables got touched, etc.) are promoted to real,
typed top-level columns instead of being buried inside one opaque JSON
string. `raw_json` is kept on every table as a forensic catch-all for
every field NOT promoted -- so nothing is lost for a source's long tail
of fields, and a source's payload shape can keep drifting without a
schema migration every time, but the fields people actually filter on
(see ../ARCHITECTURE.md's query cookbook) get real column statistics,
not a full-partition JSON scan.

Promoted columns are extracted via `get_json_object(raw_json, '$.path')`,
not native nested field access on the DataFrame Spark infers from the raw
JSON -- deliberately. Spark's JSON schema inference only includes fields
actually present in THIS run's batch of files; a field genuinely missing
from every record in one run's (small) batch would make native access
(`df["field"]`) throw an AnalysisException that run, even though the
field is a normal, expected part of the payload. Extracting from the
already-serialized `raw_json` string is immune to that -- a missing path
just evaluates to NULL, exactly the semantics a sparse/optional field
should have.

IDEMPOTENCY -- unchanged from the previous design: every record is
identified by `record_id` = sha256(raw_json), and writes go through
Iceberg's MERGE INTO (WHEN NOT MATCHED THEN INSERT), so re-merging an
already-present record is always a safe no-op.

RAW LANDING ZONE RETENTION -- unchanged: raw files are deleted once
confirmed merged into Iceberg AND at least 30 days old. See
../ARCHITECTURE.md's "Raw landing retention" section for the full
reasoning (and why this bucket deliberately has no Object Lock).

MAINTENANCE -- compaction (`ALTER TABLE ... EXECUTE optimize`) and orphan
file cleanup (`EXECUTE remove_orphan_files`) run separately, daily, via
../spark/iceberg-maintenance-cronjob.yaml -- NOT in this job, since
compaction is expensive relative to an hourly incremental load and
doesn't need to run that often. See that file's header for why
`expire_snapshots` is deliberately NOT part of maintenance here (a
Nessie-specific GC safety guard, not an oversight).
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

MIN_AGE_SECONDS_BEFORE_DELETE = 30 * 24 * 60 * 60  # 30 days

TABLES_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("catalog", StringType()),
    StructField("schema", StringType()),
    StructField("table", StringType()),
]))

# Per-source: the table name, the extra promoted columns (name, Spark SQL
# type, JSON path into raw_json), and which column to sort written data by
# -- sorting data by the field it's most commonly filtered on clusters
# matching rows together within a file, which is what makes Parquet's
# per-file min/max stats actually useful for skipping files at query time
# (without this, every file in a partition looks like it might contain any
# value, so none can be skipped).
SOURCE_CONFIG = {
    "ranger": {
        "table": "ranger_audit",
        "columns": [
            ("req_user", "STRING", "$.reqUser"),
            ("access_type", "STRING", "$.access"),
            ("resource", "STRING", "$.resource"),
            ("resource_type", "STRING", "$.resType"),
            ("repo", "STRING", "$.repo"),
            ("result", "INT", "$.result"),
            ("req_data", "STRING", "$.reqData"),
        ],
        "sort_column": "req_user",
    },
    "trino": {
        "table": "trino_query_audit",
        "columns": [
            ("user_name", "STRING", "$.context.user"),
            ("source", "STRING", "$.context.source"),
            ("remote_address", "STRING", "$.context.remoteClientAddress"),
            ("query_id", "STRING", "$.metadata.queryId"),
            ("query_state", "STRING", "$.metadata.queryState"),
            ("query_text", "STRING", "$.metadata.query"),
            # tables is handled separately below -- it's an array of
            # structs (catalog/schema/table per referenced table), not a
            # scalar get_json_object path.
        ],
        "sort_column": "user_name",
    },
    "superset": {
        "table": "superset_audit",
        "columns": [
            ("user_id", "INT", "$.user_id"),
            ("action", "STRING", "$.action"),
            ("dashboard_id", "INT", "$.dashboard_id"),
            ("slice_id", "INT", "$.slice_id"),
            ("duration_ms", "INT", "$.duration_ms"),
        ],
        "sort_column": "user_id",
    },
}


def main(source: str) -> None:
    if source not in SOURCE_CONFIG:
        raise SystemExit(f"unknown source {source!r}, expected one of {list(SOURCE_CONFIG)}")

    config = SOURCE_CONFIG[source]
    table_name = config["table"]
    raw_path = f"s3a://audit-logs-raw/raw/{source}/"
    table = f"iceberg.audit.{table_name}"
    has_tables_column = source == "trino"

    spark = SparkSession.builder.appName(f"audit-archive-{source}").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.audit")

    promoted_cols_ddl = ",\n            ".join(f"{name} {sql_type}" for name, sql_type, _ in config["columns"])
    tables_col_ddl = ",\n            tables ARRAY<STRUCT<catalog: STRING, schema: STRING, table: STRING>>" if has_tables_column else ""

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            record_id  STRING,
            event_date DATE,
            event_time TIMESTAMP,
            {promoted_cols_ddl}{tables_col_ddl},
            raw_json   STRING
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    # Schema migration for tables created before these columns existed --
    # safe to run every time, Iceberg errors (caught below) if a column
    # already exists.
    for name, sql_type, _ in config["columns"]:
        try:
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({name} {sql_type})")
        except Exception:
            pass  # already has the column
    if has_tables_column:
        try:
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS (tables ARRAY<STRUCT<catalog: STRING, schema: STRING, table: STRING>>)")
        except Exception:
            pass
    try:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (record_id STRING)")
    except Exception:
        pass

    # Backfill: idempotent/cheap to run every time (WHERE excludes rows
    # already backfilled). Covers both pre-migration rows (no promoted
    # columns at all) and the original record_id migration.
    spark.sql(f"UPDATE {table} SET record_id = sha2(raw_json, 256) WHERE record_id IS NULL")
    for name, sql_type, path in config["columns"]:
        escaped_path = path.replace("'", "''")
        extract_expr = f"get_json_object(raw_json, '{escaped_path}')"
        if sql_type == "INT":
            extract_expr = f"CAST({extract_expr} AS INT)"
        spark.sql(f"""
            UPDATE {table} SET {name} = {extract_expr}
            WHERE {name} IS NULL AND get_json_object(raw_json, '{escaped_path}') IS NOT NULL
        """)
    if has_tables_column:
        spark.sql(f"""
            UPDATE {table}
            SET tables = from_json(get_json_object(raw_json, '$.metadata.tables'), 'array<struct<catalog:string,schema:string,table:string>>')
            WHERE tables IS NULL AND get_json_object(raw_json, '$.metadata.tables') IS NOT NULL
        """)

    # Sort order -- see module docstring for why this matters for query
    # pruning. Safe/cheap to set every run; it only affects how FUTURE
    # writes lay out data, it's not a data-rewriting operation itself.
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {config['sort_column']}")

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

    # Promoted columns, extracted from raw_json (see module docstring for
    # why not native nested field access).
    select_cols = ["record_id", "event_date", "event_time"]
    df_with_promoted = df
    for name, sql_type, path in config["columns"]:
        expr = F.get_json_object(F.col("raw_json"), path)
        if sql_type == "INT":
            expr = expr.cast("int")
        df_with_promoted = df_with_promoted.withColumn(name, expr)
        select_cols.append(name)
    if has_tables_column:
        df_with_promoted = df_with_promoted.withColumn(
            "tables",
            F.from_json(F.get_json_object(F.col("raw_json"), "$.metadata.tables"), TABLES_ARRAY_SCHEMA),
        )
        select_cols.append("tables")
    select_cols.append("raw_json")

    input_files = sorted(row["_input_file"] for row in df.select("_input_file").distinct().collect())

    out = df_with_promoted.select(*select_cols)
    out.createOrReplaceTempView("new_records")

    insert_cols = ", ".join(select_cols)
    insert_vals = ", ".join(f"s.{c}" for c in select_cols)

    try:
        spark.sql(f"""
            MERGE INTO {table} t
            USING new_records s
            ON t.record_id = s.record_id
            WHEN NOT MATCHED THEN INSERT ({insert_cols})
            VALUES ({insert_vals})
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
