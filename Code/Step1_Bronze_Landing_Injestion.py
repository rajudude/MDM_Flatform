# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Step 1 — Bronze Landing: Dynamic, Config-Driven Raw JSON Ingestion
# MAGIC **Pipeline:** Dynamic MDM Platform | **Layer:** Bronze
# MAGIC
# MAGIC **Purpose:** Land raw JSON payloads from **any number of source domains**
# MAGIC (Retail/Consumer, Healthcare/Patient, and future domains) into a single
# MAGIC generic `bronze.sland_mdm` table, driven entirely by metadata in
# MAGIC `bronze.domain_config` — no code changes needed to onboard a new domain.
# MAGIC
# MAGIC ## Design change vs. the original single-domain script
# MAGIC | Before | Now |
# MAGIC |---|---|
# MAGIC | `SOURCE_PATH` hard-coded in a cell | Driven by `bronze.domain_config` rows |
# MAGIC | One table per domain (`sland_consumer`) | One generic table (`sland_mdm`) + `domain_code` column |
# MAGIC | `record_uuid` pulled from JSON root (`$['@RecordUUID']`) — **bug**: the UUID actually lives under `Consumer` or `Patient`, so this always returned NULL | `record_uuid` extracted using each domain's configured `root_entity_name` (e.g. `Consumer`, `Patient`), built dynamically per domain |
# MAGIC | Adding a new domain = copy/paste a new notebook | Adding a new domain = insert one row into `domain_config` |
# MAGIC
# MAGIC | Table | PK | Notes |
# MAGIC |---|---|---|
# MAGIC | `bronze.domain_config` | `domain_code` | One row per source domain — the "brain" of this notebook |
# MAGIC | `bronze.batch_control` | `batch_id` | One row per ingestion run (now scoped per domain) |
# MAGIC | `bronze.sland_mdm` | `sln_id` | One row per raw JSON file, across all domains |
# MAGIC
# MAGIC > **Fix note:** Delta Lake does not support `DEFAULT` column values in DDL.
# MAGIC > All timestamp and status defaults are supplied explicitly in the Python write logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Create Catalog / Bronze Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS MDM_Platform
# MAGIC COMMENT 'Dynamic multi-domain MDM Platform — raw landing through golden record';
# MAGIC
# MAGIC USE CATALOG MDM_Platform;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Bronze / raw-landing layer for all MDM domains (Retail, Healthcare, ...)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Create Domain Config Table (the control plane)
# MAGIC This is what makes the notebook "dynamic". Every source domain is a row here.
# MAGIC To onboard a new domain (e.g. Loyalty, Insurance), insert a new row —
# MAGIC **no notebook changes required**, as long as the JSON has a `Header` block
# MAGIC and a single top-level entity object containing `@RecordUUID`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.domain_config (
# MAGIC     domain_code        STRING,   -- e.g. 'RETAIL', 'HEALTHCARE'
# MAGIC     domain_name        STRING,   -- friendly name
# MAGIC     source_code        STRING,   -- e.g. 'RTL', 'HCR' — feed/source system tag
# MAGIC     root_entity_name   STRING,   -- top-level JSON key holding the record, e.g. 'Consumer', 'Patient'
# MAGIC     source_path        STRING,   -- Volume path to land from
# MAGIC     active_flag        BOOLEAN,
# MAGIC     creation_dt        TIMESTAMP,
# MAGIC     updation_dt        TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Control table: one row per source domain feeding the dynamic MDM platform';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed / upsert config rows for the two domains we have today
# MAGIC Edit `source_path` values to match your actual Volume layout.
# MAGIC Re-running this cell is safe (MERGE, not INSERT).

# COMMAND ----------

from pyspark.sql import Row

domain_rows = [
    Row(domain_code="RETAIL",
        domain_name="Retail Consumer",
        source_code="IDN",
        root_entity_name="Consumer",
        source_path="/Volumes/mdm_platform/bronze/retail_consumer",
        active_flag=True),
    Row(domain_code="HEALTHCARE",
        domain_name="Healthcare Patient",
        source_code="HCR",
        root_entity_name="Patient",
        source_path="/Volumes/mdm_platform/bronze/healthcare_patient",
        active_flag=True),
]

df_config = spark.createDataFrame(domain_rows)

df_config.createOrReplaceTempView("stg_domain_config")

spark.sql("""
    MERGE INTO bronze.domain_config AS tgt
    USING stg_domain_config AS src
    ON tgt.domain_code = src.domain_code
    WHEN MATCHED THEN UPDATE SET
        tgt.domain_name       = src.domain_name,
        tgt.source_code       = src.source_code,
        tgt.root_entity_name  = src.root_entity_name,
        tgt.source_path       = src.source_path,
        tgt.active_flag       = src.active_flag,
        tgt.updation_dt       = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
        (domain_code, domain_name, source_code, root_entity_name, source_path,
         active_flag, creation_dt, updation_dt)
    VALUES
        (src.domain_code, src.domain_name, src.source_code, src.root_entity_name,
         src.source_path, src.active_flag, current_timestamp(), current_timestamp())
""")

display(spark.table("bronze.domain_config"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Create Batch Control Table
# MAGIC Now scoped per domain via `domain_code`, so Retail and Healthcare batches
# MAGIC never collide and can be tracked/rerun independently.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.batch_control (
# MAGIC     batch_id      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     batch_name    STRING,
# MAGIC     domain_code   STRING,
# MAGIC     source_path   STRING,
# MAGIC     status        STRING,
# MAGIC     record_count  BIGINT,
# MAGIC     creation_dt   TIMESTAMP,
# MAGIC     updation_dt   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Tracks each ingestion batch per domain — status: STARTED | COMPLETED | FAILED';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Create Generic Landing Table
# MAGIC One table for every domain. `domain_code` + `source_code` tell you which
# MAGIC feed a row came from; `root_entity_name` records which JSON key the
# MAGIC `record_uuid` was pulled from, for auditability.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.sland_mdm (
# MAGIC     sln_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     batch_id           BIGINT,
# MAGIC     domain_code        STRING,
# MAGIC     source_code        STRING,
# MAGIC     root_entity_name   STRING,
# MAGIC     raw_payload        STRING,
# MAGIC     source_file        STRING,
# MAGIC     document_uuid      STRING,
# MAGIC     record_uuid        STRING,
# MAGIC     creation_dt        TIMESTAMP,
# MAGIC     updation_dt        TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (domain_code, batch_id)
# MAGIC COMMENT 'Raw JSON payload landing table — Bronze layer, all domains';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Ingestion Loop
# MAGIC Reads the **active** rows from `bronze.domain_config` and runs the same
# MAGIC land-and-track logic for each one. Adding a third domain later means
# MAGIC adding a row in 1.2 — this cell does not change.

# COMMAND ----------

from pyspark.sql import functions as F

BATCH_NAME = "mdm_dynamic_load"

active_domains = (
    spark.table("bronze.domain_config")
         .filter(F.col("active_flag") == True)
         .collect()
)

print(f">>> Active domains found: {[d['domain_code'] for d in active_domains]}")

run_summary = []

for domain in active_domains:

    domain_code      = domain["domain_code"]
    source_code      = domain["source_code"]
    root_entity_name = domain["root_entity_name"]
    source_path      = domain["source_path"]

    print(f"\n=== Processing domain: {domain_code} ({source_code}) ===")
    print(f">>> Source path      : {source_path}")
    print(f">>> Root entity name : {root_entity_name}")

    # ---- Read + transform (no batch_id yet — batch is opened only if there's --
    # ---- something new to load, see below) ------------------------------------
    # record_uuid path is built dynamically from root_entity_name, e.g.
    #   $.Consumer['@RecordUUID']   for RETAIL
    #   $.Patient['@RecordUUID']    for HEALTHCARE
    record_uuid_path = f"$.{root_entity_name}['@RecordUUID']"

    try:
        df_raw = (
            spark.read
                 .option("wholetext", True)
                 .text(source_path)
            .withColumn("raw_payload",      F.col("value"))
            .withColumn("domain_code",      F.lit(domain_code))
            .withColumn("source_code",      F.lit(source_code))
            .withColumn("root_entity_name", F.lit(root_entity_name))
            .withColumn("source_file",      F.col("_metadata.file_path"))
            .withColumn("document_uuid",
                F.get_json_object(F.col("raw_payload"), "$.Header['@RecordUUID']"))
            .withColumn("document_uuid",
                F.get_json_object(F.col("raw_payload"), "$.Header.DocumentUUID"))
            .withColumn("record_uuid",
                F.get_json_object(F.col("raw_payload"), record_uuid_path))
            .withColumn("creation_dt",  F.current_timestamp())
            .withColumn("updation_dt",  F.current_timestamp())
            .drop("value")
            .select(
                "domain_code", "source_code", "root_entity_name",
                "raw_payload", "source_file", "document_uuid", "record_uuid",
                "creation_dt", "updation_dt"
            )
        )

        raw_file_count = df_raw.count()
        print(f">>> Files read from volume : {raw_file_count}")

        # ---- Skip already-processed files -----------------------------------
        # Anti-join against source_file values already landed for this domain in
        # bronze.sland_mdm, so re-running the notebook (or new files dropped
        # alongside old ones) does not re-ingest/duplicate previously processed files.
        df_already_landed = (
            spark.table("bronze.sland_mdm")
                 .filter(F.col("domain_code") == domain_code)
                 .select("source_file")
                 .distinct()
        )

        df_new = df_raw.join(df_already_landed, on="source_file", how="left_anti")

        record_count  = df_new.count()
        skipped_count = raw_file_count - record_count

        print(f">>> Already-processed files skipped : {skipped_count}")
        print(f">>> New files to land               : {record_count}")

    except Exception as e:
        raise RuntimeError(f"[{domain_code}] Failed reading source path [{source_path}]: {e}")

    # ---- If nothing new, do NOT open a batch — batch_id must not increase ----
    if record_count == 0:
        print(f">>> No new files for domain {domain_code} — all files already processed. "
              f"Skipping batch creation (batch_id will not increment).")
        run_summary.append((domain_code, None, 0))
        continue

    # ---- Open batch (only now, since we know there's new data) ---------------
    spark.sql(f"""
        INSERT INTO bronze.batch_control
            (batch_name, domain_code, source_path, status, record_count, creation_dt, updation_dt)
        VALUES
            ('{BATCH_NAME}', '{domain_code}', '{source_path}', 'STARTED', 0,
             current_timestamp(), current_timestamp())
    """)

    batch_id = spark.sql(f"""
        SELECT MAX(batch_id) AS bid
        FROM   bronze.batch_control
        WHERE  domain_code = '{domain_code}'
        AND    status = 'STARTED'
    """).collect()[0]["bid"]

    print(f">>> Batch opened — batch_id = {batch_id}")

    df_land = df_new.withColumn("batch_id", F.lit(batch_id).cast("bigint")) \
                     .select(
                         "batch_id", "domain_code", "source_code", "root_entity_name",
                         "raw_payload", "source_file", "document_uuid", "record_uuid",
                         "creation_dt", "updation_dt"
                     )

    # ---- Write ---------------------------------------------------------
    try:
        (df_land
         .write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "false")
         .saveAsTable("bronze.sland_mdm"))

        print(f">>> Written {record_count} rows to bronze.sland_mdm for domain {domain_code}")

    except Exception as e:
        spark.sql(f"""
            UPDATE bronze.batch_control
            SET    status      = 'FAILED',
                   updation_dt = current_timestamp()
            WHERE  batch_id    = {batch_id}
        """)
        raise RuntimeError(f"[{domain_code}] Failed writing to bronze.sland_mdm: {e}")

    # ---- Close batch -----------------------------------------------------
    spark.sql(f"""
        UPDATE bronze.batch_control
        SET    status       = 'COMPLETED',
               record_count = {record_count},
               updation_dt  = current_timestamp()
        WHERE  batch_id     = {batch_id}
    """)

    print(f">>> Batch {batch_id} COMPLETED — {record_count} records landed for {domain_code}.")

    run_summary.append((domain_code, batch_id, record_count))

print("\n=== Run summary ===")
for d, b, c in run_summary:
    if b is None:
        print(f"  {d}: no new files — batch not created, records=0")
    else:
        print(f"  {d}: batch_id={b}, records={c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 — QA: Verify Landing Across All Domains

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Batch control history, all domains
# MAGIC SELECT
# MAGIC     batch_id,
# MAGIC     batch_name,
# MAGIC     domain_code,
# MAGIC     status,
# MAGIC     record_count,
# MAGIC     creation_dt,
# MAGIC     updation_dt,
# MAGIC     ROUND((unix_timestamp(updation_dt) - unix_timestamp(creation_dt)), 1) AS duration_secs
# MAGIC FROM   bronze.batch_control
# MAGIC ORDER  BY batch_id DESC
# MAGIC LIMIT  20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- delete from  bronze.batch_control
# MAGIC -- where batch_id in (4,5)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview latest completed batch per domain
# MAGIC SELECT
# MAGIC     s.sln_id,
# MAGIC     s.batch_id,
# MAGIC     s.domain_code,
# MAGIC     s.source_code,
# MAGIC     s.root_entity_name,
# MAGIC     s.document_uuid,
# MAGIC     s.record_uuid,
# MAGIC     s.source_file,
# MAGIC     LEFT(s.raw_payload, 150) AS payload_preview,
# MAGIC     s.creation_dt
# MAGIC FROM   bronze.sland_mdm s
# MAGIC JOIN (
# MAGIC     SELECT domain_code, MAX(batch_id) AS batch_id
# MAGIC     FROM   bronze.batch_control
# MAGIC     WHERE  status = 'COMPLETED'
# MAGIC     GROUP  BY domain_code
# MAGIC ) latest
# MAGIC   ON s.domain_code = latest.domain_code
# MAGIC AND s.batch_id     = latest.batch_id
# MAGIC ORDER  BY s.domain_code, s.sln_id
# MAGIC LIMIT  50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity: no null record_uuid per domain (every payload must resolve one)
# MAGIC SELECT
# MAGIC     domain_code,
# MAGIC     COUNT(*)                                              AS total_records,
# MAGIC     SUM(CASE WHEN record_uuid IS NULL THEN 1 ELSE 0 END)  AS missing_record_uuid,
# MAGIC     SUM(CASE WHEN document_uuid IS NULL THEN 1 ELSE 0 END) AS missing_document_uuid,
# MAGIC     COUNT(DISTINCT record_uuid)                           AS distinct_uuids,
# MAGIC     COUNT(DISTINCT source_file)                           AS distinct_files
# MAGIC FROM bronze.sland_mdm
# MAGIC GROUP BY domain_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest batch_id per domain, for downstream steps to pick up
# MAGIC SELECT domain_code, MAX(batch_id) AS latest_completed_batch_id
# MAGIC FROM   bronze.batch_control
# MAGIC WHERE  status = 'COMPLETED'
# MAGIC GROUP  BY domain_code;