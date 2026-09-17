# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Step 3 — Bronze → Silver: Data Cleansing & Match Key Generation
# MAGIC **Pipeline:** Dynamic MDM Platform | **Layer:** Silver
# MAGIC
# MAGIC Reads from the multi-domain Bronze tables produced by Step 2
# MAGIC (`bronze.sourceconsumer` / `sourcemedia` / `sourcephone` / `sourceaddress` /
# MAGIC `sourceoptin`, all carrying `domain_code`), and produces cleansed Silver
# MAGIC equivalents — **per domain, isolated** — ready for domain-scoped match &
# MAGIC merge in Step 4.
# MAGIC
# MAGIC ## Silver Table Structure (mirrors Bronze naming convention)
# MAGIC
# MAGIC | Silver Table | PK | FK | Source |
# MAGIC |---|---|---|---|
# MAGIC | `silver.sourceconsumer` | `src_id` | same value as `bronze.sourceconsumer.src_id` (winning row) | Cleansed root + name match keys |
# MAGIC | `silver.sourcemedia` | `srme_id` | `srme_src_id` → silver.sourceconsumer.src_id | Cleansed email + quality code |
# MAGIC | `silver.sourcephone` | `srph_id` | `srph_src_id` → silver.sourceconsumer.src_id | Cleansed phone + quality code |
# MAGIC | `silver.sourceaddress` | `srad_id` | `srad_src_id` → silver.sourceconsumer.src_id | Cleansed address + match key |
# MAGIC | `silver.sourceoptin` | `srop_id` | `srop_src_id` → silver.sourceconsumer.src_id | Opt-in preferences (pass-through) |
# MAGIC
# MAGIC ## 0 — Truncate-and-load semantics
# MAGIC Every time this notebook runs, **all 5 Silver tables are fully
# MAGIC truncated and reloaded** from each domain's current latest completed
# MAGIC Bronze batch — this is a snapshot layer, not an accumulating history.
# MAGIC There is no "already processed, skip" gate here (that pattern still
# MAGIC applies to Step 1/Step 2, which are append-only landing/parsing logs).
# MAGIC Running Step 3 twice in a row with no new Bronze batch simply reloads
# MAGIC the same result; running it after a new batch completes reloads the
# MAGIC newest snapshot. `silver.parse_control` is kept as a **run history
# MAGIC log** for auditing (what ran, when, how many rows) — it no longer
# MAGIC blocks reprocessing.
# MAGIC
# MAGIC ## 1 — Latest-batch-only + duplicate resolution
# MAGIC For each domain, only the **latest completed Bronze parse batch**
# MAGIC (`bronze.parse_control`, status = `COMPLETED`) is read — matching the
# MAGIC same idempotency pattern used in Step 2.
# MAGIC
# MAGIC Within that batch, the same source entity can legitimately appear more
# MAGIC than once — e.g. the same person's JSON file landed twice, or two
# MAGIC different files for the same `source_entity_id` arrived in one run.
# MAGIC **Duplicate key** = `(domain_code, source_code, source_entity_id)`,
# MAGIC falling back to `record_uuid` when `source_entity_id` is null.
# MAGIC When duplicates are found, **only the row with `MAX(src_id)`** — the
# MAGIC most recently parsed profile — is carried into Silver; the loser
# MAGIC row(s) and *all of their child records* (media/phone/address/optin)
# MAGIC are dropped, since the winning `src_id` is the complete, authoritative
# MAGIC profile for that entity in this batch.
# MAGIC
# MAGIC ## 2 — Match keys generated
# MAGIC | Key | Table | Logic |
# MAGIC |---|---|---|
# MAGIC | `first_name_match_key` | sourceconsumer | UPPER, alphanumeric-only `local_first_name` |
# MAGIC | `last_name_match_key` | sourceconsumer | UPPER, alphanumeric-only `local_last_name` |
# MAGIC | `full_name_match_key` | sourceconsumer | UPPER, alphanumeric-only 3-tier derived full name |
# MAGIC | `email_match_key` | sourcemedia | LOWER TRIM email address |
# MAGIC | `phone_match_key` | sourcephone | Digits-only phone number |
# MAGIC | `address_match_key` | sourceaddress | UPPER `ADDRESS1\|CITY\|PROVINCE\|POSTALCODE` concat |
# MAGIC
# MAGIC ## 3 — Contact quality / validity
# MAGIC | Table | Quality column | Values | Null handling |
# MAGIC |---|---|---|---|
# MAGIC | `silver.sourcephone` | `srph_quality_code` | `VALID` / `INVALID` | null/empty phone → `INVALID`, desc = "Phone number is null" |
# MAGIC | `silver.sourcemedia` | `srme_quality_code` | `VALID` / `INVALID` | null/empty email → `INVALID`, desc = "Email address is null" |
# MAGIC
# MAGIC Both tables also carry a boolean `valid_contact` (derived from the
# MAGIC quality code) and a `valid_contact_desc` explaining the specific reason,
# MAGIC so downstream survivorship logic can filter on either the code or the
# MAGIC human-readable reason.
# MAGIC
# MAGIC ## Name preference (3-tier), same convention as before
# MAGIC | Tier | Condition | Outcome |
# MAGIC |---|---|---|
# MAGIC | 1st | Both `local_first_name` AND `local_last_name` present | `FIRSTNAME LASTNAME` |
# MAGIC | 2nd | Only one of them present | Use whichever is available |
# MAGIC | 3rd | Both null/sentinel/empty | Use `local_full_name` as-is |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Use Catalog / Create Silver Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG MDM_Platform;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Silver / cleansed layer for Dynamic MDM Platform, all domains';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — DDL: silver.sourceconsumer
# MAGIC `src_id` is NOT identity — it is copied directly from the winning
# MAGIC (`MAX(src_id)`) Bronze row for that entity, so Bronze and Silver share
# MAGIC the same natural key without a lookup/join downstream.

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog mdm_platform;
# MAGIC
# MAGIC select * from silver.sourceconsumer;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.sourceconsumer (
# MAGIC     src_id                      BIGINT      NOT NULL,   -- PK = winning bronze.sourceconsumer.src_id
# MAGIC     sln_id                      BIGINT,
# MAGIC     batch_id                    BIGINT,
# MAGIC     domain_code                 STRING,
# MAGIC     source_code                 STRING,
# MAGIC     root_entity_name            STRING,
# MAGIC     record_uuid                 STRING,
# MAGIC     dedup_key                   STRING,                 -- (domain_code, source_code, source_entity_id/record_uuid)
# MAGIC     duplicate_count             INT,                    -- how many bronze rows collapsed into this one
# MAGIC     -- ── Header (pass-through) ────────────────────────────────────────────
# MAGIC     action                      STRING,
# MAGIC     document_timestamp          TIMESTAMP,
# MAGIC     document_uuid               STRING,
# MAGIC     -- ── SourceSystem (pass-through) ──────────────────────────────────────
# MAGIC     source_system_code          STRING,
# MAGIC     source_timestamp            TIMESTAMP,
# MAGIC     affiliate_code              STRING,
# MAGIC     market_code                 STRING,
# MAGIC     division_code               STRING,
# MAGIC     brand_code                  STRING,
# MAGIC     source_entity_id            STRING,
# MAGIC     -- ── BestRecord (pass-through) ────────────────────────────────────────
# MAGIC     universal_key               STRING,
# MAGIC     -- ── PersonalData (cleansed) ──────────────────────────────────────────
# MAGIC     salutation                  STRING,
# MAGIC     local_first_name            STRING,
# MAGIC     local_last_name             STRING,
# MAGIC     local_full_name             STRING,
# MAGIC     birth_day                   INT,
# MAGIC     birth_month                 INT,
# MAGIC     birth_year                  INT,
# MAGIC     birth_date                  DATE,                   -- derived from birth_day/month/year when all present
# MAGIC     reg_date                    TIMESTAMP,
# MAGIC     reg_touchpoint_code         STRING,
# MAGIC     preferred_touchpoint_code   STRING,
# MAGIC     assigned_staff_code         STRING,
# MAGIC     written_language_code       STRING,
# MAGIC     country_code_iso3           STRING,
# MAGIC     gender_code                 STRING,
# MAGIC     active_flag                 BOOLEAN,
# MAGIC     first_engagement_date       TIMESTAMP,
# MAGIC     marital_status_code         STRING,
# MAGIC     nationality_code_iso3       STRING,
# MAGIC     patient_category_code       STRING,
# MAGIC     -- ── Match keys ───────────────────────────────────────────────────────
# MAGIC     first_name_match_key        STRING,
# MAGIC     last_name_match_key         STRING,
# MAGIC     full_name_match_key         STRING,
# MAGIC     -- ── Data quality ─────────────────────────────────────────────────────
# MAGIC     is_name_valid               BOOLEAN,
# MAGIC     -- ── Audit ────────────────────────────────────────────────────────────
# MAGIC     creation_dt                 TIMESTAMP,
# MAGIC     updation_dt                 TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
# MAGIC COMMENT 'Silver cleansed consumer/patient master, all domains — src_id PK mirrors bronze.sourceconsumer';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — DDL: silver.sourcemedia
# MAGIC `srme_quality_code` = `VALID` / `INVALID`, with explicit null handling.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.sourcemedia (
# MAGIC     srme_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     srme_src_id         BIGINT      NOT NULL,   -- FK → silver.sourceconsumer.src_id
# MAGIC     srme_bronze_id      BIGINT,                 -- FK → bronze.sourcemedia.srme_id (traceability)
# MAGIC     type_code           STRING,
# MAGIC     email_address       STRING,
# MAGIC     email_match_key     STRING,
# MAGIC     is_primary          BOOLEAN,
# MAGIC     srme_quality_code   STRING,                 -- VALID | INVALID
# MAGIC     valid_contact       BOOLEAN,
# MAGIC     valid_contact_desc  STRING,
# MAGIC     creation_dt         TIMESTAMP,
# MAGIC     updation_dt         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver cleansed email contacts, all domains — srme_id PK, srme_src_id FK, srme_quality_code = VALID|INVALID';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — DDL: silver.sourcephone
# MAGIC `srph_quality_code` = `VALID` / `INVALID`, with explicit null handling.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.sourcephone (
# MAGIC     srph_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     srph_src_id         BIGINT      NOT NULL,   -- FK → silver.sourceconsumer.src_id
# MAGIC     srph_bronze_id      BIGINT,                 -- FK → bronze.sourcephone.srph_id (traceability)
# MAGIC     type_code           STRING,
# MAGIC     phone_number        STRING,
# MAGIC     phone_match_key     STRING,
# MAGIC     is_primary          BOOLEAN,
# MAGIC     srph_quality_code   STRING,                 -- VALID | INVALID
# MAGIC     valid_contact       BOOLEAN,
# MAGIC     valid_contact_desc  STRING,
# MAGIC     creation_dt         TIMESTAMP,
# MAGIC     updation_dt         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver cleansed phone contacts, all domains — srph_id PK, srph_src_id FK, srph_quality_code = VALID|INVALID';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 — DDL: silver.sourceaddress

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.sourceaddress (
# MAGIC     srad_id                     BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     srad_src_id                 BIGINT      NOT NULL,   -- FK → silver.sourceconsumer.src_id
# MAGIC     srad_bronze_id              BIGINT,                 -- FK → bronze.sourceaddress.srad_id
# MAGIC     type_code                   STRING,
# MAGIC     address1                    STRING,
# MAGIC     address2                    STRING,
# MAGIC     address3                    STRING,
# MAGIC     city_description_local      STRING,
# MAGIC     province_description_local  STRING,
# MAGIC     country_code_iso3           STRING,
# MAGIC     postal_code                 STRING,
# MAGIC     is_primary                  BOOLEAN,
# MAGIC     address_match_key           STRING,
# MAGIC     is_valid_address            BOOLEAN,
# MAGIC     valid_contact               BOOLEAN,
# MAGIC     valid_contact_desc          STRING,
# MAGIC     creation_dt                 TIMESTAMP,
# MAGIC     updation_dt                 TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver cleansed address contacts, all domains — srad_id PK, srad_src_id FK';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 — DDL: silver.sourceoptin

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.sourceoptin (
# MAGIC     srop_id                     BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     srop_src_id                 BIGINT      NOT NULL,   -- FK → silver.sourceconsumer.src_id
# MAGIC     srop_bronze_id              BIGINT,                 -- FK → bronze.sourceoptin.srop_id
# MAGIC     communication_channel_code  STRING,
# MAGIC     opt_in_flag                 BOOLEAN,
# MAGIC     opt_in_timestamp            TIMESTAMP,
# MAGIC     creation_dt                 TIMESTAMP,
# MAGIC     updation_dt                 TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver opt-in preferences, all domains — srop_id PK, srop_src_id FK';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 — DDL: silver.parse_control
# MAGIC A **run history log**, not an idempotency gate — since Silver tables
# MAGIC are truncated and fully reloaded on every run, there's nothing to
# MAGIC "skip". This table records what ran, when, and how many rows/duplicates
# MAGIC were involved, for auditing.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.parse_control (
# MAGIC     parse_id      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     domain_code   STRING      NOT NULL,
# MAGIC     batch_id      BIGINT      NOT NULL,
# MAGIC     status        STRING,                 -- STARTED | COMPLETED | FAILED
# MAGIC     record_count  BIGINT,
# MAGIC     duplicate_count BIGINT,
# MAGIC     creation_dt   TIMESTAMP,
# MAGIC     updation_dt   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Run history log for Silver cleansing — NOT a skip gate; Silver tables are truncated & reloaded every run';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 — Cleansing & Validation Helpers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

NULL_SENTINELS = ["-", "", "N/A", "UNKNOWN", "NULL", "NA"]

def clean_name_col(c):
    """NULL-out sentinel name values, then trim."""
    return F.when(
        F.upper(F.trim(c)).isin(NULL_SENTINELS), F.lit(None)
    ).otherwise(F.trim(c))

def clean_str_upper(c):
    """UPPER TRIM, strip non-alphanumeric except spaces — used for name match keys."""
    return F.upper(F.trim(F.regexp_replace(F.coalesce(c, F.lit("")), r"[^A-Za-z0-9 ]", "")))

def clean_email(c):
    """LOWER TRIM email."""
    return F.lower(F.trim(c))

def clean_phone_digits(c):
    """Digits only."""
    return F.regexp_replace(F.trim(F.coalesce(c, F.lit(""))), r"[^0-9]", "")

# ── Regex-based validity checks ───────────────────────────────────────────────
EMAIL_REGEX = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"

def is_valid_email_expr(col_name):
    return F.col(col_name).rlike(EMAIL_REGEX)

def is_valid_phone_expr(col_name):
    # Digits-only phone expected to be 7–15 digits (covers local + E.164-length numbers)
    return (F.length(F.col(col_name)) >= 7) & (F.length(F.col(col_name)) <= 15)

# ── 3-tier name preference ────────────────────────────────────────────────────
def derive_full_name(fn_col, ln_col, full_col):
    fn_ok  = fn_col.isNotNull()
    ln_ok  = ln_col.isNotNull()
    either = fn_ok | ln_ok
    return (
        F.when(fn_ok & ln_ok, F.concat_ws(" ", fn_col, ln_col))
         .when(either,        F.concat_ws(" ", fn_col, ln_col))
         .otherwise(full_col)
    )

print(">>> Cleansing helpers loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.9 — Resolve Latest Completed Bronze Batch, Per Domain
# MAGIC Every active domain's latest `COMPLETED` Bronze parse batch (from
# MAGIC `bronze.parse_control`) is picked up **unconditionally** on every run —
# MAGIC there's no skip check here, since the Silver tables get truncated and
# MAGIC fully reloaded below regardless.

# COMMAND ----------

domains_to_process = spark.sql("""
    SELECT domain_code, MAX(batch_id) AS batch_id
    FROM   bronze.parse_control
    WHERE  status = 'COMPLETED'
    GROUP  BY domain_code
""").collect()

print(">>> Latest completed Bronze parse batch per domain (will reload all of these):")
for row in domains_to_process:
    print(f"    {row['domain_code']}: batch_id = {row['batch_id']}")

if not domains_to_process:
    print("\n>>> No completed Bronze batches found for any domain — nothing to load. "
          "Silver tables will NOT be truncated (nothing to replace them with).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.10 — Truncate All Silver Tables (Full Reload Each Run)
# MAGIC Only runs when there's at least one domain with a completed Bronze
# MAGIC batch to reload — this avoids wiping Silver to empty on a run where
# MAGIC Bronze has nothing new (or nothing at all) to offer yet.
# MAGIC
# MAGIC Note: `TRUNCATE TABLE` clears rows but does **not** reset the
# MAGIC `GENERATED ALWAYS AS IDENTITY` counters on `srme_id` / `srph_id` /
# MAGIC `srad_id` / `srop_id` — those will keep counting up across runs even
# MAGIC though the underlying data was wiped. That's expected and harmless;
# MAGIC it just means surrogate key values won't restart at 1 after a reload.

# COMMAND ----------

if domains_to_process:
    for tbl in [
        "silver.sourceconsumer",
        "silver.sourcemedia",
        "silver.sourcephone",
        "silver.sourceaddress",
        "silver.sourceoptin",
    ]:
        spark.sql(f"TRUNCATE TABLE {tbl}")
        print(f">>> Truncated {tbl}")
else:
    print(">>> Skipping truncate — no domains to reload.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.11 — Per-Domain Cleansing Function

# COMMAND ----------

def _cleanse_domain_batch(row):

    domain_code = row["domain_code"]
    batch_id    = row["batch_id"]

    print(f"\n=== Cleansing domain: {domain_code} | batch_id: {batch_id} ===")

    # ---- Open silver.parse_control record ------------------------------------
    spark.sql(f"""
        INSERT INTO silver.parse_control
            (domain_code, batch_id, status, record_count, duplicate_count, creation_dt, updation_dt)
        VALUES
            ('{domain_code}', {batch_id}, 'STARTED', 0, 0,
             current_timestamp(), current_timestamp())
    """)

    # ---- Pull this domain's latest-batch Bronze consumer rows ----------------
    df_bronze_con = spark.sql(f"""
        SELECT *
        FROM   bronze.sourceconsumer
        WHERE  domain_code = '{domain_code}'
        AND    batch_id    = {batch_id}
    """)

    raw_count = df_bronze_con.count()
    print(f">>> Bronze rows read for this batch: {raw_count}")

    # ---- Duplicate resolution: keep MAX(src_id) per dedup key -----------------
    # dedup_key = (domain_code, source_code, source_entity_id) — falls back to
    # record_uuid when source_entity_id is null, so every row still gets a key.
    df_bronze_con = df_bronze_con.withColumn(
        "dedup_key",
        F.concat_ws("|",
            F.col("domain_code"),
            F.coalesce(F.col("source_code"), F.lit("")),
            F.coalesce(F.col("source_entity_id"), F.col("record_uuid"), F.lit(""))
        )
    )

    w_dedup = Window.partitionBy("dedup_key").orderBy(F.col("src_id").desc())

    df_bronze_con = (
        df_bronze_con
        .withColumn("_rn", F.row_number().over(w_dedup))
        .withColumn("duplicate_count", F.count("*").over(Window.partitionBy("dedup_key")))
    )

    df_winners = df_bronze_con.filter(F.col("_rn") == 1).drop("_rn")

    winner_count = df_winners.count()
    duplicate_rows_dropped = raw_count - winner_count
    print(f">>> Distinct entities after dedup: {winner_count} "
          f"({duplicate_rows_dropped} duplicate row(s) collapsed via MAX(src_id))")

    # Set of winning src_id values — child tables (media/phone/address/optin)
    # are filtered to only these, so loser rows' children are dropped too.
    winner_src_ids_df = df_winners.select("src_id")

    # ================================================================
    # 3.10a — silver.sourceconsumer
    # ================================================================
    df_silver_con = (
        df_winners
        .withColumn("local_first_name", clean_name_col(F.col("local_first_name")))
        .withColumn("local_last_name",  clean_name_col(F.col("local_last_name")))
        .withColumn("local_full_name",  clean_name_col(F.col("local_full_name")))
        .withColumn("local_full_name",
            derive_full_name(
                F.col("local_first_name"),
                F.col("local_last_name"),
                F.col("local_full_name")
            )
        )
        .withColumn("first_name_match_key", clean_str_upper(F.col("local_first_name")))
        .withColumn("last_name_match_key",  clean_str_upper(F.col("local_last_name")))
        .withColumn("full_name_match_key",  clean_str_upper(F.col("local_full_name")))
        .withColumn("is_name_valid",
            F.col("local_first_name").isNotNull() | F.col("local_last_name").isNotNull()
        )
        # ── Derived birth_date: only when all three parts are present and valid ──
        .withColumn("birth_date",
            F.when(
                F.col("birth_day").isNotNull() &
                F.col("birth_month").isNotNull() &
                F.col("birth_year").isNotNull(),
                F.to_date(
                    F.concat_ws("-",
                        F.col("birth_year").cast("string"),
                        F.lpad(F.col("birth_month").cast("string"), 2, "0"),
                        F.lpad(F.col("birth_day").cast("string"), 2, "0")
                    ), "yyyy-MM-dd"
                )
            ).otherwise(F.lit(None).cast("date"))
        )
        .withColumn("creation_dt", F.current_timestamp())
        .withColumn("updation_dt", F.current_timestamp())
    )

    (df_silver_con
     .write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable("silver.sourceconsumer"))

    print(f">>> silver.sourceconsumer — {df_silver_con.count()} rows written.")

    # ================================================================
    # 3.10b — silver.sourcemedia (email quality validation)
    # ================================================================
    df_bronze_media_raw = spark.table("bronze.sourcemedia").alias("bm")
    df_bronze_media = (
        df_bronze_media_raw
        .join(winner_src_ids_df.alias("w"), F.col("bm.srme_src_id") == F.col("w.src_id"), "inner")
        .select(
            F.col("bm.srme_id").alias("srme_bronze_id"),
            F.col("bm.srme_src_id"), F.col("bm.type_code"),
            F.col("bm.email_address"), F.col("bm.is_primary")
        )
    )

    df_silver_media = (
        df_bronze_media
        .withColumn("email_address", clean_email(F.col("email_address")))
        .withColumn("email_match_key", F.col("email_address"))
        .withColumn("srme_quality_code",
            F.when(F.col("email_address").isNull() | (F.col("email_address") == ""), "INVALID")
             .when(~is_valid_email_expr("email_address"), "INVALID")
             .otherwise("VALID")
        )
        .withColumn("valid_contact", F.col("srme_quality_code") == "VALID")
        .withColumn("valid_contact_desc",
            F.when(F.col("email_address").isNull() | (F.col("email_address") == ""),
                   "Email address is null")
             .when(~is_valid_email_expr("email_address"),
                   "Invalid email — does not match email format")
             .otherwise("Valid email")
        )
        .withColumn("creation_dt", F.current_timestamp())
        .withColumn("updation_dt", F.current_timestamp())
        .select(
            "srme_src_id", "srme_bronze_id", "type_code", "email_address",
            "email_match_key", "is_primary", "srme_quality_code",
            "valid_contact", "valid_contact_desc", "creation_dt", "updation_dt"
        )
    )

    (df_silver_media
     .write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable("silver.sourcemedia"))

    print(f">>> silver.sourcemedia — {df_silver_media.count()} rows written.")

    # ================================================================
    # 3.10c — silver.sourcephone (phone quality validation)
    # ================================================================
    df_bronze_phone_raw = spark.table("bronze.sourcephone").alias("bp")
    df_bronze_phone = (
        df_bronze_phone_raw
        .join(winner_src_ids_df.alias("w"), F.col("bp.srph_src_id") == F.col("w.src_id"), "inner")
        .select(
            F.col("bp.srph_id").alias("srph_bronze_id"),
            F.col("bp.srph_src_id"), F.col("bp.type_code"),
            F.col("bp.phone_number"), F.col("bp.is_primary")
        )
    )

    df_silver_phone = (
        df_bronze_phone
        .withColumn("phone_number", clean_phone_digits(F.col("phone_number")))
        .withColumn("phone_match_key", F.col("phone_number"))
        .withColumn("srph_quality_code",
            F.when(F.col("phone_number").isNull() | (F.col("phone_number") == ""), "INVALID")
             .when(~is_valid_phone_expr("phone_number"), "INVALID")
             .otherwise("VALID")
        )
        .withColumn("valid_contact", F.col("srph_quality_code") == "VALID")
        .withColumn("valid_contact_desc",
            F.when(F.col("phone_number").isNull() | (F.col("phone_number") == ""),
                   "Phone number is null")
             .when(~is_valid_phone_expr("phone_number"),
                   "Invalid phone — digit count out of 7-15 range")
             .otherwise("Valid phone")
        )
        .withColumn("creation_dt", F.current_timestamp())
        .withColumn("updation_dt", F.current_timestamp())
        .select(
            "srph_src_id", "srph_bronze_id", "type_code", "phone_number",
            "phone_match_key", "is_primary", "srph_quality_code",
            "valid_contact", "valid_contact_desc", "creation_dt", "updation_dt"
        )
    )

    (df_silver_phone
     .write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable("silver.sourcephone"))

    print(f">>> silver.sourcephone — {df_silver_phone.count()} rows written.")

    # ================================================================
    # 3.10d — silver.sourceaddress
    # ================================================================
    df_bronze_addr_raw = spark.table("bronze.sourceaddress").alias("ba")
    df_bronze_addr = (
        df_bronze_addr_raw
        .join(winner_src_ids_df.alias("w"), F.col("ba.srad_src_id") == F.col("w.src_id"), "inner")
        .select(
            F.col("ba.srad_id").alias("srad_bronze_id"),
            F.col("ba.srad_src_id"), F.col("ba.type_code"),
            F.col("ba.address1"), F.col("ba.address2"), F.col("ba.address3"),
            F.col("ba.city_description_local"), F.col("ba.province_description_local"),
            F.col("ba.country_code_iso3"), F.col("ba.postal_code"), F.col("ba.is_primary")
        )
    )

    df_silver_addr = (
        df_bronze_addr
        .withColumn("city_description_local", F.upper(F.trim(F.col("city_description_local"))))
        .withColumn("province_description_local", F.upper(F.trim(F.col("province_description_local"))))
        .withColumn("address_match_key",
            F.upper(F.concat_ws("|",
                F.trim(F.coalesce(F.col("address1"), F.lit(""))),
                F.trim(F.coalesce(F.col("city_description_local"), F.lit(""))),
                F.trim(F.coalesce(F.col("province_description_local"), F.lit(""))),
                F.trim(F.coalesce(F.col("postal_code"), F.lit("")))
            ))
        )
        .withColumn("is_valid_address",
            F.col("city_description_local").isNotNull() &
            F.col("province_description_local").isNotNull()
        )
        .withColumn("valid_contact",
            F.col("city_description_local").isNotNull() &
            F.col("province_description_local").isNotNull()
        )
        .withColumn("valid_contact_desc",
            F.when(
                F.col("city_description_local").isNull() & F.col("province_description_local").isNull(),
                "City and province missing"
            )
            .when(F.col("city_description_local").isNull(),     "City missing")
            .when(F.col("province_description_local").isNull(), "Province missing")
            .otherwise("Valid address")
        )
        .withColumn("creation_dt", F.current_timestamp())
        .withColumn("updation_dt", F.current_timestamp())
        .select(
            "srad_src_id", "srad_bronze_id", "type_code", "address1", "address2", "address3",
            "city_description_local", "province_description_local", "country_code_iso3",
            "postal_code", "is_primary", "address_match_key", "is_valid_address",
            "valid_contact", "valid_contact_desc", "creation_dt", "updation_dt"
        )
    )

    (df_silver_addr
     .write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable("silver.sourceaddress"))

    print(f">>> silver.sourceaddress — {df_silver_addr.count()} rows written.")

    # ================================================================
    # 3.10e — silver.sourceoptin (pass-through)
    # ================================================================
    df_bronze_optin_raw = spark.table("bronze.sourceoptin").alias("bo")
    df_bronze_optin = (
        df_bronze_optin_raw
        .join(winner_src_ids_df.alias("w"), F.col("bo.srop_src_id") == F.col("w.src_id"), "inner")
        .select(
            F.col("bo.srop_id").alias("srop_bronze_id"),
            F.col("bo.srop_src_id"), F.col("bo.communication_channel_code"),
            F.col("bo.opt_in_flag"), F.col("bo.opt_in_timestamp")
        )
    )

    df_silver_optin = (
        df_bronze_optin
        .withColumn("creation_dt", F.current_timestamp())
        .withColumn("updation_dt", F.current_timestamp())
        .select(
            "srop_src_id", "srop_bronze_id", "communication_channel_code",
            "opt_in_flag", "opt_in_timestamp", "creation_dt", "updation_dt"
        )
    )

    (df_silver_optin
     .write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable("silver.sourceoptin"))

    print(f">>> silver.sourceoptin — {df_silver_optin.count()} rows written.")

    return winner_count, duplicate_rows_dropped

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.12 — Run Cleansing for Each Domain's Latest Batch

# COMMAND ----------

for row in domains_to_process:

    domain_code = row["domain_code"]
    batch_id    = row["batch_id"]

    try:
        final_count, dup_dropped = _cleanse_domain_batch(row)

        spark.sql(f"""
            UPDATE silver.parse_control
            SET    status          = 'COMPLETED',
                   record_count    = {final_count},
                   duplicate_count = {dup_dropped},
                   updation_dt     = current_timestamp()
            WHERE  domain_code     = '{domain_code}'
            AND    batch_id        = {batch_id}
            AND    status          = 'STARTED'
        """)
        print(f">>> silver.parse_control COMPLETED — {domain_code} batch_id={batch_id}, "
              f"{final_count} entities written, {dup_dropped} duplicates collapsed")

    except Exception as e:
        spark.sql(f"""
            UPDATE silver.parse_control
            SET    status      = 'FAILED',
                   updation_dt = current_timestamp()
            WHERE  domain_code = '{domain_code}'
            AND    batch_id    = {batch_id}
            AND    status      = 'STARTED'
        """)
        raise RuntimeError(f"[{domain_code}] Step 3 cleansing failed for batch_id={batch_id}: {e}")

print("\n>>> Step 3 complete — all Silver tables cleansed and loaded for all domains.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.13 — QA Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver run history (informational only — tables are truncated &
# MAGIC -- fully reloaded every run, so this is not a dedup/skip check)
# MAGIC SELECT parse_id, domain_code, batch_id, status, record_count, duplicate_count,
# MAGIC        creation_dt, updation_dt
# MAGIC FROM   silver.parse_control
# MAGIC ORDER  BY parse_id DESC
# MAGIC LIMIT  20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts per Silver table, by domain
# MAGIC SELECT domain_code, COUNT(*) AS row_count, MAX(batch_id) AS latest_batch
# MAGIC FROM   silver.sourceconsumer
# MAGIC GROUP  BY domain_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver.sourcemedia'  AS table_name, COUNT(*) AS row_count FROM silver.sourcemedia
# MAGIC UNION ALL
# MAGIC SELECT 'silver.sourcephone',   COUNT(*) FROM silver.sourcephone
# MAGIC UNION ALL
# MAGIC SELECT 'silver.sourceaddress', COUNT(*) FROM silver.sourceaddress
# MAGIC UNION ALL
# MAGIC SELECT 'silver.sourceoptin',   COUNT(*) FROM silver.sourceoptin;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicate resolution audit — entities where more than one Bronze row collapsed
# MAGIC SELECT domain_code, dedup_key, src_id, duplicate_count, record_uuid, batch_id
# MAGIC FROM   silver.sourceconsumer
# MAGIC WHERE  duplicate_count > 1
# MAGIC ORDER  BY duplicate_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Name key quality, by domain
# MAGIC SELECT
# MAGIC     domain_code,
# MAGIC     COUNT(*)                                    AS total,
# MAGIC     SUM(CAST(is_name_valid AS INT))             AS valid_names,
# MAGIC     COUNT(first_name_match_key)                 AS has_first_name_key,
# MAGIC     COUNT(last_name_match_key)                  AS has_last_name_key,
# MAGIC     COUNT(full_name_match_key)                  AS has_full_name_key,
# MAGIC     COUNT(birth_date)                           AS has_birth_date
# MAGIC FROM silver.sourceconsumer
# MAGIC GROUP BY domain_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Phone validity breakdown (srph_quality_code)
# MAGIC SELECT type_code, is_primary, srph_quality_code, valid_contact_desc, COUNT(*) AS cnt
# MAGIC FROM   silver.sourcephone
# MAGIC GROUP  BY type_code, is_primary, srph_quality_code, valid_contact_desc
# MAGIC ORDER  BY type_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Email validity breakdown (srme_quality_code)
# MAGIC SELECT srme_quality_code, valid_contact_desc, COUNT(*) AS cnt
# MAGIC FROM   silver.sourcemedia
# MAGIC GROUP  BY srme_quality_code, valid_contact_desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FK integrity check: orphan child rows (must return 0 for all)
# MAGIC SELECT 'sourcemedia orphans'   AS check_name, COUNT(*) AS orphan_count
# MAGIC FROM   silver.sourcemedia m
# MAGIC WHERE  NOT EXISTS (SELECT 1 FROM silver.sourceconsumer c WHERE c.src_id = m.srme_src_id)
# MAGIC UNION ALL
# MAGIC SELECT 'sourcephone orphans',   COUNT(*)
# MAGIC FROM   silver.sourcephone p
# MAGIC WHERE  NOT EXISTS (SELECT 1 FROM silver.sourceconsumer c WHERE c.src_id = p.srph_src_id)
# MAGIC UNION ALL
# MAGIC SELECT 'sourceaddress orphans', COUNT(*)
# MAGIC FROM   silver.sourceaddress a
# MAGIC WHERE  NOT EXISTS (SELECT 1 FROM silver.sourceconsumer c WHERE c.src_id = a.srad_src_id)
# MAGIC UNION ALL
# MAGIC SELECT 'sourceoptin orphans',   COUNT(*)
# MAGIC FROM   silver.sourceoptin o
# MAGIC WHERE  NOT EXISTS (SELECT 1 FROM silver.sourceconsumer c WHERE c.src_id = o.srop_src_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview: consumer/patient root joined with phone + email + address, by domain
# MAGIC SELECT
# MAGIC     c.src_id, c.domain_code, c.source_entity_id,
# MAGIC     c.first_name_match_key, c.last_name_match_key, c.full_name_match_key, c.birth_date,
# MAGIC     ph.phone_match_key, ph.srph_quality_code,
# MAGIC     em.email_match_key, em.srme_quality_code,
# MAGIC     ad.address_match_key
# MAGIC FROM   silver.sourceconsumer c
# MAGIC LEFT   JOIN silver.sourcephone   ph ON ph.srph_src_id = c.src_id
# MAGIC LEFT   JOIN silver.sourcemedia   em ON em.srme_src_id = c.src_id
# MAGIC LEFT   JOIN silver.sourceaddress ad ON ad.srad_src_id = c.src_id
# MAGIC ORDER  BY c.domain_code, c.src_id;