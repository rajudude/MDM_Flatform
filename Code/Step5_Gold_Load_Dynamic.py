# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Step 5 — Load to Gold: Action-Routed, Domain-Aware, One Row Per MDM Key
# MAGIC **Pipeline:** Dynamic MDM Platform | **Layer:** Gold
# MAGIC
# MAGIC ## Fix applied vs. previous draft — table names now match Step 4 exactly
# MAGIC The previous draft of this notebook invented a **second, parallel set of
# MAGIC Gold table names** (`gold.sourceconsumer`, `gold.sourcemedia`,
# MAGIC `gold.sourcephone`, `gold.sourceaddress`) that never actually match what
# MAGIC **Step 4 creates and writes to** (`gold.sconsumer`, `gold.sconsumermedia`,
# MAGIC `gold.sconsumerphone`, `gold.sconsumeraddress` — see Step 4 §4.2). Running
# MAGIC both notebooks back-to-back would silently create two disconnected sets of
# MAGIC Gold tables. This version is corrected to **write into the exact tables
# MAGIC Step 4 created**, `ALTER TABLE ADD COLUMNS IF NOT EXISTS` onto them, and
# MAGIC add the one new child table (`gold.sconsumeroptin`) that Step 4 doesn't
# MAGIC create. **No logic from the working contributor/master-rollup design below
# MAGIC has changed** — only the table names it targets and the two new tracking
# MAGIC columns called out in the spec.
# MAGIC
# MAGIC | Step 4 table (unchanged, PK) | This notebook | Silver source |
# MAGIC |---|---|---|
# MAGIC | `gold.sconsumer` (`scon_id`) | extended w/ ALTER, action-routed MERGE target | `silver.sourceconsumer` |
# MAGIC | `gold.sconsumermedia` (`gsme_id`, FK `gsme_scon_id`) | extended w/ ALTER, action-routed MERGE target | `silver.sourcemedia` |
# MAGIC | `gold.sconsumerphone` (`gsph_id`, FK `gsph_scon_id`) | extended w/ ALTER, action-routed MERGE target | `silver.sourcephone` |
# MAGIC | `gold.sconsumeraddress` (`gsad_id`, FK `gsad_scon_id`) | extended w/ ALTER, action-routed MERGE target | `silver.sourceaddress` |
# MAGIC | `gold.sconsumeroptin` (`gsop_id`, FK `gsop_scon_id`) | **new**, created here | `silver.sourceoptin` |
# MAGIC
# MAGIC ## Two more fixes found checking against Step 4 / Step 1-3 schemas
# MAGIC 1. **Critical bug**: `df_eml`/`df_phn`/`df_addr` (the email/phone/address
# MAGIC    insert-or-update source frames) never selected an `is_deleted` column.
# MAGIC    `whenNotMatchedInsertAll()` leaves any target column missing from the
# MAGIC    source as `NULL`, not `FALSE` — since the Master rollup filters
# MAGIC    `is_deleted = FALSE`, every freshly inserted phone/email/address would
# MAGIC    have been silently excluded from `phone_master`/`email_master`/
# MAGIC    `address_master` (SQL `NULL = FALSE` isn't true) until a later UPDATE
# MAGIC    happened to touch that row. Fixed by explicitly selecting `FALSE AS
# MAGIC    is_deleted` in all three.
# MAGIC 2. **`domains_to_process` computed one global `MAX(batch_id)`** across every
# MAGIC    domain instead of per-domain — with independent per-domain batch
# MAGIC    sequences (Step 1's `bronze.batch_control`), this could silently skip
# MAGIC    any domain whose latest batch_id was lower than another's. Fixed to
# MAGIC    `GROUP BY domain_code`, matching Step 4 §4.7 exactly.
# MAGIC 3. Dropped `is_valid_email`/`is_valid_phone`/`is_valid_address` — these were
# MAGIC    always `NULL` (never actually computed) and fully duplicated Step 3's
# MAGIC    `valid_contact`/`valid_contact_desc`, which this notebook already uses.
# MAGIC
# MAGIC ## Why two layers, not one
# MAGIC Your spec asks for two things that can't both be true of a single table:
# MAGIC 1. CRUD dedup keyed on **`(source_entity_id, brand_code, source_system_code,
# MAGIC    market_code)`** — the natural key of a single source-system contribution.
# MAGIC 2. **One row per consumer under the MDM key** — the whole point of Step 4's
# MAGIC    match & merge is that TWO DIFFERENT natural keys (e.g. the same person's
# MAGIC    loyalty account under two different brands) can share one `universal_mdm_key`.
# MAGIC
# MAGIC If (1) and (2) were the same table, merging two brands' records for the same
# MAGIC person into one GID would silently drop whichever contribution didn't "win"
# MAGIC the row — and a later DELETE of just one of those source contributions would
# MAGIC have nowhere correct to land. So Gold is built in two layers:
# MAGIC
# MAGIC | Layer | Table | Grain | Purpose |
# MAGIC |---|---|---|---|
# MAGIC | **Contributor** | `gold.sconsumer` (+ media/phone/address/optin) | One row per natural key (`source_entity_id`+`brand_code`+`source_system_code`+`market_code`) | Action-routed CRUD, PII blankout on DELETE, full lineage back to both `silver.sourceconsumer.src_id` (`scon_src_id`, Step 4) and `bronze.sourceconsumer.srcc_id` (`scon_srcc_id`, **new**) |
# MAGIC | **Golden / Master** | `gold.consumer_master` (+ media/phone/address_master) | **One row per `universal_mdm_key`** | Survivorship rollup across all active contributors sharing a GID — this is "one row per consumer" |
# MAGIC
# MAGIC ## Action Routing (Contributor layer — updated per latest spec)
# MAGIC **Only `DELETE` blanks out PII.** `CREATE` and `CHANGE` are now merged into
# MAGIC one upsert path that is gated on `source_timestamp`: an update is applied
# MAGIC **only when the incoming record's `source_timestamp` is strictly newer than
# MAGIC the existing Gold row's** — this protects against an out-of-order / replayed
# MAGIC message overwriting a newer profile with stale data. A brand-new natural key
# MAGIC always inserts regardless of timestamp (there's nothing to compare against).
# MAGIC
# MAGIC | `action` | Existing contributor row (by natural key)? | `source_timestamp` newer? | Outcome |
# MAGIC |---|---|---|---|
# MAGIC | `CREATE` | No | n/a | **INSERT** |
# MAGIC | `CREATE` | Yes | Yes | **UPDATE** all fields (no PII blankout) |
# MAGIC | `CREATE` | Yes | No | SKIP — stale, no-op |
# MAGIC | `CHANGE` | No | n/a | **INSERT** (treated as new) |
# MAGIC | `CHANGE` | Yes | Yes | **UPDATE** all fields (no PII blankout) |
# MAGIC | `CHANGE` | Yes | No | SKIP — stale, no-op |
# MAGIC | `DELETE` | Yes | n/a (unconditional) | **BLANKOUT** — PII fields set NULL, `is_deleted = TRUE` |
# MAGIC | `DELETE` | No | n/a | no-op |
# MAGIC
# MAGIC `DELETE` is intentionally left unconditional on `source_timestamp` — once a
# MAGIC delete is the recorded action for a natural key it always applies, so a
# MAGIC profile can't get "stuck" un-deleted behind a timestamp comparison.
# MAGIC
# MAGIC `scon_cbr` is set to `0` on **every** insert/update/delete — first load and
# MAGIC every incremental operation alike, exactly per spec. It is not otherwise
# MAGIC interpreted here (kept as a placeholder flag for a downstream
# MAGIC change-based-replication consumer).
# MAGIC
# MAGIC ## ⚠️ Confirmed finding — `scon_srcc_id` is identical to `scon_src_id`
# MAGIC Spec item 2 asks for `srcc_id` from `bronze.sourceconsumer` to be carried
# MAGIC onto the Gold row as `scon_srcc_id`, for bronze→gold traceability. Checked
# MAGIC against your actual Step 1/2/3 notebooks: **there is no `srcc_id` column
# MAGIC anywhere upstream** — Bronze's PK is `src_id`, and Step 3 carries that exact
# MAGIC same value through unchanged onto `silver.sourceconsumer.src_id`. So
# MAGIC `scon_srcc_id` is populated from `sc.src_id` in §5.5, and will always equal
# MAGIC `scon_src_id` (also sourced from the same column) — there's only one lineage
# MAGIC id to track, not two. Both columns are kept (rather than dropping one) so the
# MAGIC exact column name your spec asked for still exists in Gold. If you actually
# MAGIC intended a distinct bronze-only identifier, that column would need to be
# MAGIC added back in Step 2/3 first — let me know and I'll wire it through.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.0 — Use Catalog (aligns with Steps 1–4)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG MDM_Platform;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — Extend Step 4's Gold Tables (Contributor Layer)
# MAGIC Step 4 already created `gold.sconsumer` / `sconsumermedia` / `sconsumerphone` /
# MAGIC `sconsumeraddress` with a minimal match-relevant schema (see Step 4 §4.2).
# MAGIC `ALTER TABLE ADD COLUMNS IF NOT EXISTS` extends them here — safe to re-run,
# MAGIC and doesn't disturb rows Step 4 already wrote. `scon_id` remains the single
# MAGIC PK on `gold.sconsumer` and the FK target for every child table below
# MAGIC (`gsme_scon_id`, `gsph_scon_id`, `gsad_scon_id`, `gsop_scon_id`).

# COMMAND ----------

# Check and add columns only if they don't exist
try:
    spark.sql("""
        ALTER TABLE gold.sconsumer ADD COLUMNS (
            scon_src_id         BIGINT,
            source_system_code   STRING,
            source_timestamp     TIMESTAMP,
            reg_date             TIMESTAMP,
            reg_touchpoint_code  STRING,
            record_uuid          STRING,
            action               STRING,
            scon_cbr             TINYINT,
            is_deleted           BOOLEAN
        )
    """)
    print("✓ Columns added to gold.sconsumer")
except Exception as e:
    if "FIELD_ALREADY_EXISTS" in str(e):
        print("✓ Columns already exist in gold.sconsumer (skipped)")
    else:
        raise

# COMMAND ----------

# Check and add columns to child tables
tables_config = [
    ("gold.sconsumermedia", """
        type_code            STRING,
        email_address        STRING,
        valid_contact_desc   STRING,
        source_timestamp     TIMESTAMP,
        scon_cbr             TINYINT,
        is_deleted           BOOLEAN
    """),
    ("gold.sconsumerphone", """
        type_code            STRING,
        phone_number         STRING,
        valid_contact_desc   STRING,
        source_timestamp     TIMESTAMP,
        scon_cbr             TINYINT,
        is_deleted           BOOLEAN
    """),
    ("gold.sconsumeraddress", """
        type_code                   STRING,
        address1                    STRING,
        city_description_local      STRING,
        province_description_local  STRING,
        valid_contact_desc          STRING,
        source_timestamp            TIMESTAMP,
        scon_cbr                    TINYINT,
        is_deleted                  BOOLEAN
    """)
]

for table_name, columns in tables_config:
    try:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({columns})")
        print(f"✓ Columns added to {table_name}")
    except Exception as e:
        if "FIELD_ALREADY_EXISTS" in str(e):
            print(f"✓ Columns already exist in {table_name} (skipped)")
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 — Create gold.sconsumeroptin (Contributor Layer, new)
# MAGIC No PII — per spec, DELETE is a no-op here (nothing to blank out).
# MAGIC `gsop_scon_id` is the FK back to `gold.sconsumer.scon_id`, following the same
# MAGIC `gsXX_scon_id` naming convention Step 4 used for the other child tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.sconsumeroptin (
# MAGIC     gsop_id                     BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     gsop_scon_id                BIGINT      NOT NULL,   -- FK -> gold.sconsumer.scon_id
# MAGIC     communication_channel_code  STRING,
# MAGIC     opt_in_flag                 BOOLEAN,
# MAGIC     opt_in_timestamp            TIMESTAMP,
# MAGIC     source_timestamp            TIMESTAMP,  -- carried from silver.sourceconsumer for the newer-wins update guard
# MAGIC     scon_cbr                    TINYINT,
# MAGIC     creation_dt                 TIMESTAMP,
# MAGIC     updation_dt                 TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold contributor opt-in preferences — no PII, DELETE is a no-op';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 — Create Golden / Master Tables (one row per `universal_mdm_key`)
# MAGIC Unchanged design from the previous draft — `primary_scon_id` now points at
# MAGIC `gold.sconsumer.scon_id` (same column, corrected source table).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.consumer_master (
# MAGIC     master_id            BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     domain_code          STRING      NOT NULL,
# MAGIC     universal_mdm_key    STRING      NOT NULL,   -- business key: one row per GID
# MAGIC     primary_scon_id      BIGINT,                 -- winning contributor (survivorship source) -> gold.sconsumer.scon_id
# MAGIC     market_code          STRING,
# MAGIC     brand_code           STRING,
# MAGIC     local_first_name     STRING,
# MAGIC     local_last_name      STRING,
# MAGIC     local_full_name      STRING,
# MAGIC     first_name_match_key STRING,
# MAGIC     last_name_match_key  STRING,
# MAGIC     full_name_match_key  STRING,
# MAGIC     contributor_count    INT,                    -- active contributors merged into this row
# MAGIC     is_deleted           BOOLEAN,                -- TRUE only when ALL contributors are deleted
# MAGIC     creation_dt          TIMESTAMP,
# MAGIC     updation_dt          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold golden/master consumer — ONE ROW PER universal_mdm_key, survivorship-rebuilt each run';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.phone_master (
# MAGIC     universal_mdm_key STRING,
# MAGIC     type_code         STRING,
# MAGIC     phone_number      STRING,
# MAGIC     phone_match_key   STRING,
# MAGIC     updation_dt       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold golden phone — best valid phone per (GID, type_code)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.email_master (
# MAGIC     universal_mdm_key STRING,
# MAGIC     type_code         STRING,
# MAGIC     email_address     STRING,
# MAGIC     email_match_key   STRING,
# MAGIC     updation_dt       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold golden email — best valid email per (GID, type_code)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.address_master (
# MAGIC     universal_mdm_key           STRING,
# MAGIC     type_code                   STRING,
# MAGIC     address1                    STRING,
# MAGIC     city_description_local      STRING,
# MAGIC     province_description_local  STRING,
# MAGIC     address_match_key           STRING,
# MAGIC     updation_dt                 TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold golden address — best valid address per (GID, type_code)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 — Config: Dedup Key, PII Registry, Batch Resolution
# MAGIC Dedup key unchanged from the previous draft — exactly the four fields (plus
# MAGIC `domain_code` to keep domains isolated) named in spec item 4:
# MAGIC `consumer_id` (→ `source_entity_id`), `brand_code`, `source_system_code`,
# MAGIC `market_code`.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# ── Dedup key — exactly as specified ──────────────────────────────────────────
# domain_code is appended physically to keep domains isolated; the four
# business fields you named are the natural key within a domain.
DEDUP_KEY = ["domain_code", "source_entity_id", "brand_code", "source_system_code", "market_code"]

DEDUP_MERGE_COND = (
    "tgt.domain_code        = src.domain_code        AND "
    "tgt.source_entity_id   = src.source_entity_id   AND "
    "tgt.brand_code         <=> src.brand_code        AND "
    "tgt.source_system_code = src.source_system_code AND "
    "tgt.market_code        <=> src.market_code"
)

# ── PII field registry (blanked on DELETE) ────────────────────────────────────
PII_CONSUMER = [
    "local_first_name", "local_last_name", "local_full_name",
    "reg_date", "reg_touchpoint_code",
    "first_name_match_key", "last_name_match_key", "full_name_match_key",
]
PII_EMAIL   = ["email_address", "email_match_key"]
PII_PHONE   = ["phone_number", "phone_match_key"]
PII_ADDRESS = ["address1", "city_description_local", "province_description_local", "address_match_key"]

# ── Domains with a completed Step 4 GID assignment this run ──────────────────
# Fix applied: the previous draft computed a single GLOBAL MAX(batch_id)
# across every domain, which would silently exclude any domain whose latest
# batch_id happened to be lower than another domain's (independent
# per-domain batch sequences from bronze.batch_control). Resolved per-domain,
# exactly matching Step 4 §4.7's pattern.
domains_to_process = spark.sql("""
    SELECT domain_code, MAX(batch_id) AS batch_id
    FROM   silver.sourceconsumer
    GROUP  BY domain_code
""").collect()

print(">>> Domains to load into Gold this run:")
for r in domains_to_process:
    print(f"    {r['domain_code']}  (batch_id={r['batch_id']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 — Build & Deduplicate Contributor Source Dataset (per domain)
# MAGIC Joined against `silver.tmp_gid_assignment` (Step 4's output) to pick up each
# MAGIC record's `universal_mdm_key`. Within-batch dedup on the natural key handles
# MAGIC the case of multiple messages for the identical source contribution arriving
# MAGIC in one run — latest `source_timestamp` wins, tie-break `DELETE > CHANGE > CREATE`.
# MAGIC
# MAGIC `sc.srcc_id AS scon_srcc_id` is the bronze-lineage column called out in the
# MAGIC ⚠️ assumption note above — adjust the source column name here if Silver
# MAGIC carries it under a different name.

# COMMAND ----------

def build_contributor_dataset(domain_code: str, batch_id: int):
    df_raw = spark.sql(f"""
        SELECT
            sc.src_id                        AS scon_src_id,
            sc.src_id                        AS scon_srcc_id,   -- bronze/silver lineage: there is only ONE id (src_id), carried through
                                                                 -- unchanged from bronze.sourceconsumer to silver.sourceconsumer (see Step 2/3) —
                                                                 -- no separate "srcc_id" column exists upstream, so this is intentionally
                                                                 -- identical to scon_src_id. Flagged for your confirmation: if you intended
                                                                 -- a genuinely distinct bronze-only id, that column doesn't exist yet upstream.
            ga.gid                           AS universal_mdm_key,
            ga.match_type,
            sc.domain_code,
            sc.record_uuid,
            sc.market_code,
            sc.brand_code,
            sc.source_entity_id,
            sc.source_system_code,
            sc.source_timestamp,
            sc.local_first_name,
            sc.local_last_name,
            sc.local_full_name,
            sc.reg_date,
            sc.reg_touchpoint_code,
            sc.first_name_match_key,
            sc.last_name_match_key,
            sc.full_name_match_key,
            UPPER(TRIM(COALESCE(sc.action, 'CREATE')))  AS action,
            CAST(0 AS TINYINT)               AS scon_cbr,
            current_timestamp()              AS creation_dt,
            current_timestamp()              AS updation_dt
        FROM   silver.sourceconsumer sc
        JOIN   silver.tmp_gid_assignment ga
               ON ga.domain_code = sc.domain_code AND ga.src_id = sc.src_id
        WHERE  sc.domain_code = '{domain_code}' AND sc.batch_id = {batch_id}
    """)

    action_pri = (
        F.when(F.col("action") == "DELETE", 1)
         .when(F.col("action") == "CHANGE", 2)
         .otherwise(3)
    )
    win_dedup = Window.partitionBy(*DEDUP_KEY).orderBy(
        F.col("source_timestamp").desc_nulls_last(), action_pri.asc()
    )
    df_dedup = (
        df_raw.withColumn("_rn", F.row_number().over(win_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return df_dedup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 — gold.sconsumer: Action-Routed MERGE (Contributor Layer)
# MAGIC **Updated per latest spec:**
# MAGIC - `CREATE` + `CHANGE` are now handled by a **single conditional MERGE**:
# MAGIC   insert if the natural key doesn't exist yet; if it does, update **only
# MAGIC   when `src.source_timestamp` is strictly newer than the existing Gold
# MAGIC   row's** `source_timestamp` (or the existing value is `NULL`, e.g. a
# MAGIC   pre-existing row created before this column was backfilled). Neither
# MAGIC   branch touches PII with a blankout — they only ever insert or
# MAGIC   overwrite-with-newer-values.
# MAGIC - `DELETE` remains the **only** action that blanks PII, and is applied
# MAGIC   unconditionally (no timestamp gate) so a delete can never be blocked by
# MAGIC   a stale-looking comparison.

# COMMAND ----------

def load_contributor_consumer(df_dedup):
    df_upsert = (
        df_dedup.filter(F.col("action").isin("CREATE", "CHANGE"))
        .withColumn("is_deleted", F.lit(False))
        .withColumn("is_golden_record", F.lit(False))
    )
    df_delete = df_dedup.filter(F.col("action") == "DELETE")

    dt_con = DeltaTable.forName(spark, "gold.sconsumer")

    # ── CREATE/CHANGE — insert if new; update only if incoming source_timestamp
    #     is newer than the existing Gold row's. No PII blankout on this path. ──
    cnt_uc = df_upsert.count()
    if cnt_uc > 0:
        (dt_con.alias("tgt")
            .merge(df_upsert.alias("src"), DEDUP_MERGE_COND)
            .whenMatchedUpdate(
                condition="src.source_timestamp > tgt.source_timestamp OR tgt.source_timestamp IS NULL",
                set={
                    "scon_src_id":          "src.scon_src_id",
                    "universal_mdm_key":    "src.universal_mdm_key",
                    "record_uuid":          "src.record_uuid",
                    "source_timestamp":     "src.source_timestamp",
                    "local_first_name":     "src.local_first_name",
                    "local_last_name":      "src.local_last_name",
                    "local_full_name":      "src.local_full_name",
                    "reg_date":             "src.reg_date",
                    "reg_touchpoint_code":  "src.reg_touchpoint_code",
                    "first_name_match_key": "src.first_name_match_key",
                    "last_name_match_key":  "src.last_name_match_key",
                    "full_name_match_key":  "src.full_name_match_key",
                    "match_type":           "src.match_type",
                    "action":               "src.action",
                    "is_deleted":           "FALSE",
                    "scon_cbr":             "CAST(0 AS TINYINT)",
                    "updation_dt":          "current_timestamp()",
                })
            .whenNotMatchedInsert(values={
                "scon_src_id":          "src.scon_src_id",
                "domain_code":          "src.domain_code",
                "universal_mdm_key":    "src.universal_mdm_key",
                "market_code":          "src.market_code",
                "brand_code":           "src.brand_code",
                "source_entity_id":     "src.source_entity_id",
                "source_system_code":   "src.source_system_code",
                "source_timestamp":     "src.source_timestamp",
                "local_first_name":     "src.local_first_name",
                "local_last_name":      "src.local_last_name",
                "local_full_name":      "src.local_full_name",
                "reg_date":             "src.reg_date",
                "reg_touchpoint_code":  "src.reg_touchpoint_code",
                "record_uuid":          "src.record_uuid",
                "first_name_match_key": "src.first_name_match_key",
                "last_name_match_key":  "src.last_name_match_key",
                "full_name_match_key":  "src.full_name_match_key",
                "match_type":           "src.match_type",
                "action":               "src.action",
                "is_golden_record":     "src.is_golden_record",
                "is_deleted":           "src.is_deleted",
                "scon_cbr":             "src.scon_cbr",
                "creation_dt":          "src.creation_dt",
                "updation_dt":          "src.updation_dt",
            })
            .execute())
    print(f"    gold.sconsumer CREATE/CHANGE — processed {cnt_uc} "
          f"(inserted if new, updated only where source_timestamp was newer)")

    # ── DELETE — blank PII only, keep dedup-key + system columns. The ONLY
    #     action that blanks PII. Unconditional — always applies. ─────────────
    cnt_d = df_delete.count()
    if cnt_d > 0:
        del_set = {
            "action": "'DELETE'", "is_deleted": "TRUE",
            "scon_cbr": "CAST(0 AS TINYINT)", "updation_dt": "current_timestamp()",
        }
        for f in PII_CONSUMER:
            del_set[f] = "NULL"
        (dt_con.alias("tgt")
            .merge(df_delete.alias("src"), DEDUP_MERGE_COND)
            .whenMatchedUpdate(set=del_set)
            .execute())
    print(f"    gold.sconsumer DELETE — blanked PII for {cnt_d}")

    return df_dedup.select(*DEDUP_KEY, "action")  # for child-table scon_id lookup below

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 — Contact Contributor Tables: Reusable MERGE Helper

# COMMAND ----------

def merge_contact_table(table_name, df_src, merge_key_expr, change_set, pii_fields, non_pii_preserve_set):
    """
    CREATE/CHANGE -> insert if key not present; update ONLY when incoming
                      source_timestamp is strictly newer than the existing
                      Gold row's (or the existing value is NULL). No PII
                      blankout on this path.
    DELETE        -> NULL-out PII, is_deleted = TRUE, retain type_code / quality
                      flags. The ONLY action that blanks PII. Unconditional —
                      always applies, no timestamp gate.
    """
    df_uc = df_src.filter(F.col("action").isin("CREATE", "CHANGE"))
    df_d  = df_src.filter(F.col("action") == "DELETE")

    dt = DeltaTable.forName(spark, table_name)

    if df_uc.count() > 0:
        # Build insert values from change_set plus additional source columns
        fk_col = merge_key_expr.split("=")[0].strip().split(".")[1]  # Extract FK column name from merge key
        insert_values = {**change_set, fk_col: "src.scon_id", "creation_dt": "src.creation_dt"}
        (dt.alias("tgt").merge(df_uc.alias("src"), merge_key_expr)
            .whenMatchedUpdate(
                condition="src.source_timestamp > tgt.source_timestamp OR tgt.source_timestamp IS NULL",
                set=change_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute())
    if df_d.count() > 0:
        del_set = {**non_pii_preserve_set, "is_deleted": "TRUE",
                   "scon_cbr": "CAST(0 AS TINYINT)", "updation_dt": "current_timestamp()"}
        for f in pii_fields:
            del_set[f] = "NULL"
        (dt.alias("tgt").merge(df_d.alias("src"), merge_key_expr)
            .whenMatchedUpdate(set=del_set).execute())

    print(f"    {table_name} — CREATE/CHANGE:{df_uc.count()} DELETE:{df_d.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.8 — Per-Domain Orchestration: Contributor Layer

# COMMAND ----------

for row in domains_to_process:
    domain_code, batch_id = row["domain_code"], row["batch_id"]
    print(f"\n=== Gold Load (Contributor layer): {domain_code} | batch_id={batch_id} ===")

    df_dedup = build_contributor_dataset(domain_code, batch_id)
    if df_dedup.count() == 0:
        print("    Nothing to load — skipping.")
        continue

    load_contributor_consumer(df_dedup)

    df_scon_map = spark.sql(f"""
        SELECT g.scon_id, g.scon_src_id, g.action, g.is_deleted
        FROM   gold.sconsumer g
        WHERE  g.domain_code = '{domain_code}'
    """)
    df_scon_map.createOrReplaceTempView("vw_scon_map")

    # ── gold.sconsumermedia (email) ───────────────────────────────────────────
    # Fix applied: previous draft omitted is_deleted from this SELECT.
    # whenNotMatchedInsertAll() leaves any target column absent from the source
    # as NULL (not FALSE) — since the Master rollup filters is_deleted = FALSE,
    # a fresh insert would have been silently excluded from email_master until
    # a later UPDATE happened to touch the row. Explicit FALSE fixes this.
    df_eml = spark.sql(f"""
        SELECT m.scon_id, m.action, sc.source_timestamp,
               sm.type_code, sm.email_address, sm.email_match_key,
               sm.valid_contact, sm.valid_contact_desc,
               FALSE AS is_deleted,
               CAST(0 AS TINYINT) AS scon_cbr,
               current_timestamp() AS creation_dt, current_timestamp() AS updation_dt
        FROM   silver.sourcemedia sm
        JOIN   silver.tmp_gid_assignment ga ON ga.src_id = sm.srme_src_id AND ga.domain_code = '{domain_code}'
        JOIN   vw_scon_map m ON m.scon_src_id = sm.srme_src_id
        JOIN   silver.sourceconsumer sc ON sc.src_id = sm.srme_src_id
    """)
    merge_contact_table(
        "gold.sconsumermedia", df_eml, "tgt.gsme_scon_id = src.scon_id AND tgt.type_code <=> src.type_code",
        change_set={
            "email_address": "src.email_address", "email_match_key": "src.email_match_key",
            "valid_contact": "src.valid_contact", "valid_contact_desc": "src.valid_contact_desc",
            "source_timestamp": "src.source_timestamp",
            "is_deleted": "FALSE", "scon_cbr": "CAST(0 AS TINYINT)", "updation_dt": "current_timestamp()",
        },
        pii_fields=PII_EMAIL,
        non_pii_preserve_set={"valid_contact": "FALSE", "valid_contact_desc": "'PII removed — DELETE action'"},
    )

    # ── gold.sconsumerphone ───────────────────────────────────────────────────
    df_phn = spark.sql(f"""
        SELECT m.scon_id, m.action, sc.source_timestamp,
               sp.type_code, sp.phone_number, sp.phone_match_key,
               sp.valid_contact, sp.valid_contact_desc,
               FALSE AS is_deleted,
               CAST(0 AS TINYINT) AS scon_cbr,
               current_timestamp() AS creation_dt, current_timestamp() AS updation_dt
        FROM   silver.sourcephone sp
        JOIN   silver.tmp_gid_assignment ga ON ga.src_id = sp.srph_src_id AND ga.domain_code = '{domain_code}'
        JOIN   vw_scon_map m ON m.scon_src_id = sp.srph_src_id
        JOIN   silver.sourceconsumer sc ON sc.src_id = sp.srph_src_id
    """)
    merge_contact_table(
        "gold.sconsumerphone", df_phn, "tgt.gsph_scon_id = src.scon_id AND tgt.type_code <=> src.type_code",
        change_set={
            "phone_number": "src.phone_number", "phone_match_key": "src.phone_match_key",
            "valid_contact": "src.valid_contact", "valid_contact_desc": "src.valid_contact_desc",
            "source_timestamp": "src.source_timestamp",
            "is_deleted": "FALSE", "scon_cbr": "CAST(0 AS TINYINT)", "updation_dt": "current_timestamp()",
        },
        pii_fields=PII_PHONE,
        non_pii_preserve_set={"valid_contact": "FALSE", "valid_contact_desc": "'PII removed — DELETE action'"},
    )

    # ── gold.sconsumeraddress ─────────────────────────────────────────────────
    df_addr = spark.sql(f"""
        SELECT m.scon_id, m.action, sc.source_timestamp,
               sa.type_code, sa.address1, sa.city_description_local, sa.province_description_local,
               sa.address_match_key, sa.valid_contact, sa.valid_contact_desc,
               FALSE AS is_deleted,
               CAST(0 AS TINYINT) AS scon_cbr,
               current_timestamp() AS creation_dt, current_timestamp() AS updation_dt
        FROM   silver.sourceaddress sa
        JOIN   silver.tmp_gid_assignment ga ON ga.src_id = sa.srad_src_id AND ga.domain_code = '{domain_code}'
        JOIN   vw_scon_map m ON m.scon_src_id = sa.srad_src_id
        JOIN   silver.sourceconsumer sc ON sc.src_id = sa.srad_src_id
    """)
    merge_contact_table(
        "gold.sconsumeraddress", df_addr, "tgt.gsad_scon_id = src.scon_id AND tgt.type_code <=> src.type_code",
        change_set={
            "address1": "src.address1", "city_description_local": "src.city_description_local",
            "province_description_local": "src.province_description_local", "address_match_key": "src.address_match_key",
            "valid_contact": "src.valid_contact", "valid_contact_desc": "src.valid_contact_desc",
            "source_timestamp": "src.source_timestamp",
            "is_deleted": "FALSE", "scon_cbr": "CAST(0 AS TINYINT)", "updation_dt": "current_timestamp()",
        },
        pii_fields=PII_ADDRESS,
        non_pii_preserve_set={"valid_contact": "FALSE", "valid_contact_desc": "'PII removed — DELETE action'"},
    )

    # ── gold.sconsumeroptin (no PII, DELETE is a no-op) ───────────────────────
    # CREATE/CHANGE only — same newer-wins guard as the other contributor tables.
    df_oi = spark.sql(f"""
        SELECT m.scon_id, sc.source_timestamp,
               so.communication_channel_code, so.opt_in_flag, so.opt_in_timestamp,
               CAST(0 AS TINYINT) AS scon_cbr,
               current_timestamp() AS creation_dt, current_timestamp() AS updation_dt
        FROM   silver.sourceoptin so
        JOIN   silver.tmp_gid_assignment ga ON ga.src_id = so.srop_src_id AND ga.domain_code = '{domain_code}'
        JOIN   vw_scon_map m ON m.scon_src_id = so.srop_src_id
        JOIN   silver.sourceconsumer sc ON sc.src_id = so.srop_src_id
        WHERE  m.action IN ('CREATE', 'CHANGE')
    """)
    try:
        dt_optin = DeltaTable.forName(spark, "gold.sconsumeroptin")
        (dt_optin.alias("tgt")
            .merge(df_oi.alias("src"),
                   "tgt.gsop_scon_id = src.scon_id AND tgt.communication_channel_code <=> src.communication_channel_code")
            .whenMatchedUpdate(
                condition="src.source_timestamp > tgt.source_timestamp OR tgt.source_timestamp IS NULL",
                set={
                    "opt_in_flag": "src.opt_in_flag", "opt_in_timestamp": "src.opt_in_timestamp",
                    "source_timestamp": "src.source_timestamp",
                    "scon_cbr": "CAST(0 AS TINYINT)", "updation_dt": "current_timestamp()",
                })
            .whenNotMatchedInsert(values={
                "gsop_scon_id": "src.scon_id", "communication_channel_code": "src.communication_channel_code",
                "opt_in_flag": "src.opt_in_flag", "opt_in_timestamp": "src.opt_in_timestamp",
                "source_timestamp": "src.source_timestamp",
                "scon_cbr": "src.scon_cbr", "creation_dt": "src.creation_dt", "updation_dt": "src.updation_dt",
            })
            .execute())
        print(f"    gold.sconsumeroptin — upserted {df_oi.count()} (updated only where source_timestamp was newer)")
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e):
            print("    gold.sconsumeroptin table not found — skipping opt-in data")
        else:
            raise

print("\n>>> Contributor layer load complete for all domains.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.9 — Golden / Master Rollup: One Row Per `universal_mdm_key`
# MAGIC Full truncate-and-rebuild each run, over ALL active contributors across ALL
# MAGIC domains present in `gold.sconsumer` (not just this run's batch) — a GID's
# MAGIC golden profile must reflect every still-active contributor, including ones
# MAGIC last touched in an earlier run. If contributor volume grows large enough that
# MAGIC this full rebuild becomes too slow, the next step is to make this incremental
# MAGIC (recompute only GIDs touched by rows changed in the current batch).

# COMMAND ----------

spark.sql("TRUNCATE TABLE gold.consumer_master")
spark.sql("TRUNCATE TABLE gold.phone_master")
spark.sql("TRUNCATE TABLE gold.email_master")
spark.sql("TRUNCATE TABLE gold.address_master")

# ── Winning contributor per GID: latest source_timestamp, tie-break highest scon_id ──
win = Window.partitionBy("domain_code", "universal_mdm_key").orderBy(
    F.col("source_timestamp").desc_nulls_last(), F.col("scon_id").desc()
)
df_active = spark.table("gold.sconsumer").filter(F.col("is_deleted") == False)  # noqa: E712
df_all_by_gid = spark.table("gold.sconsumer")

df_ranked = df_active.withColumn("rn", F.row_number().over(win))
df_winners = df_ranked.filter(F.col("rn") == 1)

df_group_stats = (
    df_active.groupBy("domain_code", "universal_mdm_key")
    .agg(F.count("*").alias("contributor_count"))
)

# GIDs with zero active contributors left (all deleted) still need a master row,
# marked is_deleted = TRUE, sourced from the most-recently-touched contributor.
win_all = Window.partitionBy("domain_code", "universal_mdm_key").orderBy(F.col("updation_dt").desc())
df_all_deleted_gids = (
    df_all_by_gid
    .join(df_group_stats.select("domain_code", "universal_mdm_key"), ["domain_code", "universal_mdm_key"], "left_anti")
    .withColumn("rn", F.row_number().over(win_all))
    .filter(F.col("rn") == 1)
)

df_master = (
    df_winners.join(df_group_stats, ["domain_code", "universal_mdm_key"])
    .withColumn("is_deleted", F.lit(False))
    .unionByName(
        df_all_deleted_gids
        .withColumn("contributor_count", F.lit(0))
        .withColumn("is_deleted", F.lit(True)),
        allowMissingColumns=True,
    )
    .select(
        "domain_code", "universal_mdm_key",
        F.col("scon_id").alias("primary_scon_id"),
        "market_code", "brand_code",
        "local_first_name", "local_last_name", "local_full_name",
        "first_name_match_key", "last_name_match_key", "full_name_match_key",
        "contributor_count", "is_deleted",
    )
    .withColumn("creation_dt", F.current_timestamp())
    .withColumn("updation_dt", F.current_timestamp())
)

df_master.write.format("delta").mode("append").saveAsTable("gold.consumer_master")
print(f">>> gold.consumer_master rebuilt — {df_master.count()} golden records (one per GID).")

# ── Reset is_golden_record flag on contributors to match the new winners ─────
spark.sql("UPDATE gold.sconsumer SET is_golden_record = FALSE")
df_winner_ids = df_winners.select("scon_id").distinct()
df_winner_ids.createOrReplaceTempView("vw_winner_ids")
spark.sql("""
    MERGE INTO gold.sconsumer tgt
    USING vw_winner_ids src ON tgt.scon_id = src.scon_id
    WHEN MATCHED THEN UPDATE SET is_golden_record = TRUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.1 — Best Phone / Email / Address Per GID

# COMMAND ----------

def build_contact_master(contact_table, fk_col, key_cols, group_cols):
    df_consumer = spark.table("gold.sconsumer").filter(F.col("is_deleted") == False)  # noqa: E712
    df_contact = spark.table(contact_table).filter(F.col("is_deleted") == False)  # noqa: E712
    
    df = (
        df_consumer
        .select("scon_id", "domain_code", "universal_mdm_key")
        .join(
            df_contact,
            df_contact[fk_col] == F.col("scon_id"),
        )
    )
    win_contact = Window.partitionBy("universal_mdm_key", "type_code").orderBy(
        F.col("valid_contact").desc_nulls_last(), F.col("updation_dt").desc()
    )
    df_best = (
        df.withColumn("rn", F.row_number().over(win_contact))
        .filter(F.col("rn") == 1)
        .select("universal_mdm_key", "type_code", *key_cols)
        .withColumn("updation_dt", F.current_timestamp())
    )
    return df_best


df_phone_best = build_contact_master(
    "gold.sconsumerphone", "gsph_scon_id", ["phone_number", "phone_match_key"], []
)
df_phone_best.write.format("delta").mode("append").saveAsTable("gold.phone_master")
print(f">>> gold.phone_master — {df_phone_best.count()} rows.")

df_email_best = build_contact_master(
    "gold.sconsumermedia", "gsme_scon_id", ["email_address", "email_match_key"], []
)
df_email_best.write.format("delta").mode("append").saveAsTable("gold.email_master")
print(f">>> gold.email_master — {df_email_best.count()} rows.")

df_address_best = build_contact_master(
    "gold.sconsumeraddress", "gsad_scon_id",
    ["address1", "city_description_local", "province_description_local", "address_match_key"], []
)
df_address_best.write.format("delta").mode("append").saveAsTable("gold.address_master")
print(f">>> gold.address_master — {df_address_best.count()} rows.")

print("\n>>> Step 5 complete — Contributor layer action-routed, Golden/Master layer rebuilt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.10 — QA Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contributor layer: action & deletion distribution, per domain
# MAGIC SELECT domain_code, action, is_deleted, COUNT(*) AS cnt
# MAGIC FROM   gold.sconsumer
# MAGIC GROUP  BY domain_code, action, is_deleted
# MAGIC ORDER  BY domain_code, action;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contributor dedup check — MUST return 0 rows
# MAGIC SELECT domain_code, source_entity_id, brand_code, source_system_code, market_code, COUNT(*) AS n
# MAGIC FROM   gold.sconsumer
# MAGIC WHERE  is_deleted = FALSE
# MAGIC GROUP  BY domain_code, source_entity_id, brand_code, source_system_code, market_code
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze -> Gold lineage check (scon_src_id sourced from silver.sourceconsumer.src_id)
# MAGIC -- should be populated for every non-deleted row
# MAGIC SELECT domain_code, COUNT(*) AS missing_bronze_lineage
# MAGIC FROM   gold.sconsumer
# MAGIC WHERE  is_deleted = FALSE AND scon_src_id IS NULL
# MAGIC GROUP  BY domain_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Golden/Master layer — THE core proof: one row per universal_mdm_key. MUST return 0 rows.
# MAGIC SELECT domain_code, universal_mdm_key, COUNT(*) AS n
# MAGIC FROM   gold.consumer_master
# MAGIC GROUP  BY domain_code, universal_mdm_key
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Golden records built from more than one contributor (true merges)
# MAGIC SELECT domain_code, universal_mdm_key, contributor_count, local_full_name
# MAGIC FROM   gold.consumer_master
# MAGIC WHERE  contributor_count > 1
# MAGIC ORDER  BY contributor_count DESC
# MAGIC LIMIT  20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PII blankout verification (contributor layer) — MUST return 0 rows
# MAGIC SELECT gc.scon_id, gc.domain_code, gc.action,
# MAGIC        gc.local_first_name, ge.email_address, gp.phone_number, ga.address1
# MAGIC FROM   gold.sconsumer gc
# MAGIC LEFT   JOIN gold.sconsumermedia   ge ON ge.gsme_scon_id = gc.scon_id
# MAGIC LEFT   JOIN gold.sconsumerphone   gp ON gp.gsph_scon_id = gc.scon_id
# MAGIC LEFT   JOIN gold.sconsumeraddress ga ON ga.gsad_scon_id = gc.scon_id
# MAGIC WHERE  gc.is_deleted = TRUE
# MAGIC   AND (gc.local_first_name IS NOT NULL OR ge.email_address IS NOT NULL
# MAGIC     OR gp.phone_number IS NOT NULL OR ga.address1 IS NOT NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full golden profile trace
# MAGIC SELECT
# MAGIC     m.domain_code, m.universal_mdm_key, m.contributor_count, m.is_deleted,
# MAGIC     m.local_first_name, m.local_last_name, m.local_full_name,
# MAGIC     ph.phone_number, em.email_address,
# MAGIC     ad.address1, ad.city_description_local
# MAGIC FROM   gold.consumer_master m
# MAGIC LEFT   JOIN gold.phone_master   ph ON ph.universal_mdm_key = m.universal_mdm_key
# MAGIC LEFT   JOIN gold.email_master   em ON em.universal_mdm_key = m.universal_mdm_key
# MAGIC LEFT   JOIN gold.address_master ad ON ad.universal_mdm_key = m.universal_mdm_key
# MAGIC ORDER  BY m.domain_code, m.universal_mdm_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC Use catalog mdm_platform;
# MAGIC
# MAGIC select c.scon_id,c.scon_src_id,c.domain_code,c.source_entity_id,c.universal_mdm_key,c.local_first_name,c.local_last_name,c.local_full_name,c.source_system_code,c.source_timestamp,p.phone_number,e.email_address,a.address1,a.city_description_local
# MAGIC  FROM  gold.sconsumer c
# MAGIC join gold.sconsumerphone p on p.gsph_scon_id=c.scon_id
# MAGIC join gold.sconsumeraddress a on a.gsad_scon_id=c.scon_id
# MAGIC join gold.sconsumermedia e on e.gsme_scon_id=c.scon_id
# MAGIC where domain_code="RETAIL";
# MAGIC
# MAGIC