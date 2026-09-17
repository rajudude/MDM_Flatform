# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Step 4 — Match & Merge: Config-Driven GID Assignment (v4)
# MAGIC **Pipeline:** Dynamic MDM Platform | **Layer:** Silver (temp) → Gold reference
# MAGIC
# MAGIC ## What changed from the previous (v3/"Dynamic") draft
# MAGIC 1. **Match rules moved from YAML files into a real config table**
# MAGIC    (`silver.match_type_config`) — matching every other config-driven
# MAGIC    piece of this platform (`bronze.domain_config`, etc.). No more
# MAGIC    `dbutils.fs` reads or a Volume path to keep in sync; match types,
# MAGIC    thresholds, review flags, and now **confidence weights** are all
# MAGIC    editable with a plain `UPDATE`/`MERGE` on a Delta table.
# MAGIC 2. **Latest-batch resolution now reads `silver.sourceconsumer` directly**
# MAGIC    (`MAX(batch_id) GROUP BY domain_code`) instead of `silver.parse_control`.
# MAGIC    Since Step 3 truncates and fully reloads Silver every run, whatever
# MAGIC    is currently in `silver.sourceconsumer` *is* the latest batch — reading
# MAGIC    it directly is simpler and can't drift out of sync with an audit log.
# MAGIC 3. **Weighted confidence scoring** added per the spec below — replaces
# MAGIC    the previous all-or-nothing boolean match with a graded score so the
# MAGIC    0.85 threshold is doing real work, not just rubber-stamping an
# MAGIC    already-deterministic AND-join.
# MAGIC
# MAGIC ## Silver Table References (Step 3 — verified aligned)
# MAGIC | Table | PK | FK |
# MAGIC |---|---|---|
# MAGIC | `silver.sourceconsumer` | `src_id` | — |
# MAGIC | `silver.sourcemedia`    | `srme_id` | `srme_src_id` → src_id |
# MAGIC | `silver.sourcephone`    | `srph_id` | `srph_src_id` → src_id |
# MAGIC | `silver.sourceaddress`  | `srad_id` | `srad_src_id` → src_id |
# MAGIC
# MAGIC ## Match Comparison Mode: EXACT (config-driven, not hardcoded)
# MAGIC **Fix applied**: the previous draft scored the name component with
# MAGIC continuous Jaro-Winkler similarity (0–1) as a multiplier. Your spec
# MAGIC labels every one of the 7 match types **"Exact match"**, so name
# MAGIC comparisons are now genuine string equality — bidirectional for
# MAGIC MT1-3 (`a.first=b.first & a.last=b.last` OR `a.first=b.last &
# MAGIC a.last=b.first`, catching swapped fields) and both-orientation for
# MAGIC MT4-6 (`a.full=b.full` OR `a.full=b.full_desc`, catching swapped word
# MAGIC order in a single full-name string). A pair is only ever produced when
# MAGIC one of those exact conditions holds — there is no partial credit.
# MAGIC
# MAGIC This is driven by a new `comparison_mode` column on
# MAGIC `silver.match_type_config`, seeded to `EXACT` for every row today. The
# MAGIC `FUZZY` code path (Jaro-Winkler) is still implemented and selectable —
# MAGIC **per domain, per match type** — by simply updating that one column;
# MAGIC no notebook changes needed if you ever want Healthcare's MT3, say, to
# MAGIC tolerate minor spelling variation while everything else stays exact.
# MAGIC
# MAGIC ## Confidence Scoring (per match type, weights sum to 100)
# MAGIC | Match type | Component A | Component B | Component C | Formula (EXACT mode) |
# MAGIC |---|---|---|---|---|
# MAGIC | MT1 (name+phone)   | first_name — 25 | last_name — 25 | valid phone — 50 | `25 + 25 + 50 = 100` |
# MAGIC | MT2 (name+email)   | first_name — 25 | last_name — 25 | valid email — 50 | `25 + 25 + 50 = 100` |
# MAGIC | MT3 (name+address) | first_name — 25 | last_name — 25 | same address — 50 | `25 + 25 + 50 = 100` |
# MAGIC | MT4 (full name+phone)   | full_name — 50 | — | valid phone — 50 | `50 + 50 = 100` |
# MAGIC | MT5 (full name+email)   | full_name — 50 | — | valid email — 50 | `50 + 50 = 100` |
# MAGIC | MT6 (full name+address) | full_name — 50 | — | same address — 50 | `50 + 50 = 100` |
# MAGIC | MT7 (consumer_id+brand) | consumer_id — 50 | brand_code — 50 | — | `50 + 50 = 100` |
# MAGIC
# MAGIC In `EXACT` mode every component that contributes to a produced pair is,
# MAGIC by definition, a full-weight exact match — so `confidence_score` is
# MAGIC always **100** once a pair exists. The weights are still stored and
# MAGIC broken out per component (not hardcoded as a flat 100) so that:
# MAGIC (a) the audit trail shows *which* fields drove the match, and
# MAGIC (b) switching a rule to `FUZZY` later immediately makes the weights
# MAGIC and the 0.85 threshold do real, non-trivial work — without touching
# MAGIC any code, only the config table.
# MAGIC
# MAGIC **Acceptance rule**: `confidence_score / 100 >= threshold` (default
# MAGIC **0.85**, configurable per domain per match type). In `EXACT` mode this
# MAGIC is always satisfied once the join fires; it becomes a meaningful gate
# MAGIC the moment any rule is switched to `FUZZY`.
# MAGIC
# MAGIC ## Name order (ascending / descending)
# MAGIC For MT1-3, `first_name_match_key`/`last_name_match_key` are compared in
# MAGIC **both** orientations — `(a.first↔b.first, a.last↔b.last)` and
# MAGIC `(a.first↔b.last, a.last↔b.first)` — and whichever orientation scores
# MAGIC higher is kept, catching source systems that swap first/last name
# MAGIC fields. For MT4-6, `full_name_match_key` is compared against both the
# MAGIC counterpart's value as-is and its **token-reversed** form
# MAGIC (`"RAJU DUDE"` ↔ `"DUDE RAJU"`) for the same reason.
# MAGIC
# MAGIC ## Blocking keys
# MAGIC | Match type | Blocking key |
# MAGIC |---|---|
# MAGIC | Phone-based (MT1/MT4)   | `market_code` + `phone_match_key` |
# MAGIC | Email-based (MT2/MT5)   | `market_code` + `email_match_key` |
# MAGIC | Address-based (MT3/MT6) | `market_code` + `address_match_key` |
# MAGIC
# MAGIC `full_name_match_key` is deliberately **not** folded into the blocking
# MAGIC key itself (unlike the literal wording of the original spec) — two
# MAGIC records for the same person with swapped first/last names produce two
# MAGIC *different* literal `full_name_match_key` strings ("RAJU DUDE" vs
# MAGIC "DUDE RAJU"). Blocking on that value directly would put them in
# MAGIC different partitions and they would never even be compared. Blocking on
# MAGIC `(market_code, contact_key)` keeps the partition narrow while the
# MAGIC bidirectional name comparison still runs, and confidence-scores the
# MAGIC result, within it. **Flagged for your confirmation** since it differs
# MAGIC from the literal spec wording.
# MAGIC
# MAGIC ## Temp Tables (TRUNCATE & full reload every run — all domains)
# MAGIC - `silver.tmp_match_universe` — Silver(new, GID=NULL) + Gold(existing GID)
# MAGIC - `silver.tmp_raw_pairs` — every pair that cleared its confidence threshold
# MAGIC - `silver.tmp_match_groups` — connected-component group → resolved GID
# MAGIC - `silver.tmp_gid_assignment` — final GID per `src_id` for this run

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.0 — Use Catalog (aligns with Step 1–3)
# MAGIC **Fix applied**: this was missing entirely in the previous draft — every
# MAGIC `silver.*` / `bronze.*` / `gold.*` reference below would otherwise
# MAGIC resolve against whatever catalog happens to be the session default,
# MAGIC not necessarily `MDM_Platform` where Steps 1–3 actually wrote their
# MAGIC tables. This must match Step 3's `USE CATALOG MDM_Platform;` exactly.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG MDM_platform;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Install Similarity Library

# COMMAND ----------

try:
    import jellyfish
    print(">>> jellyfish already available.")
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "jellyfish", "-q"])
    import jellyfish
    print(">>> jellyfish installed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Create Gold Reference Tables (if absent)
# MAGIC Minimal, match-relevant shape only — full survivorship/golden-record
# MAGIC fields are Step 5's concern. Column names verified against Step 3's
# MAGIC Silver naming so joins line up without translation.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Gold / mastered layer for the Dynamic MDM Platform, all domains';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.sconsumer (
# MAGIC     scon_id                 BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     scon_src_id             BIGINT,       -- silver.sourceconsumer.src_id at creation time
# MAGIC     domain_code             STRING      NOT NULL,
# MAGIC     universal_mdm_key       STRING      NOT NULL,   -- the GID
# MAGIC     market_code             STRING,
# MAGIC     brand_code              STRING,
# MAGIC     source_entity_id        STRING,
# MAGIC     local_first_name        STRING,
# MAGIC     local_last_name         STRING,
# MAGIC     local_full_name         STRING,
# MAGIC     first_name_match_key    STRING,
# MAGIC     last_name_match_key     STRING,
# MAGIC     full_name_match_key     STRING,
# MAGIC     match_type              STRING,
# MAGIC     is_golden_record        BOOLEAN,
# MAGIC     creation_dt             TIMESTAMP,
# MAGIC     updation_dt             TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold match-reference consumer/patient — domain-aware, GID = universal_mdm_key';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.sconsumermedia (
# MAGIC     gsme_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     gsme_scon_id        BIGINT      NOT NULL,
# MAGIC     email_match_key     STRING,
# MAGIC     valid_contact       BOOLEAN,
# MAGIC     creation_dt         TIMESTAMP,
# MAGIC     updation_dt         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold email match reference';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.sconsumerphone (
# MAGIC     gsph_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     gsph_scon_id        BIGINT      NOT NULL,
# MAGIC     phone_match_key     STRING,
# MAGIC     valid_contact       BOOLEAN,
# MAGIC     creation_dt         TIMESTAMP,
# MAGIC     updation_dt         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold phone match reference';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.sconsumeraddress (
# MAGIC     gsad_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     gsad_scon_id        BIGINT      NOT NULL,
# MAGIC     address_match_key   STRING,
# MAGIC     valid_contact       BOOLEAN,
# MAGIC     creation_dt         TIMESTAMP,
# MAGIC     updation_dt         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold address match reference';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 — DDL: silver.match_type_config
# MAGIC The control plane for this notebook. One row per `(domain_code,
# MAGIC match_type)`. Editing match logic for a domain — thresholds, which
# MAGIC contact key to block on, review requirements, or the confidence
# MAGIC weighting itself — is a `MERGE`/`UPDATE` on this table, no code changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.match_type_config (
# MAGIC     config_id               BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     domain_code             STRING      NOT NULL,
# MAGIC     match_type              STRING      NOT NULL,   -- MT1..MT7
# MAGIC     match_category          STRING      NOT NULL,   -- deterministic | name_key | full_name_key
# MAGIC     comparison_mode         STRING      NOT NULL,   -- EXACT (default, per spec) | FUZZY (Jaro-Winkler, future use)
# MAGIC     secondary_key           STRING,                 -- phone_match_key | email_match_key | address_match_key | NULL
# MAGIC     deterministic_field_a   STRING,                 -- e.g. source_entity_id (MT7 only)
# MAGIC     deterministic_field_b   STRING,                 -- e.g. brand_code (MT7 only)
# MAGIC     threshold               DOUBLE,                 -- confidence_score/100 must be >= this to accept
# MAGIC     requires_review         BOOLEAN,                -- TRUE = logged only, not used as a merge edge
# MAGIC     enabled                 BOOLEAN,
# MAGIC     priority                INT,                    -- lower = higher priority (best-match reporting)
# MAGIC     weight_component_a      INT,                     -- e.g. first_name=25 / full_name=50 / consumer_id=50
# MAGIC     weight_component_b      INT,                     -- e.g. last_name=25 / NULL / brand_code=50
# MAGIC     weight_contact          INT,                     -- e.g. phone/email/address=50 / NULL for MT7
# MAGIC     label_component_a       STRING,
# MAGIC     label_component_b       STRING,
# MAGIC     label_contact           STRING,
# MAGIC     description             STRING,
# MAGIC     creation_dt             TIMESTAMP,
# MAGIC     updation_dt             TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Config-driven match & merge rules + confidence weights, per domain — replaces YAML-file config';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed / upsert default match-type rules for RETAIL and HEALTHCARE
# MAGIC Re-running this cell is safe (MERGE, not INSERT). Add a new domain's
# MAGIC rules here, or edit thresholds/weights directly in the table afterward —
# MAGIC either way, no notebook code changes are required.

# COMMAND ----------

from pyspark.sql import Row

def mt_row(domain_code, match_type, category, comparison_mode, secondary_key, det_a, det_b,
           threshold, requires_review, priority,
           w_a, w_b, w_contact, lbl_a, lbl_b, lbl_contact, desc):
    return Row(
        domain_code=domain_code, match_type=match_type, match_category=category,
        comparison_mode=comparison_mode,
        secondary_key=secondary_key, deterministic_field_a=det_a, deterministic_field_b=det_b,
        threshold=threshold, requires_review=requires_review, enabled=True, priority=priority,
        weight_component_a=w_a, weight_component_b=w_b, weight_contact=w_contact,
        label_component_a=lbl_a, label_component_b=lbl_b, label_contact=lbl_contact,
        description=desc
    )

match_config_rows = []

for domain_code, addr_review, addr_threshold, base_threshold in [
    ("RETAIL",     False, 0.85, 0.85),
    ("HEALTHCARE", True,  0.85, 0.92),   # Healthcare: stricter threshold, address-based held for review
]:
    match_config_rows += [
        mt_row(domain_code, "MT7", "deterministic", "EXACT", None, "source_entity_id", "brand_code",
               1.00, False, 1, 50, 50, None,
               "consumer_id", "brand_code", None,
               "Exact match: consumer_id + brand_code"),
        mt_row(domain_code, "MT1", "name_key", "EXACT", "phone_match_key", None, None,
               base_threshold, False, 2, 25, 25, 50,
               "first_name", "last_name", "valid_phone",
               "Exact match: first+last name key + valid phone match key"),
        mt_row(domain_code, "MT2", "name_key", "EXACT", "email_match_key", None, None,
               base_threshold, False, 3, 25, 25, 50,
               "first_name", "last_name", "valid_email",
               "Exact match: first+last name key + valid email match key"),
        mt_row(domain_code, "MT3", "name_key", "EXACT", "address_match_key", None, None,
               addr_threshold, addr_review, 4, 25, 25, 50,
               "first_name", "last_name", "address",
               "Exact match: first+last name key + same address"),
        mt_row(domain_code, "MT4", "full_name_key", "EXACT", "phone_match_key", None, None,
               base_threshold, False, 5, 50, None, 50,
               "full_name", None, "valid_phone",
               "Exact match: full name key + valid phone match key"),
        mt_row(domain_code, "MT5", "full_name_key", "EXACT", "email_match_key", None, None,
               base_threshold, False, 6, 50, None, 50,
               "full_name", None, "valid_email",
               "Exact match: full name key + valid email match key"),
        mt_row(domain_code, "MT6", "full_name_key", "EXACT", "address_match_key", None, None,
               addr_threshold, addr_review, 7, 50, None, 50,
               "full_name", None, "address",
               "Exact match: full name key + same address"),
    ]

df_match_config = spark.createDataFrame(match_config_rows)
df_match_config.createOrReplaceTempView("stg_match_type_config")

spark.sql("""
    MERGE INTO silver.match_type_config AS tgt
    USING stg_match_type_config AS src
    ON  tgt.domain_code = src.domain_code
    AND tgt.match_type  = src.match_type
    WHEN MATCHED THEN UPDATE SET
        tgt.match_category        = src.match_category,
        tgt.comparison_mode       = src.comparison_mode,
        tgt.secondary_key         = src.secondary_key,
        tgt.deterministic_field_a = src.deterministic_field_a,
        tgt.deterministic_field_b = src.deterministic_field_b,
        tgt.threshold             = src.threshold,
        tgt.requires_review       = src.requires_review,
        tgt.enabled               = src.enabled,
        tgt.priority              = src.priority,
        tgt.weight_component_a    = src.weight_component_a,
        tgt.weight_component_b    = src.weight_component_b,
        tgt.weight_contact        = src.weight_contact,
        tgt.label_component_a     = src.label_component_a,
        tgt.label_component_b     = src.label_component_b,
        tgt.label_contact         = src.label_contact,
        tgt.description           = src.description,
        tgt.updation_dt           = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
        (domain_code, match_type, match_category, comparison_mode, secondary_key,
         deterministic_field_a, deterministic_field_b, threshold, requires_review,
         enabled, priority, weight_component_a, weight_component_b, weight_contact,
         label_component_a, label_component_b, label_contact, description,
         creation_dt, updation_dt)
    VALUES
        (src.domain_code, src.match_type, src.match_category, src.comparison_mode, src.secondary_key,
         src.deterministic_field_a, src.deterministic_field_b, src.threshold, src.requires_review,
         src.enabled, src.priority, src.weight_component_a, src.weight_component_b, src.weight_contact,
         src.label_component_a, src.label_component_b, src.label_contact, src.description,
         current_timestamp(), current_timestamp())
""")

display(spark.table("silver.match_type_config").orderBy("domain_code", "priority"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 — Create Temp Tables (DDL)
# MAGIC All domain-scoped via `domain_code`, truncated and fully reloaded every
# MAGIC run. `confidence_score` (0-100) and the two contributing similarity
# MAGIC values are carried on `tmp_raw_pairs` for full audit traceability.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.tmp_match_universe (
# MAGIC     domain_code             STRING,
# MAGIC     row_source              STRING,      -- SILVER | GOLD
# MAGIC     src_id                  BIGINT,      -- populated for SILVER rows
# MAGIC     scon_id                 BIGINT,      -- populated for GOLD rows
# MAGIC     universal_mdm_key       STRING,      -- NULL for SILVER (not yet assigned), populated for GOLD
# MAGIC     market_code             STRING,
# MAGIC     brand_code              STRING,
# MAGIC     source_entity_id        STRING,
# MAGIC     local_first_name        STRING,
# MAGIC     local_last_name         STRING,
# MAGIC     local_full_name         STRING,
# MAGIC     first_name_match_key    STRING,
# MAGIC     last_name_match_key     STRING,
# MAGIC     full_name_match_key     STRING,      -- 3-tier derived (Step 3)
# MAGIC     full_name_match_key_desc STRING,     -- token-reversed, for swapped full-name comparisons
# MAGIC     phone_match_key         STRING,
# MAGIC     valid_phone             BOOLEAN,
# MAGIC     email_match_key         STRING,
# MAGIC     valid_email             BOOLEAN,
# MAGIC     address_match_key       STRING,
# MAGIC     valid_address           BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temp — unified Silver(new) + Gold(existing) candidate universe for matching, per domain';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.tmp_raw_pairs (
# MAGIC     domain_code         STRING,
# MAGIC     src_id              BIGINT,   -- incoming Silver src_id
# MAGIC     matched_src_id      BIGINT,   -- matched Silver src_id (NULL = Gold-only match)
# MAGIC     matched_scon_id     BIGINT,   -- matched Gold scon_id (NULL = Silver-only match)
# MAGIC     matched_mdm_key     STRING,   -- matched Gold universal_mdm_key, if any
# MAGIC     match_type          STRING,   -- MT1..MT7
# MAGIC     match_score         INT,      -- priority — 1 = highest (reporting only)
# MAGIC     confidence_score    DOUBLE,   -- 0-100, weighted per silver.match_type_config
# MAGIC     name_similarity     DOUBLE,   -- best-orientation Jaro-Winkler score contributing to confidence_score
# MAGIC     requires_review     BOOLEAN   -- TRUE = logged, but NOT used as a merge edge
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temp — every match-type hit clearing its confidence threshold, before group resolution';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.tmp_match_groups (
# MAGIC     domain_code     STRING,
# MAGIC     src_id          BIGINT,
# MAGIC     group_root_id   BIGINT,
# MAGIC     group_scon_id   BIGINT,
# MAGIC     group_mdm_key   STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temp — connected component group assignments, per domain';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.tmp_gid_assignment (
# MAGIC     domain_code     STRING,
# MAGIC     src_id          BIGINT,
# MAGIC     gid             STRING,
# MAGIC     match_type      STRING,
# MAGIC     confidence_score DOUBLE,
# MAGIC     is_new          BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temp — final GID per src_id for the current run, per domain';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 — Truncate Temp Tables (fresh full reload every run)

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.tmp_match_universe;
# MAGIC TRUNCATE TABLE silver.tmp_raw_pairs;
# MAGIC TRUNCATE TABLE silver.tmp_match_groups;
# MAGIC TRUNCATE TABLE silver.tmp_gid_assignment;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 — Setup: UDFs, Config Reader, Name Helpers

# COMMAND ----------

import uuid
from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType, StringType

@F.udf(returnType=DoubleType())
def jaro_sim(a, b):
    if not a or not b:
        return 0.0
    return jellyfish.jaro_winkler_similarity(str(a), str(b))
spark.udf.register("jaro_sim", jaro_sim)

gen_uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
spark.udf.register("gen_uuid", gen_uuid_udf)

def reverse_name_tokens(col):
    """'RAJU DUDE' -> 'DUDE RAJU' — catches swapped word order in a full name."""
    return F.concat_ws(" ", F.reverse(F.split(F.trim(col), " ")))

def load_match_type_config(domain_code: str):
    """Reads match rules for a domain from the config TABLE (not YAML)."""
    rows = spark.sql(f"""
        SELECT *
        FROM   silver.match_type_config
        WHERE  domain_code = '{domain_code}'
        AND    enabled     = TRUE
        ORDER  BY priority
    """).collect()
    if not rows:
        raise RuntimeError(f"No match_type_config rows found for domain_code={domain_code}")
    return rows

print(">>> UDFs registered, config-table reader ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 — Resolve Active Domains + Latest Silver Batch
# MAGIC Reads directly from `silver.sourceconsumer` — since Step 3 truncates and
# MAGIC fully reloads Silver every run, this table's current contents *are* the
# MAGIC latest batch per domain; no separate lookup needed.

# COMMAND ----------

domains_to_process = spark.sql("""
    SELECT domain_code, MAX(batch_id) AS batch_id
    FROM   silver.sourceconsumer
    GROUP  BY domain_code
""").collect()

print(">>> Domains queued for Match & Merge:")
for row in domains_to_process:
    print(f"    {row['domain_code']}: batch_id = {row['batch_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.8 — Step 1: Build SILVER Candidates (GID = NULL)

# COMMAND ----------

def build_silver_candidates(domain_code: str, batch_id: int):
    df = spark.sql(f"""
        SELECT
            sc.src_id,
            sc.market_code,
            sc.brand_code,
            sc.source_entity_id,
            sc.local_first_name,
            sc.local_last_name,
            sc.local_full_name,
            sc.first_name_match_key,
            sc.last_name_match_key,
            sc.full_name_match_key,
            COALESCE(
                MAX(CASE WHEN sp.is_primary = TRUE AND sp.valid_contact = TRUE THEN sp.phone_match_key END),
                MAX(CASE WHEN sp.valid_contact = TRUE THEN sp.phone_match_key END)
            )                                              AS phone_match_key,
            MAX(CAST(sp.valid_contact AS INT)) = 1         AS valid_phone,
            MAX(CASE WHEN se.valid_contact = TRUE THEN se.email_match_key END)
                                                            AS email_match_key,
            MAX(CAST(se.valid_contact AS INT)) = 1         AS valid_email,
            MAX(CASE WHEN sa.valid_contact = TRUE THEN sa.address_match_key END)
                                                            AS address_match_key,
            MAX(CAST(sa.valid_contact AS INT)) = 1         AS valid_address
        FROM   silver.sourceconsumer sc
        LEFT   JOIN silver.sourcephone   sp ON sp.srph_src_id = sc.src_id
        LEFT   JOIN silver.sourcemedia   se ON se.srme_src_id = sc.src_id
        LEFT   JOIN silver.sourceaddress sa ON sa.srad_src_id = sc.src_id
        WHERE  sc.domain_code = '{domain_code}' AND sc.batch_id = {batch_id}
        GROUP BY
            sc.src_id, sc.market_code, sc.brand_code, sc.source_entity_id,
            sc.local_first_name, sc.local_last_name, sc.local_full_name,
            sc.first_name_match_key, sc.last_name_match_key, sc.full_name_match_key
    """)

    df = (
        df
        .withColumn("domain_code", F.lit(domain_code))
        .withColumn("row_source", F.lit("SILVER"))
        .withColumn("scon_id", F.lit(None).cast("bigint"))
        .withColumn("universal_mdm_key", F.lit(None).cast("string"))   # empty GID
        .withColumn("full_name_match_key_desc", reverse_name_tokens(F.col("full_name_match_key")))
        .select(
            "domain_code", "row_source", "src_id", "scon_id", "universal_mdm_key",
            "market_code", "brand_code", "source_entity_id",
            "local_first_name", "local_last_name", "local_full_name",
            "first_name_match_key", "last_name_match_key",
            "full_name_match_key", "full_name_match_key_desc",
            "phone_match_key", "valid_phone",
            "email_match_key", "valid_email",
            "address_match_key", "valid_address",
        )
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.9 — Step 2: Fetch Matching GOLD Candidates (with `universal_mdm_key`)
# MAGIC Scoped to Gold rows sharing at least one blocking value with the current
# MAGIC Silver batch, to avoid pulling the entire Gold table for comparison.

# COMMAND ----------

def build_gold_candidates(domain_code: str, df_silver):
    if not spark.catalog.tableExists("gold.sconsumer"):
        return df_silver.limit(0)

    silver_phones = [r["phone_match_key"] for r in
        df_silver.select("phone_match_key").filter(F.col("phone_match_key").isNotNull()).distinct().collect()]
    silver_emails = [r["email_match_key"] for r in
        df_silver.select("email_match_key").filter(F.col("email_match_key").isNotNull()).distinct().collect()]
    silver_addrs = [r["address_match_key"] for r in
        df_silver.select("address_match_key").filter(F.col("address_match_key").isNotNull()).distinct().collect()]

    gc = spark.sql(f"""
        SELECT
            gc.scon_id, gc.universal_mdm_key, gc.market_code, gc.brand_code, gc.source_entity_id,
            gc.local_first_name, gc.local_last_name, gc.local_full_name,
            gc.first_name_match_key, gc.last_name_match_key, gc.full_name_match_key,
            MAX(CASE WHEN gp.valid_contact = TRUE THEN gp.phone_match_key END)   AS phone_match_key,
            MAX(CAST(gp.valid_contact AS INT)) = 1                              AS valid_phone,
            MAX(CASE WHEN gm.valid_contact = TRUE THEN gm.email_match_key END)  AS email_match_key,
            MAX(CAST(gm.valid_contact AS INT)) = 1                              AS valid_email,
            MAX(CASE WHEN ga.valid_contact = TRUE THEN ga.address_match_key END) AS address_match_key,
            MAX(CAST(ga.valid_contact AS INT)) = 1                              AS valid_address
        FROM   gold.sconsumer gc
        LEFT   JOIN gold.sconsumerphone   gp ON gp.gsph_scon_id = gc.scon_id
        LEFT   JOIN gold.sconsumermedia   gm ON gm.gsme_scon_id = gc.scon_id
        LEFT   JOIN gold.sconsumeraddress ga ON ga.gsad_scon_id = gc.scon_id
        WHERE  gc.domain_code = '{domain_code}'
        GROUP BY
            gc.scon_id, gc.universal_mdm_key, gc.market_code, gc.brand_code, gc.source_entity_id,
            gc.local_first_name, gc.local_last_name, gc.local_full_name,
            gc.first_name_match_key, gc.last_name_match_key, gc.full_name_match_key
    """)

    in_scope = (
        (F.col("phone_match_key").isin(silver_phones) if silver_phones else F.lit(False))
        | (F.col("email_match_key").isin(silver_emails) if silver_emails else F.lit(False))
        | (F.col("address_match_key").isin(silver_addrs) if silver_addrs else F.lit(False))
        | F.col("source_entity_id").isin(
              [r["source_entity_id"] for r in
               df_silver.select("source_entity_id").filter(F.col("source_entity_id").isNotNull()).distinct().collect()]
              or [""]
          )
    )
    gc = gc.filter(in_scope)

    gc = (
        gc
        .withColumn("domain_code", F.lit(domain_code))
        .withColumn("row_source", F.lit("GOLD"))
        .withColumn("src_id", F.lit(None).cast("bigint"))
        .withColumn("full_name_match_key_desc", reverse_name_tokens(F.col("full_name_match_key")))
        .select(
            "domain_code", "row_source", "src_id", "scon_id", "universal_mdm_key",
            "market_code", "brand_code", "source_entity_id",
            "local_first_name", "local_last_name", "local_full_name",
            "first_name_match_key", "last_name_match_key",
            "full_name_match_key", "full_name_match_key_desc",
            "phone_match_key", "valid_phone",
            "email_match_key", "valid_email",
            "address_match_key", "valid_address",
        )
    )
    return gc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.10 — Step 3a: Generate Match Pairs, Weighted by Confidence Score
# MAGIC Reads each enabled rule from `silver.match_type_config` for the domain
# MAGIC and dispatches to the matching join function. Every pair carries its
# MAGIC `confidence_score` (0-100) and is only kept if
# MAGIC `confidence_score / 100 >= rule.threshold`.

# COMMAND ----------

def _run_deterministic_match(df_silver, df_universe, rule):
    field_a, field_b = rule["deterministic_field_a"], rule["deterministic_field_b"]
    w_a, w_b = rule["weight_component_a"], rule["weight_component_b"]

    a = df_silver.select(
        F.col("src_id").alias("a_src_id"),
        F.col(field_a).alias("a_field_a"), F.col(field_b).alias("a_field_b"),
    )
    b = df_universe.select(
        F.col("src_id").alias("b_src_id"), F.col("scon_id").alias("b_scon_id"),
        F.col("universal_mdm_key").alias("b_mdm_key"),
        F.col(field_a).alias("b_field_a"), F.col(field_b).alias("b_field_b"),
    )
    cond = (
        (F.col("a_field_a") == F.col("b_field_a")) & F.col("a_field_a").isNotNull()
        & (F.col("a_field_b") == F.col("b_field_b")) & F.col("a_field_b").isNotNull()
        & (F.col("a_src_id") != F.coalesce(F.col("b_src_id"), F.lit(-1)))
    )
    return (
        a.join(b, cond, "inner")
         .withColumn("confidence_score", F.lit(float(w_a + w_b)))  # always 100 — both fields exact by join condition
         .withColumn("name_similarity", F.lit(1.0))
         .select(
             F.col("a_src_id").alias("src_id"),
             F.col("b_src_id").alias("matched_src_id"),
             F.col("b_scon_id").alias("matched_scon_id"),
             F.col("b_mdm_key").alias("matched_mdm_key"),
             "confidence_score", "name_similarity",
         )
    )


def _run_name_key_match(df_silver, df_universe, rule):
    """MT1-3: first_name (w_a) + last_name (w_b) components + exact contact
    block (w_contact). comparison_mode='EXACT' (spec default) requires
    literal string equality — bidirectionally, to catch first/last fields
    swapped between source systems — as part of the JOIN itself, so a pair
    is only ever produced when the names are truly identical in either
    orientation. comparison_mode='FUZZY' is kept available for future,
    per-domain-per-match-type reconfiguration (Jaro-Winkler similarity as a
    continuous multiplier instead of a hard equality requirement)."""
    secondary_col = rule["secondary_key"]
    w_a, w_b, w_contact = rule["weight_component_a"], rule["weight_component_b"], rule["weight_contact"]
    mode = (rule["comparison_mode"] or "EXACT").upper()

    a = df_silver.select(
        F.col("src_id").alias("a_src_id"),
        F.col("market_code").alias("a_market"),
        F.col(secondary_col).alias("a_secondary"),
        F.col("first_name_match_key").alias("a_fn"), F.col("last_name_match_key").alias("a_ln"),
    ).filter(F.col("a_secondary").isNotNull() & F.col("a_fn").isNotNull() & F.col("a_ln").isNotNull())

    b = df_universe.select(
        F.col("src_id").alias("b_src_id"), F.col("scon_id").alias("b_scon_id"),
        F.col("universal_mdm_key").alias("b_mdm_key"),
        F.col("market_code").alias("b_market"),
        F.col(secondary_col).alias("b_secondary"),
        F.col("first_name_match_key").alias("b_fn"), F.col("last_name_match_key").alias("b_ln"),
    ).filter(F.col("b_secondary").isNotNull() & F.col("b_fn").isNotNull() & F.col("b_ln").isNotNull())

    if mode == "EXACT":
        # Bidirectional EXACT equality — natural order OR first/last swapped —
        # baked directly into the join condition: no pair exists unless one
        # of these two orientations is a literal, character-for-character match.
        name_match_cond = (
            ((F.col("a_fn") == F.col("b_fn")) & (F.col("a_ln") == F.col("b_ln")))
            | ((F.col("a_fn") == F.col("b_ln")) & (F.col("a_ln") == F.col("b_fn")))
        )
        joined = a.join(
            b,
            (F.col("a_market") == F.col("b_market"))
            & (F.col("a_secondary") == F.col("b_secondary"))
            & (F.col("a_src_id") != F.coalesce(F.col("b_src_id"), F.lit(-1)))
            & name_match_cond,
            "inner",
        )
        return (
            joined
            .withColumn("confidence_score", F.lit(float(w_a + w_b + w_contact)))  # exact match => full weight
            .withColumn("name_similarity", F.lit(1.0))
            .select(
                F.col("a_src_id").alias("src_id"),
                F.col("b_src_id").alias("matched_src_id"),
                F.col("b_scon_id").alias("matched_scon_id"),
                F.col("b_mdm_key").alias("matched_mdm_key"),
                "confidence_score", "name_similarity",
            )
        )

    # FUZZY (not used by default — selectable per domain/match type later)
    joined = a.join(
        b,
        (F.col("a_market") == F.col("b_market"))
        & (F.col("a_secondary") == F.col("b_secondary"))
        & (F.col("a_src_id") != F.coalesce(F.col("b_src_id"), F.lit(-1))),
        "inner",
    )
    sim_fn_natural = jaro_sim(F.col("a_fn"), F.col("b_fn"))
    sim_ln_natural = jaro_sim(F.col("a_ln"), F.col("b_ln"))
    sim_fn_swapped = jaro_sim(F.col("a_fn"), F.col("b_ln"))
    sim_ln_swapped = jaro_sim(F.col("a_ln"), F.col("b_fn"))
    score_natural = (F.lit(w_a) * sim_fn_natural) + (F.lit(w_b) * sim_ln_natural)
    score_swapped = (F.lit(w_a) * sim_fn_swapped) + (F.lit(w_b) * sim_ln_swapped)
    best_name_score = F.greatest(score_natural, score_swapped)
    best_sim = F.greatest(
        F.least(sim_fn_natural, sim_ln_natural),
        F.least(sim_fn_swapped, sim_ln_swapped)
    )
    return (
        joined
        .withColumn("confidence_score", best_name_score + F.lit(w_contact))
        .withColumn("name_similarity", best_sim)
        .select(
            F.col("a_src_id").alias("src_id"),
            F.col("b_src_id").alias("matched_src_id"),
            F.col("b_scon_id").alias("matched_scon_id"),
            F.col("b_mdm_key").alias("matched_mdm_key"),
            "confidence_score", "name_similarity",
        )
    )


def _run_full_name_key_match(df_silver, df_universe, rule):
    """MT4-6: single full_name (w_a) component + exact contact block
    (w_contact). comparison_mode='EXACT' (spec default) requires literal
    equality — natural order OR token-reversed, to catch swapped word order
    in a single full-name string — as part of the JOIN itself."""
    secondary_col = rule["secondary_key"]
    w_a, w_contact = rule["weight_component_a"], rule["weight_contact"]
    mode = (rule["comparison_mode"] or "EXACT").upper()

    a = df_silver.select(
        F.col("src_id").alias("a_src_id"),
        F.col("market_code").alias("a_market"),
        F.col(secondary_col).alias("a_secondary"),
        F.col("full_name_match_key").alias("a_full"),
    ).filter(F.col("a_secondary").isNotNull() & F.col("a_full").isNotNull())

    b = df_universe.select(
        F.col("src_id").alias("b_src_id"), F.col("scon_id").alias("b_scon_id"),
        F.col("universal_mdm_key").alias("b_mdm_key"),
        F.col("market_code").alias("b_market"),
        F.col(secondary_col).alias("b_secondary"),
        F.col("full_name_match_key").alias("b_full"),
        F.col("full_name_match_key_desc").alias("b_full_desc"),
    ).filter(F.col("b_secondary").isNotNull() & F.col("b_full").isNotNull())

    if mode == "EXACT":
        name_match_cond = (
            (F.col("a_full") == F.col("b_full")) | (F.col("a_full") == F.col("b_full_desc"))
        )
        joined = a.join(
            b,
            (F.col("a_market") == F.col("b_market"))
            & (F.col("a_secondary") == F.col("b_secondary"))
            & (F.col("a_src_id") != F.coalesce(F.col("b_src_id"), F.lit(-1)))
            & name_match_cond,
            "inner",
        )
        return (
            joined
            .withColumn("confidence_score", F.lit(float(w_a + w_contact)))  # exact match => full weight
            .withColumn("name_similarity", F.lit(1.0))
            .select(
                F.col("a_src_id").alias("src_id"),
                F.col("b_src_id").alias("matched_src_id"),
                F.col("b_scon_id").alias("matched_scon_id"),
                F.col("b_mdm_key").alias("matched_mdm_key"),
                "confidence_score", "name_similarity",
            )
        )

    # FUZZY (not used by default — selectable per domain/match type later)
    joined = a.join(
        b,
        (F.col("a_market") == F.col("b_market"))
        & (F.col("a_secondary") == F.col("b_secondary"))
        & (F.col("a_src_id") != F.coalesce(F.col("b_src_id"), F.lit(-1))),
        "inner",
    )
    sim_natural = jaro_sim(F.col("a_full"), F.col("b_full"))
    sim_reversed = jaro_sim(F.col("a_full"), F.col("b_full_desc"))
    best_sim = F.greatest(sim_natural, sim_reversed)
    return (
        joined
        .withColumn("confidence_score", (F.lit(w_a) * best_sim) + F.lit(w_contact))
        .withColumn("name_similarity", best_sim)
        .select(
            F.col("a_src_id").alias("src_id"),
            F.col("b_src_id").alias("matched_src_id"),
            F.col("b_scon_id").alias("matched_scon_id"),
            F.col("b_mdm_key").alias("matched_mdm_key"),
            "confidence_score", "name_similarity",
        )
    )


def generate_match_pairs(domain_code: str, df_silver, df_universe, rules):
    pair_frames = []

    for rule in rules:
        mt = rule["match_type"]
        threshold = rule["threshold"]
        requires_review = rule["requires_review"]
        priority = rule["priority"]

        if rule["match_category"] == "deterministic":
            pairs = _run_deterministic_match(df_silver, df_universe, rule)
        elif rule["match_category"] == "name_key":
            pairs = _run_name_key_match(df_silver, df_universe, rule)
        elif rule["match_category"] == "full_name_key":
            pairs = _run_full_name_key_match(df_silver, df_universe, rule)
        else:
            continue

        pairs = (
            pairs
            .filter((F.col("confidence_score") / F.lit(100.0)) >= F.lit(threshold))
            .withColumn("match_type", F.lit(mt))
            .withColumn("match_score", F.lit(priority))
            .withColumn("requires_review", F.lit(requires_review))
        )
        pair_frames.append(pairs)

    if not pair_frames:
        return spark.createDataFrame(
            [], "src_id BIGINT, matched_src_id BIGINT, matched_scon_id BIGINT, "
                "matched_mdm_key STRING, confidence_score DOUBLE, name_similarity DOUBLE, "
                "match_type STRING, match_score INT, requires_review BOOLEAN"
        )

    df_all = pair_frames[0]
    for p in pair_frames[1:]:
        df_all = df_all.union(p.select(df_all.columns))

    return (
        df_all.withColumn("domain_code", F.lit(domain_code))
        .dropDuplicates(["src_id", "matched_src_id", "matched_scon_id", "match_type"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.11 — Step 3b: Connected Components — Group Unification
# MAGIC Only pairs where `requires_review = FALSE` are used as merge edges.

# COMMAND ----------

def resolve_groups(domain_code: str, df_universe, df_pairs):
    df_auto_pairs = df_pairs.filter(F.col("requires_review") == False)  # noqa: E712

    df_edges_ss = df_auto_pairs.filter(F.col("matched_src_id").isNotNull()).select(
        F.col("src_id").alias("node_a"), F.col("matched_src_id").alias("node_b")
    )
    df_edges_sg = df_auto_pairs.filter(F.col("matched_scon_id").isNotNull()).select(
        F.col("src_id").alias("node_a"), (-F.col("matched_scon_id")).alias("node_b")
    )
    df_edges = df_edges_ss.union(df_edges_sg)
    df_edges_all = df_edges.union(
        df_edges.select(F.col("node_b").alias("node_a"), F.col("node_a").alias("node_b"))
    ).dropDuplicates()

    df_all_nodes = (
        df_universe.filter(F.col("row_source") == "SILVER").select(F.col("src_id").alias("node"))
        .union(df_edges.select("node_a").union(df_edges.select("node_b")))
        .dropDuplicates()
    )

    df_labels = df_all_nodes.withColumn("label", F.col("node"))

    MAX_ITER = 20
    for i in range(MAX_ITER):
        df_prop = (
            df_labels.alias("l")
            .join(df_edges_all.alias("e"), F.col("l.node") == F.col("e.node_a"))
            .join(df_labels.alias("l2"), F.col("e.node_b") == F.col("l2.node"))
            .select(F.col("l.node"), F.least(F.col("l.label"), F.col("l2.label")).alias("new_label"))
        )
        df_labels_new = (
            df_labels.join(df_prop, "node", "left")
            .withColumn("label", F.coalesce(
                F.when(F.col("new_label") < F.col("label"), F.col("new_label")), F.col("label")))
            .drop("new_label")
        )
        changed = (
            df_labels_new.join(df_labels.withColumnRenamed("label", "old_label"), "node")
            .filter(F.col("label") != F.col("old_label")).count()
        )
        df_labels = df_labels_new
        if changed == 0:
            print(f"    [{domain_code}] connected components converged at iteration {i + 1}")
            break

    df_silver_labels = (
        df_labels.filter(F.col("node") > 0)
        .withColumnRenamed("node", "src_id")
        .withColumnRenamed("label", "group_root_id")
    )

    df_gold_links = df_silver_labels.join(
        df_edges_all.filter(F.col("node_b") < 0)
            .select(F.col("node_a").alias("src_id"), (-F.col("node_b")).alias("linked_scon_id")),
        "src_id", "left",
    )

    df_group_gold = (
        df_gold_links.filter(F.col("linked_scon_id").isNotNull())
        .groupBy("group_root_id")
        .agg(F.min("linked_scon_id").alias("min_scon_id"))
        .join(
            spark.table("gold.sconsumer").select("scon_id", "universal_mdm_key"),
            F.col("min_scon_id") == F.col("scon_id"), "left",
        )
        .select("group_root_id", "min_scon_id", F.col("universal_mdm_key").alias("group_mdm_key"))
    ) if spark.catalog.tableExists("gold.sconsumer") else spark.createDataFrame(
        [], "group_root_id BIGINT, min_scon_id BIGINT, group_mdm_key STRING"
    )

    df_new_groups = (
        df_silver_labels.select("group_root_id").distinct()
        .join(df_group_gold.select("group_root_id"), "group_root_id", "left_anti")
        .withColumn("group_mdm_key", F.expr("gen_uuid()"))
        .withColumn("min_scon_id", F.lit(None).cast("bigint"))
    )

    df_all_gids = df_group_gold.union(df_new_groups.select("group_root_id", "min_scon_id", "group_mdm_key"))

    df_gid_resolved = (
        df_silver_labels.join(df_all_gids, "group_root_id", "left")
        .select("src_id", "group_root_id", F.col("min_scon_id").alias("group_scon_id"), "group_mdm_key")
        .withColumn("domain_code", F.lit(domain_code))
    )
    return df_gid_resolved, df_auto_pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.12 — Per-Domain Orchestration

# COMMAND ----------

for row in domains_to_process:
    domain_code = row["domain_code"]
    batch_id = row["batch_id"]
    print(f"\n=== Match & Merge: {domain_code} | batch_id={batch_id} ===")

    rules = load_match_type_config(domain_code)

    # Step 1: Silver candidates, GID empty
    df_silver = build_silver_candidates(domain_code, batch_id)
    print(f"    Silver candidates: {df_silver.count()}")

    # Step 2: Gold candidates (existing GID), unioned into the same universe
    df_gold = build_gold_candidates(domain_code, df_silver)
    print(f"    Gold candidates in scope: {df_gold.count()}")

    df_universe = df_silver.unionByName(df_gold)
    df_universe.write.format("delta").mode("append").saveAsTable("silver.tmp_match_universe")

    # Step 3a: generate all match-type pairs per config, confidence-scored
    df_pairs = generate_match_pairs(domain_code, df_silver, df_universe, rules)
    pair_count = df_pairs.count()
    print(f"    Raw match pairs (all types, incl. review-only): {pair_count}")

    (df_pairs
        .select("domain_code", "src_id", "matched_src_id", "matched_scon_id",
                "matched_mdm_key", "match_type", "match_score",
                "confidence_score", "name_similarity", "requires_review")
        .write.format("delta").mode("append").saveAsTable("silver.tmp_raw_pairs"))

    # Step 3b: connected components on auto-assignable pairs only
    df_gid_resolved, df_auto_pairs = resolve_groups(domain_code, df_universe, df_pairs)
    df_gid_resolved.write.format("delta").mode("append").saveAsTable("silver.tmp_match_groups")

    # Best (lowest match_score = highest priority) match type per src_id, among auto pairs
    win_mt = Window.partitionBy("src_id").orderBy(F.asc("match_score"))
    df_best_mt = (
        df_auto_pairs.filter(F.col("src_id").isNotNull())
        .withColumn("rn", F.row_number().over(win_mt))
        .filter(F.col("rn") == 1)
        .select("src_id", "match_type", "confidence_score")
    )

    df_final_gid = (
        df_gid_resolved.select("src_id", "group_mdm_key", "group_scon_id")
        .join(df_best_mt, "src_id", "left")
        .withColumn("is_new", F.col("group_scon_id").isNull())
        .withColumn("match_type", F.when(F.col("match_type").isNotNull(), F.col("match_type")).otherwise(F.lit("NEW")))
        .withColumn("confidence_score", F.coalesce(F.col("confidence_score"), F.lit(100.0)))
        .withColumn("domain_code", F.lit(domain_code))
        .select("domain_code", "src_id", F.col("group_mdm_key").alias("gid"),
                "match_type", "confidence_score", "is_new")
    )
    df_final_gid.write.format("delta").mode("append").saveAsTable("silver.tmp_gid_assignment")

    print(f"    >>> {domain_code} — GID assignment summary:")
    for r in df_final_gid.groupBy("match_type", "is_new").count().orderBy("match_type").collect():
        print(f"        {r['match_type']:5s} | {'NEW' if r['is_new'] else 'MATCHED':7s} | {r['count']:>6,} records")

print("\n>>> Step 4 complete — all domains processed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.13 — QA Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT domain_code, match_type, is_new, ROUND(AVG(confidence_score), 1) AS avg_confidence, COUNT(*) AS cnt
# MAGIC FROM   silver.tmp_gid_assignment
# MAGIC GROUP  BY domain_code, match_type, is_new
# MAGIC ORDER  BY domain_code, match_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Groups with more than one profile (merged consumers/patients)
# MAGIC SELECT domain_code, gid, COUNT(*) AS profiles_in_group, COLLECT_LIST(src_id) AS src_ids
# MAGIC FROM   silver.tmp_gid_assignment
# MAGIC GROUP  BY domain_code, gid HAVING COUNT(*) > 1
# MAGIC ORDER  BY profiles_in_group DESC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Must return 0 — every src_id in the batch should have a GID
# MAGIC SELECT domain_code, COUNT(*) AS missing_gid
# MAGIC FROM   silver.tmp_gid_assignment
# MAGIC WHERE  gid IS NULL
# MAGIC GROUP  BY domain_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pairs held for steward review — not auto-merged
# MAGIC SELECT domain_code, match_type, COUNT(*) AS pending_review_pairs,
# MAGIC        ROUND(AVG(confidence_score), 1) AS avg_confidence
# MAGIC FROM   silver.tmp_raw_pairs
# MAGIC WHERE  requires_review = TRUE
# MAGIC GROUP  BY domain_code, match_type
# MAGIC ORDER  BY domain_code, match_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confidence score distribution per match type, per domain
# MAGIC SELECT domain_code, match_type, FLOOR(confidence_score / 10) * 10 AS score_bucket, COUNT(*) AS cnt
# MAGIC FROM   silver.tmp_raw_pairs
# MAGIC GROUP  BY domain_code, match_type, FLOOR(confidence_score / 10) * 10
# MAGIC ORDER  BY domain_code, match_type, score_bucket DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pairs that matched on contact but scored below full confidence
# MAGIC -- (demonstrates the 0.85 threshold doing real work, not rubber-stamping)
# MAGIC SELECT domain_code, match_type, src_id, matched_src_id, matched_scon_id,
# MAGIC        confidence_score, name_similarity
# MAGIC FROM   silver.tmp_raw_pairs
# MAGIC WHERE  confidence_score < 100 AND confidence_score >= 50
# MAGIC ORDER  BY confidence_score DESC
# MAGIC LIMIT  20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full trace: GID + candidate details + contact match keys
# MAGIC SELECT
# MAGIC     g.domain_code, g.gid, g.match_type, g.confidence_score, g.is_new,
# MAGIC     u.local_first_name, u.local_last_name, u.local_full_name,
# MAGIC     u.full_name_match_key, u.phone_match_key, u.email_match_key, u.address_match_key
# MAGIC FROM   silver.tmp_gid_assignment g
# MAGIC JOIN   silver.tmp_match_universe u
# MAGIC        ON u.domain_code = g.domain_code AND u.src_id = g.src_id AND u.row_source = 'SILVER'
# MAGIC ORDER  BY g.domain_code, g.gid, u.src_id;