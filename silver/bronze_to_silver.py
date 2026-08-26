# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import Window
from pyspark.sql.functions import (
    lit, to_json, struct, current_timestamp, col,
    max as spark_max, count as spark_count, row_number
)
from datetime import datetime, timedelta

CATALOG = "project01_databricks01"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine table

# COMMAND ----------

def write_to_quarantine(quarantine_df, table_name, check_name, column_name=None, failure_reason=None):
    """Row-level failures. One row per failed record per check."""
    if len(quarantine_df.take(1)) == 0:
        return
    (quarantine_df
        .withColumn("table_name", lit(table_name))
        .withColumn("check_name", lit(check_name))
        .withColumn("column_name", lit(column_name))
        .withColumn("failed_record", to_json(struct(*quarantine_df.columns)))
        .withColumn("failure_reason", lit(failure_reason))
        .withColumn("processed_date", current_timestamp())
        .select("table_name", "check_name", "column_name", "failed_record", "failure_reason", "processed_date")
        .write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.silver.quarantine")
    )


def write_table_level_quarantine(table_name, check_name, failure_reason, detail_value=None):
    """Table-level failures (freshness). No single failing row — failed_record holds diagnostic detail."""
    row = spark.createDataFrame(
        [(table_name, check_name, str(detail_value), failure_reason)],
        ["table_name", "check_name", "detail", "failure_reason"]
    )
    (row
        .withColumn("column_name", lit(None).cast("string"))
        .withColumn("failed_record", to_json(struct(col("detail"))))
        .withColumn("processed_date", current_timestamp())
        .select("table_name", "check_name", "column_name", "failed_record", "failure_reason", "processed_date")
        .write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.silver.quarantine")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table config
# MAGIC id_col = primary null_check + dedup key. zip_col/range_col/format_regex/extra_format_checks/dup_check
# MAGIC only set where confirmed necessary against real bronze data.

# COMMAND ----------

TABLE_CONFIG = {
    "customers":   {"table": "bronze.customers",  "id_col": "customer_id",  "zip_col": "customer_zip_code_prefix"},
    "sellers":     {"table": "bronze.sellers",     "id_col": "seller_id",    "zip_col": "seller_zip_code_prefix"},
    "orders":      {"table": "bronze.orders",      "id_col": "order_id",     "dup_check": True},
    "products":    {"table": "bronze.products",    "id_col": "product_id"},
    "payments":    {"table": "bronze.payments",    "id_col": "order_id",     "range_col": "payment_value"},
    "order_items": {"table": "bronze.order_items", "id_col": "order_id"},
    "reviews": {
        "table": "bronze.reviews",
        "id_col": "review_id",
        "format_regex": r'^[0-9a-f]{32}$',
        "extra_format_checks": {"order_id": r'^[0-9a-f]{32}$'},
        "dup_check": True,
    },
}

MERGE_KEYS = {
    "customers":   ["customer_id"],
    "sellers":     ["seller_id"],
    "products":    ["product_id"],
    "orders":      ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments":    ["order_id", "payment_sequential"],
    "reviews":     ["review_id"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row-level checks — chained per table

# COMMAND ----------

valid_dfs = {}


for name, cfg in TABLE_CONFIG.items():
    df = spark.read.table(cfg["table"])
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")
    id_col = cfg["id_col"]

    # null_check — every table's key column
    null_mask = col(id_col).isNotNull()
    write_to_quarantine(df.filter(~null_mask), name, "null_check", id_col, f"{id_col} is null")
    df = df.filter(null_mask)

    # format_check — primary id column (reviews: rejects shifted-comment garbage)
    if cfg.get("format_regex"):
        fmt_mask = col(id_col).rlike(cfg["format_regex"])
        write_to_quarantine(df.filter(~fmt_mask), name, "format_check", id_col, f"{id_col} fails format check")
        df = df.filter(fmt_mask)

    # format_check — any secondary columns (reviews: order_id also shifted in some rows)
    for extra_col, pattern in cfg.get("extra_format_checks", {}).items():
        extra_mask = col(extra_col).rlike(pattern)
        write_to_quarantine(df.filter(~extra_mask), name, "format_check", extra_col, f"{extra_col} fails format check")
        df = df.filter(extra_mask)

    # zip_check — customers/sellers
    if cfg.get("zip_col"):
        zip_col = cfg["zip_col"]
        df = df.withColumn(zip_col, col(zip_col).cast("int"))
        zip_mask = col(zip_col).isNotNull() & col(zip_col).rlike(r'^[0-9]')
        write_to_quarantine(df.filter(~zip_mask), name, "zip_check", zip_col, f"{zip_col} fails format check")
        df = df.filter(zip_mask)

    # range_check — payments
    if cfg.get("range_col"):
        range_col = cfg["range_col"]
        df = df.withColumn(range_col, col(range_col).cast("double"))
        range_mask = col(range_col).isNotNull() & (col(range_col) >= 0)
        write_to_quarantine(df.filter(~range_mask), name, "range_check", range_col, f"{range_col} is negative or null")
        df = df.filter(range_mask)

    # duplicate_check — orders (true clones) + reviews (real dupes), window dedup by processing_time desc
    if cfg.get("dup_check"):
        dup_window = Window.partitionBy(id_col).orderBy(col("processing_time").desc())
        ranked_df = df.withColumn("_rn", row_number().over(dup_window))

        dup_rows = ranked_df.filter(col("_rn") > 1).drop("_rn")
        write_to_quarantine(dup_rows, name, "duplicate_check", id_col, f"{id_col} is duplicated")

        df = ranked_df.filter(col("_rn") == 1).drop("_rn")

    valid_dfs[name] = df
    print(f"{name}: valid rows after all row-level checks = {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Freshness check (table-level)

# COMMAND ----------

FRESHNESS_CUTOFF_HOURS = 72
cutoff = datetime.now() - timedelta(hours=FRESHNESS_CUTOFF_HOURS)

for name, cfg in TABLE_CONFIG.items():
    max_row = spark.read.table(cfg["table"]).agg(spark_max(col("processing_time")).alias("max_processing_time"))
    max_processing_time = max_row.collect()[0]["max_processing_time"]

    print(f"{name}: max_processing_time = {max_processing_time}")

    if max_processing_time is None or max_processing_time < cutoff:
        write_table_level_quarantine(
            table_name=name,
            check_name="freshness_check",
            failure_reason=f"stale_data: max processing_time ({max_processing_time}) older than {FRESHNESS_CUTOFF_HOURS} hours",
            detail_value=max_processing_time,
        )
        print(f"  -> STALE: written to quarantine")
    else:
        print(f"  -> FRESH")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE INTO Silver
# MAGIC Requires target tables to already exist with matching schema.
# MAGIC Run each table's CREATE TABLE (empty, matching valid_dfs[name] schema) once before first MERGE.

# COMMAND ----------

for name, keys in MERGE_KEYS.items():
    target_table = f"{CATALOG}.silver.{name}"
    source_df = valid_dfs[name]
    source_view = f"src_{name}"
    source_df.createOrReplaceTempView(source_view)

    match_cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} s
        ON {match_cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    
    print(f"{name}: MERGE complete into {target_table} keyed on {keys}")