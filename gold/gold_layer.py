# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, datetime

CATALOG = "project01_databricks01"


# COMMAND ----------

GOLD_MERGE_KEYS = {
    "fact_orders": ["order_id", "order_item_id"],
    "dim_sellers": ["seller_id"],
    "dim_products": ["product_id"],
}

# On-time delivery threshold: delivered_customer_date <= estimated_delivery_date -> On Time
# Realistic buffer of 0 days used (Olist's estimate is already a promise date to the customer).
SLA_BUFFER_DAYS = 0

# -----------------------------------------------------------------------------
# Load Silver
# -----------------------------------------------------------------------------
orders       = spark.table(f"{CATALOG}.silver.orders")
order_items  = spark.table(f"{CATALOG}.silver.order_items")
payments     = spark.table(f"{CATALOG}.silver.payments")
customers    = spark.table(f"{CATALOG}.silver.customers")
sellers      = spark.table(f"{CATALOG}.silver.sellers")
products     = spark.table(f"{CATALOG}.silver.products")
reviews      = spark.table(f"{CATALOG}.silver.reviews")

# COMMAND ----------

def write_to_quarantine(df, table_name, check_name, column_name, reason_col="failure_reason"):
    """df must contain a `failure_reason` string column and a `record` json-ish struct/string."""
    if len(df.take(1)) == 0:
        return
    out = (
        df.select(
            F.lit(table_name).alias("table_name"),
            F.lit(check_name).alias("check_name"),
            F.lit(column_name).alias("column_name"),
            F.to_json(F.struct(*[c for c in df.columns if c != reason_col])).alias("failed_record"),
            F.col(reason_col).alias("failure_reason"),
            F.current_timestamp().alias("processed_date"),
        )
    )
    out.write.mode("append").saveAsTable(f"{CATALOG}.silver.quarantine")


def log_completeness(gold_table_name, silver_count, gold_count):
    diff = silver_count - gold_count
    row = spark.createDataFrame(
        [(gold_table_name, "completeness_check", "row_count",
          f'{{"silver_count": {silver_count}, "gold_count": {gold_count}, "diff": {diff}}}',
          "silver_to_gold row count mismatch" if diff != 0 else "counts match")],
        ["table_name", "check_name", "column_name", "failed_record", "failure_reason"],
    ).withColumn("processed_date", F.current_timestamp())
    row.write.mode("append").saveAsTable(f"{CATALOG}.silver.quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FACT_ORDERS

# COMMAND ----------

# --- payments aggregated to order level (payments don't map 1:1 to items) ---
payments_agg = (
    payments.groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("order_payment_total"),
        F.collect_set("payment_type").alias("payment_types"),
        F.max("payment_installments").alias("max_installments"),
    )
)

# --- review score aggregated to order level
reviews_agg = reviews.groupBy("order_id").agg(F.avg("review_score").alias("order_review_score"))

# --- referential check: order_items rows whose order_id/product_id/seller_id don't resolve ---
oi_checked = (
    order_items.alias("oi")
    .join(orders.select("order_id").alias("o"), "order_id", "left")
    .join(products.select("product_id").alias("p"), "product_id", "left")
    .join(sellers.select("seller_id").alias("s"), "seller_id", "left")
    .withColumn("missing_order", F.col("o.order_id").isNull())
    .withColumn("missing_product", F.col("p.product_id").isNull())
    .withColumn("missing_seller", F.col("s.seller_id").isNull())
)

unmatched = oi_checked.filter("missing_order OR missing_product OR missing_seller").withColumn(
    "failure_reason",
    F.concat_ws(
        "; ",
        F.when(F.col("missing_order"), F.lit("order_id not found in silver.orders")),
        F.when(F.col("missing_product"), F.lit("product_id not found in silver.products")),
        F.when(F.col("missing_seller"), F.lit("seller_id not found in silver.sellers")),
    ),
)
# write_to_quarantine(
#     unmatched.select("order_id", "order_item_id", "product_id", "seller_id", "failure_reason"),
#     "fact_orders", "referential_check", "order_id/product_id/seller_id",
# )

# only fully-resolved rows proceed into the fact table
oi_valid = oi_checked.filter("NOT (missing_order OR missing_product OR missing_seller)").select(
    "oi.order_id", "order_item_id", "product_id", "seller_id",
    "shipping_limit_date", "price", "freight_value",
)

# --- order-level total value (sum of price+freight across all items in the order) ---
order_totals = (
    oi_valid.withColumn("line_total", F.col("price") + F.col("freight_value"))
    .groupBy("order_id")
    .agg(F.sum("line_total").alias("order_total_value"))
)

# --- delivery SLA status ---
orders_sla = orders.withColumn(
    "delivery_sla_status",
    F.when(F.col("order_delivered_customer_date").isNull() & (F.col("order_status") == "canceled"), "Canceled")
    .when(F.col("order_delivered_customer_date").isNull(), "Pending")
    .when(
        F.col("order_delivered_customer_date")
        <= F.date_add(F.col("order_estimated_delivery_date"), SLA_BUFFER_DAYS),
        "On Time",
    )
    .otherwise("Late"),
)

# --- order-level total value (sum of price+freight across all items in the order) ---
order_totals = (
    oi_valid.withColumn("line_total", F.col("price") + F.col("freight_value"))
    .groupBy("order_id")
    .agg(F.sum("line_total").alias("order_total_value"))
)

# --- delivery SLA status ---
orders_sla = orders.withColumn(
    "delivery_sla_status",
    F.when(F.col("order_delivered_customer_date").isNull() & (F.col("order_status") == "canceled"), "Canceled")
    .when(F.col("order_delivered_customer_date").isNull(), "Pending")
    .when(
        F.col("order_delivered_customer_date")
        <= F.date_add(F.col("order_estimated_delivery_date"), SLA_BUFFER_DAYS),
        "On Time",
    )
    .otherwise("Late"),
)


# COMMAND ----------

fact_orders = (
    oi_valid
    .join(orders_sla, "order_id", "inner")
    .join(order_totals, "order_id", "left")
    .join(payments_agg, "order_id", "left")
    .join(reviews_agg, "order_id", "left")
    .withColumn("seller_payout_amount", F.col("price") + F.col("freight_value"))
    .select(
        "order_id", "order_item_id", "product_id", "seller_id", "customer_id",
        "order_status", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "delivery_sla_status",
        "price", "freight_value", "seller_payout_amount",
        "order_total_value", "order_payment_total", "payment_types", "max_installments",
        "order_review_score",
    )
    .withColumn("gold_processing_time", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM_SELLERS / DIM_PRODUCTS

# COMMAND ----------

dim_sellers = (
    sellers.alias("s")
    .join(
        fact_orders.groupBy("seller_id").agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("seller_payout_amount").alias("total_payout"),
            F.avg("order_review_score").alias("avg_review_score"),
        ),
        "seller_id", "left",
    )
    .select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
        F.coalesce("total_orders", F.lit(0)).alias("total_orders"),
        F.coalesce("total_payout", F.lit(0.0)).alias("total_payout"),
        "avg_review_score",
    )
    .withColumn("processing_time", F.current_timestamp())
)

dim_products = (
    products.alias("p")
    .join(
        fact_orders.groupBy("product_id").agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("price").alias("total_sales_value"),
            F.avg("order_review_score").alias("avg_review_score"),
        ),
        "product_id", "left",
    )
    .select(
        "product_id", "product_category_name",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
        F.coalesce("total_orders", F.lit(0)).alias("total_orders"),
        F.coalesce("total_sales_value", F.lit(0.0)).alias("total_sales_value"),
        "avg_review_score",
    )
    .withColumn("processing_time", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT

# COMMAND ----------

def merge_into_gold(df, table_name):
    full_table = f"{CATALOG}.gold.{table_name}"
    keys = GOLD_MERGE_KEYS[table_name]

    if not spark.catalog.tableExists(full_table):
        df.write.format("delta").option("mergeSchema", "true").saveAsTable(full_table)
        return

    from delta.tables import DeltaTable
    delta_tbl = DeltaTable.forName(spark, full_table)
    merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    (
        delta_tbl.alias("target")
        .merge(df.alias("source"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


merge_into_gold(fact_orders, "fact_orders")
merge_into_gold(dim_sellers, "dim_sellers")
merge_into_gold(dim_products, "dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completeness check: Silver row count vs Gold row count

# COMMAND ----------


log_completeness("fact_orders", order_items.count(), spark.table(f"{CATALOG}.gold.fact_orders").count())
log_completeness("dim_sellers", sellers.count(), spark.table(f"{CATALOG}.gold.dim_sellers").count())
log_completeness("dim_products", products.count(), spark.table(f"{CATALOG}.gold.dim_products").count())

# COMMAND ----------

print("Gold layer run complete:", datetime.now())
