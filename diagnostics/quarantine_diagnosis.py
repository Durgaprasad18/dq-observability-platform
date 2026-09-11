# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import json
import time
import requests
from pyspark.sql import functions as F
from datetime import date

CATALOG = "project01_databricks01"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull quarantine rows, group by failure pattern, build prompt per group, call LLM

# COMMAND ----------

# =============================================================================
# AI ROOT CAUSE DIAGNOSIS — reads silver.quarantine, groups failures,
# asks Databricks-hosted GPT OSS 20B for likely root cause + suggested fix,
# writes results to silver.quarantine_diagnosis.
# =============================================================================

# --- fill these in ---
MODEL_SERVICE_NAME = "project01_databricks01.silver.dq_diagnosis_llm"  # UC-governed model service
MAX_OUTPUT_TOKENS = 400

dbutils.widgets.text("run_date", str(date.today()))
run_date = dbutils.widgets.get("run_date")

SAMPLES_PER_GROUP = 5  # how many failed_record examples sent to the LLM per group


quarantine = spark.table(f"{CATALOG}.silver.quarantine").filter(
    F.to_date("processed_date") == F.lit(run_date)
)

if quarantine.limit(1).count() == 0:
    print(f"No quarantine records for {run_date} — nothing to diagnose.")
    dbutils.notebook.exit("no_failures")

groups = (
    quarantine.groupBy("table_name", "check_name", "column_name")
    .agg(
        F.count("*").alias("failure_count"),
        F.collect_list("failure_reason").alias("all_reasons"),
        F.collect_list("failed_record").alias("all_records"),
    )
    .collect()
)

def build_prompt(table_name, check_name, column_name, failure_count, reasons, records):
    sample_reasons = list(dict.fromkeys(reasons))[:SAMPLES_PER_GROUP]  # dedup, cap
    sample_records = records[:SAMPLES_PER_GROUP]

    return f"""You are a data quality engineer reviewing pipeline failures in a Databricks lakehouse.

Table: {table_name}
Check: {check_name}
Column: {column_name}
Total failing records today: {failure_count}

Sample failure reasons:
{json.dumps(sample_reasons, indent=2)}

Sample failed records (JSON, up to {SAMPLES_PER_GROUP}):
{json.dumps(sample_records, indent=2)}

Respond ONLY with valid JSON, no markdown fences, no preamble:
{{
  "root_cause": "one or two sentences on the most likely underlying cause",
  "category": "one of: source_data_quality | ingestion_bug | schema_drift | upstream_system_change | business_rule_edge_case | unknown",
  "suggested_fix": "concrete, actionable next step",
  "confidence": "high | medium | low"
}}"""


def call_model(prompt, max_retries=3):
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    token = ctx.apiToken().get()
    workspace_url = ctx.apiUrl().get()
    url = f"{workspace_url}/ai-gateway/mlflow/v1/responses"
    body = {
        "model": MODEL_SERVICE_NAME,
        "max_output_tokens": 1000,  # raised — reasoning eats budget before JSON answer
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": "You are a precise, terse data quality diagnostics assistant. Always respond with valid JSON only."}]},
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    for attempt in range(max_retries):
        resp = requests.post(url, headers=headers, json=body, timeout=60)
        if resp.status_code == 429:
            wait = min(int(resp.headers.get("Retry-After", 15 * (attempt + 1))), 30)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "incomplete":
            reason = data.get("incomplete_details", {}).get("reason", "unknown")
            raise ValueError(f"Response incomplete, reason: {reason}")

        content = None
        for item in data.get("output", []):
            if item.get("type") == "message":
                content = item["content"][0]["text"].strip()
                break
        if not content:
            raise ValueError(f"No message content: {json.dumps(data)[:500]}")
        if content.startswith("```"):
            content = content.strip("`")
            content = content[content.find("{"):content.rfind("}") + 1]
        return json.loads(content)

    raise RuntimeError("Exceeded retries on 429")


diagnoses = []
for row in groups:
    prompt = build_prompt(
        row["table_name"], row["check_name"], row["column_name"],
        row["failure_count"], row["all_reasons"], row["all_records"],
    )
    try:
        result = call_model(prompt)
        diagnoses.append({
            "table_name": row["table_name"],
            "check_name": row["check_name"],
            "column_name": row["column_name"],
            "failure_count": row["failure_count"],
            "root_cause": result.get("root_cause", ""),
            "category": result.get("category", "unknown"),
            "suggested_fix": result.get("suggested_fix", ""),
            "confidence": result.get("confidence", "low"),
            "llm_call_status": "success",
        })
    except Exception as e:
        diagnoses.append({
            "table_name": row["table_name"],
            "check_name": row["check_name"],
            "column_name": row["column_name"],
            "failure_count": row["failure_count"],
            "root_cause": None,
            "category": "unknown",
            "suggested_fix": None,
            "confidence": None,
            "llm_call_status": f"error: {str(e)[:500]}",
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write results

# COMMAND ----------

schema = "table_name STRING, check_name STRING, column_name STRING, failure_count LONG, root_cause STRING, category STRING, suggested_fix STRING, confidence STRING, llm_call_status STRING"

diagnosis_df = (
    spark.createDataFrame(diagnoses, schema=schema)
    .withColumn("run_date", F.lit(run_date).cast("date"))
    .withColumn("diagnosed_at", F.current_timestamp())
)

full_table = f"{CATALOG}.silver.quarantine_diagnosis"
if not spark.catalog.tableExists(full_table):
    diagnosis_df.write.format("delta").saveAsTable(full_table)
else:
    diagnosis_df.write.format("delta").mode("append").saveAsTable(full_table)

print(f"Diagnosed {len(diagnoses)} failure patterns for {run_date}.")
display(diagnosis_df)