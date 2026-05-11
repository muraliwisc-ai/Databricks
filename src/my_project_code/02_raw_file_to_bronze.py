# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Raw File to Bronze
# MAGIC Reads the raw JSON file produced by notebook 01 and writes it to a Bronze Delta table,
# MAGIC preserving the original structure as much as possible. Adds audit columns.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
dbutils.widgets.text("env", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
env = dbutils.widgets.get("env")

table = f"{catalog}.{schema}.bronze_flight_raw"
print(f"Target: {table}")

# COMMAND ----------

# Get the file path from the upstream task; fall back to "latest file in folder"
try:
    raw_file_path = dbutils.jobs.taskValues.get(
        taskKey="API_to_File", key="raw_file_path", debugValue=""
    )
except Exception:
    raw_file_path = ""

if not raw_file_path:
    # Fallback: pick up the most recent file in the landing dir
    raw_dir = f"/Volumes/{catalog}/{schema}/{volume}/raw/flights"
    files = sorted(
        dbutils.fs.ls(raw_dir),
        key=lambda f: f.modificationTime,
        reverse=True,
    )
    if not files:
        raise RuntimeError(f"No raw files found in {raw_dir}")
    raw_file_path = files[0].path

print(f"Reading raw file: {raw_file_path}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_user, lit

# Read JSON as a single-row DataFrame — keeps the raw nested structure
df = (
    spark.read
    .option("multiline", "true")
    .json(raw_file_path)
)

# Add audit columns
df_bronze = (
    df
    .withColumn("source_file", lit(raw_file_path))
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("update_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
    .withColumn("updated_by", current_user())
)

print(f"Schema of bronze frame:")
df_bronze.printSchema()
print(f"Row count: {df_bronze.count()}")

# COMMAND ----------

# Write to bronze — append mode so each run adds a row
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(table)
)

print(f"Wrote bronze row to {table}")

# COMMAND ----------

# Pass along to next task
dbutils.jobs.taskValues.set(key="bronze_table", value=table)

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": table}))
