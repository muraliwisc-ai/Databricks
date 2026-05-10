# Databricks notebook source
# MAGIC %md
# MAGIC # Muralis Project — Example Notebook
# MAGIC Reads catalog/schema from job parameters and writes a sample row.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("env", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
env = dbutils.widgets.get("env")

print(f"Running in env: {env}")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.example_table (
        id BIGINT,
        event_name STRING,
        env STRING,
        event_timestamp TIMESTAMP
    )
""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df = spark.createDataFrame(
    [(1, "deployment_test")],
    ["id", "event_name"],
).withColumn("env", lit(env)).withColumn("event_timestamp", current_timestamp())

df.write.mode("append").saveAsTable(f"{catalog}.{schema}.example_table")

print(f"Wrote 1 row to {catalog}.{schema}.example_table")
