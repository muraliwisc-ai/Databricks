# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver to Gold
# MAGIC Builds reporting/analytics tables on top of silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("env", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
env = dbutils.widgets.get("env")

silver_table = f"{catalog}.{schema}.silver_flight_positions"
gold_summary_table = f"{catalog}.{schema}.gold_flight_summary"
gold_country_table = f"{catalog}.{schema}.gold_flights_by_country"
gold_ground_air_table = f"{catalog}.{schema}.gold_ground_vs_air"
gold_avg_alt_table = f"{catalog}.{schema}.gold_avg_altitude_by_country"
gold_top10_table = f"{catalog}.{schema}.gold_top10_countries"

print(f"Reading from {silver_table}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, current_timestamp, current_user, lit, desc
)

silver = spark.table(silver_table)
print(f"Silver row count: {silver.count()}")

# COMMAND ----------

# 1. Count of flights by country
flights_by_country = (
    silver.groupBy("origin_country")
    .agg(count("*").alias("flight_count"))
    .orderBy(desc("flight_count"))
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
)

(flights_by_country.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(gold_country_table))
print(f"Wrote {gold_country_table}")

# COMMAND ----------

# 2. Flights currently on ground vs in air
ground_vs_air = (
    silver.groupBy("on_ground")
    .agg(count("*").alias("aircraft_count"))
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
)

(ground_vs_air.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(gold_ground_air_table))
print(f"Wrote {gold_ground_air_table}")

# COMMAND ----------

# 3. Average altitude by country (only in-air aircraft with valid altitude)
avg_altitude = (
    silver
    .filter(col("on_ground") == False)
    .filter(col("baro_altitude").isNotNull())
    .groupBy("origin_country")
    .agg(avg("baro_altitude").alias("avg_baro_altitude_m"))
    .orderBy(desc("avg_baro_altitude_m"))
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
)

(avg_altitude.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(gold_avg_alt_table))
print(f"Wrote {gold_avg_alt_table}")

# COMMAND ----------

# 4. Top 10 countries by active (in-air) aircraft
top10 = (
    silver
    .filter(col("on_ground") == False)
    .groupBy("origin_country")
    .agg(count("*").alias("active_aircraft_count"))
    .orderBy(desc("active_aircraft_count"))
    .limit(10)
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
)

(top10.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(gold_top10_table))
print(f"Wrote {gold_top10_table}")

# COMMAND ----------

# Build the combined summary table the task asks for
summary = (
    silver.agg(
        count("*").alias("total_aircraft"),
        spark_sum(when(col("on_ground") == True, 1).otherwise(0)).alias("on_ground_count"),
        spark_sum(when(col("on_ground") == False, 1).otherwise(0)).alias("in_air_count"),
        avg(when(col("on_ground") == False, col("baro_altitude"))).alias("avg_in_air_altitude_m"),
    )
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
)

(summary.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(gold_summary_table))
print(f"Wrote {gold_summary_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "tables": [
        gold_summary_table,
        gold_country_table,
        gold_ground_air_table,
        gold_avg_alt_table,
        gold_top10_table,
    ]
}))
