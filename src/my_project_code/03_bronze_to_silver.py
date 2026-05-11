# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze to Silver
# MAGIC Flattens the `states` array from bronze into one clean row per aircraft.
# MAGIC OpenSky returns each state as a positional array — we name the columns explicitly.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("env", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
env = dbutils.widgets.get("env")

bronze_table = f"{catalog}.{schema}.bronze_flight_raw"
silver_table = f"{catalog}.{schema}.silver_flight_positions"
print(f"Bronze: {bronze_table}")
print(f"Silver: {silver_table}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, explode, trim, current_timestamp, current_user, lit, to_timestamp,
    from_unixtime
)

# Read latest bronze rows. Each bronze row contains a `states` array.
# In append mode, the most recent row is the freshest API pull —
# we take only the latest row to keep silver as a "snapshot of now" table.
bronze_df = spark.table(bronze_table)
latest_row = (
    bronze_df
    .orderBy(col("insert_dttm").desc())
    .limit(1)
)

# Explode states array into one row per aircraft
exploded = (
    latest_row
    .select(
        col("api_response_time"),
        explode(col("states")).alias("state")
    )
)

# COMMAND ----------

# OpenSky `states` is a positional array of 17 elements.
# Index positions per: https://openskynetwork.github.io/opensky-api/rest.html
# 0: icao24, 1: callsign, 2: origin_country, 3: time_position, 4: last_contact,
# 5: longitude, 6: latitude, 7: baro_altitude, 8: on_ground, 9: velocity,
# 10: true_track, 11: vertical_rate, 12: sensors, 13: geo_altitude, 14: squawk,
# 15: spi, 16: position_source

silver_df = (
    exploded.select(
        trim(col("state")[0].cast("string")).alias("icao24"),
        trim(col("state")[1].cast("string")).alias("callsign"),
        trim(col("state")[2].cast("string")).alias("origin_country"),
        to_timestamp(from_unixtime(col("state")[3].cast("long"))).alias("time_position"),
        to_timestamp(from_unixtime(col("state")[4].cast("long"))).alias("last_contact"),
        col("state")[5].cast("double").alias("longitude"),
        col("state")[6].cast("double").alias("latitude"),
        col("state")[7].cast("double").alias("baro_altitude"),
        col("state")[8].cast("boolean").alias("on_ground"),
        col("state")[9].cast("double").alias("velocity"),
        col("state")[10].cast("double").alias("true_track"),
        col("state")[11].cast("double").alias("vertical_rate"),
        col("state")[13].cast("double").alias("geo_altitude"),
        trim(col("state")[14].cast("string")).alias("squawk"),
        col("state")[15].cast("boolean").alias("spi"),
        col("state")[16].cast("int").alias("position_source"),
        col("api_response_time"),
    )
    # Drop records with no aircraft identifier
    .filter(col("icao24").isNotNull() & (col("icao24") != ""))
    # Audit columns
    .withColumn("env", lit(env))
    .withColumn("insert_dttm", current_timestamp())
    .withColumn("update_dttm", current_timestamp())
    .withColumn("inserted_by", current_user())
    .withColumn("updated_by", current_user())
)

print(f"Silver row count: {silver_df.count()}")
silver_df.printSchema()

# COMMAND ----------

# Overwrite silver each run — silver is a "current snapshot" table
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Wrote silver table: {silver_table}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "table": silver_table}))
