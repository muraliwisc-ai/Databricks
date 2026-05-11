# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — API to Raw File
# MAGIC Pulls live flight data from OpenSky and lands the raw JSON in a Volume.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
dbutils.widgets.text("env", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
env = dbutils.widgets.get("env")

print(f"env={env}  target={catalog}.{schema}  volume={volume}")

# COMMAND ----------

# Ensure schema and volume exist (idempotent — safe to re-run)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# COMMAND ----------

import os
import json
import time
import requests
from datetime import datetime, timezone

raw_dir = f"/Volumes/{catalog}/{schema}/{volume}/raw/flights"
# Use plain os.makedirs against the Volume path — no dbutils.fs needed
os.makedirs(raw_dir, exist_ok=True)
print(f"Raw landing dir ready: {raw_dir}")

# COMMAND ----------

API_URL = "https://opensky-network.org/api/states/all"
TIMEOUT_SECONDS = 30
MAX_RETRIES = 3


def fetch_opensky(url: str, timeout: int, retries: int) -> dict:
    """GET with retry + exponential backoff. Raises on final failure."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"Attempt {attempt}/{retries} -> GET {url}")
            resp = requests.get(
                url,
                timeout=timeout,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "muralis-flights-pipeline/1.0",
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict) or "states" not in payload:
                raise ValueError("Unexpected API response shape: missing 'states' key")
            print(f"OK — received {len(payload.get('states') or [])} flight states")
            return payload
        except (requests.exceptions.RequestException, ValueError) as exc:
            last_err = exc
            wait = 2 ** attempt
            print(f"Attempt {attempt} failed: {exc}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"OpenSky API failed after {retries} attempts: {last_err}")


api_response_ts = datetime.now(timezone.utc)
try:
    payload = fetch_opensky(API_URL, TIMEOUT_SECONDS, MAX_RETRIES)
except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)}))
    raise

# COMMAND ----------

# Enrich with ingest metadata so downstream layers can audit
payload["api_response_time"] = api_response_ts.isoformat()
payload["ingest_source"] = "opensky-network.org"

# COMMAND ----------

# Write the JSON file DIRECTLY to the Volume — no dbutils.fs.cp needed.
# Volumes are accessible as regular OS paths on serverless.
filename_ts = api_response_ts.strftime("%Y%m%dT%H%M%SZ")
filename = f"flights_{filename_ts}.json"
output_path = f"{raw_dir}/{filename}"

with open(output_path, "w") as f:
    json.dump(payload, f)

print(f"Wrote raw file: {output_path}")

# COMMAND ----------

# Pass the path to the next task via task values
dbutils.jobs.taskValues.set(key="raw_file_path", value=output_path)
dbutils.jobs.taskValues.set(key="api_response_time", value=api_response_ts.isoformat())

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "path": output_path,
    "states_count": len(payload.get("states") or [])
}))
