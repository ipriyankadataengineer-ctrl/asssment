# Databricks notebook source
# MAGIC %md
# MAGIC # SkyPoints Airline Loyalty Program - Distributed ETL Pipeline
# MAGIC ### Production PySpark on Azure Databricks Serverless + ADLS Gen2 Lakehouse
# MAGIC **Scale**: Multi-Billion Records/Day | **Storage Format**: Delta Lake with Unity Catalog & Micro-Partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Environment & ADLS Gen2 Ingestion Setup
# MAGIC Works out-of-the-box on both **Databricks Serverless** and **Classic Clusters**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

STORAGE_ACCOUNT = "stskypointsspeubmfodhieo"
STORAGE_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=stskypointsspeubmfodhieo;AccountKey=zLZlVBkQAwt+nXc8K0y1FX22dlLC8kHBqt4MSi9vsPDo6/eJImCKW66CbbMLuBqfnbyT453YnGpO+ASt6qdBrA==;EndpointSuffix=core.windows.net"

# Use Azure Storage SDK for direct authentication on Serverless Compute
from azure.storage.blob import BlobServiceClient
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

# Read raw member profile flat file from ADLS Gen2 landing container
raw_flat_bytes = blob_service.get_blob_client("landing", "sample_member_feed.dat").download_blob().readall()
raw_lines = [line.strip() for line in raw_flat_bytes.decode("utf-8").splitlines() if line.strip()]

# Read raw JSON redemption feed from ADLS Gen2 landing container
raw_json_bytes = blob_service.get_blob_client("landing", "sample_redemptions.json").download_blob().readall()
raw_json_data = json.loads(raw_json_bytes.decode("utf-8"))

print(f"Loaded {len(raw_lines)} raw flat-file lines and {len(raw_json_data)} JSON member feeds from ADLS Gen2.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Deliverable 1 & 2: Ingestion, Quality Gates & Staging Derived Metrics
# MAGIC - Filters Header (|H|) lines
# MAGIC - Computes **Age** from Date of Birth
# MAGIC - Computes **Stale_Member** flag (days_since_flight > 90 or never flown)

# COMMAND ----------

# Create distributed Spark DataFrame from raw landing lines
raw_df = spark.createDataFrame([(line,) for line in raw_lines], ["value"])

# Filter for Detail (|D|) lines
detail_df = raw_df.filter(F.col("value").startswith("|D|") | F.col("value").startswith("D|"))

# Split pipe-delimited tokens
split_col = F.split(F.regexp_replace(F.col("value"), "^\\|", ""), "\\|")

parsed_df = detail_df.select(
    F.trim(split_col.getItem(1)).alias("member_name"),
    F.trim(split_col.getItem(2)).alias("member_id"),
    F.to_date(F.trim(split_col.getItem(3)), "yyyyMMdd").alias("enrollment_date"),
    F.to_date(F.trim(split_col.getItem(4)), "yyyyMMdd").alias("last_flight_date"),
    F.nullif(F.trim(split_col.getItem(5)), "").alias("tier_code"),
    F.nullif(F.trim(split_col.getItem(6)), "").alias("agent_name"),
    F.nullif(F.trim(split_col.getItem(7)), "").alias("state"),
    F.upper(F.nullif(F.trim(split_col.getItem(8)), "")).alias("country"),
    # Fix lost leading zeroes in DOB (e.g. 3051985 -> 03051985)
    F.lpad(F.trim(split_col.getItem(9)), 8, "0").alias("raw_dob"),
    F.coalesce(F.nullif(F.trim(split_col.getItem(10)), ""), F.lit("A")).alias("is_active")
).withColumn(
    "date_of_birth",
    F.coalesce(
        F.to_date(F.col("raw_dob"), "MMddyyyy"),
        F.to_date(F.col("raw_dob"), "ddMMyyyy")
    )
).drop("raw_dob")

# Deliverable 5: Data Quality Gates (Mandatory fields & Date validation)
valid_condition = (
    F.col("member_id").isNotNull() & (F.col("member_id") != "") &
    F.col("member_name").isNotNull() & (F.col("member_name") != "") &
    F.col("enrollment_date").isNotNull() &
    F.col("country").isNotNull()
)

# Quarantine dead-letter routing for invalid records
quarantine_df = parsed_df.filter(~valid_condition).withColumn(
    "rejection_reason",
    F.concat_ws("; ",
        F.when(F.col("member_id").isNull(), "Missing member_id"),
        F.when(F.col("member_name").isNull(), "Missing member_name"),
        F.when(F.col("enrollment_date").isNull(), "Invalid enrollment_date"),
        F.when(F.col("country").isNull(), "Missing country")
    )
)
quarantine_df.write.format("delta").mode("append").saveAsTable("quarantine_members")

# Enriched Staging DataFrame (Deliverable 2)
stg_df = parsed_df.filter(valid_condition).select(
    "*",
    # Deliverable 2 Metric 1: Age
    F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).cast("int").alias("age"),
    # Deliverable 2 Metric 2: Stale_Member Flag (> 90 days or never flown)
    F.when(
        F.col("last_flight_date").isNull() | (F.datediff(F.current_date(), F.col("last_flight_date")) > 90),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("stale_member"),
    F.datediff(F.current_date(), F.col("last_flight_date")).alias("days_since_flight"),
    F.current_timestamp().alias("ingestion_timestamp")
)

stg_df.write.format("delta").mode("overwrite").saveAsTable("stg_member_profiles")
display(stg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Deliverable 3: Windowed Deduplication ("Latest Record Wins") & Country Routing
# MAGIC Elena moved from USA to Canada: window ranking ensures she is only active in `table_can`.

# COMMAND ----------

window_spec = Window.partitionBy("member_id").orderBy(
    F.col("last_flight_date").desc_nulls_last(),
    F.col("enrollment_date").desc()
)

deduped_df = stg_df.withColumn("rank", F.row_number().over(window_spec)) \
                   .filter(F.col("rank") == 1) \
                   .drop("rank")

# Save master table partitioned by country
deduped_df.write.format("delta").mode("overwrite").partitionBy("country").saveAsTable("members_by_country")

# Materialize individual country target tables as requested in assessment
for country_code in ["USA", "IND", "CAN", "PHIL", "AU"]:
    country_df = deduped_df.filter(F.col("country") == country_code)
    table_name = f"table_{country_code.lower()}"
    country_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Materialized Gold Delta Table: {table_name}")

display(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Deliverable 4: Semi-Structured JSON Redemptions Flattening & Member 360

# COMMAND ----------

# Flatten nested JSON array into transaction records
flat_txns = []
for item in raw_json_data:
    m_id = item.get("member_id")
    f_date = item.get("feed_date")
    for r in item.get("redemptions", []):
        flat_txns.append({
            "txn_id": r.get("txn_id"),
            "member_id": m_id,
            "feed_date": f_date,
            "txn_date": r.get("txn_date"),
            "partner": r.get("partner"),
            "miles_redeemed": int(r.get("miles_redeemed", 0)),
            "status": r.get("status", "UNKNOWN")
        })

flattened_redemptions_df = spark.createDataFrame(flat_txns).select(
    F.col("txn_id"),
    F.col("member_id"),
    F.to_date(F.col("feed_date"), "yyyyMMdd").alias("feed_date"),
    F.to_date(F.col("txn_date"), "yyyyMMdd").alias("txn_date"),
    F.col("partner"),
    F.col("miles_redeemed"),
    F.upper(F.col("status")).alias("status"),
    F.current_timestamp().alias("ingestion_timestamp")
)

flattened_redemptions_df.write.format("delta").mode("overwrite").saveAsTable("fact_redemptions")

# Member 360 Analytical View
redemption_agg_df = flattened_redemptions_df.groupBy("member_id").agg(
    F.count("txn_id").alias("total_redemptions"),
    F.sum("miles_redeemed").alias("total_miles_redeemed"),
    F.sum(F.when(F.col("status") == "COMPLETED", F.col("miles_redeemed")).otherwise(0)).alias("completed_miles"),
    F.sum(F.when(F.col("status") == "PENDING", F.col("miles_redeemed")).otherwise(0)).alias("pending_miles"),
    F.max("txn_date").alias("last_redemption_date")
)

member_360_df = deduped_df.join(
    F.broadcast(redemption_agg_df),
    on="member_id",
    how="left"
).fillna(0, subset=["total_redemptions", "total_miles_redeemed", "completed_miles", "pending_miles"])

member_360_df.write.format("delta").mode("overwrite").saveAsTable("member_redemptions_360")
display(member_360_df)
