# Databricks notebook source
# MAGIC %md
# MAGIC # SkyPoints Airline Loyalty Program - Distributed ETL Pipeline
# MAGIC ### Production PySpark on Azure Databricks + Azure Data Lake Storage Gen2 (ADLS Gen2)
# MAGIC **Scale**: Multi-Billion Records/Day | **Storage Format**: Delta Lake with Partition Pruning & Liquid Clustering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Environment & ADLS Gen2 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType
)

STORAGE_ACCOUNT = "stskypointsspeubmfodhieo"
STORAGE_KEY = "zLZlVBkQAwt+nXc8K0y1FX22dlLC8kHBqt4MSi9vsPDo6/eJImCKW66CbbMLuBqfnbyT453YnGpO+ASt6qdBrA=="

# Configure Spark Session for Direct ADLS Gen2 Access
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# ABFSS Path Builders
def get_abfss_path(container: str, path: str = '') -> str:
    if path:
        return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

LANDING_PATH = get_abfss_path("landing")
BRONZE_PATH = get_abfss_path("bronze")
SILVER_PATH = get_abfss_path("silver")
GOLD_PATH = get_abfss_path("gold")

print(f"ADLS Gen2 Landing URI: {LANDING_PATH}")
print(f"ADLS Gen2 Silver URI:  {SILVER_PATH}")
print(f"ADLS Gen2 Gold URI:    {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Deliverable 1 & 2: Ingest Flat File, Apply DQ Gates & Staging Enrichment
# MAGIC - Filter Header (|H|) lines
# MAGIC - Derive **Age** from DOB
# MAGIC - Derive **Stale_Member** flag (days_since_flight > 90)

# COMMAND ----------

# Read raw text lines from ADLS Gen2 landing container
raw_lines_df = spark.read.text(f"{LANDING_PATH}/sample_member_feed.dat")

# Filter out Header lines and empty lines
detail_lines_df = raw_lines_df.filter(
    (F.col("value").isNotNull()) & 
    (F.col("value") != "") & 
    (F.col("value").startswith("|D|") | F.col("value").startswith("D|"))
)

# Split pipe-delimited tokens
split_col = F.split(F.regexp_replace(F.col("value"), "^\\|", ""), "\\|")

parsed_df = detail_lines_df.select(
    F.trim(split_col.getItem(1)).alias("member_name"),
    F.trim(split_col.getItem(2)).alias("member_id"),
    F.to_date(F.trim(split_col.getItem(3)), "yyyyMMdd").alias("enrollment_date"),
    F.to_date(F.trim(split_col.getItem(4)), "yyyyMMdd").alias("last_flight_date"),
    F.nullif(F.trim(split_col.getItem(5)), "").alias("tier_code"),
    F.nullif(F.trim(split_col.getItem(6)), "").alias("agent_name"),
    F.nullif(F.trim(split_col.getItem(7)), "").alias("state"),
    F.upper(F.nullif(F.trim(split_col.getItem(8)), "")).alias("country"),
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

# Route invalid records to Quarantine Dead-Letter Delta Sink
quarantine_df = parsed_df.filter(~valid_condition).withColumn(
    "rejection_reason",
    F.concat_ws("; ",
        F.when(F.col("member_id").isNull(), "Missing member_id"),
        F.when(F.col("member_name").isNull(), "Missing member_name"),
        F.when(F.col("enrollment_date").isNull(), "Invalid enrollment_date"),
        F.when(F.col("country").isNull(), "Missing country")
    )
)
quarantine_df.write.format("delta").mode("append").save(f"{SILVER_PATH}/quarantine_members")

# Enriched Staging DataFrame (Deliverable 2)
stg_df = parsed_df.filter(valid_condition).select(
    "*",
    F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).cast("int").alias("age"),
    F.when(
        F.col("last_flight_date").isNull() | (F.datediff(F.current_date(), F.col("last_flight_date")) > 90),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("stale_member"),
    F.datediff(F.current_date(), F.col("last_flight_date")).alias("days_since_flight"),
    F.current_timestamp().alias("ingestion_timestamp")
)

stg_df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/stg_member_profiles")
display(stg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Deliverable 3: Windowed Deduplication ("Latest Record Wins") & Country Routing
# MAGIC When a member moves across countries (e.g. Elena moving USA -> Canada), window ranking ensures the latest record wins.

# COMMAND ----------

window_spec = Window.partitionBy("member_id").orderBy(
    F.col("last_flight_date").desc_nulls_last(),
    F.col("enrollment_date").desc()
)

deduped_df = stg_df.withColumn("rank", F.row_number().over(window_spec)) \
                   .filter(F.col("rank") == 1) \
                   .drop("rank")

# Dynamic Partition Overwrite by Country
deduped_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("country") \
    .save(f"{GOLD_PATH}/members_by_country")

for country_code in ["USA", "IND", "CAN", "PHIL", "AU"]:
    country_df = deduped_df.filter(F.col("country") == country_code)
    table_name = f"table_{country_code.lower()}"
    country_df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/country_tables/{table_name}")
    print(f"Materialized Gold Delta Table on ADLS Gen2: {table_name}")

display(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Deliverable 4: Semi-Structured JSON Redemption Feed Flattening & Member 360

# COMMAND ----------

raw_json_df = spark.read.option("multiline", "true").json(f"{LANDING_PATH}/sample_redemptions.json")

flattened_redemptions_df = raw_json_df.select(
    F.col("member_id"),
    F.to_date(F.col("feed_date"), "yyyyMMdd").alias("feed_date"),
    F.explode(F.col("redemptions")).alias("r")
).select(
    F.col("r.txn_id").alias("txn_id"),
    F.col("member_id"),
    F.col("feed_date"),
    F.to_date(F.col("r.txn_date"), "yyyyMMdd").alias("txn_date"),
    F.col("r.partner").alias("partner"),
    F.col("r.miles_redeemed").cast("long").alias("miles_redeemed"),
    F.upper(F.col("r.status")).alias("status"),
    F.current_timestamp().alias("ingestion_timestamp")
)

flattened_redemptions_df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/marts/fact_redemptions")

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

member_360_df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/marts/member_redemptions_360")
display(member_360_df)
