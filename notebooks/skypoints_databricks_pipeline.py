# Databricks notebook source
# MAGIC %md
# MAGIC # SkyPoints Airline Loyalty Program - Distributed ETL Pipeline
# MAGIC ### Production PySpark on Azure Databricks + ADLS Gen2 via Native ABFS Connector
# MAGIC **Architecture**: Medallion (Bronze / Silver / Gold) | **Auth**: Storage Account Key via ABFS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configure Native ABFS Connection to ADLS Gen2
# MAGIC No SDK downloads needed. Spark reads directly from ADLS Gen2 using the built-in ABFS driver.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

STORAGE_ACCOUNT = "stskypointsspeubmfodhieo"
STORAGE_KEY = "zLZlVBkQAwt+nXc8K0y1FX22dlLC8kHBqt4MSi9vsPDo6/eJImCKW66CbbMLuBqfnbyT453YnGpO+ASt6qdBrA=="

# Configure Spark to authenticate to ADLS Gen2 using storage account key
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)
spark.conf.set("spark.sql.ansi.enabled", "false")

def adls(container, path=""):
    """Helper: returns the ABFS path for a container/path."""
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

print(f"ABFS connector configured for: {STORAGE_ACCOUNT}.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Dynamic Parameter Ingestion from ADF Pipeline Parameters

# COMMAND ----------

# ADF passes these via baseParameters; defaults used when run manually
try:
    dbutils.widgets.text("member_file", "sample_member_feed.txt")
    dbutils.widgets.text("redemption_file", "sample_redemptions.json")
    dbutils.widgets.text("container", "landing")
    member_file = dbutils.widgets.get("member_file")
    redemption_file = dbutils.widgets.get("redemption_file")
    landing_container = dbutils.widgets.get("container")
except Exception:
    # Fallback: auto-detect any member flat file and redemption JSON in landing
    landing_files = [f.name for f in dbutils.fs.ls(adls("landing"))]
    member_cands = [f for f in landing_files if ("member" in f.lower() or "feed" in f.lower())
                    and f.lower().endswith((".csv", ".txt", ".dat"))]
    redemp_cands = [f for f in landing_files if "redemption" in f.lower() and f.lower().endswith(".json")]
    member_file = member_cands[0] if member_cands else "sample_member_feed.txt"
    redemption_file = redemp_cands[0] if redemp_cands else "sample_redemptions.json"
    landing_container = "landing"

print(f"Landing container : {landing_container}")
print(f"Member feed       : {member_file}")
print(f"Redemption feed   : {redemption_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Bronze Layer — Raw Ingestion via ABFS (No Download!)
# MAGIC Spark reads directly from ADLS Gen2 into distributed DataFrames.

# COMMAND ----------

# Read flat file directly from ADLS Gen2 via Spark (no blob download)
raw_df = spark.read.text(adls(landing_container, member_file))
print(f"Loaded {raw_df.count()} raw lines from {landing_container}/{member_file}")

# Archive to Bronze (copy via ABFS)
dbutils.fs.cp(adls(landing_container, member_file), adls("bronze", f"members/{member_file}"), recurse=False)
dbutils.fs.cp(adls(landing_container, redemption_file), adls("bronze", f"redemptions/{redemption_file}"), recurse=False)
print("Bronze: raw feeds archived via ABFS")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Deliverable 1 & 2: Parse, Quality Gates & Staging Enrichment

# COMMAND ----------

# Filter Detail (|D|) lines only
detail_df = raw_df.filter(F.col("value").startswith("|D|") | F.col("value").startswith("D|"))

split_col = F.split(F.regexp_replace(F.col("value"), "^\\|", ""), "\\|")

def clean_str(col_idx):
    c = F.trim(split_col.getItem(col_idx))
    return F.when((c == "") | (c.isNull()), None).otherwise(c)

parsed_df = detail_df.select(
    clean_str(1).alias("member_name"),
    clean_str(2).alias("member_id"),
    F.to_date(clean_str(3), "yyyyMMdd").alias("enrollment_date"),
    F.to_date(clean_str(4), "yyyyMMdd").alias("last_flight_date"),
    clean_str(5).alias("tier_code"),
    clean_str(6).alias("agent_name"),
    clean_str(7).alias("state"),
    F.upper(clean_str(8)).alias("country"),
    F.lpad(F.coalesce(clean_str(9), F.lit("")), 8, "0").alias("raw_dob"),
    F.coalesce(clean_str(10), F.lit("A")).alias("is_active")
).withColumn(
    "date_of_birth",
    F.coalesce(
        F.expr("try_to_date(raw_dob, 'MMddyyyy')"),
        F.expr("try_to_date(raw_dob, 'ddMMyyyy')")
    )
).drop("raw_dob")

# Deliverable 5: Mandatory field quality gates
valid_condition = (
    F.col("member_id").isNotNull() & (F.col("member_id") != "") &
    F.col("member_name").isNotNull() & (F.col("member_name") != "") &
    F.col("enrollment_date").isNotNull() &
    F.col("country").isNotNull()
)

# Silver Quarantine — dead-letter routing for invalid records
quarantine_df = parsed_df.filter(~valid_condition).withColumn(
    "rejection_reason",
    F.concat_ws("; ",
        F.when(F.col("member_id").isNull() | (F.col("member_id") == ""), "Missing member_id"),
        F.when(F.col("member_name").isNull() | (F.col("member_name") == ""), "Missing member_name"),
        F.when(F.col("enrollment_date").isNull(), "Invalid enrollment_date"),
        F.when(F.col("country").isNull(), "Missing country")
    )
)

quarantine_count = quarantine_df.count()
print(f"Quarantined records: {quarantine_count}")

if quarantine_count > 0:
    quarantine_df.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(adls("silver", "quarantine/quarantine_members"))

# Deliverable 2: Enriched Staging with Age & Stale_Member
stg_df = parsed_df.filter(valid_condition).withColumn(
    "age",
    F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).cast("int")
).withColumn(
    "stale_member",
    F.when(
        F.col("last_flight_date").isNull() | (F.datediff(F.current_date(), F.col("last_flight_date")) > 90),
        F.lit("Y")
    ).otherwise(F.lit("N"))
).withColumn(
    "days_since_flight",
    F.datediff(F.current_date(), F.col("last_flight_date"))
).withColumn("ingestion_timestamp", F.current_timestamp())

stg_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").save(adls("silver", "staging/stg_member_profiles"))
print(f"Silver: {stg_df.count()} valid members written to staging")
display(stg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Deliverable 3: "Latest Record Wins" Deduplication & Country Routing

# COMMAND ----------

window_spec = Window.partitionBy("member_id").orderBy(
    F.col("last_flight_date").desc_nulls_last(),
    F.col("enrollment_date").desc()
)

deduped_df = stg_df.withColumn("rank", F.row_number().over(window_spec)) \
                   .filter(F.col("rank") == 1).drop("rank")

# Gold: master partitioned table
deduped_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("country").format("delta").save(adls("gold", "members_by_country"))

# Gold: individual country target tables
for country_code in ["USA", "IND", "CAN", "PHIL", "AU"]:
    c_df = deduped_df.filter(F.col("country") == country_code)
    c_df.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(adls("gold", f"country_tables/table_{country_code.lower()}"))
    print(f"Gold: table_{country_code.lower()} -> {c_df.count()} members")

display(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Deliverable 4: JSON Redemptions Flattening & Member 360 View

# COMMAND ----------

# Read JSON directly from ADLS Gen2 via Spark ABFS
redemptions_df = spark.read.option("multiLine", True).json(adls(landing_container, redemption_file))

flattened_df = redemptions_df.select(
    F.col("member_id"),
    F.col("feed_date"),
    F.explode("redemptions").alias("txn")
).select(
    F.col("txn.txn_id").alias("txn_id"),
    F.col("member_id"),
    F.to_date(F.col("feed_date"), "yyyyMMdd").alias("feed_date"),
    F.to_date(F.col("txn.txn_date"), "yyyyMMdd").alias("txn_date"),
    F.col("txn.partner").alias("partner"),
    F.col("txn.miles_redeemed").cast("int").alias("miles_redeemed"),
    F.upper(F.col("txn.status")).alias("status"),
    F.current_timestamp().alias("ingestion_timestamp")
)

flattened_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").save(adls("gold", "marts/fact_redemptions"))

redemption_agg_df = flattened_df.groupBy("member_id").agg(
    F.count("txn_id").alias("total_redemptions"),
    F.sum("miles_redeemed").alias("total_miles_redeemed"),
    F.sum(F.when(F.col("status") == "COMPLETED", F.col("miles_redeemed")).otherwise(0)).alias("completed_miles"),
    F.sum(F.when(F.col("status") == "PENDING", F.col("miles_redeemed")).otherwise(0)).alias("pending_miles"),
    F.max("txn_date").alias("last_redemption_date")
)

member_360_df = deduped_df.join(F.broadcast(redemption_agg_df), on="member_id", how="left") \
    .fillna(0, subset=["total_redemptions", "total_miles_redeemed", "completed_miles", "pending_miles"])

member_360_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").save(adls("gold", "marts/member_redemptions_360"))

print(f"Gold: fact_redemptions -> {flattened_df.count()} transactions")
print(f"Gold: member_360 -> {member_360_df.count()} members with redemption summary")
display(member_360_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Pipeline Complete — All Medallion Tiers Written to ADLS Gen2

# COMMAND ----------

print("=== SkyPoints ETL Pipeline Completed Successfully ===")
print(f"Bronze : members/{member_file}, redemptions/{redemption_file}")
print(f"Silver : staging/stg_member_profiles (Delta), quarantine/quarantine_members (Delta)")
print(f"Gold   : country_tables/table_[can|ind|phil|au|usa] (Delta)")
print(f"Gold   : marts/fact_redemptions, marts/member_redemptions_360 (Delta)")
