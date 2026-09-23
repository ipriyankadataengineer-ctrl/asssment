# Databricks notebook source
# MAGIC %md
# MAGIC # SkyPoints Airline Loyalty Program - Distributed ETL Pipeline
# MAGIC ### Production PySpark on Azure Databricks + ADLS Gen2 Lakehouse
# MAGIC **Scale**: Multi-Billion Records/Day | **Architecture**: Medallion (Bronze / Silver / Gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Install dependencies & setup ADLS Gen2 connection

# COMMAND ----------

# BUG FIX 1: Install azure-storage-blob on the job cluster before importing
# MAGIC %pip install azure-storage-blob --quiet

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

STORAGE_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=stskypointsspeubmfodhieo;AccountKey=zLZlVBkQAwt+nXc8K0y1FX22dlLC8kHBqt4MSi9vsPDo6/eJImCKW66CbbMLuBqfnbyT453YnGpO+ASt6qdBrA==;EndpointSuffix=core.windows.net"

from azure.storage.blob import BlobServiceClient
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Dynamic Parameter Ingestion from ADF pipeline parameters

# COMMAND ----------

# Read ADF pipeline parameters passed via baseParameters; fall back to dynamic blob scan
try:
    dbutils.widgets.text("member_file", "sample_member_feed.txt")
    dbutils.widgets.text("redemption_file", "sample_redemptions.json")
    dbutils.widgets.text("container", "landing")
    target_member_blob = dbutils.widgets.get("member_file")
    target_redemption_blob = dbutils.widgets.get("redemption_file")
    target_container = dbutils.widgets.get("container")
except Exception:
    landing_blobs = [b.name for b in blob_service.get_container_client("landing").list_blobs()]
    member_cands = [b for b in landing_blobs if ("member" in b.lower() or "feed" in b.lower()) and b.lower().endswith((".csv", ".txt", ".dat"))]
    redemp_cands = [b for b in landing_blobs if ("redemption" in b.lower() or "txn" in b.lower()) and b.lower().endswith(".json")]
    target_member_blob = member_cands[0] if member_cands else "sample_member_feed.txt"
    target_redemption_blob = redemp_cands[0] if redemp_cands else "sample_redemptions.json"
    target_container = "landing"

print(f"Landing container : {target_container}")
print(f"Member feed       : {target_member_blob}")
print(f"Redemption feed   : {target_redemption_blob}")

raw_flat_bytes = blob_service.get_blob_client(target_container, target_member_blob).download_blob().readall()
raw_lines = [line.strip() for line in raw_flat_bytes.decode("utf-8").splitlines() if line.strip()]

raw_json_bytes = blob_service.get_blob_client(target_container, target_redemption_blob).download_blob().readall()
raw_json_data = json.loads(raw_json_bytes.decode("utf-8"))

print(f"Loaded {len(raw_lines)} flat-file lines and {len(raw_json_data)} JSON redemption feeds.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Deliverable 1 & 2: Parse flat file, Quality Gates, Staging Enrichment

# COMMAND ----------

raw_df = spark.createDataFrame([(line,) for line in raw_lines], ["value"])

# Filter Detail (|D|) lines only
detail_df = raw_df.filter(F.col("value").startswith("|D|") | F.col("value").startswith("D|"))

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
    F.lpad(F.trim(split_col.getItem(9)), 8, "0").alias("raw_dob"),
    F.coalesce(F.nullif(F.trim(split_col.getItem(10)), ""), F.lit("A")).alias("is_active")
).withColumn(
    "date_of_birth",
    F.coalesce(
        F.to_date(F.col("raw_dob"), "MMddyyyy"),
        F.to_date(F.col("raw_dob"), "ddMMyyyy")
    )
).drop("raw_dob")

# Deliverable 5: Mandatory field Quality Gates
valid_condition = (
    F.col("member_id").isNotNull() & (F.col("member_id") != "") &
    F.col("member_name").isNotNull() & (F.col("member_name") != "") &
    F.col("enrollment_date").isNotNull() &
    F.col("country").isNotNull()
)

# BUG FIX 2: Use managed table path with 'default' catalog to avoid Unity Catalog errors on job clusters
quarantine_df = parsed_df.filter(~valid_condition).withColumn(
    "rejection_reason",
    F.concat_ws("; ",
        F.when(F.col("member_id").isNull() | (F.col("member_id") == ""), "Missing member_id"),
        F.when(F.col("member_name").isNull() | (F.col("member_name") == ""), "Missing member_name"),
        F.when(F.col("enrollment_date").isNull(), "Invalid enrollment_date"),
        F.when(F.col("country").isNull(), "Missing country")
    )
)

# Only write quarantine table if there are invalid records
quarantine_count = quarantine_df.count()
if quarantine_count > 0:
    quarantine_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_members")
    print(f"Quarantined {quarantine_count} invalid records -> quarantine_members table")
else:
    print("No quarantine records - all members passed quality gates")

# Deliverable 2: Enriched Staging with derived metrics Age & Stale_Member
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

stg_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("stg_member_profiles")
display(stg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Deliverable 3: Window Deduplication "Latest Record Wins" & Country Routing

# COMMAND ----------

window_spec = Window.partitionBy("member_id").orderBy(
    F.col("last_flight_date").desc_nulls_last(),
    F.col("enrollment_date").desc()
)

deduped_df = stg_df.withColumn("rank", F.row_number().over(window_spec)) \
                   .filter(F.col("rank") == 1) \
                   .drop("rank")

deduped_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("country").saveAsTable("members_by_country")

for country_code in ["USA", "IND", "CAN", "PHIL", "AU"]:
    country_df = deduped_df.filter(F.col("country") == country_code)
    table_name = f"table_{country_code.lower()}"
    country_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"Materialized Gold table: {table_name} ({country_df.count()} members)")

display(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Deliverable 4: JSON Redemptions Flattening & Member 360 View

# COMMAND ----------

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

flattened_redemptions_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_redemptions")

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

member_360_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("member_redemptions_360")
display(member_360_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. ADLS Gen2 Medallion Tier Synchronization (Bronze / Silver / Gold)

# COMMAND ----------

# BUG FIX 3: Robust serializer handles Timestamp, Date, Decimal types without crashing
def sync_df_to_adls(df, container, blob_path):
    records = [row.asDict(recursive=True) for row in df.collect()]
    for r in records:
        for k, v in r.items():
            if hasattr(v, "isoformat"):      # datetime.date and datetime.datetime
                r[k] = v.isoformat()
            elif hasattr(v, "__float__"):     # Decimal
                r[k] = float(v)
    json_bytes = json.dumps(records, indent=2, default=str).encode("utf-8")
    blob_service.get_blob_client(container, blob_path).upload_blob(json_bytes, overwrite=True)
    print(f"  Synced {len(records):>4} records -> adls://{container}/{blob_path}")

print("=== Syncing Bronze ===")
blob_service.get_blob_client("bronze", f"members/{target_member_blob}").upload_blob(raw_flat_bytes, overwrite=True)
blob_service.get_blob_client("bronze", f"redemptions/{target_redemption_blob}").upload_blob(raw_json_bytes, overwrite=True)
print(f"  Archived raw feeds -> bronze/members & bronze/redemptions")

print("=== Syncing Silver ===")
sync_df_to_adls(stg_df, "silver", "staging/stg_member_profiles.json")
if quarantine_count > 0:
    sync_df_to_adls(quarantine_df, "silver", "quarantine/quarantine_members.json")

print("=== Syncing Gold ===")
for country_code in ["USA", "IND", "CAN", "PHIL", "AU"]:
    c_df = deduped_df.filter(F.col("country") == country_code)
    sync_df_to_adls(c_df, "gold", f"country_tables/table_{country_code.lower()}.json")

sync_df_to_adls(flattened_redemptions_df, "gold", "marts/fact_redemptions.json")
sync_df_to_adls(member_360_df, "gold", "marts/member_redemptions_360.json")

print("\nAll Medallion tiers synced to ADLS Gen2 successfully!")
