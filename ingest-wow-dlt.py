# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Simplify ETL with Delta Live Table
# MAGIC 
# MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-1.png" width="700"/>
# MAGIC 
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC 
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC 
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC 
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC 
# MAGIC ## Our Delta Live Table pipeline
# MAGIC 
# MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
# MAGIC 
# MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_sql&dt=DLT">
# MAGIC <!-- [metadata={"description":"Full DLT demo, going into details. Use loan dataset",
# MAGIC  "authors":["dillon.bostwick@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "dlt"]}}] -->

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-2.png" width="500"/>
# MAGIC 
# MAGIC Our raw data is being sent to a blob storage. 
# MAGIC 
# MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
# MAGIC 
# MAGIC Autoloader is available in Python using the `cloud_files` format and can be used with a variety of format (json, csv, avro...):
# MAGIC 
# MAGIC 
# MAGIC #### STREAMING LIVE TABLE 
# MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. Without `STREAMING`, you will scan and ingest all the data available at once. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) for more details

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------

tmpdir = f"/dbfs/tmp/System-User"

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

@dlt.create_table(comment="Player game session data with start and end data")
def BZ_wow_player_sessions():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/sessions_landing/"))

# COMMAND ----------

@dlt.create_table(comment="character level events with timestamp")
def BZ_wow_character_level_events():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/level_events_landing/"))

# COMMAND ----------

@dlt.create_table(comment="Event when characters change zones")
def BZ_wow_character_zone_events():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/zone_events_landing/"))

# COMMAND ----------

@dlt.create_table(comment="Raw player data")
def BZ_wow_player_data():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/player_data_landing/"))

# COMMAND ----------

@dlt.create_table(comment="Character guild change events")
def BZ_wow_character_guild_events():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/guild_events_landing/"))

# COMMAND ----------

@dlt.create_table(comment="Zone list with custom map coordinates")
def BZ_wow_zone_coordinates():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/location_coords_landing/"))

# COMMAND ----------

@dlt.create_table(comment="List of zones")
def BZ_wow_zones():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/zones_landing/"))

# COMMAND ----------

@dlt.create_table(comment="Zone locations & game versions")
def BZ_wow_locations():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{tmpdir}/locations_landing/"))

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Silver layer: joining tables while ensuring data quality
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-3.png" width="500"/>
# MAGIC 
# MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
# MAGIC 
# MAGIC To consume only increment from the Bronze layer like `BZ_raw_txs`, we'll be using the `read_stream` function: `dlt.read_stream("BZ_raw_txs")`
# MAGIC 
# MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
# MAGIC 
# MAGIC #### Expectations
# MAGIC By defining expectations (`@dlt.expect`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

# COMMAND ----------

@dlt.view(comment="Used to compute subscription lengths")
def SLV_wow_subscription_session_changes():
  return dlt.read("BZ_wow_player_sessions") \
    .withColumn("minutes_since_last_session", (
      F.round((F.unix_timestamp(F.col("start_timestamp")) - F.unix_timestamp(F.lag(F.col("end_timestamp"), 1).over(Window.partitionBy("char").orderBy("start_timestamp"))))/60))) \
    .na.fill(value=0,subset=["minutes_since_last_session"]) \
    .withColumn("session_length", F.when(F.round((F.unix_timestamp(F.col('end_timestamp')) - F.unix_timestamp(F.col('start_timestamp')))/60) == 0, 9).otherwise(F.round((F.unix_timestamp(F.col('end_timestamp')) - F.unix_timestamp(F.col('start_timestamp')))/60))) \
    .withColumn("changed",  F.when(F.col("minutes_since_last_session") >= 30000, 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC Here we use the new Apply Changes api to support updates with the change data feeds. Use cases such as latest snapshot and SCD type 1 & 2 can be done using apply changes.

# COMMAND ----------

dlt.create_streaming_live_table("SLV_wow_latest_char_level")
dlt.apply_changes(
  target = "SLV_wow_latest_char_level",
  source = "BZ_wow_character_level_events",
  keys = ["char"],
  sequence_by = "Timestamp",
  column_list  = ["char", "level"],
  stored_as_scd_type = 1
)

# COMMAND ----------

dlt.create_streaming_live_table("SLV_wow_latest_char_zone")
dlt.apply_changes(
  target = "SLV_wow_latest_char_zone",
  source = "BZ_wow_character_zone_events",
  keys = ["char"],
  sequence_by = "Timestamp",
  column_list  = ["char", "zone"],
  stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.create_table(comment="session aggregation")
def SLV_wow_session_agg():
  cumulative_window = Window.partitionBy(
    'char'
  ).orderBy(
    'sessionid'
  # in this case we will use rangeBetween for the sum
  ).rangeBetween(
  # In this case we need to use Window.unboundedPreceding to catch all earlier rows
    Window.unboundedPreceding, 0
  )
  return (
    dlt.read("BZ_wow_player_sessions") \
      .withColumn("session_length", F.when(F.round((F.unix_timestamp(F.col('end_timestamp')) - F.unix_timestamp(F.col('start_timestamp')))/60) == 0, 9).otherwise(F.round((F.unix_timestamp(F.col('end_timestamp')) - F.unix_timestamp(F.col('start_timestamp')))/60))) \
      .withColumn("session_gap", (
        F.round((F.unix_timestamp(F.col("start_timestamp")) - F.unix_timestamp(F.lag(F.col("end_timestamp"), 1).over(Window.partitionBy("char").orderBy("start_timestamp"))))/60))) \
        .na.fill(value=0,subset=["session_gap"]) \
      .withColumn("total_playtime", F.sum(F.col("session_length")).over(cumulative_window)) \
      .withColumn("avg_session_length", F.avg(F.col("session_length")).over(cumulative_window))
  )

# COMMAND ----------

@dlt.create_table(comment="zone changes agg by session")
def SLV_wow_zone_change_agg():
  zonedf = dlt.read("BZ_wow_character_zone_events")
  sessions = dlt.read("BZ_wow_player_sessions")
  return ( 
    sessions.join(zonedf,
                  [sessions.char == zonedf.char, zonedf.Timestamp.between(sessions.start_timestamp,sessions.end_timestamp)],
                  'left') \
      .groupBy(sessions.char,sessions.sessionid) \
      .agg(F.count(zonedf.zone).alias('num_of_zone_changes'))
  )

# COMMAND ----------

@dlt.create_table(comment="level changes agg by session")
def SLV_wow_level_change_agg():
  zonedf = dlt.read("BZ_wow_character_zone_events")
  sessions = dlt.read("BZ_wow_player_sessions")
  leveldf = dlt.read("BZ_wow_character_level_events")
  return ( 
    sessions.join(leveldf, [sessions.char == leveldf.char, leveldf.Timestamp.between(sessions.start_timestamp,sessions.end_timestamp)], 'left') \
      .groupBy(sessions.char,sessions.sessionid) \
      .agg(
        (F.count("Level")+1).alias('num_of_level_changes'),
        F.max("Level").alias('end_level')) \
      .na.fill(value=1,subset=["end_level"])
  )

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Gold layer
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-4.png" width="500"/>
# MAGIC 
# MAGIC Our last step is to materialize the Gold Layer.
# MAGIC 
# MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll can add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

# COMMAND ----------

@dlt.table
def GLD_wow_player_status():
  return dlt.read("SLV_wow_latest_char_level") \
    .join(dlt.read("BZ_wow_player_data"), ["char"]) \
    .join(dlt.read("SLV_wow_latest_char_zone"), ["char"])

# COMMAND ----------

@dlt.table
def GLD_wow_player_session_agg():
  sessions = dlt.read("SLV_wow_session_agg")
  return sessions.join(dlt.read("SLV_wow_zone_change_agg"), ["char","sessionid"]) \
    .join(dlt.read("SLV_wow_level_change_agg"), ["char","sessionid"]) \
    .select(
      sessions.char,
      sessions.sessionid,
      "start_timestamp",
      "end_timestamp",
      "session_length",
      "session_gap",
      "total_playtime",
      "avg_session_length",
      "num_of_zone_changes",
      "num_of_level_changes",
      "end_level"
    )

# COMMAND ----------

@dlt.create_table(comment="player subscription aggregations")
def GLD_wow_player_subscriptions_agg():
  return dlt.read("SLV_wow_subscription_session_changes") \
    .withColumn("group_id", F.sum("changed").over(Window.partitionBy("char").orderBy("end_timestamp"))).drop("changed")\
    .groupBy('char','group_id').agg( \
      F.min('start_timestamp').alias('start_timestamp'), \
      F.max('end_timestamp').alias('end_timestamp'),
      F.count('sessionid').alias('num_of_sessions'), \
      F.round(F.avg("minutes_since_last_session")).alias("avg_session_gap"), \
      F.round(F.avg("session_length")).alias("avg_session_time"), \
      F.sum('session_length').alias('total_subscription_length')) \
    .drop('group_id') \
    .withColumn("subscriptionid", F.monotonically_increasing_id())
