# Databricks notebook source
# MAGIC %run "./notebook_config"

# COMMAND ----------

# MAGIC %md
# MAGIC #World of Warcraft data download

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/wow_download
# MAGIC kaggle datasets download -d mylesoneill/warcraft-avatar-history --force -p /tmp/wow_download

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/wow_download
# MAGIC unzip warcraft-avatar-history.zip

# COMMAND ----------

tmpdir = f"/dbfs/tmp/System-User"

for file in ['location_coords','locations','zones','wowah_data']:
  dbutils.fs.mv(f"file:/tmp/wow_download/{file}.csv", f"{tmpdir}/{file}_landing/{file}.csv")
  print(file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform api data to match real data formats

# COMMAND ----------

dataset_start_time = '2005-12-31T23:59:46.000+0000'
dataset_end_time = '2009-01-10T05:08:59.000+0000'

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType() \
    .add("char",StringType(),True) \
    .add("level",IntegerType(),True) \
    .add("race",StringType(),True) \
    .add("charclass",StringType(),True) \
    .add("zone",StringType(),True) \
    .add("guild",StringType(),True) \
    .add("event_timestamp",StringType(),True)

apiDF = spark.read \
  .format("csv") \
  .option("header", "true") \
  .schema(schema) \
  .load(f"{tmpdir}/wowah_data_landing/wowah_data.csv") \
  .withColumn('timestamp', F.to_timestamp(F.col('event_timestamp'), "MM/dd/yy HH:mm:ss"))

# COMMAND ----------

display(apiDF) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix Sessions Data

# COMMAND ----------

sessiondf = apiDF.withColumn("changed",  F.when(
    (F.round((F.unix_timestamp(F.col("Timestamp")) - F.unix_timestamp(F.lag(F.col("Timestamp"), 1).over(Window.partitionBy("char").orderBy("Timestamp"))))/60).isNull()) | \
    (F.round((F.unix_timestamp(F.col("Timestamp")) - F.unix_timestamp(F.lag(F.col("Timestamp"), 1).over(Window.partitionBy("char").orderBy("Timestamp"))))/60) > 19), 1).otherwise(0))

sessiondf = sessiondf.withColumn("group_id", F.sum("changed").over(Window.partitionBy("char").orderBy("Timestamp"))).drop("changed")\
    .groupBy('char','group_id').agg( \
      F.min('Timestamp').alias('start_timestamp'), \
      F.max('Timestamp').alias('end_timestamp')) \
    .drop('group_id') \
    .withColumn("sessionid", F.monotonically_increasing_id())

sessiondf.coalesce(1).write.option("header","true").csv(f"{tmpdir}/sessions")
display(sessiondf)

# COMMAND ----------

data_location = f"{tmpdir}/sessions"

files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, f"{tmpdir}/sessions_landing/sessions.csv")
dbutils.fs.rm(data_location, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix level event data

# COMMAND ----------

playerleveldf = apiDF \
    .withColumn("changed", F.when(
      ((F.col("level") - F.lag(F.col("level"), 1).over(Window.partitionBy("char").orderBy("Timestamp"))) >= 1) | ((F.col("level") - F.lag(F.col("level"), 1).over(Window.partitionBy("char").orderBy("Timestamp"))).isNull()), 1).otherwise(0)) \
    .filter("changed = 1") \
    .select("char", "level", "zone", "Timestamp")

playerleveldf.coalesce(1).write.option("header","true").csv(f"{tmpdir}/level_events")
display(playerleveldf)

# COMMAND ----------

data_location = f"{tmpdir}/level_events"

files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, f"{tmpdir}/level_events_landing/level_events.csv")
dbutils.fs.rm(data_location, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix zone change events

# COMMAND ----------

zonedf = apiDF \
    .withColumn("changed", F.when(
      (F.col("zone") != F.lag(F.col("zone"), 1).over(Window.partitionBy("char").orderBy("Timestamp"))) | (F.col("Timestamp") == F.first(F.col("Timestamp")).over(Window.partitionBy("char").orderBy("Timestamp"))), 1).otherwise(0)) \
    .filter("changed = 1") \
    .select("char", "zone", "Timestamp")

zonedf.coalesce(1).write.option("header","true").csv(f"{tmpdir}/zone_events")
display(zonedf)

# COMMAND ----------

data_location = f"{tmpdir}/zone_events"

files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, f"{tmpdir}/zone_events_landing/zone_events.csv")
dbutils.fs.rm(data_location, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player Data

# COMMAND ----------

playerdf = apiDF.select("char", "race", "charclass").distinct()

playerdf.coalesce(1).write.option("header","true").csv(f"{tmpdir}/player_data")
display(playerdf)

# COMMAND ----------

data_location = f"{tmpdir}/player_data"

files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, f"{tmpdir}/player_data_landing/player_data.csv")
dbutils.fs.rm(data_location, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guild Change Events

# COMMAND ----------

guilddf = apiDF \
  .withColumn("guild", F.when(F.col("guild").isNull(), -1).otherwise(F.col("guild"))) \
  .withColumn("changed", F.when(
    (F.col("guild") != F.lag(F.col("guild"), 1).over(Window.partitionBy("char").orderBy("Timestamp"))) | (F.col("Timestamp") == F.first(F.col("Timestamp")).over(Window.partitionBy("char").orderBy("Timestamp"))), 1) \
    .otherwise(0)) \
  .filter("changed = 1") \
  .select("char", "level", "zone", "guild", "Timestamp")

guilddf.coalesce(1).write.option("header","true").csv(f"{tmpdir}/guild_events")
display(guilddf)

# COMMAND ----------

data_location = f"{tmpdir}/guild_events"

files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, f"{tmpdir}/guild_events_landing/guild_events.csv")
dbutils.fs.rm(data_location, recurse = True)
