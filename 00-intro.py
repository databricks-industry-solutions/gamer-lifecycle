# Databricks notebook source
# MAGIC %md
# MAGIC <a href="https://www.databricks.com/solutions/accelerators"><img src='https://github.com/databricks-industry-solutions/.github/raw/main/profile/solacc_logo_wide.png'></img></a>
# MAGIC
# MAGIC
# MAGIC The series of assets inside this repo show a few of the capabilities possible when leveraging databricks as the analytics platform of choice in your studio. Like any good game there are choices and the assets here let you explore a variety of real world solutions.
# MAGIC
# MAGIC
# MAGIC Included are currently three solutions that each leverage Machine Learning, Data Engineering and BI. You can expect to see best practices on leveraging databricks products such as Delta, Delta Live Tables, MLFlow, and DB SQL to solve these challenges.
# MAGIC 1. Tackling Toxicity with In-Game text
# MAGIC 2. Understanding Player Churn Risk & Lifetime Value
# MAGIC 3. Leveraging In-Game Match Telemetry to make data driven design & balancing choices

# COMMAND ----------

# DBTITLE 0,UntitledD
# MAGIC %md First, download the source data for this accelerator.

# COMMAND ----------

# MAGIC %run ./config/dota_download

# COMMAND ----------

# MAGIC %run ./config/wow_download
