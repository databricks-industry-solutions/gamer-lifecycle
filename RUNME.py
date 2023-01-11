# Databricks notebook source
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉
# MAGIC 
# MAGIC **The Steps**
# MAGIC 1. Simply attach this notebook to a cluster and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC 
# MAGIC 2. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC 
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC 
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC 
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC 
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-specific. Keep in mind that rerunning this script again after modification resets them for other users too.
# MAGIC 
# MAGIC 2. If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 

# COMMAND ----------

# DBTITLE 1,Define DLT pipelines for DOTA and WOW
import re
from solacc.companion import NotebookSolutionCompanion

useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = useremail.split('@')[0]
username_sql = re.sub('\W', '_', username)
tmpdir = f"/dbfs/tmp/{username}/"
tmpdir_dbfs = f"/tmp/{username}"
database_name = f"gaming_{username_sql}"
database_location = f"{tmpdir}gaming"

dota_pipeline_json = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 2
                  }
              }
          ],
          "development": True,
          "continuous": False,
          "edition": "advanced",
          "libraries": [
              {
                  "notebook": {
                      "path": f"01a-ingest-dota-dlt"
                  }
              }
          ],
          "name": f"{username}_dota",
          "storage": f"{database_location}/dlt",
          "target": f"{database_name}",
          "allow_duplicate_names": "true"
      }

wow_pipeline_json = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 2
                  }
              }
          ],
          "development": True,
          "continuous": False,
          "edition": "advanced",
          "libraries": [
              {
                  "notebook": {
                      "path": f"01b-ingest-wow-dlt"
                  }
              }
          ],
          "name": f"{username}_wow",
          "storage": f"{database_location}/dlt",
          "target": f"{database_name}",
          "allow_duplicate_names": "true"
      }

# COMMAND ----------

# DBTITLE 1,Create a database to track the pipelines associated with the solution accelerator
spark.sql(f"CREATE DATABASE IF NOT EXISTS databricks_solacc LOCATION '/databricks_solacc/'")
spark.sql(f"CREATE TABLE IF NOT EXISTS databricks_solacc.dlt (path STRING, pipeline_id STRING, solacc STRING)")
dlt_config_table = "databricks_solacc.dlt"

# COMMAND ----------

# DBTITLE 1,Create DLT pipelines to ingest WOW and DOTA data
pipeline_id_wow = NotebookSolutionCompanion().deploy_pipeline(wow_pipeline_json, dlt_config_table, spark)
pipeline_id_dota = NotebookSolutionCompanion().deploy_pipeline(dota_pipeline_json, dlt_config_table, spark)

# COMMAND ----------

# DBTITLE 1,Define the overall orchestration workflow
workflow_json = {
        "timeout_seconds": 36000,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "CME"
        },
        "tasks": [
            {
                "task_key": "Download_WOW",
                "notebook_task": {
                    "notebook_path": f"/config/wow-download",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "gamer_lifecycle_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Download_DOTA",
                "notebook_task": {
                    "notebook_path": f"/config/dota_download",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "gamer_lifecycle_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Ingest_DOTA",
                "depends_on": [
                    {
                        "task_key": "Download_DOTA"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id_dota
                },
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Ingest_WOW",
                "depends_on": [
                    {
                        "task_key": "Download_WOW"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id_wow
                },
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "ML_WOW_Player_Churn",
                "depends_on": [
                    {
                        "task_key": "Ingest_WOW"
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"02b-ml-wow-player-churn",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "gamer_lifecycle_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "ML_DOTA_Toxicity",
                "depends_on": [
                    {
                        "task_key": "Ingest_DOTA"
                    }
                ],
                "libraries": [
                    {
                        "maven": {
                            "coordinates": "com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.0"
                        }
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"02a-ml-dota-toxicity",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "gamer_lifecycle_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "gamer_lifecycle_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                    "spark_conf": {
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                        "spark.kryoserializer.buffer.max": "2000M"
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"}, # different from standard API - this is multi-cloud friendly
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "num_workers": 4
                }
            }
        ],
        "format": "MULTI_TASK"
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(workflow_json, run_job=run_job)
