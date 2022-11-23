# Databricks notebook source
# MAGIC %pip install kaggle spark-nlp==4.0.0 git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

import re
import os
import json
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

import numpy as np
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier,GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import sparknlp
from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *

from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MultilabelMetrics
from pyspark.sql.functions import lit,when,col,array,array_contains,array_remove,regexp_replace,size,when
from pyspark.sql.types import ArrayType,DoubleType,StringType

from pyspark.ml.evaluation import MultilabelClassificationEvaluator

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

from pyspark.sql.functions import col, struct
from pyspark.sql.types import *

# COMMAND ----------

useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = useremail.split('@')[0]
username_sql = re.sub('\W', '_', username)
tmpdir = f"/dbfs/tmp/{username}/"
tmpdir_dbfs = f"/tmp/{username}"
database_name = f"gaming_{username_sql}"
database_location = f"{tmpdir}gaming"

# COMMAND ----------

os.environ['KAGGLE_USERNAME'] = dbutils.secrets.get("solution-accelerator-cicd", "kaggle_username") # replace with your own credential here temporarily or set up a secret scope with your credential

os.environ['KAGGLE_KEY'] = dbutils.secrets.get("solution-accelerator-cicd", "kaggle_key") # replace with your own credential here temporarily or set up a secret scope with your credential

os.environ['tmpdir'] = tmpdir

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} location '{database_location}'")
spark.sql(f"USE {database_name}")

# COMMAND ----------

try:
  mlflow.set_experiment(f"/Users/{useremail}/gaming_experiment") # will try creating experiment if it doesn't exist; but when two notebooks with this code executes at the same time, could trigger a race-condition
except:
  pass

# COMMAND ----------

print(username)
print(tmpdir)
print(database_name)
