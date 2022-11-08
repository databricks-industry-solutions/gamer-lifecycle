# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------

# MAGIC %md #Model Development

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/playerltv/training.png"; width=40%>
# MAGIC </div>

# COMMAND ----------

mlflow.autolog()

player_session_df = spark.read.table(f"hive_metastore.{database_name}.GLD_wow_player_session_agg").withColumn("churned",  F.when((F.lag(F.col("session_gap"), -1).over(Window.partitionBy("char").orderBy("start_timestamp"))) >= 30000, 1).otherwise(0))
  
churn_weight = 1 - (player_session_df.filter("churned == 1").count() / player_session_df.count())
non_churn_weight = (player_session_df.filter("churned == 1").count() / player_session_df.count())

player_session_features_df = player_session_df \
  .withColumn("weight", F.when(F.col("churned") == 1, churn_weight).otherwise(non_churn_weight)) \

trainingData, testData = player_session_features_df.randomSplit([0.7, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Transformer Pipeline

# COMMAND ----------

# Transform all features into a vector using VectorAssembler
assemblerInputs = ["num_of_level_changes", "end_level", "num_of_zone_changes", "session_length", "session_gap", "total_playtime", "avg_session_length"]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# StringIndexer: Read input column "label" (digits) and annotate them as categorical values.
indexer = StringIndexer(inputCol="churned", outputCol="indexedLabel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gradient-boosted Tree Classifier

# COMMAND ----------

# DBTITLE 1,Training Function
def train_gbt(maxDepth, maxBins, maxIter):
  '''
  This train() function:
   - takes hyperparameters as inputs (for tuning later)
   - returns the F1 score on the validation dataset
 
  Wrapping code as a function makes it easier to reuse the code later with Hyperopt.
  '''
  # Use MLflow to track training.
  # Specify "nested=True" since this single model will be logged as a child run of Hyperopt's run.
  with mlflow.start_run(nested=True):
    mlflow.log_param("Model Type", "Gradient-boosted")
    
    # Train a GBT model.
    gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="features", predictionCol="prediction", weightCol="weight", maxIter=maxIter, maxDepth=maxDepth, maxBins=maxBins)
    
    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="f1")
    
    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[indexer, assembler, gbt])
    
    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)
    
    # Make predictions.
    predictions = model.transform(testData)
    
    validation_metric = evaluator.evaluate(predictions)
    
    print(validation_metric)
  
  return model, validation_metric

# COMMAND ----------

# DBTITLE 1,Hyperopt Training Wrapper Function
def train_with_hyperopt_train_gbt(params):
  """
  An example train method that calls into MLlib.
  This method is passed to hyperopt.fmin().
  
  :param params: hyperparameters as a dict. Its structure is consistent with how search space is defined. See below.
  :return: dict with fields 'loss' (scalar loss) and 'status' (success/failure status of run)
  """
  # For integer parameters, make sure to convert them to int type if Hyperopt is searching over a continuous range of values.
  maxDepth = int(params['maxDepth'])
  maxBins = int(params['maxBins'])
  maxIter = int(params['maxIter'])
 
  model, f1_score = train_gbt(maxDepth, maxBins, maxIter)
  
  # Hyperopt expects you to return a loss (for which lower is better), so take the negative of the f1_score (for which higher is better).
  loss = - f1_score
  return {'loss': loss, 'status': STATUS_OK}

# COMMAND ----------

# DBTITLE 1,Hyperopt
space = {
  'maxDepth': hp.uniform('maxDepth', 2, 15),
  'maxBins': hp.uniform('maxBins', 10, 50),
  'maxIter': hp.uniform('maxIter', 5, 20)
}

algo=tpe.suggest
 
with mlflow.start_run():
  best_params = fmin(
    fn=train_with_hyperopt_train_gbt,
    space=space,
    algo=algo,
    max_evals=10
  )

mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets look at the experiments and choose the model we want to push to the model registry
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/toxicity/mlflow-experiments.png"; width=60%>
# MAGIC </div>

# COMMAND ----------

# Print out the parameters that produced the best model
best_params

gradient_initial_model, initial_gradient_val_f1_metric = train_gbt(1,2,4)
gradient_final_model, final_gradient_val_f1_score = train_gbt(int(best_params['maxDepth']), int(best_params['maxBins']), int(best_params['maxIter']))

print(f"On the test data, the initial (untuned) model achieved F1 score {initial_gradient_val_f1_metric}, and the final (tuned) model achieved {final_gradient_val_f1_score}.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inference 

# COMMAND ----------

model_name = "gaming_accelerator"
stage = 'production'

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}/{stage}")

player_sessions_df = spark.read.table(f"hive_metastore.{database_name}.GLD_wow_player_session_agg")
player_churn_df = player_sessions_df.withColumn("prediction", loaded_model(F.struct(*player_sessions_df.columns))).filter(F.col("prediction") == 1)

player_churn_df.select("char","sessionid","prediction").write.mode("overwrite").saveAsTable(f"hive_metastore.{database_name}.SLV_wow_player_churn_risk")
display(player_churn_df)
