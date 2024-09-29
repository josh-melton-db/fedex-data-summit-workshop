# Databricks notebook source
# DBTITLE 1,Import Config, Read Data
from util.configuration import config
import mlflow
import pandas as pd
mlflow.set_registry_uri('databricks-uc')

features = spark.read.table(config['feature_table'])
features_pdf = features.toPandas()
model_uri = f"models:/{config['catalog']}.{config['schema']}.{config['model_name']}@Production"

# COMMAND ----------

# DBTITLE 1,Load model, predict on Pandas
production_model = mlflow.pyfunc.load_model(model_uri)
features_pdf... = ... # TODO: add a column for predictions using the model from the line above
display(features_pdf)

# COMMAND ----------

# DBTITLE 1,Load model, predict in Spark
model_udf = mlflow.pyfunc.spark_udf(spark, model_uri)
features_with_predictions = ... # TODO: add a "predictions" column to make predictions using the distributed spark udf above
display(features_with_predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC Once you've completed this exercise, try to:
# MAGIC - Point a [Genie](https://learn.microsoft.com/en-us/azure/databricks/genie/) to this resulting table and see if it could answer common questions
# MAGIC - Complete the Data Engineering, BI, and Genie Python Exercises. See if you can make predictions in real time for the anomaly detection table in that pipeline instead of the hard coded filters!
# MAGIC - Improve the accuracy of your model
# MAGIC - Run this process on the whole dataset using the parallel ML techniques [demonstrated here](https://github.com/databricks-industry-solutions/iot_distributed_ml/blob/main/4%20Parallel%20ML.py)
# MAGIC - Create a [UC Function](https://docs.databricks.com/en/udf/unity-catalog.html) similar to the ones below. Try adding it to an LLM tool in the [AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html) to give the LLM real time insights into your data or the ability to run Python scripts

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC CREATE FUNCTION target_catalog.target_schema.run_query()
# MAGIC RETURNS TABLE(
# MAGIC   model_id STRING
# MAGIC )
# MAGIC SELECT model_id
# MAGIC FROM source_catalog.source_schema.source_table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC CREATE FUNCTION target_catalog.target_schema.greet(s STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC   return f"Hello, {s}"
# MAGIC $$
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: create a function like below and add it to a Genie Space as a trusted asset or to an AI Playground LLM as a tool
# MAGIC CREATE OR REPLACE FUNCTION ... (
# MAGIC   start_timestamp STRING COMMENT "A start date, formatted like '2023-05-26' or '2024-01-01'" DEFAULT "2023-01-01", 
# MAGIC   end_timestamp STRING COMMENT "An end date, formatted like '2023-05-26' or '2024-01-01'" DEFAULT "2023-12-31"
# MAGIC ) RETURNS TABLE (
# MAGIC   model_id STRING, 
# MAGIC   ...
# MAGIC ) COMMENT "..."  RETURN 
# MAGIC SELECT *
# MAGIC FROM ...
