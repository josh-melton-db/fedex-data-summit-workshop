# Databricks notebook source
from util.configuration import config
import mlflow
import pandas as pd

features = spark.read.table(config['feature_table'])
features_pdf = features.toPandas()
model_uri = f"models:/{config['catalog']}.{config['schema']}.{config['model_name']}@Production"

# COMMAND ----------

production_model = mlflow.pyfunc.load_model(model_uri)
features_pdf['predictions'] = production_model.predict(features_pdf)
display(features_pdf)

# COMMAND ----------

model_udf = mlflow.pyfunc.spark_udf(spark, model_uri)
features_with_predictions = features.withColumn("predictions", model_udf(*features.columns))
display(features_with_predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC Once you've completed this exercise, try to:
# MAGIC - Point a [Genie](https://learn.microsoft.com/en-us/azure/databricks/genie/) to this resulting table and see if it could answer common questions
# MAGIC - Complete the Data Engineering, BI, and Genie Python Exercises. See if you can make predictions in real time for the anomaly detection table in that pipeline instead of the hard coded filters!
# MAGIC - Improve the accuracy of your model
# MAGIC - Run this process on the whole dataset using the parallel ML techniques [demonstrated here](https://github.com/databricks-industry-solutions/iot_distributed_ml/blob/main/4%20Parallel%20ML.py)
# MAGIC - Create a [UC Function](https://docs.databricks.com/en/udf/unity-catalog.html) similar to the ones below. Try adding it to an LLM tool in the [AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html) to give the LLM real time insights into your data or run Python scripts

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
