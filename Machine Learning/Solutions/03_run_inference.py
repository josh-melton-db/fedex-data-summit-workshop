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
# MAGIC - Complete the Data Engineering, BI, and Genie exercise. See if you can make predictions in real time in that pipeline instead of hard coded filters!
# MAGIC - Improve the accuracy of your model
# MAGIC - Complete this using parallel ML techniques as [demonstrated here](https://github.com/databricks-industry-solutions/iot_distributed_ml/blob/main/4%20Parallel%20ML.py)
