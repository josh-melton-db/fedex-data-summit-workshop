# Databricks notebook source
# DBTITLE 1,Import Config
from util.configuration import config

# COMMAND ----------

# DBTITLE 1,Read Data
from pyspark.sql.functions import col

sensor_df = spark.read.csv("/Volumes/workshop/source_data/sensor_bronze", header=True)
inspection_df = spark.read.csv("/Volumes/workshop/source_data/inspection_bronze", header=True)

inspection_df = (
    inspection_df.na.fill({"defect": 0})
    .withColumn("defect", col("defect").cast("float").cast("int"))
)

joined_df = sensor_df.join(inspection_df, ["device_id", "timestamp"], "left")
display(joined_df)

# COMMAND ----------

# DBTITLE 1,Narrow Dataset
import pandas as pd

highest_count_device_id = (
    joined_df.where('defect=1')
    .groupBy('device_id').count() 
    .orderBy('count', ascending=False)  # Let's tackle the most problematic device in Pandas first, and
).first()[0]                            # later use Spark's distributed processing on the larger dataset
device_pandas_df = joined_df.where(f'device_id = {highest_count_device_id}').toPandas()

# COMMAND ----------

# DBTITLE 1,Create Features
features = ... # TODO: write the code to generate the required features

# COMMAND ----------

# DBTITLE 1,Address Nulls
features = features... # TODO: make sure you've filled in any null values if required for your model

# COMMAND ----------

# DBTITLE 1,Write Data
spark.createDataFrame(features).write.mode('overwrite').saveAsTable(config['feature_table'])

# COMMAND ----------


