# Databricks notebook source
from util.configuration import config

# COMMAND ----------

from pyspark.sql.functions import col

sensor_df = spark.read.csv("/Volumes/workshop/source_data/sensor_bronze", header=True)
inspection_df = spark.read.csv("/Volumes/workshop/source_data/inspection_bronze", header=True)

inspection_df = (
    inspection_df.na.fill({"defect": 0})
    .withColumn("defect", col("defect").cast("float").cast("int"))
)

joined_df = sensor_df.join(inspection_df, ["device_id", "timestamp"])
display(joined_df)

# COMMAND ----------

import pandas as pd

highest_count_device_id = (
    joined_df.where('defect=1')
    .groupBy('device_id').count() 
    .orderBy('count', ascending=False)  # Let's tackle the most problematic device in Pandas first, and
).first()[0]                            # later use Spark's distributed processing on the larger dataset
device_pandas_df = joined_df.where(f'device_id = {highest_count_device_id}').toPandas()

# COMMAND ----------

encoded_factory = pd.get_dummies(device_pandas_df['factory_id'], prefix='ohe')
encoded_model = pd.get_dummies(device_pandas_df['model_id'], prefix='ohe')
features = pd.concat([device_pandas_df.drop('factory_id', axis=1).drop('model_id', axis=1), encoded_factory, encoded_model], axis=1)
features = features.drop('timestamp', axis=1)

# COMMAND ----------

features = features.fillna(method='ffill')
features = features.fillna(0)

# COMMAND ----------

spark.createDataFrame(features).write.mode('overwrite').saveAsTable(config['feature_table'])

# COMMAND ----------


