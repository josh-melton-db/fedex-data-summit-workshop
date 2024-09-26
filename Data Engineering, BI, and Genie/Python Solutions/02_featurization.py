# Databricks notebook source
# MAGIC %md
# MAGIC ### 2. Featurization
# MAGIC
# MAGIC In this notebook, we'll add the condition based threshold monitoring defined by our field maintenance engineers to flag engines that may require an inspection. Next, we'll pull our datasets together and calculate some interesting time series features such as an exponential moving average. 
# MAGIC
# MAGIC To do this, we'll get the configuration from our setup and begin creating our tables. Our rules for flagging defects are straightforward to add in DLT - the anomaly detected table will serve as a source for our automated alerts system, which can send emails, slack, teams, or generic webhook messages when certain conditions are met. 

# COMMAND ----------

# DBTITLE 1,Anomaly Warnings
import dlt
from pyspark.sql.functions import col, when, avg, expr, window

@dlt.table(
    name='anomaly_detected',
    comment='Uses physics-based modeling to predict anomaly warnings. Feeds Databricks SQL Alerts'
)
def calculate_anomaly_rules():
    bronze = dlt.readStream('sensor_bronze')
    return ( 
        bronze.where(
            ((col('delay') > 155) & (col('rotation_speed') > 800)) | 
            (col('temperature') > 101) |
            (col('density') > 4.6) & (col('air_pressure') < 840)  
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using dlt.read() rather than spark.readStream(), meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we can more easily create interesting operations like windowing functions. Create some features that will be valuable to leverage in our dashboard and Genie

# COMMAND ----------

# DBTITLE 1,Silver Inspection Table
@dlt.table(
    name='inspection_silver',
    comment='Joins bronze sensor data with defect reports' 
)
def create_timeseries_features():
    sensors = dlt.read('sensor_bronze')
    inspections = dlt.read('inspection_bronze').drop('_rescued_data')

    joined_data = sensors.join(inspections, ["device_id", "timestamp"], "left")

    joined_data = joined_data.select(
        '*',
        when(col("air_pressure") < 0, -col("air_pressure")).otherwise(col("air_pressure")).alias("corrected_air_pressure")
    ).drop("air_pressure")
    return (
        joined_data.groupBy(
            window("timestamp", "60 minutes").alias("timestamp_window"),
            "device_id", "model_id", "factory_id", "trip_id", "defect"
        ).agg(
            avg("temperature").alias("temperature"),
            avg("density").alias("density"),
            avg("delay").alias("delay"),
            avg("rotation_speed").alias("rotation_speed"),
            avg("corrected_air_pressure").alias("air_pressure")
        ).select(
            "timestamp_window", "device_id", "model_id", "factory_id", "trip_id", "defect",
            "temperature", "density", "delay", "rotation_speed", "air_pressure"
        )
    )

# COMMAND ----------


