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
    comment='...' # TODO: add table description
)
def calculate_anomaly_rules():
    bronze = dlt.readStream('sensor_bronze')
    return ( 
        ... # TODO: filter down to only the rows that meet anomalous conditions: delay greater than 155, rotation speed greater than 800, temperature greater than 101, density greater than 4.6, and air pressure less than 840
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using dlt.read() rather than spark.readStream(), meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we can more easily create interesting operations like windowing functions. Create some features that will be valuable to leverage in our dashboard and Genie

# COMMAND ----------

# DBTITLE 1,Silver Inspection Table
@dlt.table(
    name='inspection_silver',
    comment='...' # TODO: add table description 
)
def create_timeseries_features():
    sensors = dlt.read('sensor_bronze')
    inspections = dlt.read('inspection_bronze').drop('_rescued_data')

    joined_data = ... # TODO: join the dataframes together on device_id and timestamp
    joined_data = ... # TODO: flip the sign of the air pressure column where it's negative

    return (
        joined_data.groupBy(
            ... # TODO: group by a time window and the dimensional columns device_id, model_id, factory_id, trip_id, and defect
        ).agg(
            ... # TODO: calculate some aggregations on the metric columns like temperature, density, delay, rotation speed, and air pressure
        ).select(
            ... # TODO: select the columns that will be used for reporting and for downstream gold tables
        )
    )

# COMMAND ----------


