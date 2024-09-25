# Databricks notebook source
# MAGIC %md
# MAGIC ### 2. Featurization
# MAGIC
# MAGIC In this notebook, we'll add the condition based threshold monitoring defined by our field maintenance engineers to flag engines that may require an inspection. Next, we'll pull our datasets together and calculate some interesting time series features such as an exponential moving average. 
# MAGIC
# MAGIC To do this, we'll get the configuration from our setup and begin creating our tables. Our rules for flagging defects are straightforward to add in DLT - the anomaly detected table will serve as a source for our automated alerts system, which can send emails, slack, teams, or generic webhook messages when certain conditions are met. 

# COMMAND ----------

# DBTITLE 1,Anomaly Warnings
from util.configuration import config
import dlt
from pyspark.sql.functions import col, when, avg, expr
from pyspark.sql import Window

@dlt.table(
    name=config['anomaly_name'],
    comment='...' # TODO: add table description
)
def calculate_anomaly_rules():
    bronze = dlt.readStream(config['sensor_name'])
    return ( 
        ...
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using dlt.read() rather than spark.readStream(), meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we can more easily create interesting operations like windowing functions. Create some features that will be valuable to leverage in our dashboard and Genie

# COMMAND ----------

# DBTITLE 1,Silver Inspection Table
from pyspark.sql.functions import window

@dlt.table(
    name=config['silver_name'],
    comment='...' # TODO: add table description 
)
def create_timeseries_features():
    inspections = dlt.read(config['inspection_name']).drop('_rescued_data')
    windowSpec = ... # TODO: define a window to calculate over
    sensors = (
        dlt.read(config['sensor_name'])
        .drop('_rescued_data') 
        .withColumn('air_pressure', ...) # TODO: Flip the sign when negative otherwise keep it the same
        ... # TODO: calculate additional features over the window
    )
    return (
        ... # TODO: join the dataframes together on device_id and timestamp
    )

# COMMAND ----------


