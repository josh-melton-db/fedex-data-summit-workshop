-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 2. Featurization
-- MAGIC
-- MAGIC In this notebook, we'll add the condition based threshold monitoring defined by our field maintenance engineers to flag engines that may require an inspection. Next, we'll pull our datasets together and calculate some interesting time series features such as an exponential moving average. 
-- MAGIC
-- MAGIC To do this, we'll get the configuration from our setup and begin creating our tables. Our rules for flagging defects are straightforward to add in DLT - the anomaly detected table will serve as a source for our automated alerts system, which can send emails, slack, teams, or generic webhook messages when certain conditions are met. 

-- COMMAND ----------

-- DBTITLE 1,Anomaly Warnings
CREATE OR REFRESH STREAMING TABLE anomaly_detected
COMMENT '...' -- TODO: Add a table description
AS 
SELECT * 
FROM ...(LIVE.sensor_bronze) -- TODO: read from the sensor bronze table as a stream
WHERE ... -- TODO: filter down to only the rows that meet anomalous conditions: delay greater than 155, rotation speed greater than 800, temperature greater than 101, density greater than 4.6, and air pressure less than 840

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using LIVE.table_name rather than cloud_files(), meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we're joining the two bronze tables together and adding some windowed calculations

-- COMMAND ----------

-- DBTITLE 1,Silver Inspection Table
CREATE OR REFRESH MATERIALIZED VIEW inspection_silver
AS 
WITH joined_data AS (
  SELECT * EXCEPT (...), -- TODO: drop the inspection table's device_id and timestamp columns and the sensor table's air_pressure column
    -- TODO: when the air_pressure column is negative, flip the sign to positive. Otherwise, keep it the same
  FROM LIVE.sensor_bronze sensor
  JOIN LIVE.inspection_bronze inspection 
      -- TODO: join on device_id and timestamp
)
SELECT 
  -- TODO: SELECT the grouped columns and some aggregations on the measures (temperature, density, delay, 
  --       rotation_speed, and air_pressure)
FROM joined_data
GROUP BY ... -- TODO: Add a window function to calculate the average of the sensor data over some window

-- COMMAND ----------


