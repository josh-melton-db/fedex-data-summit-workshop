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
AS 
SELECT * 
FROM STREAM(LIVE.sensor_bronze)
WHERE 
    delay > 155 
    AND rotation_speed > 800
    AND temperature > 86
    AND density < 4.1
    AND air_pressure < 840

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using LIVE.table_name rather than cloud_files(), meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we're joining the two bronze tables together and adding some windowed calculations

-- COMMAND ----------

-- DBTITLE 1,Silver Inspection Table
CREATE OR REFRESH MATERIALIZED VIEW inspection_silver
AS 
WITH joined_data AS (
  SELECT * EXCEPT (inspection.device_id, inspection.timestamp, sensor.air_pressure),
    CASE 
      WHEN air_pressure < 0 THEN -air_pressure 
      ELSE air_pressure 
    END AS air_pressure
  FROM LIVE.sensor_bronze sensor
  JOIN LIVE.inspection_bronze inspection 
      ON sensor.device_id = inspection.device_id AND sensor.timestamp = inspection.timestamp
)
SELECT 
  window as timestamp_window, device_id, model_id, factory_id, trip_id, defect,
  AVG(temperature) as temperature, AVG(density) as density, AVG(delay) as delay, 
  AVG(rotation_speed) as rotation_speed, AVG(air_pressure) as air_pressure 
FROM joined_data
GROUP BY window("timestamp", "60 minutes"), device_id, model_id, factory_id, trip_id, defect

-- COMMAND ----------


