-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 2. Featurization
-- MAGIC
-- MAGIC In this notebook, we'll add the condition based threshold monitoring defined by our field maintenance engineers to flag engines that may require an inspection. Next, we'll pull our datasets together and calculate some interesting time series features such as an exponential moving average. This poses a couple of challenges: 
-- MAGIC - How do we handle null, missing, or irregular data in our time series?
-- MAGIC - How do we calculate time series features such as exponential moving average in parallel on a very large dataset, without growing cost exponentially with data volume?
-- MAGIC - How do we pull together our datasets when the timestamps don't line up? In this case, our inspection defect warning might get flagged hours after the sensor data is generated. We need a join that allows "price is right" rules - attach the most recent sensor data to our inspection warning data, without exceeding the inspection timestamp. This way we can identify the leading, rather than lagging, indicators for more proactive maintenance events.
-- MAGIC </br>
-- MAGIC
-- MAGIC All of these things might require a complex, custom library specific to time series data. Luckily, Databricks has done the hard part for you! We'll use the open source library [Tempo](https://databrickslabs.github.io/tempo/) from Databricks Labs to make these challenging operations simple. First things first, we installed dbl-tempo. Next we'll get the configuration from our setup and begin creating our tables. First of all, our rules for flagging defects are straightforward to add in DLT - the anomaly detected table will serve as a source for our automated alerts system, which can send emails, slack, teams, or generic webhook messages when certain conditions are met. 

-- COMMAND ----------

-- DBTITLE 1,Anomaly Warnings
CREATE OR REFRESH STREAMING TABLE anomaly_detected
AS 
SELECT * 
FROM STREAM(LIVE.sensor_bronze)
WHERE 
    delay > 155 
    AND rotation_speed > 800
    AND temperature > 101
    AND density > 4.6
    AND air_pressure < 840

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using LIVE.table_name rather than cloud_files(), meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we're joining the two bronze tables together and adding some windowed calculations

-- COMMAND ----------

-- DBTITLE 1,Silver Inspection Table
CREATE OR REFRESH MATERIALIZED VIEW inspection_silver
AS 
WITH base_data AS (
  SELECT
    * EXCEPT(air_pressure, _rescued_data),
    CASE 
      WHEN air_pressure < 0 THEN -air_pressure 
      ELSE air_pressure 
    END AS air_pressure
  FROM LIVE.sensor_bronze
),
windowed_sensor_data AS (
  SELECT *,
    AVG(temperature) OVER (
      PARTITION BY model_id, factory_id, device_id, trip_id
      ORDER BY CAST(timestamp AS LONG)
      RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
    ) AS sensor_temperature,
    AVG(density) OVER (
      PARTITION BY model_id, factory_id, device_id, trip_id
      ORDER BY CAST(timestamp AS LONG)
      RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
    ) AS sensor_density,
    AVG(delay) OVER (
      PARTITION BY model_id, factory_id, device_id, trip_id
      ORDER BY CAST(timestamp AS LONG)
      RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
    ) AS sensor_delay,
    AVG(rotation_speed) OVER (
      PARTITION BY model_id, factory_id, device_id, trip_id
      ORDER BY CAST(timestamp AS LONG)
      RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
    ) AS sensor_rotation_speed,
    AVG(air_pressure) OVER (
      PARTITION BY model_id, factory_id, device_id, trip_id
      ORDER BY CAST(timestamp AS LONG)
      RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
    ) AS sensor_air_pressure
  FROM base_data
)

SELECT * EXCEPT (inspection.device_id, inspection.timestamp)
FROM windowed_sensor_data sensor
JOIN LIVE.inspection_bronze inspection 
    ON sensor.device_id = inspection.device_id AND sensor.timestamp = inspection.timestamp

-- COMMAND ----------


