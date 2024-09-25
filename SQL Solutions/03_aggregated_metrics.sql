-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 3. Aggregated Metrics
-- MAGIC In our final set of calculations, we'll create an aggregate (gold layer) table that calculates defect rates by device, factory, and model id. This will give us some insight into the effectiveness of the physics-based rules from our field maintenance engineering team and allow us to monitor trends over time. In our dashboard in the next notebook, this table will serve as the basis for monitoring defect rate by factory.

-- COMMAND ----------

-- DBTITLE 1,Gold Inspection Table
CREATE MATERIALIZED VIEW inspection_gold
AS 
SELECT
    device_id,
    factory_id,
    model_id,
    defect,
    COUNT(*) AS count,
    AVG(temperature) AS average_temperature,
    AVG(density) AS average_density,
    AVG(delay) AS average_delay,
    AVG(rotation_speed) AS average_rotation_speed,
    AVG(air_pressure) AS average_air_pressure
FROM
    LIVE.inspection_silver
GROUP BY
    device_id,
    factory_id,
    model_id,
    defect
