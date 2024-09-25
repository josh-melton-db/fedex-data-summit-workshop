-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 3. Aggregated Metrics
-- MAGIC In our final set of calculations, we'll create an aggregate (gold layer) table that calculates defect rates by device, factory, and model id. This will give us some insight into the effectiveness of the physics-based rules from our field maintenance engineering team and allow us to monitor trends over time. In our dashboard in the next notebook, this table will serve as the basis for monitoring defect rate by factory.

-- COMMAND ----------

-- DBTITLE 1,Gold Inspection Table
CREATE MATERIALIZED VIEW inspection_gold
AS 
SELECT ...
    -- TODO: find the counts, averages, and other statistical features of the dimensions you're grouping on
FROM LIVE.inspection_silver
... -- TODO: group by device_id, factory_id, model_id, and defect
