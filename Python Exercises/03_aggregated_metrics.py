# Databricks notebook source
# MAGIC %md
# MAGIC ### 3. Aggregated Metrics
# MAGIC In our final set of calculations, we'll create an aggregate (gold layer) table that calculates defect rates by device, factory, and model id. This will give us some insight into the effectiveness of the physics-based rules from our field maintenance engineering team and allow us to monitor trends over time. In our dashboard in the next notebook, this table will serve as the basis for monitoring defect rate by factory.

# COMMAND ----------

# DBTITLE 1,Gold Inspection Table
from util.configuration import config
import dlt
from pyspark.sql.functions import lit, col, sum as ps_sum, when, avg, count, first

@dlt.table(
    name=config['gold_name'],
    comment='Aggregates defects by categorical variables'
)
def aggregate_gold_table():
    silver = dlt.read(config['silver_name'])
    return (
        silver
        ... # TODO: group by device_id, factory_id, model_id, and defect
        ... # TODO: find the counts, averages, and other statistical features of the dimensions you're grouping on 
    )
