# Databricks notebook source
# MAGIC %md
# MAGIC ## 4. Actionable Insights
# MAGIC This notebook will create a dashboard, an alert, and a Genie Space

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard and Alerting

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our ETL pipeline built out, let's surface some results! 
# MAGIC
# MAGIC Go to the alerts tab and create a new alert based on the count of anomalous devices in our silver streaming table. This alert can notify the end users you specify when our anomaly warnings are triggered (note that `you'll need to refresh the alert` first by using a schedule or the button in the top right of the UI). If you run your pipeline continuously, you can set the alert to refresh as frequently as you'd like for real time alerting.

# COMMAND ----------

# MAGIC %md
# MAGIC Go to the "Dashboards" tab and create a dashboard that makes it easy to see our defect rate by factory or relate defect rates to variables like Density or Temperature.

# COMMAND ----------

# MAGIC %md
# MAGIC We're landing new data as the workshop goes on. If you run your pipeline again, you'll incrementally ingest the new data. You might see in the DLT event log that some Materialized Views while run as COMPLETE_RECOMPUTE while others show something like GROUP_AGGREGATE or PARTITION_OVERWRITE - what's happening here is DLT automatically determines the most efficient way to get your results, and if there's a shortcut available DLT will take it in order to reduce processing time. For example, consider the scenario where only some devices are updated. Wouldn't it be faster if we avoided re-computing the aggregations in our gold table for devices that didn't receive an update? With Materialized Views, we don't have to worry about answering that question. Out of the box we get simple, great performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Spaces
# MAGIC Now that we’ve generated visuals for the common requirements we can instantly create a text-to-insights tool to allow the consumers of our dashboard to ask ad-hoc questions of the data using natural language. Go to the three dots in the top right of your dashboard and select "Create Genie Space". Your Genie Space will be created with each of the queries and datasets that our dashboard referenced already embedded in the context of the Space. By adding more specific instructions and descriptions of our queries, we’ll enable the system to answer common questions more accurately. You can do this in the "Instructions" menu on the left after creating the Space

# COMMAND ----------

# MAGIC %md
# MAGIC Our Genie will likely have trouble with queries that require context about our business logic. For example, if our product lines are simply the model_ids with the numbers removed, or that our maintenance expenses are calculated by multiplying the distinct count of device_ids where defect=1. Here we can add example queries and explain to Genie how to answer questions with complex answers like "What were our maintenance expenses by product line"?

# COMMAND ----------

# MAGIC %md
# MAGIC It might also be the case that a common relationship in your data model is difficult for Genie to piece together. In addition to the queries from our dashboard, we might include an extra example that exemplifies a complex metric or series of joins. For the particularly common or critical examples, you can create trusted assets. We can use trusted assets to define templated tools that Genie can use to answer questions. In this example, we let Genie determine the right start and end date, without leaving it to chance that Genie will correctly determine the entire complex query. Try [our documentation](https://docs.databricks.com/en/genie/trusted-assets.html) for instructions on how to create a trusted asset.

# COMMAND ----------

# DBTITLE 1,Add Trusted Asset
# MAGIC %sql
# MAGIC -- TODO: optional - create a function and add it to your Genie Space as a trusted asset
# MAGIC CREATE OR REPLACE FUNCTION ... (
# MAGIC   start_timestamp STRING COMMENT "A start date, formatted like '2023-05-26' or '2024-01-01'" DEFAULT "2023-01-01", 
# MAGIC   end_timestamp STRING COMMENT "An end date, formatted like '2023-05-26' or '2024-01-01'" DEFAULT "2023-12-31"
# MAGIC ) RETURNS TABLE (
# MAGIC   model_id STRING, 
# MAGIC   ...
# MAGIC ) COMMENT "..."  RETURN 
# MAGIC SELECT *
# MAGIC FROM ...

# COMMAND ----------

# DBTITLE 1,Test Function
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ...("2023-01-01", "2023-12-31")

# COMMAND ----------

# MAGIC %md
# MAGIC We can also provide guidance for how to join tables together by adding primary key / foreign key relationships. Here, we tag the model_id column as a primary key in the maintenance_price table, indicating to Genie and analysts alike that this is the right column to use as a join key

# COMMAND ----------

# DBTITLE 1,Annotate Table
# MAGIC %sql
# MAGIC -- TODO: note the primary key of your table to assist Genie in generating accurate SQL
# MAGIC ALTER TABLE ...
# MAGIC ALTER COLUMN model_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE ...
# MAGIC ADD CONSTRAINT model_pk PRIMARY KEY(model_id);

# COMMAND ----------

# MAGIC %md
# MAGIC Once a Genie Space is deployed, users will inevitably think of new questions that Genie hasn’t been primed to answer, whether the dataset isn’t included in the Space or Genie requires more informative metadata. We can start the feedback loop of iteratively improving our dashboard based on user feedback by checking the Monitoring tab of the Genie Space.
