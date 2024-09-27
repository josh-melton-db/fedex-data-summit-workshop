# Databricks notebook source
# MAGIC %md
# MAGIC ## 4. Actionable Insights
# MAGIC Make sure you use a Unity Catalog enabled Machine Learning Runtime cluster to run the final setup! This notebook will create a dashboard, an alert, and an example of landing more data which we can process incrementally with our new end-to-end pipeline. The dashboard can be used to identify and surface insights using the results of our pipeline, while the alert can be configured to notify stakeholders to take action after some threshold in anomalies is met. First off, let's install the Databricks SDK and import the required functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard and Alerting

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our ETL pipeline built out, let's surface some results! Run the cell below and check the dashboards folder for your Lakeview Dashboard. Feel free to edit or move the widgets and click publish to share your insights with others. You can also schedule the dashboard to email users with fresh insights at regular intervals. The dashboard makes it easy to see our defect rate by factory or relate defect rates to variables like Density or Temperature.
# MAGIC
# MAGIC We've also created a SQL Alert titled `iot_anomaly_detection_<username>` in your Home folder and linked it below. This alert will notify the end users you specify when our anomaly warnings are triggered (note that `you'll need to refresh the alert` first by using a schedule or the button in the top right of the UI). If you run your pipeline continuously, you can set the alert to refresh as frequently as you'd like for real time alerting.

# COMMAND ----------

# DBTITLE 1,Install SDK
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Install Dashboard and Alerting
from util.resource_creation import create_sql_assets
from util.configuration import config
create_sql_assets(config, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that you've completed these exercises, consider trying:
# MAGIC - Customizing a [Genie Space](https://learn.microsoft.com/en-us/azure/databricks/genie/)
# MAGIC - Building your own Dashboard
# MAGIC - The ML exercises. See if you can use the ML model in your streaming pipeline!
# MAGIC - Try out the AI Playground. See how tool usage works - functions can be used in Genie or your own custom LLMs!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Compute
# MAGIC If you run your pipeline again, you'll incrementally ingest the new data. You might see in the DLT event log that some Materialized Views while run as COMPLETE_RECOMPUTE while others show something like GROUP_AGGREGATE or PARTITION_OVERWRITE - what's happening here is DLT automatically determines the most efficient way to get your results, and if there's a shortcut available DLT will take it in order to reduce processing time. For example, consider the scenario where only some devices are updated. Wouldn't it be faster if we avoided re-computing the aggregations in our gold table for devices that didn't receive an update? With Materialized Views, we don't have to worry about answering that question. Out of the box we get simple, great performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Spaces
# MAGIC Now that we’ve generated visuals for the common requirements we can instantly create a text-to-insights tool to allow the consumers of our dashboard to ask ad-hoc questions of the data using natural language. Click the menu in the top right of the dashboard and select "Create Genie Space". Our Genie Space will be created with each of the queries and datasets that our dashboard referenced already embedded in the context of the Space. By adding more specific instructions and descriptions of our queries, we’ll enable the system to answer common questions more accurately. You can do this in the "Instructions" menu on the left after creating the Space

# COMMAND ----------

# MAGIC %md
# MAGIC Our Genie will likely have trouble with queries that require context about our business logic. For example, if our product lines are simply the model_ids with the numbers removed, or that our maintenance expenses are calculated by multiplying the distinct count of device_ids where defect=1. Here we can add example queries and explain to Genie how to answer questions with complex answers like "What were our maintenance expenses by product line"?

# COMMAND ----------

# MAGIC %md
# MAGIC Once a Genie Space is deployed, users will inevitably think of new questions that Genie hasn’t been primed to answer, whether the dataset isn’t included in the Space or Genie requires more informative metadata. We can start the feedback loop of iteratively improving our dashboard based on user feedback by checking the Monitoring tab of the Genie Space.
