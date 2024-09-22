# Databricks notebook source
# MAGIC %md
# MAGIC ## 4. Actionable Insights
# MAGIC Make sure you use a Unity Catalog enabled Machine Learning Runtime cluster to run the final setup! This notebook will create a dashboard, an alert, and an example of landing more data which we can process incrementally with our new end-to-end pipeline. The dashboard can be used to identify and surface insights using the results of our pipeline, while the alert can be configured to notify stakeholders to take action after some threshold in anomalies is met. First off, let's install the Databricks SDK and import the required functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard and Alerting

# COMMAND ----------

# DBTITLE 1,Install SDK
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Import Libraries
from util.onboarding_setup import dgconfig, new_data_config
from util.data_generator import generate_iot, land_more_data
from util.resource_creation import create_sql_assets
from util.configuration import config
from util.data_generator import land_financials

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our ETL pipeline built out, let's surface some results! Run the cell below and check the dashboards folder for your Lakeview Dashboard. Feel free to edit or move the widgets and click publish to share your insights with others. You can also schedule the dashboard to email users with fresh insights at regular intervals. The dashboard makes it easy to see our defect rate by factory or relate defect rates to variables like Density or Temperature.
# MAGIC
# MAGIC We've also created a SQL Alert titled `iot_anomaly_detection_<username>` in your Home folder and linked it below. This alert will notify the end users you specify when our anomaly warnings are triggered (note that `you'll need to refresh the alert` first by using a schedule or the button in the top right of the UI). If you run your pipeline continuously, you can set the alert to refresh as frequently as you'd like for real time alerting.

# COMMAND ----------

# DBTITLE 1,Install Dashboard and Alerting
create_sql_assets(config, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC In the next cell we'll land about 10,000 more rows worth of raw data files in the landing zone. Try running the pipeline again and notice the number of rows processed by our autoloader streaming tables - it's only the newly arrived data, meaning we don't waste compute (and therefore cost!) reprocessing data we've already seen. You can see the data arrive immediately by refreshing the dashboard

# COMMAND ----------

# DBTITLE 1,Land More Data
new_dgconfig = new_data_config(dgconfig)
land_more_data(spark, dbutils, config, new_dgconfig)
land_financials(spark, config) # Adds reference tables for maintenance/list prices

# COMMAND ----------

# MAGIC %md
# MAGIC If you run your pipeline again, you'll incrementally ingest the new data. You might see in the DLT event log that some Materialized Views while run as COMPLETE_RECOMPUTE while others show something like GROUP_AGGREGATE or PARTITION_OVERWRITE - what's happening here is DLT automatically determines the most efficient way to get your results, and if there's a shortcut available DLT will take it in order to reduce processing time. For example, consider the scenario where only some devices are updated. Wouldn't it be faster if we avoided re-computing the aggregations in our gold table for devices that didn't receive an update? With Materialized Views, we don't have to worry about answering that question. Out of the box we get simple, great performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Spaces
# MAGIC Now that we’ve generated visuals for the common requirements we can instantly create a text-to-insights tool to allow the consumers of our dashboard to ask ad-hoc questions of the data using natural language. Click the menu as shown in the screenshot below and select "Create Genie Space". Our Genie Space will be created with each of the queries and datasets that our dashboard referenced already embedded in the context of the Space. By adding more specific instructions and descriptions of our queries, we’ll enable the system to answer common questions more accurately. You can do this in the "Instructions" menu on the left after creating the Space

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/databricks-industry-solutions/iot_time_series_analysis/master/images/create_genie.png?raw=true' width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC Our Genie will likely have trouble with queries that require context about our business logic. For example, if our product lines are simply the model_ids with the numbers removed, or that our maintenance expenses are calculated by multiplying the distinct count of device_ids where defect=1. Here we can add example queries and explain to Genie how to answer questions with complex answers like "What were our maintenance expenses by product line"?

# COMMAND ----------

# DBTITLE 1,Add Example Query
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   regexp_replace(model_id, '[^A-Za-z]', '') as product_line, 
# MAGIC   avg(list_price) avg_list_price,
# MAGIC   SUM(maintenance_expenses) as total_maintenance_expenses 
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     iot.model_id, iot.device_count, 
# MAGIC     list_prices.list_price, maintenance_prices.repair_price,
# MAGIC     iot.device_count * maintenance_prices.repair_price as maintenance_expenses
# MAGIC   FROM (
# MAGIC       SELECT model_id, COUNT(DISTINCT device_id) AS device_count
# MAGIC       FROM default.iot_anomaly_detection_josh_melton_databricks_com.inspection_silver
# MAGIC       WHERE defect=1
# MAGIC       GROUP BY model_id
# MAGIC     ) iot
# MAGIC   JOIN 
# MAGIC     default.iot_anomaly_detection_josh_melton_databricks_com.list_price list_prices
# MAGIC     ON iot.model_id = list_prices.model_id
# MAGIC   JOIN 
# MAGIC     default.iot_anomaly_detection_josh_melton_databricks_com.maintenance_price maintenance_prices
# MAGIC     ON iot.model_id = maintenance_prices.model_id
# MAGIC )
# MAGIC GROUP BY regexp_replace(model_id, '[^A-Za-z]', '')

# COMMAND ----------

# MAGIC %md
# MAGIC It might also be the case that a common relationship in your data model is difficult for Genie to piece together. In addition to the queries from our dashboard, we might include an extra example that exemplifies a complex metric or series of joins. For the particularly common or critical examples, you can create trusted assets. We can use trusted assets to define templated tools that Genie can use to answer questions. In this example, we let Genie determine the right start and end date, without leaving it to chance that Genie will correctly determine the entire complex query. Try [our documentation](https://docs.databricks.com/en/genie/trusted-assets.html) for instructions on how to create a trusted asset.

# COMMAND ----------

# DBTITLE 1,Add Trusted Asset
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION default.iot_anomaly_detection_josh_melton_databricks_com.maintenance_expenses (
# MAGIC   start_timestamp STRING COMMENT "A start date, formatted like '2023-05-26' or '2024-01-01'" DEFAULT "2023-01-01", 
# MAGIC   end_timestamp STRING COMMENT "An end date, formatted like '2023-05-26' or '2024-01-01'" DEFAULT "2023-12-31"
# MAGIC ) RETURNS TABLE (
# MAGIC   model_id STRING, 
# MAGIC   failed_device_count INT, 
# MAGIC   repair_price DECIMAL(10, 2), 
# MAGIC   maintenance_expenses DECIMAL(10, 2)
# MAGIC ) COMMENT "Determines the maintenances expenses incurred between the start and end dates"  RETURN 
# MAGIC SELECT
# MAGIC   iot.model_id, iot.device_count, maintenance_prices.repair_price, 
# MAGIC   iot.device_count * maintenance_prices.repair_price as maintenance_expenses
# MAGIC FROM (
# MAGIC     SELECT model_id, SUM(`count`) as device_count
# MAGIC     FROM default.iot_anomaly_detection_josh_melton_databricks_com.inspection_gold
# MAGIC     WHERE (
# MAGIC       defect = 1 
# MAGIC       AND ((isnull(maintenance_expenses.start_timestamp) AND isnull(maintenance_expenses.end_timestamp))
# MAGIC       OR (isnull(maintenance_expenses.start_timestamp) AND first_timestamp <= maintenance_expenses.end_timestamp)
# MAGIC       OR (isnull(maintenance_expenses.end_timestamp) AND first_timestamp >= maintenance_expenses.start_timestamp)
# MAGIC       OR (first_timestamp >= maintenance_expenses.start_timestamp AND first_timestamp <= maintenance_expenses.end_timestamp))
# MAGIC     )
# MAGIC     GROUP BY model_id
# MAGIC ) iot
# MAGIC JOIN 
# MAGIC   default.iot_anomaly_detection_josh_melton_databricks_com.maintenance_price maintenance_prices
# MAGIC   ON iot.model_id = maintenance_prices.model_id

# COMMAND ----------

# DBTITLE 1,Test Function
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.iot_anomaly_detection_josh_melton_databricks_com.maintenance_expenses("2023-01-01", "2023-12-31")

# COMMAND ----------

# MAGIC %md
# MAGIC We can also provide guidance for how to join tables together by adding primary key / foreign key relationships. Here, we tag the model_id column as a primary key in the maintenance_price table, indicating to Genie and analysts alike that this is the right column to use as a join key

# COMMAND ----------

# DBTITLE 1,Annotate Table
# MAGIC %sql
# MAGIC ALTER TABLE default.iot_anomaly_detection_josh_melton_databricks_com.maintenance_price
# MAGIC ALTER COLUMN model_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE default.iot_anomaly_detection_josh_melton_databricks_com.maintenance_price
# MAGIC ADD CONSTRAINT model_pk PRIMARY KEY(model_id);

# COMMAND ----------

# MAGIC %md
# MAGIC Once a Genie Space is deployed, users will inevitably think of new questions that Genie hasn’t been primed to answer, whether the dataset isn’t included in the Space or Genie requires more informative metadata. We can start the feedback loop of iteratively improving our dashboard based on user feedback by checking the Monitoring tab of the Genie Space.
# MAGIC <img src='https://raw.githubusercontent.com/databricks-industry-solutions/iot_time_series_analysis/master/images/genie_monitoring.png?raw=true' width=800>
