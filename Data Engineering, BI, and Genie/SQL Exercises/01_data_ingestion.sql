-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### 1. Data Ingestion
-- MAGIC
-- MAGIC In this notebook, we create two [Streaming Tables](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html) that read the newly arrived data that's landing in your UC Volume. We'll run our process manually, but you can use a schedule or a [file arrivel trigger](https://docs.databricks.com/en/workflows/jobs/file-arrival-triggers.html) for incremental batch processing. You can run the same pipeline in continuous mode for real-time processing. For more details about the differences, skim our [documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html). First thing's first, be sure you've run the setup!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To get started running the DLT code below, you can simply connect to the DLT Pipeline as your compute resource in the top right and hit shift+enter in the DLT cell to validate the output of the table. To actually run the pipeline, you can click "start" in the top right once you've connected to the DLT Pipeline, or open the pipeline menu in a new page and run it from there.
-- MAGIC </br></br>
-- MAGIC For our first table, we've got raw CSV files landing in cloud storage, in this case in a Unity Catalog Volume. By using a streaming table we ensure our ETL is _incremental_, meaning we process each row exactly once. By only operating on newly arrived data, we eliminate the cost of re-processing rows we've already seen. Another convenient feature we make use of is [Autoloader](https://docs.databricks.com/en/delta-live-tables/load.html#load-files-from-cloud-object-storage), the Databricks approach for incrementally reading files from cloud storage (thus the 'cloudFiles' format below). By providing schema hints for Autoloader we get three benefits: 
-- MAGIC - We cast columns to types if possible, which can be more efficient than reading everything as a string when we're confident about the type of a column
-- MAGIC - Any columns that don't match our schema hints get saved in the '_rescued_data' column, which means we can continue processing of valid data and reprocess invalid data later
-- MAGIC - We can infer the types of columns we're unsure of later, providing flexibility to handle changing schemas
-- MAGIC </br></br>
-- MAGIC The schema inference and rescued data capabilities of Autoloader particularly come in handy when we have upstream producers of data that change the schema of the data without warning, which is unfortunately common with other teams or third party vendors. Now we've got an approach for handling it! Check out the example below, or to learn more try our [schema inference and evolution documentation](https://docs.databricks.com/en/ingestion/auto-loader/schema.html). 
-- MAGIC ```
-- MAGIC CREATE OR REFRESH STREAMING TABLE customers
-- MAGIC AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv")
-- MAGIC ```
-- MAGIC
-- MAGIC Finally, with DLT we get [expecations](https://docs.databricks.com/en/delta-live-tables/expectations.html). Expectations allow our pipeline to automatically monitor and enforce data quality
-- MAGIC ```
-- MAGIC CREATE OR REFRESH MATERIALIZED VIEW count_verification(
-- MAGIC   CONSTRAINT no_rows_dropped EXPECT (a_count == b_count)
-- MAGIC ) AS SELECT * FROM
-- MAGIC   (SELECT COUNT(*) AS a_count FROM LIVE.tbla),
-- MAGIC   (SELECT COUNT(*) AS b_count FROM LIVE.tblb)
-- MAGIC ```
-- MAGIC Without further ado, let's define our first table

-- COMMAND ----------

-- DBTITLE 1,Bronze Sensor Table
CREATE OR REFRESH STREAMING TABLE sensor_bronze
(
    CONSTRAINT ... -- TODO: Flag but retain any values for air_pressure which are negative https://docs.databricks.com/en/delta-live-tables/expectations.html 
)
COMMENT '...' -- TODO: add a description to the table
AS 
SELECT * 
FROM ...("/Volumes/workshop/source_data/sensor_bronze", "csv") -- TODO: read the csv files from cloud storage using Autoloader (check the docs here https://docs.databricks.com/en/delta-live-tables/load.html#load-files-from-cloud-object-storage)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next, we'll do the same with the landing zone for our inspection warnings data. Given some set of behavior, we can get defect warnings from the edge. Since the defects don't get flagged immediately, we'll want to put these datasets together and leverage them to make forward looking predictions about defective behavior in order to be more proactive about maintenance events in the field.

-- COMMAND ----------

-- DBTITLE 1,Bronze Inspection Table
CREATE OR REFRESH STREAMING TABLE inspection_bronze
(
    ... -- TODO: Drop any values with null timestamps or device ids https://docs.databricks.com/en/delta-live-tables/expectations.html#multiple-expectations
)
COMMENT '...' -- TODO: Add a description to the table
AS 
SELECT * 
FROM ...("/Volumes/workshop/source_data/inspection_bronze", "csv") -- TODO: read files from cloud storage using Autoloader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check out the DLT graph for real time updates of data quality!
