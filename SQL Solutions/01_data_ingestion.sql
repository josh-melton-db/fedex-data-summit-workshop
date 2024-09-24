-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### 1. Data Ingestion
-- MAGIC
-- MAGIC In this notebook, we create two [Streaming Tables](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html) that read the newly arrived data that's landing in your UC Volume. For your use case, you may point your stream to receive events from a message queue like Kafka. We'll run our process manually, but you can use a schedule or a [file arrivel trigger](https://docs.databricks.com/en/workflows/jobs/file-arrival-triggers.html) for incremental batch processing. You can run the same pipeline in continuous mode for real-time processing. For more details about the differences, skim our [documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html). First thing's first, we'll get the configuration we used in the setup notebook. Be sure you've run the setup!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To get started running the DLT code below, you can simply connect to the DLT Pipeline as your compute resource in the top right and hit shift+enter in the DLT cell to validate the output of the table. To actually run the pipeline, you can click "start" in the top right once you've connected to the DLT Pipeline, or open the pipeline menu in a new page and run it from there.
-- MAGIC </br></br>
-- MAGIC For our first table, we've got raw CSV files landing in cloud storage, in this case in a Unity Catalog Volume. By using a streaming table we ensure our ETL is _incremental_, meaning we process each row exactly once. By only operating on newly arrived data, we eliminate the cost of re-processing rows we've already seen. Another convenient feature we make use of is Autoloader, the Databricks approach for incrementally reading files from cloud storage (thus the 'cloudFiles' format below). By providing schema hints for Autoloader we get three benefits: 
-- MAGIC - We cast columns to types if possible, which can be more efficient than reading everything as a string when we're confident about the type of a column
-- MAGIC - Any columns that don't match our schema hints get saved in the '_rescued_data' column, which means we can continue processing of valid data and reprocess invalid data later
-- MAGIC - We can infer the types of columns we're unsure of later, providing flexibility to handle changing schemas
-- MAGIC </br></br>
-- MAGIC The schema inference and rescued data capabilities of Autoloader particularly come in handy when we have upstream producers of data that change the schema of the data without warning, which is unfortunately common with other teams or third party vendors. Now we've got an approach for handling it! To learn more, try our [schema inference and evolution documentation](https://docs.databricks.com/en/ingestion/auto-loader/schema.html). Without further ado, let's define our first table

-- COMMAND ----------

-- USE CATALOG default; -- insert the catalog from your setup here
-- USE SCHEMA iot_anomaly_detection_josh_melton_databricks_com;-- insert the catalog from your setup here

-- COMMAND ----------

-- DBTITLE 1,Bronze Sensor Table
CREATE OR REFRESH STREAMING TABLE sensor_bronze
(
    CONSTRAINT valid_pressure EXPECT (air_pressure > 0) ON VIOLATION DROP ROW
)
COMMENT 'Loads raw sensor data into the bronze layer'
AS 
SELECT * 
FROM cloud_files("/Volumes/workshop/source_data/sensor_bronze", "csv")
-- do we need schema hints, schema location?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next, we'll do the same with the landing zone for our inspection warnings data. Given some set of behavior, we can get defect warnings from the edge. Since the defects don't get flagged immediately, we'll want to put these datasets together and leverage them to make forward looking predictions about defective behavior in order to be more proactive about maintenance events in the field.

-- COMMAND ----------

-- DBTITLE 1,Bronze Inspection Table
CREATE OR REFRESH STREAMING TABLE inspection_bronze
(
    CONSTRAINT valid_timestamp EXPECT (`timestamp` is not null) ON VIOLATION DROP ROW,
    CONSTRAINT valid_device_id EXPECT (device_id is not null) ON VIOLATION DROP ROW
)
COMMENT 'Loads raw inspection files into the bronze layer'
AS 
SELECT * 
FROM cloud_files("/Volumes/workshop/source_data/inspection_bronze", "csv")
-- do we need schema hints, schema location options?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check out the DLT graph for real time updates of data quality!
