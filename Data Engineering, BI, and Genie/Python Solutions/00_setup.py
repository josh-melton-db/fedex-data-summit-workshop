# Databricks notebook source
# MAGIC %md
# MAGIC ### 0. Setup
# MAGIC Use a Unity Catalog enabled ML cluster for this setup, or try connecting to "serverless". _This notebook will reset any data in the schema_, create the required data objects, and write some newly generated raw files into the landing zone in Unity Catalog [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html). Once the setup notebook is done (it will only take a minute or two), you can open the DLT pipeline it creates and move on to notebook one, Data Ingestion!

# COMMAND ----------

# DBTITLE 1,Install SDK
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Run Setup
from util.onboarding_setup import set_config, reset_tables, dgconfig, new_data_config
from util.data_generator import generate_iot, land_more_data, land_financials
from util.resource_creation import create_pipeline
from util.configuration import config


config = set_config(dbutils)
reset_tables(spark, config, dbutils)
create_pipeline(config, dbutils)

# COMMAND ----------

print("Your locations for the rest of the workshop:")
print("catalog: ", config['catalog'])
print("schema: ", config['schema'])

# COMMAND ----------

config

# COMMAND ----------


