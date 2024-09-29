# Databricks notebook source
# MAGIC %md
# MAGIC ### 0. Setup
# MAGIC Make sure you use a Unity Catalog enabled ML cluster for this setup! Use the "workshop-ml-cluster" policy to create one. _The first notebook will reset any data in the schema_, and create the required data objects. Once the setup notebook is done (it will only take a minute or two), you can open the DLT pipeline it creates and move on to notebook one, Data Ingestion!
# MAGIC
# MAGIC Throughout the exercises, you'll see `...` representing where to write code, followed by `#TODO:` comments defining what code to write. Feel free to go off script, or grab code from the solutions folder if you get stuck

# COMMAND ----------

# DBTITLE 1,Install SDK
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Run Setup
from util.onboarding_setup import set_config, reset_tables

config = set_config(dbutils)
reset_tables(spark, config, dbutils)

# COMMAND ----------

print("Your locations for the rest of the workshop:")
print("catalog: ", config['catalog'])
print("schema: ", config['schema'])

# COMMAND ----------


