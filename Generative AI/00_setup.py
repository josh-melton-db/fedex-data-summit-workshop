# Databricks notebook source
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from util.onboarding_setup import set_config, reset_tables

config = set_config(dbutils)
reset_tables(spark, config, dbutils)

# COMMAND ----------

print("Your locations for the rest of the workshop:")
print("catalog: ", config['catalog'])
print("schema: ", config['schema'])
