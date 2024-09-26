# Databricks notebook source
from util.configuration import config


features = spark.read.table(config['feature_table']).toPandas()
train = ... # TODO: create your test/train dataset. Don't leak data!
test = ...

X_train = ...
X_test = ...
y_train = ...
y_test = ..
X_train.head()

# COMMAND ----------

import mlflow


with mlflow.start_run(run_name='...') as run:
    # TODO: train your model and log it to mlflow

# COMMAND ----------

# DBTITLE 1,Find the best run
from mlflow.tracking import MlflowClient
client = MlflowClient()

runs = client.search_runs(run.info.experiment_id, order_by=['metrics.recall DESC']) # TODO: pick your best run. In this example,
lowest_f1_run_id = runs[0].info.run_id                                              # we use the best recall

# COMMAND ----------

# DBTITLE 1,Register the model
model_uri = f'runs:/{lowest_f1_run_id}/model'
model_details = mlflow.register_model(model_uri=model_uri, name=f"{config['catalog']}.{config['schema']}.{config['model_name']}")

# COMMAND ----------

# DBTITLE 1,Set model as "Production"
client.set_registered_model_alias(f"{config['catalog']}.{config['schema']}.{config['model_name']}", 'Production', model_details.version)

# COMMAND ----------


