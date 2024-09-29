# Databricks notebook source
# DBTITLE 1,Create Test/Train Data
from util.configuration import config


features = spark.read.table(config['feature_table']).toPandas()
train = ... # TODO: create your test/train dataset. Don't leak data!
test = ...

X_train = ...
X_test = ...
y_train = ...
y_test = ...
X_train.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Below, we'll use MLflow to log the model that we train to make predictions. This allows us to track numerous attempts and select the best model, then deploy the best model to production. Below is an example, or try [our documentation](https://docs.databricks.com/en/machine-learning/track-model-development/index.html#use-autologging-to-track-model-development) for more detail. Make a couple attempts before picking the best model, and keep in mind that we're valuing Recall highly in this scenario in order to minimize false negatives.
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC # Enable autolog()
# MAGIC mlflow.sklearn.autolog()
# MAGIC
# MAGIC # With autolog() enabled, all model parameters, a model score, and the fitted model are automatically logged.  
# MAGIC with mlflow.start_run():
# MAGIC   
# MAGIC   # Set the model parameters. 
# MAGIC   n_estimators = 100
# MAGIC   max_depth = 6
# MAGIC   max_features = 3
# MAGIC   
# MAGIC   # Create and train model.
# MAGIC   rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
# MAGIC   rf.fit(X_train, y_train)
# MAGIC   
# MAGIC   # Use the model to make predictions on the test dataset.
# MAGIC   predictions = rf.predict(X_test)
# MAGIC
# MAGIC   # Define a metric to use to evaluate the model.
# MAGIC   mse = mean_squared_error(y_test, predictions)
# MAGIC     
# MAGIC   # Log the value of the metric from this run.
# MAGIC   mlflow.log_metric("mse", mse)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Train and Log Model
import mlflow

with mlflow.start_run() as run:
    # TODO: train your model and log it to mlflow

# COMMAND ----------

# DBTITLE 1,Find the best run
mlflow.set_registry_uri('databricks-uc')
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


