# Databricks notebook source
from util.configuration import config


features = spark.read.table(config['feature_table']).toPandas()
train = features.iloc[:int(len(features) * 0.8)]
test = features.iloc[int(len(features) * 0.8):]

X_train = train.drop('defect', axis=1)
X_test = test.drop('defect', axis=1)
y_train = train['defect']
y_test = test['defect']
X_train.head()

# COMMAND ----------

from imblearn.over_sampling import SMOTE
from collections import Counter

counter1 = Counter(y_train)
oversample = SMOTE()
X_train_oversampled, y_train_oversampled = oversample.fit_resample(X_train, y_train)
counter2 = Counter(y_train_oversampled)
print(counter1, counter2)

# COMMAND ----------

import pandas as pd
import mlflow
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, recall_score
from mlflow.models.signature import infer_signature
import uuid
import matplotlib.pyplot as plt
import mlflow


with mlflow.start_run(run_name='RandomForestClassifier') as run:
    # Create model, train it, and create predictions
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train_oversampled, y_train_oversampled)
    predictions = rf.predict(X_test)

    # Log model with signature
    signature = infer_signature(X_test, predictions)
    mlflow.sklearn.log_model(rf, 'model', signature=signature)

    # Log metrics
    f1 = f1_score(y_test, predictions)
    recall = recall_score(y_test, predictions)
    mlflow.log_metric('test_f1', f1)
    mlflow.log_metric('test_recall', recall)
    mlflow.log_metric('defects_predicted', predictions.sum())

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()

runs = client.search_runs(run.info.experiment_id, order_by=['metrics.recall DESC'])
lowest_f1_run_id = runs[0].info.run_id

# COMMAND ----------

model_uri = f'runs:/{lowest_f1_run_id}/model'
model_details = mlflow.register_model(model_uri=model_uri, name=f"{config['catalog']}.{config['schema']}.{config['model_name']}")

# COMMAND ----------

client.set_registered_model_alias(f"{config['catalog']}.{config['schema']}.{config['model_name']}", 'Production', model_details.version)

# COMMAND ----------


