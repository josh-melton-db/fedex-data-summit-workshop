# Databricks notebook source
from util.configuration import config
import mlflow

features = spark.read.table(config['feature_table']).toPandas()
model_uri = f"models:/{config['catalog']}.{config['schema']}.{config['model_name']}@Production"
production_model = mlflow.pyfunc.load_model(model_uri)
features['predictions'] = production_model.predict(features)
display(features)

# COMMAND ----------

feature_data_stream = spark.readStream.table(config['feature_table'])

def make_predictions(microbatch_df, batch_id):
    df_to_predict = microbatch_df.toPandas()
    df_to_predict['predictions'] = production_model.predict(df_to_predict) # we use the same model and function to make predictions!
    spark.createDataFrame(df_to_predict).write.mode('append').saveAsTable(config['predictions_table'])

# COMMAND ----------

(
  feature_data_stream.writeStream
  .format('delta')
  .option('checkpointLocation', config['checkpoints'])
  .foreachBatch(make_predictions) # run our prediction function on each microbatch
  .trigger(availableNow=True) # if you want to run inferences in real time, comment out this line
  .queryName(f'stream_to_{config["predictions_table"]}') # use this for discoverability in the Spark UI
  .start()
).awaitTermination()

# COMMAND ----------

for stream in spark.streams:
    stream.stop()
