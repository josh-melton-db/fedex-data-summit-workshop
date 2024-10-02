# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 10 minute demo of Mosaic AI Agent Framework & Agent Evaluation
# MAGIC
# MAGIC #### TLDR; this notebook will:
# MAGIC 1. Deploy a RAG application built with [Agent Framework](https://docs.databricks.com/generative-ai/retrieval-augmented-generation.html) to the [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html) review application
# MAGIC     - The review application is used by your business stakeholders to provide feedback on your app
# MAGIC 2. Evaluate the quality of the application with [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html) and MLflow
# MAGIC     - These AI-assisted evaluations are used by developers to improve the application's quality
# MAGIC
# MAGIC #### Products used:
# MAGIC - [**Mosaic AI Agent Framework**](https://docs.databricks.com/generative-ai/retrieval-augmented-generation.html) SDK to quickly and safely build high-quality RAG applications.
# MAGIC - [**Mosaic AI Agent Evaluation**](https://docs.databricks.com/generative-ai/agent-evaluation/index.html) AI-assisted evaluation tool to determines if outputs are high-quality.  Provides an intuitive UI to get feedback from human stakeholders.
# MAGIC - [**Mosaic AI Model Serving**](https://docs.databricks.com/generative-ai/deploy-agent.html) Hosts the application's logic as a production-ready, scalable REST API.
# MAGIC - [**MLflow**](https://docs.databricks.com/mlflow/index.html) Tracks and manages the application lifecycle, including evaluation results and application code/config
# MAGIC - [**Generative AI Cookbook**](https://ai-cookbook.io/) A definitive how-to guide, backed by a code repo, for building high-quality Gen AI apps, developed in partnership with Mosaic AI’s research team.
# MAGIC
# MAGIC
# MAGIC #### Agent Evaluation review application
# MAGIC <img src="https://ai-cookbook.io/_images/review_app2.gif" style="float: left;  margin-left: 10px" width="60%">
# MAGIC
# MAGIC #### Agent Evaluation outputs in MLflow
# MAGIC <img src="https://ai-cookbook.io/_images/mlflow-eval-agent.gif" style="float: left;  margin-left: 10px" width="60%">
# MAGIC
# MAGIC #### Generative AI Cookbook
# MAGIC <img src="https://raw.githubusercontent.com/databricks/genai-cookbook/e280f9a33137682cf90868a3ac95a775e4f958ef/genai_cookbook/images/5-hands-on/cookbook.png" style="float: left;  margin-left: 10px" width="60%">

# COMMAND ----------

# MAGIC %pip install -U -qqqq databricks-agents mlflow mlflow-skinny databricks-vectorsearch databricks-sdk langchain==0.2.11 langchain_core==0.2.23 langchain_community==0.2.10 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Application configuration
# MAGIC
# MAGIC We've selected defaults for the following parameters based on your user name, but inspect and change if you prefer to use existing resources.  Any missing resources will be created in the next step.
# MAGIC
# MAGIC 1. `UC_CATALOG` & `UC_SCHEMA`: [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/create-catalogs.html#create-a-catalog) and a Schema where the output Delta Tables with the parsed/chunked documents and Vector Search indexes are stored
# MAGIC 2. `UC_MODEL_NAME`: Unity Catalog location to log and store the chain's model
# MAGIC 3. `VECTOR_SEARCH_ENDPOINT`: [Vector Search Endpoint](https://docs.databricks.com/en/generative-ai/create-query-vector-search.html#create-a-vector-search-endpoint) to host the resulting vector index

# COMMAND ----------

from util.configuration import config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Build & deploy the application
# MAGIC
# MAGIC Below is a high-level overview of the architecture we will deploy:
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic.png?raw=true" style="width: 800px; margin-left: 10px">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-prep-3.png?raw=true" style="float: right; margin-left: 10px" width="400px">
# MAGIC
# MAGIC ## 1/ Create the Vector Search Index
# MAGIC
# MAGIC First, we copy the sample data to a Delta Table and sync to a Vector Search index.  Here, we use the [gte-large-en-v1.5](https://huggingface.co/Alibaba-NLP/gte-large-en-v1.5) embedding model hosted on [Databricks Foundational Model APIs](https://docs.databricks.com/en/machine-learning/foundation-models/index.html).

# COMMAND ----------

from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient

# Workspace URL for printing links to the delta table/vector index
workspace_url = SparkSession.getActiveSession().conf.get(
    "spark.databricks.workspaceUrl", None
)

# Vector Search client
vsc = VectorSearchClient(disable_notice=True)

# Load the chunked data to Delta Table & enable change-data capture to allow the table to sync to Vector Search
chunked_docs_df = spark.read.option('header', True).csv('/Volumes/workshop/source_data/customer_service_tickets/example_data_logistics.csv')
chunked_docs_df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(config['docs_table'])
spark.sql(f"ALTER TABLE {config['docs_table']} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Embed and sync chunks to a vector index
print(f"Embedding docs & creating Vector Search Index, this will take ~5 - 10 minutes...")

# COMMAND ----------

index = vsc.create_delta_sync_index_and_wait(
    endpoint_name=config['vs_endpoint'],
    index_name=config['docs_table']+'_index',
    primary_key="ticket_number",
    source_table_name=config['docs_table'],
    pipeline_type="TRIGGERED",
    embedding_source_column="issue_description",
    embedding_model_endpoint_name="databricks-gte-large-en",
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Deploy to the review application
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-chain-1.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Now that our Vector Search index is ready, let's prepare the RAG chain and deploy it to the review application backed by a scalable-production ready REST API on Model serving.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1/ Configuring our Chain parameters
# MAGIC
# MAGIC Databricks makes it easy to parameterize your chain with MLflow Model Configurations. Later, you can tune application quality by adjusting these parameters, such as the system prompt or retrieval settings.  Most applications will include many more parameters, but for this demo, we'll keep the configuration to a minimum.

# COMMAND ----------

chain_config = {
    "llm_model_serving_endpoint_name": "databricks-meta-llama-3-1-70b-instruct",  # the foundation model we want to use
    "vector_search_endpoint_name": config['vs_endpoint'],  # Endoint for vector search
    "vector_search_index": f"{config['docs_table']}_index",
    "llm_prompt_template": """You are an assistant that answers questions. Use the following pieces of retrieved context to answer the question. Some pieces of context may be irrelevant, in which case you should not use them to form the answer.\n\nContext: {context}""", # LLM Prompt template
}

# Here, we define an input example in the schema required by Agent Framework
input_example = {"messages": [ {"role": "user", "content": "What is causing delays?"}]}

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2.1/ Log the application & view trace
# MAGIC
# MAGIC We first register the chain as an MLflow model and inspect the MLflow Trace to understand what is happening inside the chain.
# MAGIC
# MAGIC #### MLflow trace
# MAGIC <br/>
# MAGIC <img src="https://ai-cookbook.io/_images/mlflow_trace2.gif" width="80%" style="margin-left: 10px">
# MAGIC

# COMMAND ----------

import mlflow
import os

# Log the model to MLflow
with mlflow.start_run(run_name="customer_service_chatbot"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=os.path.join(
            os.getcwd(),
            "sample_rag_chain",
        ),  # Chain code file from the quick start repo
        model_config=chain_config,  # Chain configuration set above
        artifact_path="chain",  # Required by MLflow
        input_example=input_example,  # Save the chain's input schema.  MLflow will execute the chain before logging & capture it's output schema.
    )

# Test the chain locally to see the MLflow Trace
chain = mlflow.langchain.load_model(logged_chain_info.model_uri)
chain.invoke(input_example)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2.1/ Deploy the application
# MAGIC
# MAGIC Now, we:
# MAGIC 1. Register the application in Unity Catalog
# MAGIC 2. Use Agent Framework to deploy to the Quality Lab review application
# MAGIC
# MAGIC Along side the review ap, a scalable, production-ready Model Serving endpoint is also deployed.
# MAGIC
# MAGIC #### Agent Evaluation review application
# MAGIC <img src="https://ai-cookbook.io/_images/review_app2.gif" width="90%">

# COMMAND ----------

from databricks import agents
import time
from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate

# Use Unity Catalog to log the chain
mlflow.set_registry_uri('databricks-uc')

# Register the chain to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_chain_info.model_uri, 
                                                 name=config['agent_model_name'])

# Deploy to enable the Review APP and create an API endpoint
deployment_info = agents.deploy(model_name=config['agent_model_name'], 
                                model_version=uc_registered_model_info.version)

# Wait for the Review App to be ready
print("\nWaiting for endpoint to deploy.  This can take 15 - 20 minutes.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3/ Use Agent Evaluation to evaluate your application
# MAGIC
# MAGIC ## 3.1/ Have stakeholders chat your bot to build your evaluation dataset
# MAGIC
# MAGIC Normally, you would now give access to internal domain experts and have them test and review the bot.  **Your domain experts do NOT need to have Databricks Workspace access** - you can assign permissions to any user in your SSO if you have enabled [SCIM](https://docs.databricks.com/en/admin/users-groups/scim/index.html)
# MAGIC
# MAGIC This is a critical step to build or improve your evaluation dataset: have users ask questions to your bot, and provide the bot with output answer when they don't answer properly.
# MAGIC
# MAGIC Your applicaation is automatically capturing all stakeholder questions and bot responses, including the MLflow Trace for each, into Delta Tables in your Lakehouse. On top of that, Databricks makes it easy to track feedback from your end user: if the chatbot doesn't give a good answer and the user gives a thumbdown, their feedback is included in the Delta Tables.
# MAGIC
# MAGIC Your evaluation dataset forms the basis of your development workflow to improve quality: identifying the root causes of quality issues and then objectively measuring the impact of your fixes.
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <img src="https://ai-cookbook.io/_images/review_app2.gif" style="float: left;  margin-left: 10px" width="80%">

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2/ Run Evaluation of your Chain
# MAGIC
# MAGIC Now, let's use everage Agent Evaluation's specialized AI evaluators to evaluate our model performance.  Agent Evaluation is integrated into `mlflow.evaluate(...)`, all you need to do is pass `model_type="databricks-agent"`.
# MAGIC
# MAGIC For this demo, we use a toy 10 question evaluation dataset.  Read more about our [best practices](https://ai-cookbook.io/nbs/4-evaluation-eval-sets.html) on the size of your evaluation dataset.
# MAGIC
# MAGIC <img src="https://ai-cookbook.io/_images/mlflow-eval-agent.gif" style="float: left;  margin-left: 10px" width="80%">

# COMMAND ----------

import pandas as pd

sample_eval_set = [
    {
        "request_id": "5482",
        "request": "What warehouse storage safety issues are most pressing to address?",
        "expected_response": "Storage shelves are overloaded and pose a risk to collapse.",
    }
]

eval_df = pd.DataFrame(sample_eval_set)
display(eval_df)

# COMMAND ----------

with mlflow.start_run(run_id=logged_chain_info.run_id):
    # Evaluate
    eval_results = mlflow.evaluate(
        data=eval_df, # Your evaluation set
        model=logged_chain_info.model_uri, # previously logged model
        model_type="databricks-agent", # activate Mosaic AI Agent Evaluation
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # What's next?
# MAGIC
# MAGIC
# MAGIC ## Code-based quick starts for Gen AI
# MAGIC
# MAGIC | Time required | Outcome | Link |
# MAGIC |------ | ---- | ---- |
# MAGIC | 🕧 <br/> 10 minutes | Sample RAG app deployed to web-based chat app that collects feedback | ✅ |
# MAGIC | 🕧🕧🕧 <br/>60 minutes | POC RAG app with *your data* deployed to a chat UI that can collect feedback from your business stakeholders | [Deploy POC w/ your data](https://ai-cookbook.io/nbs/5-hands-on-build-poc.html)|
# MAGIC | 🕧🕧 <br/>30 minutes | Comprehensive quality/cost/latency evaluation of your POC app | - [Evaluate your POC](https://ai-cookbook.io/nbs/5-hands-on-evaluate-poc.html) <br/> - [Identify the root causes of quality issues](https://ai-cookbook.io/nbs/5-hands-on-improve-quality-step-1.html) |
# MAGIC
# MAGIC ## Read the [Generative AI Cookbook](https://ai-cookbook.io)!
# MAGIC
# MAGIC **TLDR;** the [cookbook]((https://ai-cookbook.io) and its sample code will take you from initial POC to high-quality production-ready application using [Mosaic AI Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html) and [Mosaic AI Agent Framework](https://docs.databricks.com/generative-ai/retrieval-augmented-generation.html) on the Databricks platform.
# MAGIC
# MAGIC The Databricks Generative AI Cookbook is a definitive how-to guide for building *high-quality* generative AI applications. *High-quality* applications are applications that:
# MAGIC 1. **Accurate:** provide correct responses
# MAGIC 2. **Safe:** do not deliver harmful or insecure responses
# MAGIC 3. **Governed:** respect data permissions & access controls and track lineage
# MAGIC
# MAGIC Developed in partnership with Mosaic AI's research team, this cookbook lays out Databricks best-practice development workflow for building high-quality RAG apps: *evaluation driven development.* It outlines the most relevant knobs & approaches that can increase RAG application quality and provides a comprehensive repository of sample code implementing those techniques. 
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <img src="https://ai-cookbook.io/_images/dbxquality.png" style="margin-left: 10px" width="80%">
