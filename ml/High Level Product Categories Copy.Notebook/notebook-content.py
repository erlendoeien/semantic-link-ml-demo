# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "029e2656-ce72-41b7-bfa8-f0b8a6cd2917",
# META       "default_lakehouse_name": "Gold",
# META       "default_lakehouse_workspace_id": "d1bc3c0e-7995-4f9b-b79b-b096c69fd5d9"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "name": "synapseml",
# MAGIC     "conf": {
# MAGIC         "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.10-spark3.5,com.microsoft.azure:synapseml-internal_2.12:1.0.10.0-spark3.5",
# MAGIC         "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
# MAGIC         "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
# MAGIC         "spark.yarn.user.classpath.first": "true",
# MAGIC         "spark.sql.parquet.enableVectorizedReader": "false"
# MAGIC     },
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "ML"
# MAGIC     },
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ENV

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from synapse.ml.services.openai import OpenAIEmbedding
import mlflow
mlflow.autolog(disable=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ![image-alt-text](https://images.ctfassets.net/k07f0awoib97/2n4uIQh2bAX7fRmx4AGzyY/a1bc6fa1e2d14ff247716b5f589a2099/Screen_Recording_2023-06-03_at_4.52.54_PM.gif)

# CELL ********************

df = spark.table("Gold.Products")
df = df.withColumn("Category", F.regexp_replace(df["Category"],"_", " "))
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

embedding = (
    OpenAIEmbedding()
    .setSubscriptionKey(AZURE_OPENAI_KEY)
    .setDeploymentName(AZURE_OPENAI_EMBEDDING)
    # Retrieve from Azure AI Services resource if deployed using AI Foundry
    .setCustomServiceName(AZURE_AI_SERVICE_NAME)
    # .setDeploymentName("text-embedding-ada-002")
    .setTextCol("Category")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)
categories_df = df.select("Category").distinct().dropna()
completed_df = embedding.transform(categories_df).cache()
display(completed_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.clustering import KMeans
from pyspark.ml import PipelineModel

# Trains a k-means model.
num_clusters = 18

kmeans = (
    KMeans()
    .setK(num_clusters)
    .setFeaturesCol("embeddings")
    # .setValuesCol("Category")
    .setDistanceMeasure("cosine")
    .setPredictionCol("cluster")
    .setSeed(42)
)

# Make predictions
kmeans_model = kmeans.fit(completed_df)
clusters_df = kmeans_model.transform(completed_df)
display(clusters_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(clusters_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# group to segments
clusters_grouped_df = (clusters_df
                        .groupby("cluster")
                        .agg(
                            F.collect_list("Category").alias("categories")
                        ).withColumn("categories_str", F.concat_ws(", ", "categories"))
                    )
display(clusters_grouped_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create fitting Segment names

# CELL ********************

from pyspark.ml import Pipeline, Transformer
from pyspark.sql import DataFrame

class ProductSegment(Transformer):
    """
    A custom Transformer which drops all columns that have at least one of the
    words from the banned_list in the name.
    """

    def __init__(self, groupCol: str, valueCol: str, valueColPlural: str, outputCol: str):
        super(ProductSegment, self).__init__()
        self.groupCol = groupCol
        self.valueCol = valueCol
        self.valueColPlural = valueColPlural
        self.outputCol = outputCol

        # Setup for AI Functions
        from synapse.ml.services.openai import OpenAIDefaults
        from synapse.ml.spark.aifunc.DataFrameExtensions import AIFunctions
        defaults = OpenAIDefaults()

        defaults.set_deployment_name(AZURE_OPENAI_MODEL)
        defaults.set_subscription_key(AZURE_OPENAI_KEY)
        defaults.set_URL(AZURE_OPENAI_ENDPOINT)
        defaults.set_temperature(0.4)

    def _transform(self, df: DataFrame) -> DataFrame:
        grouped_df = self._agg_to_list(df)
        return self._add_segment(grouped_df)
    
    def _agg_to_list(self, df: DataFrame) -> DataFrame:
        return (df
                    .groupby(self.groupCol)
                    .agg(
                        F.collect_list(self.valueCol).alias(self.valueColPlural)
                    ).withColumn(f"{self.valueColPlural}_str", F.concat_ws(", ", self.valueColPlural))
                )
    def _add_segment(self, df: DataFrame) -> DataFrame:
        PROMPT = f"""I will give you a list of two or more product categories. 
        Your task is to come up with a suitable collective name that best represents all of the categories I give you. 
        You should respond with only your suggestion. Include nothing else. 
        Remember to only respond with your suggestion. Product categories = {{{self.valueColPlural}_str}}"""
        return (df
                    .ai
                    .generate_response(prompt=PROMPT, 
                                        is_prompt_template=True, 
                                        output_col=self.outputCol, 
                                        error_col=f"{self.outputCol}_error")
                                    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate segment names 
PROMPT = """I will give you a list of two or more product categories. 
Your task is to come up with a suitable collective name that best represents all of the categories I give you. 
You should respond with only your suggestion. Include nothing else. 
Remember to only respond with your suggestion. Product categories = {categories_str}"""

segment_names_df = (
    clusters_grouped_df
        .ai
        .generate_response(
            prompt=PROMPT, 
            is_prompt_template=True, 
            output_col="product_segment", 
            error_col="product_segment_error")
        )
display(segment_names_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.clustering import KMeans
from synapse.ml.services.openai import OpenAIEmbedding

# End-to-end pipeline
df = spark.table("Gold.Products")
df = df.withColumn("Category", F.regexp_replace(df["Category"],"_", " "))
categories_df = df.select("Category").distinct().dropna()

# Embedding
embedding = (
    OpenAIEmbedding()
    .setSubscriptionKey(AZURE_OPENAI_KEY)
    .setDeploymentName(AZURE_OPENAI_EMBEDDING)
    .setCustomServiceName(AZURE_AI_SERVICE_NAME)
    # .setDeploymentName("text-embedding-ada-002")
    .setTextCol("Category")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)

# Trains a k-means model.
num_clusters = 18

kmeans = (
    KMeans()
    .setK(num_clusters)
    .setFeaturesCol("embeddings")
    # .setValuesCol("Category")
    .setDistanceMeasure("cosine")
    .setPredictionCol("cluster")
    .setSeed(42)
)

# Make predictions
kmeans_model = kmeans.fit(completed_df)
clusters_df = kmeans_model.transform(completed_df)

# Generating segments
product_segment_transformer = ProductSegment(
                    groupCol="cluster", 
                    valueCol="Category", 
                    valueColPlural="categories", 
                    outputCol="product_segment"
            )
end_to_end_pipe = Pipeline(stages=[embedding, kmeans, product_segment_transformer])
fitted_pipeline = end_to_end_pipe.fit(categories_df)
fitted_pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_segments = fitted_pipeline.transform(categories_df)
display(product_segments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_segments.select(F.explode("categories").alias("Category"), "product_segment").write.saveAsTable("ML.ProductSegments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
