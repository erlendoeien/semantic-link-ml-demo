# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name": "ML"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary libraries
import mlflow
import numpy as np
import pyspark.sql.functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator
from datetime import datetime
from mlflow.tracking import MlflowClient
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the model and mappings from MLflow
timestamp = datetime.now()

# Initialize MLflow client
client = MlflowClient()

# Get the latest model version from the registry
model_name = "ProductRecommender"
latest_version = max([int(mv.version) for mv in client.search_model_versions(f"name='{model_name}'")])
model_version = client.get_model_version(model_name, str(latest_version))

print(f"Loading model: {model_name} version {latest_version}")

# Get the dataset information from model tags - Strictly not necessary forRecSys
dataset_version = model_version.tags.get("dataset_version")
dataset_run_id = model_version.tags.get("dataset_run_id")

print(f"Associated dataset version: {dataset_version}")
print(f"Associated dataset run ID: {dataset_run_id}")

# Load the model
model_uri = f"models:/{model_name}/{latest_version}"
model = mlflow.spark.load_model(model_uri)

# Load the dataset artifacts
print("Loading dataset artifacts...")

# # Download and load the transformation pipeline
# pipeline_model_uri = f"runs:/{dataset_run_id}/transformation_pipeline"
# pipeline_model = mlflow.spark.load_model(pipeline_model_uri)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Download and load the customer mappings
customer_id_to_index = mlflow.artifacts.load_dict(f"runs:/{dataset_run_id}/customer_mappings.json")

# Download and load the product mappings
product_id_to_index = mlflow.artifacts.load_dict(f"runs:/{dataset_run_id}/product_mappings.json")

print(f"Loaded {len(customer_id_to_index)} customer mappings and {len(product_id_to_index)} product mappings")

# Create reverse mappings for converting indices back to IDs
customer_index_to_id = {item['userId']: item['customer_id'] for item in customer_id_to_index}
product_index_to_id = {item['itemId']: item['product_id'] for item in product_id_to_index}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Inference Personalised recommendations 

# MARKDOWN ********************

# # Analysing recommendations

# CELL ********************

# Calculate and log recommendation diversity metrics
def calculate_diversity_metrics(user_recommendations, product_count):
    """Calculate diversity metrics for recommendations"""
    # Extract all recommended products
    all_recommended_products = user_recommendations.select(
        F.col("product_id")
    ).distinct()
    
    # Calculate coverage
    coverage = (all_recommended_products.count() / product_count) * 100
    
    # Calculate popularity distribution (Gini coefficient)
    product_popularity = user_recommendations.groupBy("product_id").count().orderBy("count")
    product_popularity_pd = product_popularity.toPandas()
    
    def calculate_gini(values):
        """Calculate Gini coefficient"""
        values = sorted(values)
        n = len(values)
        if n == 0:
            return 0
        indices = np.arange(1, n + 1)
        return 1 - 2 * np.sum((n + 1 - indices) * values) / (n * np.sum(values))
    
    gini = calculate_gini(product_popularity_pd["count"].values)
    
    return {
        "catalog_coverage_percent": coverage,
        "recommendation_gini": gini
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the experiment for inference
mlflow.set_experiment("Product_RecSys_Inference")
num_user_recs = 10

with mlflow.start_run(run_name=f"{timestamp.date()}_user_item_recs"):
    # Link to the dataset and model runs
    mlflow.set_tag("dataset_run_id", model_version.tags.get("dataset_run_id", None))
    mlflow.set_tag("model_run_id", model_version.tags.get("final_model_run_id", None))
    mlflow.set_tag("model_name", model_name)
    mlflow.set_tag("model_version", model_version.version)
    mlflow.set_tag("inference_ts", timestamp)

    # get recomendations for all users
    exploded_user_product_recs = model.stages[0].recommendForAllUsers(num_user_recs).select(
            F.col("userId"),
            F.explode("recommendations").alias("recommendation")
        ).select(
            "userId",
            F.col("recommendation.itemId").alias("itemId"),
            F.col("recommendation.rating").alias("score")
        )
    # map recommendations back to product_id and user_ids (index)
    # indices back to IDs
    customer_product_recommendations_df = exploded_user_product_recs.withColumns({
        "customer_id": F.udf(lambda idx: customer_index_to_id.get(float(idx), "Unknown"))(F.col("userId")),
        "product_id": F.udf(lambda idx: product_index_to_id.get(float(idx), "Unknown"))(F.col("itemId")),
        "recommendation_ts_utc": F.lit(timestamp),
        "recommendation_model": F.lit(model_name),
        "recommendation_model_version": F.lit(int(model_version.version))
    }).select("customer_id", "product_id", "score", "recommendation_ts_utc", "recommendation_model", "recommendation_model_version")
    
    # Get total product count
    total_products = len(product_index_to_id)
    
    # Calculate diversity metrics
    print("Calculating diversity metrics")
    diversity_metrics = calculate_diversity_metrics(customer_product_recommendations_df, total_products)
    
    # Log recommendation statistics and diversity metrics
    mlflow.log_metrics({
        "users_with_recommendations": customer_product_recommendations_df.select("customer_id").distinct().count(),
        "total_user_recommendations": customer_product_recommendations_df.count(),
        **diversity_metrics
    })
    
    # Save recommendations to temporary CSV files
    
    # Log the output tables
    mlflow.log_params({
        "recommendation_ts": timestamp,
        "num_user_recs": num_user_recs
    })
    
    # Create a sample of recommendations for inspection
    user_recs_sample = customer_product_recommendations_df.limit(100)
    
    # Save samples as CSV for easy inspection
    print("Logging and storing recommendations")
    user_recs_sample_path = f"/tmp/user_recs_sample_{timestamp}.csv"
    
    user_recs_sample.toPandas().to_csv(user_recs_sample_path, index=False)
    
    # Log sample files
    mlflow.log_artifact(user_recs_sample_path, "recommendation_samples")

    # Store product recommendations in ML lakehouse
    spark.conf.set("spark.sql.caseSensitive", "true")
    customer_product_recommendations_df.write.mode("append").option("mergeSchema", "true").saveAsTable("ML.UserProductRecommendations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customer_product_recommendations_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Misc, to-do for similar products

# CELL ********************

# Function to get similar products
def get_similar_products(product_id, num_similar=10):
    """Get similar products based on the learned item factors"""
    # Find the product index
    if product_id not in product_id_to_index:
        print(f"Product ID {product_id} not found in training data")
        return None
    
    product_idx = product_id_to_index[product_id]
    
    # Get similar items
    # TODO: (use item factors and search)
    similar_items = model.similarItems(float(product_idx), num_similar)
    
    # Convert to pandas for easier handling
    similar_pd = similar_items.toPandas()
    
    # Create a DataFrame with similar products
    similar_df = pd.DataFrame([
        {
            'input_product_id': product_id,
            'similar_product_id': product_index_to_id.get(float(row['itemId']), "Unknown"),
            'similarity': float(row['similarity'])
        }
        for _, row in similar_pd.iterrows()
    ])
    
    return similar_df


# print("\nExample: Get similar products")
# # Get the first product ID from the mappings
# example_product_id = product_map[0]['product_id']
# print(f"Getting similar products for: {example_product_id}")
# similar_products = get_similar_products(example_product_id, 5)
# if similar_products is not None:
#     display(similar_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
