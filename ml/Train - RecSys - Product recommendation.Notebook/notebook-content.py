# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
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

import os
os.environ["PYSPARK_PIN_THREAD"] = "false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary libraries
import mlflow
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, SQLTransformer
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator
from datetime import datetime

from whylogs.api.pyspark.experimental import collect_dataset_profile_view
import whylogs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1. Transform and log dataset

# CELL ********************

# Set the experiment for dataset tracking
mlflow.set_experiment("Product_RecSys_Datasets")

# Dataset preparation and tracking
with mlflow.start_run(run_name="recommendation_dataset_preparation"):
    # Generate a dataset version ID
    dataset_version = datetime.now().strftime("%Y%m%d_%H%M%S")
    mlflow.set_tag("dataset_version", dataset_version)
    
    # Load the Orders dataset
    orders_df = spark.table("Gold.Orders")
    
    # For implicit feedback, we need to focus on completed orders
    completed_orders = orders_df.filter(F.col("order_status") == "delivered")
    
    # Calculate max price for normalization (needed for the SQLTransformer)
    max_price = completed_orders.agg(F.max("price")).collect()[0][0]
    
    # Create StringIndexers for customer_id and product_id
    customer_indexer = StringIndexer(inputCol="customer_id", outputCol="customerIndex")
    product_indexer = StringIndexer(inputCol="product_id", outputCol="productIndex")
    
    # Create a SQLTransformer to calculate the preference/rating
    rating_transformer = SQLTransformer().setStatement(
        f"""
        SELECT *, 
        (price / {max_price}) * 10 AS preference
        FROM __THIS__
        """
    )
    
    # Create a SQLTransformer to rename columns for ALS
    als_transformer = SQLTransformer().setStatement(
        """
        SELECT 
            customerIndex AS userId,
            productIndex AS itemId,
            preference AS rating,
            order_purchase_timestamp,
            customer_id,
            product_id
        FROM __THIS__
        """
    )
    
    # Create a pipeline for all data preparation steps
    prep_pipeline = Pipeline(stages=[
        customer_indexer, 
        product_indexer,
        rating_transformer,
        als_transformer
    ])
    
    # Apply the pipeline to the dataframe
    pipeline_model = prep_pipeline.fit(completed_orders)
    als_data = pipeline_model.transform(completed_orders)
    
    # Log dataset statistics
    total_users = als_data.select("userId").distinct().count()
    total_items = als_data.select("itemId").distinct().count()
    total_interactions = als_data.count()
    
    # Calculate sparsity
    total_possible = total_users * total_items
    sparsity = 1.0 - (total_interactions / total_possible)
    
    # Log dataset metrics
    mlflow.log_metrics({
        "total_users": total_users,
        "total_items": total_items,
        "total_interactions": total_interactions,
        "matrix_sparsity": sparsity,
        "avg_interactions_per_user": total_interactions / total_users,
        "avg_interactions_per_item": total_interactions / total_items
    })
    
    # Log distribution metrics
    def log_data_distribution_metrics(df, column_name):
        """Log distribution metrics for numerical columns"""
        stats = df.select(column_name).summary(
            "min", "25%", "50%", "75%", "max", "mean", "stddev"
        ).collect()
        
        metrics = {}
        for row in stats:
            percentile = row[0]
            value = float(row[1]) if row[1] is not None else 0
            metrics[f"{column_name}_{percentile.replace('%', 'p')}"] = value
        
        mlflow.log_metrics(metrics)
    
    log_data_distribution_metrics(als_data, "rating")

    # Profile and log the dataset profile 
    als_data_profile = collect_dataset_profile_view(als_data)
    als_data_profile.writer("mlflow").write()
    
    # Create temporary paths for saving artifacts
    temp_dataset_path = f"/lakehouse/default/Files/tmp/als_data_{dataset_version}.parquet"
    als_data.toPandas().to_parquet(temp_dataset_path)
    mlflow.log_artifact(temp_dataset_path, "dataset")
    
    # Log the transformation pipeline as a model
    mlflow.spark.log_model(pipeline_model, "transformation_pipeline")
    
    # Create mappings
    customer_map = als_data.select("customer_id", "userId").distinct().toPandas().to_dict(orient="records")
    product_map = als_data.select("product_id", "itemId").distinct().toPandas().to_dict(orient="records")
    
    # Log mappings as artifacts
    mlflow.log_dict(customer_map, "customer_mappings.json")
    mlflow.log_dict(product_map, "product_mappings.json")
    
    # Log parameters about the data source
    mlflow.log_params({
        "data_source": "Platinum.Orders",
        "filter_criteria": "order_status = delivered",
        "preference_calculation": "price / max_price * 10"
    })
    
    # Get the run ID for reference in model training
    dataset_run_id = mlflow.active_run().info.run_id
    print(f"Dataset prepared and logged with version: {dataset_version}, run_id: {dataset_run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. Train and validate

# CELL ********************

from pyspark.sql import Window

# Function to split recommendation data for training and testing
def split_recommendation_data(data, test_ratio=0.2):
    """Split recommendation data preserving time-based ordering"""    
    # First sort by timestamp
    sorted_data = data.orderBy("order_purchase_timestamp")
    
    # Add row number for splitting
    sorted_data = sorted_data.withColumn("row_number", F.row_number().over(
        Window.orderBy("order_purchase_timestamp")))
    
    # Calculate cutoff point
    count = sorted_data.count()
    train_cutoff = int(count * (1.0 - test_ratio))
    
    # Create splits
    train = sorted_data.filter(F.col("row_number") <= train_cutoff)
    test = sorted_data.filter(F.col("row_number") > train_cutoff)
    
    # Get min and max timestamps for logging
    train_timestamps = train.agg(F.min("order_purchase_timestamp").alias("min_timestamp"), 
                                F.max("order_purchase_timestamp").alias("max_timestamp")).collect()[0]
    test_timestamps = test.agg(F.min("order_purchase_timestamp").alias("min_timestamp"), 
                              F.max("order_purchase_timestamp").alias("max_timestamp")).collect()[0]
    
    return train, test, {
        "train_min_timestamp": str(train_timestamps["min_timestamp"]),
        "train_max_timestamp": str(train_timestamps["max_timestamp"]),
        "test_min_timestamp": str(test_timestamps["min_timestamp"]),
        "test_max_timestamp": str(test_timestamps["max_timestamp"]),
        "train_size": train.count(),
        "test_size": test.count()
    }

# Function to randomly split recommendation data
def random_split_recommendation_data(data, test_ratio=0.2, seed=42):
    """Split recommendation data randomly while ensuring users appear in both sets"""
    # Perform random split
    train_data, test_data = data.randomSplit([1.0 - test_ratio, test_ratio], seed=seed)
    
    # Get counts for logging
    train_size = train_data.count()
    test_size = test_data.count()
    
    # Count unique users and items in each set
    train_users = train_data.select("userId").distinct().count()
    test_users = test_data.select("userId").distinct().count()
    train_items = train_data.select("itemId").distinct().count()
    test_items = test_data.select("itemId").distinct().count()
    
    # Calculate overlap
    common_users = train_data.select("userId").intersect(test_data.select("userId")).count()
    common_items = train_data.select("itemId").intersect(test_data.select("itemId")).count()
    
    return train_data, test_data, {
        "train_size": train_size,
        "test_size": test_size,
        "train_users": train_users,
        "test_users": test_users,
        "train_items": train_items,
        "test_items": test_items,
        "common_users": common_users,
        "common_items": common_items,
        "user_overlap_percentage": (common_users / test_users) * 100,
        "item_overlap_percentage": (common_items / test_items) * 100
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the latest dataset run ID
client = mlflow.tracking.MlflowClient()
dataset_runs = client.search_runs(
    experiment_ids=[client.get_experiment_by_name("Product_RecSys_Datasets").experiment_id],
    filter_string="tags.mlflow.runName = 'recommendation_dataset_preparation'",
    order_by=["start_time DESC"],
    max_results=1
)

if not dataset_runs:
    raise Exception("No dataset preparation runs found")

dataset_run_id = dataset_runs[0].info.run_id
dataset_version = dataset_runs[0].data.tags.get("dataset_version")
dataset_artifacts_uri = dataset_runs[0].info.artifact_uri
print(f"Using dataset version: {dataset_version}, run_id: {dataset_run_id}")

# Download the dataset artifact
temp_dataset_path = client.download_artifacts(dataset_run_id, "dataset", "/lakehouse/default/Files/tmp")

# Load the dataset
als_data_path = f"{temp_dataset_path}/als_data_{dataset_version}.parquet"
als_data = spark.createDataFrame(pd.read_parquet(als_data_path))

# Load the customer and product mappings
customer_map = mlflow.artifacts.load_dict(f"runs:/{dataset_run_id}/customer_mappings.json")
product_map = mlflow.artifacts.load_dict(f"runs:/{dataset_run_id}/product_mappings.json")

# # Load the transformation pipeline instead of the dataset
# pipeline_model_uri = f"runs:/{dataset_run_id}/transformation_pipeline"
# pipeline_model = mlflow.spark.load_model(pipeline_model_uri)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import mlflow

mlflow.set_experiment("Product_RecSys_Training")

# Function to evaluate regression metrics
def evaluate_regression_metrics(predictions_df):
    """Calculate regression metrics (RMSE, MAE) for predictions"""
    metrics = {}
    
    # RMSE
    rmse_evaluator = RegressionEvaluator(
        metricName="rmse", 
        labelCol="rating", 
        predictionCol="prediction"
    )
    metrics["rmse"] = rmse_evaluator.evaluate(predictions_df)
    
    # MAE
    mae_evaluator = RegressionEvaluator(
        metricName="mae", 
        labelCol="rating", 
        predictionCol="prediction"
    )
    metrics["mae"] = mae_evaluator.evaluate(predictions_df)
    
    return metrics

# Function to prepare data for ranking metrics
def prepare_ranking_data(model, test_data, k_values=[1, 5, 10]):
    """Prepare data for ranking evaluation"""
    # Get all unique users in test set
    users = test_data.select("userId").distinct()
    
    # For each user, get their actual items from test data
    # Group by userId and collect itemIds into arrays
    # Cast itemIds to double for compatibility with RankingEvaluator
    actual = test_data.groupBy("userId") \
        .agg(F.collect_list(F.col("itemId").cast("double")).alias("actual"))
    
    # Generate top-k recommendations for each user
    max_k = max(k_values)
    
    # Generate recommendations for all users
    recommendations = model.recommendForUserSubset(users, max_k)
    
    # Extract recommended items and cast to double
    predicted = recommendations.select(
        "userId",
        F.expr("transform(recommendations.itemId, x -> cast(x as double))").alias("predicted")
    )
    
    # Join actual and predicted
    return actual.join(predicted, on="userId")

# Function to evaluate ranking metrics
def evaluate_ranking_metrics(ranking_data, k_values=[1, 5, 10]):
    """Calculate ranking metrics (MAP, NDCG) at different K values"""
    metrics = {}
    
    for k in k_values:
        # Limit predictions to top-k
        ranking_data_k = ranking_data.withColumn(
            "predicted_k", 
            F.expr(f"slice(predicted, 1, {k})")
        )
        
        # MAP@k - use MAP_k as the metric name for MLflow compatibility
        map_evaluator = RankingEvaluator(
            metricName="meanAveragePrecision",
            labelCol="actual",
            predictionCol="predicted_k"
        )
        metrics[f"MAP_k{k}"] = map_evaluator.evaluate(ranking_data_k)
        
        # NDCG@k - use NDCG_k as the metric name for MLflow compatibility
        ndcg_evaluator = RankingEvaluator(
            metricName="ndcgAtK",
            labelCol="actual",
            predictionCol="predicted_k",
            k=k
        )
        metrics[f"NDCG_k{k}"] = ndcg_evaluator.evaluate(ranking_data_k)
    
    return metrics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import mlflow

mlflow.set_experiment("Product_RecSys_Training")

# Model evaluation run
with mlflow.start_run(run_name="als_model_evaluation"):
    # Link to the dataset
    mlflow.set_tag("dataset_version", dataset_version)
    mlflow.set_tag("dataset_run_id", dataset_run_id)
    
    # Split data for evaluation
    train_data, test_data, split_metrics = random_split_recommendation_data(als_data, test_ratio=0.2)
    
    # Log split information
    mlflow.log_params({
        "split_method": "random",
        "test_ratio": 0.2,
        "random_seed": 42,
        **split_metrics
    })
    
    # Define hyperparameters for ALS model based on EDA
    als_params = {
        "rank": 50,
        "maxIter": 10,
        "regParam": 0.1,
        "implicitPrefs": True,
        "alpha": 1.0,
        "seed": 42  # Add seed for reproducibility
    }

        
    # define "poor" hyperparameters
    als_params = {
        "rank": 2,
        "maxIter": 10,
        "regParam": 20,
        "implicitPrefs": True,
        "alpha": 1.0,
        "seed": 42  # Add seed for reproducibility
    }
    
    # Initialize ALS model
    als = ALS(
        userCol="userId",
        itemCol="itemId",
        ratingCol="rating",
        coldStartStrategy="drop",
        **als_params
    )
    
    # Log model parameters
    mlflow.log_params(als_params)
    
    # Train the model on training data
    print("Training ALS model for evaluation...")
    model = als.fit(train_data)
    
    # Make predictions on test data for regression metrics
    print("Generating predictions for regression metrics...")
    predictions = model.transform(test_data)
    
    # Calculate regression metrics
    print("Evaluating regression metrics...")
    regression_metrics = evaluate_regression_metrics(predictions)
    
    # Prepare data for ranking metrics
    print("Preparing data for ranking metrics...")
    k_values = [1, 5, 10]
    ranking_data = prepare_ranking_data(model, test_data, k_values)
    
    # Calculate ranking metrics
    print("Evaluating ranking metrics...")
    ranking_metrics = evaluate_ranking_metrics(ranking_data, k_values)
    
    # Combine all metrics
    all_metrics = {**regression_metrics, **ranking_metrics}
    
    # Log all evaluation metrics
    mlflow.log_metrics(all_metrics)
    
    # Log the evaluation model to MLflow
    mlflow.spark.log_model(model, "als_evaluation_model")
    
    # Print evaluation results
    print("Model evaluation complete:")
    print(f"  RMSE: {regression_metrics['rmse']:.4f}")
    print(f"  MAE: {regression_metrics['mae']:.4f}")
    for k in k_values:
        print(f"  MAP@{k}: {ranking_metrics[f'MAP_k{k}']:.4f}")
        print(f"  NDCG@{k}: {ranking_metrics[f'NDCG_k{k}']:.4f}")
    
    # Get the evaluation run ID
    eval_run_id = mlflow.active_run().info.run_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3. Train the final model

# CELL ********************

# Train the final model on the full dataset
with mlflow.start_run(run_name="als_final_model"):
    # Link to the dataset and evaluation run
    mlflow.set_tag("dataset_version", dataset_version)
    mlflow.set_tag("dataset_run_id", dataset_run_id)
    mlflow.set_tag("evaluation_run_id", eval_run_id)
    
    # Use the same hyperparameters as in evaluation
    final_als = ALS(
        userCol="userId",
        itemCol="itemId",
        ratingCol="rating",
        coldStartStrategy="drop",
        **als_params
    )
    
    # Log model parameters
    mlflow.log_params(als_params)
    
    # Train on the full dataset
    print("Training final model on complete dataset...")
    final_model = final_als.fit(als_data)
    
    # Log the final model to MLflow
    mlflow.spark.log_model(final_model, "als_final_model")
    
    # Get the final model run ID
    final_model_run_id = mlflow.active_run().info.run_id
    
    print(f"Final model trained and logged to MLflow. Run ID: {final_model_run_id}")
    
    # Register the model in the MLflow Model Registry
    model_name = "ProductRecommender"
    model_version = mlflow.register_model(
        f"runs:/{final_model_run_id}/als_final_model",
        model_name
    )
    
    # Add tags to the registered model version
    client = mlflow.tracking.MlflowClient()
    client.set_model_version_tag(model_name, model_version.version, "dataset_version", dataset_version)
    client.set_model_version_tag(model_name, model_version.version, "dataset_run_id", dataset_run_id)
    client.set_model_version_tag(model_name, model_version.version, "evaluation_run_id", eval_run_id)
    client.set_model_version_tag(model_name, model_version.version, "final_model_run_id", final_model_run_id)
    
    # Add description with key information
    description = f"""
    Product Recommendation Model (ALS)
    
    Dataset:
    - Version: {dataset_version}
    - Run ID: {dataset_run_id}
    
    Evaluation:
    - Run ID: {eval_run_id}
    
    Final Model:
    - Run ID: {final_model_run_id}
    
    Parameters:
    - Rank: {als_params['rank']}
    - Iterations: {als_params['maxIter']}
    - Regularization: {als_params['regParam']}
    - Alpha: {als_params['alpha']}
    """
    
    client.update_model_version(
        name=model_name,
        version=model_version.version,
        description=description
    )
    
    print(f"Model registered as '{model_name}' version {model_version.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
