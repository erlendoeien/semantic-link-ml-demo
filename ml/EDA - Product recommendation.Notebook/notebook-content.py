# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name": "Gold"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load the dataset

# CELL ********************

# Load the Orders dataset
orders_df = spark.table("Gold.Orders")

# Display the schema of our dataframe
print("Orders DataFrame Schema:")
orders_df.printSchema()

# Show a sample of the data
print("\nOrders Sample:")
display(orders_df)

# Basic statistics about orders
print("\nOrder Price Summary Statistics:")
display(orders_df.describe(["price", "freight_value"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explorative data analysis
# ## Basic statistics about ratings

# CELL ********************

# Count unique customers, products, and orders
unique_customers = orders_df.select("customer_id").distinct().count()
unique_products = orders_df.select("product_id").distinct().count()
unique_orders = orders_df.select("order_id").distinct().count()
total_order_items = orders_df.count()

print(f"Number of unique customers: {unique_customers}")
print(f"Number of unique products: {unique_products}")
print(f"Number of unique orders: {unique_orders}")
print(f"Total number of order items: {total_order_items}")
print(f"Average items per order: {total_order_items / unique_orders:.2f}")

# Order status distribution
status_distribution = orders_df.groupBy("order_status").count().orderBy("count", ascending=False)
print("\nOrder Status Distribution:")
display(status_distribution)

# Product purchase frequency
product_frequency = orders_df.groupBy("product_id").count().orderBy("count", ascending=False)
print("\nMost Frequently Purchased Products:")
display(product_frequency)

# Customer purchase frequency
customer_frequency = orders_df.groupBy("customer_id").count().orderBy("count", ascending=False)
print("\nCustomers with Most Purchases:")
display(customer_frequency)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Long-tail analysis
# - all customers have 1 order and only 1

# CELL ********************

# Analyze long-tail distribution of orders per customer
customer_order_counts = orders_df.groupBy("customer_id").agg(
    F.countDistinct("order_id").alias("order_count")
)

# Convert to pandas for visualization
customer_orders_pd = customer_order_counts.toPandas()

# Plot distribution of orders per customer
plt.figure(figsize=(12, 6))
plt.hist(customer_orders_pd["order_count"], bins=30, log=True)
plt.title("Long-tail Distribution of Orders per Customer")
plt.xlabel("Number of Orders")
plt.ylabel("Number of Customers (log scale)")
plt.grid(True, alpha=0.3)
plt.show()

# Calculate percentages
total_customers = customer_orders_pd.shape[0]
single_order_customers = customer_orders_pd[customer_orders_pd["order_count"] == 1].shape[0]
percentage_single_order = (single_order_customers / total_customers) * 100

print(f"Percentage of customers with only one order: {percentage_single_order:.2f}%")

# Analyze items per order
items_per_order = orders_df.groupBy("order_id").count().withColumnRenamed("count", "items_count")
items_per_order_pd = items_per_order.toPandas()

plt.figure(figsize=(12, 6))
plt.hist(items_per_order_pd["items_count"], bins=20, log=True)
plt.title("Distribution of Number of Items per Order")
plt.xlabel("Number of Items")
plt.ylabel("Number of Orders (log scale)")
plt.grid(True, alpha=0.3)
plt.show()

# Calculate percentage of orders with only one item
total_orders = items_per_order_pd.shape[0]
single_item_orders = items_per_order_pd[items_per_order_pd["items_count"] == 1].shape[0]
percentage_single_item = (single_item_orders / total_orders) * 100

print(f"Percentage of orders with only one item: {percentage_single_item:.2f}%")

# Analyze product popularity (long-tail)
product_popularity = orders_df.groupBy("product_id").count().orderBy("count", ascending=False)
product_popularity_pd = product_popularity.toPandas()

# Calculate cumulative percentage of orders
product_popularity_pd["cumulative_count"] = product_popularity_pd["count"].cumsum()
total_item_count = product_popularity_pd["count"].sum()
product_popularity_pd["cumulative_percentage"] = (product_popularity_pd["cumulative_count"] / total_item_count) * 100

# Plot long-tail distribution of product popularity
plt.figure(figsize=(12, 6))
plt.plot(range(len(product_popularity_pd)), product_popularity_pd["cumulative_percentage"])
plt.title("Long-tail Distribution of Product Popularity")
plt.xlabel("Number of Products (Ranked by Popularity)")
plt.ylabel("Cumulative Percentage of Orders")
plt.axhline(y=80, color='r', linestyle='--', alpha=0.7)
plt.grid(True, alpha=0.3)
plt.show()

# Find how many products account for 80% of orders
products_80_percent = product_popularity_pd[product_popularity_pd["cumulative_percentage"] <= 80].shape[0]
percentage_of_catalog = (products_80_percent / unique_products) * 100

print(f"Approximately {products_80_percent} products ({percentage_of_catalog:.2f}% of catalog) account for 80% of all orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data preparation

# CELL ********************

# For implicit feedback, we need to focus on completed orders
completed_orders = orders_df.filter(F.col("order_status") == "delivered")

# Create StringIndexers for customer_id and product_id
customer_indexer = StringIndexer(inputCol="customer_id", outputCol="customerIndex")
product_indexer = StringIndexer(inputCol="product_id", outputCol="productIndex")

# Create a pipeline for data preparation
prep_pipeline = Pipeline(stages=[customer_indexer, product_indexer])

# Create a numeric value representing "implicit preference" strength:
# 1. Normalize price to be in a reasonable range
# 2. We can also incorporate frequency by counting repeat purchases
max_price = completed_orders.agg(F.max("price")).collect()[0][0]

# Calculate implicit preference value (can be customized based on business logic)
implicit_df = completed_orders.withColumn(
    "preference", 
    # Using price as an indicator of preference strength, can be adjusted
    (F.col("price") / max_price) * 10  
)

# Apply the pipeline to the dataframe
pipeline_model = prep_pipeline.fit(implicit_df)
prepared_df = pipeline_model.transform(implicit_df)

# Select the columns needed for ALS
als_data = prepared_df.select(
    F.col("customerIndex").alias("userId"),  
    F.col("productIndex").alias("itemId"),   
    F.col("preference").alias("rating")      
)

print("Prepared data for ALS:")
als_data.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data split

# CELL ********************

# Split the data into training and test sets
# Using a common 80-20 split
(training_data, test_data) = als_data.randomSplit([0.8, 0.2], seed=42)

print(f"Training data size: {training_data.count()}")
print(f"Test data size: {test_data.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Training, predicting and evaluating

# CELL ********************

# Initialize the ALS model specifically for implicit feedback
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="userId",
    itemCol="itemId",
    ratingCol="rating",
    coldStartStrategy="drop", # Handles users/items with no ratings
    implicitPrefs=True,  # Important for implicit feedback data
    alpha=1.0,  # Confidence multiplier for implicit feedback
    nonnegative=True ## Forces latent factors to be non-negative
)

# Create a Pipeline with ALS
model_pipeline = Pipeline(stages=[als])

# Train the model
model = model_pipeline.fit(training_data)

# Access the ALS model from the pipeline
als_model = model.stages[-1]

# Information about the model
print(f"Rank: {als_model.rank}")
print(f"Max Iterations: {als_model.getMaxIter()}")
print(f"Regularization Parameter: {als_model.getRegParam()}")
print(f"Alpha (confidence multiplier): {als_model.getAlpha()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test predictions

# CELL ********************

# For each user, recommend top K products
K = 10
recommendations = als_model.recommendForAllUsers(K)

# Show sample recommendations
print("Sample product recommendations for users:")
display(recommendations.select("userId", "recommendations"))

# Extract product IDs and scores from recommendations
user_recs_exploded = recommendations.select(
    "userId",
    F.explode("recommendations").alias("recommendation")
)

user_recs_exploded = user_recs_exploded.select(
    "userId",
    F.col("recommendation.itemId").alias("itemId"),
    F.col("recommendation.rating").alias("score")
)

# Map back to original customer_id and product_id
customer_map = prepared_df.select("customerIndex", "customer_id").distinct()
product_map = prepared_df.select("productIndex", "product_id").distinct()

# Join with mappings to get original IDs
recs_with_customer = user_recs_exploded.join(
    customer_map, 
    user_recs_exploded.userId == customer_map.customerIndex
)

recs_with_product = recs_with_customer.join(
    product_map,
    recs_with_customer.itemId == product_map.productIndex
)

# Final recommendations with original IDs
final_recs = recs_with_product.select(
    "customer_id", "product_id", "score"
).orderBy("customer_id", F.desc("score"))

print("Final recommendations with original IDs:")
display(final_recs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Evaluation

# CELL ********************

# For implicit feedback models, traditional RMSE isn't the best metric
# Instead, we'll use ranking metrics like Mean Average Precision (MAP)

# Prepare test data for evaluation
test_users = test_data.select("userId").distinct()
actual_items = test_data.groupBy("userId").agg(
    F.collect_list(F.col("itemId").cast("double")).alias("actualItems")  # Cast to double
)

# Get recommendations for test users
user_subset = test_users.limit(100)  # Limit for efficiency
test_recommendations = als_model.recommendForUserSubset(user_subset, K)

# Format recommendations for evaluation - cast itemIds to double
predicted_items = test_recommendations.select(
    "userId",
    F.expr("transform(recommendations.itemId, x -> CAST(x AS DOUBLE))").alias("predictedItems")
)

# Join actual and predicted items
evaluation_data = actual_items.join(predicted_items, "userId")

# Define a ranking evaluator
evaluator = RankingEvaluator(
    metricName="meanAveragePrecision",
    labelCol="actualItems",
    predictionCol="predictedItems"
)

# Calculate MAP
map_score = evaluator.evaluate(evaluation_data)
print(f"Mean Average Precision: {map_score:.4f}")

# Calculate NDCG@K
ndcg_evaluator = RankingEvaluator(
    metricName="ndcgAtK",
    labelCol="actualItems",
    predictionCol="predictedItems",
    k=10
)
ndcg = ndcg_evaluator.evaluate(evaluation_data)
print(f"NDCG@10: {ndcg:.4f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Hyperparameter search

# CELL ********************

# Hyperparameter Tuning for ALS Model with FLAML and MLflow
import mlflow
import mlflow.spark
from flaml import tune
from flaml.visualization import (
    plot_optimization_history, 
    plot_parallel_coordinate,
    plot_param_importance
)
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RankingEvaluator
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import pandas as pd
import time
import os

# Set up MLflow tracking
mlflow_experiment_name = "ALS_Recommendation_FLAML_Tuning"
mlflow.set_experiment(mlflow_experiment_name)

# Get dataset information for logging
unique_customers = training_data.select("userId").distinct().count()
unique_products = training_data.select("itemId").distinct().count()
total_interactions = training_data.count()

print(f"MLflow tracking experiment: {mlflow_experiment_name}")
print("Starting hyperparameter search with FLAML...")
start_time = time.time()


# Modified FLAML evaluation function to use ranking metrics
def evaluate_als_config(config):
    """Evaluate an ALS configuration using ranking metrics"""
    # Extract parameters from config
    rank = config["rank"]
    reg_param = config["regParam"]
    alpha = config["alpha"]
    max_iter = config["maxIter"]
    
    # Log run start
    run_start = time.time()
    
    # Initialize ALS model with the config
    als = ALS(
        rank=rank,
        regParam=reg_param,
        alpha=alpha,
        maxIter=max_iter,
        userCol="userId",
        itemCol="itemId",
        ratingCol="rating",
        coldStartStrategy="drop",
        implicitPrefs=True,
        nonnegative=True
    )
    
    try:
        # Fit the model
        als_model = als.fit(training_data)
        print("Fitted model")
        
        # Prepare test data for ranking evaluation
        test_users = test_data.select("userId").distinct()
        actual_items = test_data.groupBy("userId").agg(
            F.collect_list(F.col("itemId").cast("double")).alias("actualItems")
        )
        
        # Sample users for efficiency
        user_subset = test_users.limit(1000)
        
        # Get recommendations
        K = 10  # Number of recommendations per user
        recommendations = als_model.recommendForUserSubset(user_subset, K)
        
        # Format recommendations for evaluation
        predicted_items = recommendations.select(
            "userId",
            F.expr("transform(recommendations.itemId, x -> CAST(x AS DOUBLE))").alias("predictedItems")
        )
        
        # Join actual and predicted items
        evaluation_data = actual_items.join(predicted_items, "userId")
        
        # Calculate ranking metrics
        rank_evaluator = RankingEvaluator(
            metricName="meanAveragePrecision",
            labelCol="actualItems",
            predictionCol="predictedItems"
        )
        map_score = rank_evaluator.evaluate(evaluation_data)
        
        ndcg_evaluator = RankingEvaluator(
            metricName="ndcgAtK",
            labelCol="actualItems",
            predictionCol="predictedItems",
            k=10
        )
        ndcg = ndcg_evaluator.evaluate(evaluation_data)
        
        # Also calculate RMSE for reference (optional)
        rmse_predictions = als_model.transform(test_data).na.drop()
        rmse_evaluator = RegressionEvaluator(
            metricName="rmse", 
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = rmse_evaluator.evaluate(rmse_predictions)
        
        # Training time
        train_time = time.time() - run_start
        
        # Return metrics (FLAML minimizes the first metric by default)
        return {
            "map": map_score,         
            "ndcg": ndcg,
            "rmse": rmse,
            "train_time": train_time,
            "rank": rank,
            "regParam": reg_param,
            "alpha": alpha,
            "maxIter": max_iter
        }
    
    except Exception as e:
        # Handle any exceptions during model training
        print(f"Error during model evaluation: {e}")
        # Return a low MAP score to indicate this configuration failed
        return {
            "map": 0.0,
            "ndcg": 0.0,
            "rmse": float('inf'),
            "train_time": time.time() - run_start,
            "error": str(e)
        }

# Define the search space for FLAML
search_space = {
    "rank": tune.lograndint(lower=5, upper=200),
    "regParam": tune.loguniform(lower=0.001, upper=10.0),
    "alpha": tune.loguniform(lower=0.01, upper=100.0),
    "maxIter": tune.randint(lower=5, upper=30)
}

# Create a sample subset for faster tuning
# sample_fraction = 0.3
# sampled_training_data = training_data.sample(fraction=sample_fraction, seed=42)
# print(f"Using {sampled_training_data.count()} samples for tuning (vs {training_data.count()} total)")

# Start overall MLflow run
with mlflow.start_run(run_name="flaml_als_tuning", nested=True):
    # Log dataset info
    mlflow.log_param("dataset_size", training_data.count())
    mlflow.log_param("sampled_size", sampled_training_data.count())
    mlflow.log_param("unique_users", unique_customers)
    mlflow.log_param("unique_items", unique_products)
    mlflow.log_param("sampling_fraction", sample_fraction)
    
    # Run hyperparameter tuning with FLAML
    analysis = tune.run(
        evaluate_als_config,
        config=search_space,
        metric="rmse",
        mode="min",
        num_samples=10,  # Maximum number of trials
        time_budget_s=60*5,  # Time budget in seconds (1 hour)
        local_dir="./flaml_results",  # Directory for storing results
        verbose=2  # 0 = silent, 1 = only status updates, 2 = details
    )
    
    # Get best configuration and results
    best_config = analysis.best_config
    best_result = analysis.best_result
    
    # Log best configuration and results
    mlflow.log_params(best_config)
    mlflow.log_metrics({
        "best_rmse": best_result["rmse"],
        "best_map": best_result["map"],
        "best_train_time": best_result["train_time"]
    })
    
    # Time information
    total_time = time.time() - start_time
    mlflow.log_metric("total_tuning_time", total_time)
    
    # Print results
    print("\nFLAML tuning completed!")
    print(f"Total time: {total_time/60:.2f} minutes")
    print("\nBest configuration:")
    for param, value in best_config.items():
        print(f"  {param}: {value}")
    print(f"\nBest RMSE: {best_result['rmse']:.4f}")
    print(f"Best MAP: {best_result['map']:.4f}")
    
    # Create and log visualizations
    # 1. Optimization history
    plt.figure(figsize=(10, 6))
    plot_optimization_history(analysis)
    plt.title("FLAML Optimization History")
    plt.tight_layout()
    history_path = "optimization_history.png"
    plt.savefig(history_path)
    mlflow.log_artifact(history_path)
    plt.close()
    
    # 2. Parallel coordinates plot
    plt.figure(figsize=(12, 8))
    plot_parallel_coordinate(analysis, ["rank", "regParam", "alpha", "maxIter"])
    plt.title("Parallel Coordinates Plot")
    plt.tight_layout()
    parallel_path = "parallel_coordinates.png"
    plt.savefig(parallel_path)
    mlflow.log_artifact(parallel_path)
    plt.close()
    
    # 3. Parameter importance
    plt.figure(figsize=(10, 6))
    plot_param_importance(analysis)
    plt.title("Hyperparameter Importance")
    plt.tight_layout()
    importance_path = "parameter_importance.png"
    plt.savefig(importance_path)
    mlflow.log_artifact(importance_path)
    plt.close()
    
    # Save all results as CSV
    all_results = pd.DataFrame([trial.last_result for trial in analysis.trials])
    results_path = "all_trial_results.csv"
    all_results.to_csv(results_path, index=False)
    mlflow.log_artifact(results_path)
    
    # # Train the final model with the best configuration
    # print("\nTraining final model with best configuration...")
    # best_als = ALS(
    #     rank=best_config["rank"],
    #     regParam=best_config["regParam"],
    #     alpha=best_config["alpha"],
    #     maxIter=best_config["maxIter"],
    #     userCol="userId",
    #     itemCol="itemId",
    #     ratingCol="rating",
    #     coldStartStrategy="drop",
    #     implicitPrefs=True,
    #     nonnegative=True
    # )
    
    # best_pipeline = Pipeline(stages=[best_als])
    # best_model = best_pipeline.fit(training_data)
    
    # # Log the best model
    # mlflow.spark.log_model(best_model, "best_model")
    
    # # Make predictions with the best model
    # best_predictions = best_model.transform(test_data).na.drop()
    # final_rmse = evaluator.evaluate(best_predictions)
    # final_mae = mae_evaluator.evaluate(best_predictions)
    
    # mlflow.log_metric("final_rmse", final_rmse)
    # mlflow.log_metric("final_mae", final_mae)
    
    # print(f"Final model RMSE on full test set: {final_rmse:.4f}")
    # print(f"Final model MAE on full test set: {final_mae:.4f}")
    
    # # Generate recommendations
    # best_als_model = best_model.stages[-1]
    # recommendations = best_als_model.recommendForAllUsers(10)
    
    # # Show sample recommendations
    # print("\nSample recommendations from best model:")
    # recommendations.limit(3).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to analyze cold start users with the best model
def analyze_cold_start_with_best_model(best_model, cold_start_threshold=3):
    """Analyze how well the tuned model performs on cold start users"""
    with mlflow.start_run(run_name="cold_start_analysis"):
        # Get best ALS model
        best_als_model = best_model.stages[-1]
        
        # Identify cold start users
        user_activity = training_data.groupBy("userId").count()
        cold_start_users = user_activity.filter(F.col("count") <= cold_start_threshold)
        cold_start_user_ids = [row["userId"] for row in cold_start_users.collect()]
        
        mlflow.log_param("cold_start_threshold", cold_start_threshold)
        mlflow.log_param("num_cold_start_users", len(cold_start_user_ids))
        
        if cold_start_user_ids:
            # Get predictions for cold start users
            cold_start_test = test_data.filter(F.col("userId").isin(cold_start_user_ids))
            
            if cold_start_test.count() > 0:
                cold_start_predictions = best_model.transform(cold_start_test).na.drop()
                
                # Evaluate performance on cold start users
                cold_rmse = evaluator.evaluate(cold_start_predictions)
                cold_mae = mae_evaluator.evaluate(cold_start_predictions)
                
                # Compare with non-cold start users
                non_cold_start_test = test_data.filter(~F.col("userId").isin(cold_start_user_ids))
                non_cold_predictions = best_model.transform(non_cold_start_test).na.drop()
                non_cold_rmse = evaluator.evaluate(non_cold_predictions)
                non_cold_mae = mae_evaluator.evaluate(non_cold_predictions)
                
                # Log metrics
                mlflow.log_metric("cold_start_rmse", cold_rmse)
                mlflow.log_metric("cold_start_mae", cold_mae)
                mlflow.log_metric("non_cold_start_rmse", non_cold_rmse)
                mlflow.log_metric("non_cold_start_mae", non_cold_mae)
                mlflow.log_metric("rmse_difference", cold_rmse - non_cold_rmse)
                
                # Visualize comparison
                plt.figure(figsize=(10, 6))
                data = {
                    'User Type': ['Cold Start', 'Regular'],
                    'RMSE': [cold_rmse, non_cold_rmse],
                    'MAE': [cold_mae, non_cold_mae]
                }
                df = pd.DataFrame(data)
                
                x = np.arange(len(df['User Type']))
                width = 0.35
                
                fig, ax = plt.subplots(figsize=(10, 6))
                rects1 = ax.bar(x - width/2, df['RMSE'], width, label='RMSE')
                rects2 = ax.bar(x + width/2, df['MAE'], width, label='MAE')
                
                ax.set_ylabel('Error')
                ax.set_title('Model Performance: Cold Start vs Regular Users')
                ax.set_xticks(x)
                ax.set_xticklabels(df['User Type'])
                ax.legend()
                
                fig.tight_layout()
                
                # Save and log figure
                cold_start_plot = "cold_start_comparison.png"
                plt.savefig(cold_start_plot)
                mlflow.log_artifact(cold_start_plot)
                plt.close()
                
                print(f"\nCold Start Analysis:")
                print(f"RMSE for cold start users: {cold_rmse:.4f}")
                print(f"RMSE for regular users: {non_cold_rmse:.4f}")
                print(f"Performance gap: {cold_rmse - non_cold_rmse:.4f}")
                
                return cold_rmse, non_cold_rmse
            else:
                print("No cold start users found in test set")
                return None, None
        else:
            print("No cold start users found")
            return None, None

# Uncomment to run cold start analysis
# cold_start_rmse, regular_rmse = analyze_cold_start_with_best_model(best_model, cold_start_threshold=3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a parameter grid for tuning
param_grid = ParamGridBuilder() \
    .addGrid(als.rank, [5, 10, 15]) \
    .addGrid(als.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(als.maxIter, [5, 10]) \
    .build()

# Define cross-validator
cross_validator = CrossValidator(
    estimator=model_pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3
)

# Run cross-validation
cv_model = cross_validator.fit(training_data)

# Get the best model
best_model = cv_model.bestModel
best_als = best_model.stages[-1]

print("Best ALS model parameters:")
print(f"Rank: {best_als.rank}")
print(f"Regularization Parameter: {best_als.getRegParam()}")
print(f"Max Iterations: {best_als.getMaxIter()}")

# Make predictions with the best model
best_predictions = best_model.transform(test_data).na.drop()
best_rmse = evaluator.evaluate(best_predictions)
print(f"RMSE with best model: {best_rmse:.3f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cold start analysis

# CELL ********************

# Cold Start Analysis
# 1. Define cold start users and items
cold_start_threshold_users = 3  # Users with fewer than this many orders
cold_start_threshold_items = 5  # Items with fewer than this many orders

# Identify cold start users
user_activity = orders_df.groupBy("customer_id").count()
cold_start_users = user_activity.filter(F.col("count") < cold_start_threshold_users)
cold_start_user_ids = [row["customer_id"] for row in cold_start_users.collect()]

# Identify cold start items
item_popularity = orders_df.groupBy("product_id").count()
cold_start_items = item_popularity.filter(F.col("count") < cold_start_threshold_items)
cold_start_item_ids = [row["product_id"] for row in cold_start_items.collect()]

print(f"Number of cold start users: {cold_start_users.count()} ({cold_start_users.count() / unique_customers * 100:.2f}%)")
print(f"Number of cold start items: {cold_start_items.count()} ({cold_start_items.count() / unique_products * 100:.2f}%)")

# 2. Analyze recommendations for cold start users
# First convert customer_id to userId (index) for the cold start users
cold_start_user_indices = prepared_df.filter(
    F.col("customer_id").isin(cold_start_user_ids)
).select("customerIndex").distinct()

cold_start_user_indices_list = [row["customerIndex"] for row in cold_start_user_indices.collect()]

# Get recommendations for cold start users (if they exist in the training data)
if cold_start_user_indices_list:
    cold_start_recs = recommendations.filter(
        F.col("userId").isin(cold_start_user_indices_list)
    )
    
    print(f"Number of cold start users with recommendations: {cold_start_recs.count()}")
    
    # Join with original IDs for better readability
    cold_start_recs_exploded = cold_start_recs.select(
        "userId",
        F.explode("recommendations").alias("recommendation")
    ).select(
        "userId",
        F.col("recommendation.itemId").alias("itemId"),
        F.col("recommendation.rating").alias("score")
    )
    
    # Join with customer and product maps
    cold_start_with_ids = cold_start_recs_exploded.join(
        customer_map,
        cold_start_recs_exploded.userId == customer_map.customerIndex
    ).join(
        product_map,
        cold_start_recs_exploded.itemId == product_map.productIndex
    ).select("customer_id", "product_id", "score")
    
    # Show some recommendations for cold start users
    print("Sample recommendations for cold start users:")
    cold_start_with_ids.show(10)
    
    # Compare recommendation diversity for cold vs. non-cold start users
    # Get recommendations for non-cold start users
    non_cold_start_recs = recommendations.filter(
        ~F.col("userId").isin(cold_start_user_indices_list)
    )
    
    # Calculate unique items recommended per user
    cold_unique_items = cold_start_recs_exploded.groupBy("userId").agg(
        F.countDistinct("itemId").alias("unique_items")
    )
    
    non_cold_unique_items = non_cold_start_recs.select(
        "userId",
        F.explode("recommendations").alias("recommendation")
    ).select(
        "userId",
        F.col("recommendation.itemId").alias("itemId")
    ).groupBy("userId").agg(
        F.countDistinct("itemId").alias("unique_items")
    )
    
    # Calculate average unique items per user group
    avg_unique_cold = cold_unique_items.agg(F.avg("unique_items")).collect()[0][0]
    avg_unique_non_cold = non_cold_unique_items.agg(F.avg("unique_items")).collect()[0][0]
    
    print(f"Average unique items recommended for cold start users: {avg_unique_cold:.2f}")
    print(f"Average unique items recommended for non-cold start users: {avg_unique_non_cold:.2f}")
else:
    print("No cold start users found in the recommendation set.")

# 3. Analyze coverage of cold start items in recommendations
# Get all recommended items
all_recommended_items = recommendations.select(
    F.explode("recommendations").alias("recommendation")
).select(
    F.col("recommendation.itemId").alias("itemId")
).distinct()

# Convert cold start item IDs to indices
cold_start_item_indices = prepared_df.filter(
    F.col("product_id").isin(cold_start_item_ids)
).select("productIndex").distinct()

cold_start_item_indices_list = [row["productIndex"] for row in cold_start_item_indices.collect()]

# Count cold start items in recommendations
cold_start_items_recommended = all_recommended_items.filter(
    F.col("itemId").isin(cold_start_item_indices_list)
).count()

cold_start_items_coverage = (cold_start_items_recommended / len(cold_start_item_indices_list)) * 100 if cold_start_item_indices_list else 0

print(f"Cold start items coverage in recommendations: {cold_start_items_recommended} out of {len(cold_start_item_indices_list)} ({cold_start_items_coverage:.2f}%)")

# 4. Visualize cold start analysis
# Compare scores of recommendations for cold start vs. non-cold start users
if cold_start_user_indices_list:
    # Get scores for cold start recommendations
    cold_start_scores = cold_start_recs_exploded.select("score").toPandas()["score"]
    
    # Get scores for non-cold start recommendations
    non_cold_start_scores = non_cold_start_recs.select(
        F.explode("recommendations").alias("recommendation")
    ).select(
        F.col("recommendation.rating").alias("score")
    ).toPandas()["score"]
    
    # Create a combined dataframe for comparison
    cold_start_df = pd.DataFrame({
        'score': cold_start_scores,
        'user_type': 'Cold Start'
    })
    
    non_cold_start_df = pd.DataFrame({
        'score': non_cold_start_scores,
        'user_type': 'Regular'
    })
    
    combined_df = pd.concat([cold_start_df, non_cold_start_df])
    
    # Plot score distributions
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='user_type', y='score', data=combined_df)
    plt.title('Recommendation Score Distribution: Cold Start vs. Regular Users')
    plt.xlabel('User Type')
    plt.ylabel('Recommendation Score')
    plt.grid(True, alpha=0.3)
    plt.show()
    
    # Plot score histograms
    plt.figure(figsize=(12, 6))
    sns.histplot(data=combined_df, x='score', hue='user_type', element='step', stat='density', common_norm=False)
    plt.title('Score Distribution Comparison')
    plt.xlabel('Recommendation Score')
    plt.ylabel('Density')
    plt.grid(True, alpha=0.3)
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Attempt to improve cold start

# CELL ********************

# Enhanced ALS model with strategies for cold start problems
# Let's try a different approach by incorporating side features

# 1. Calculate average price per customer and product
customer_avg_price = completed_orders.groupBy("customer_id").agg(
    F.avg("price").alias("customer_avg_price")
)

product_avg_price = completed_orders.groupBy("product_id").agg(
    F.avg("price").alias("product_avg_price")
)

# 2. Join these with our prepared dataset to create side features
enhanced_prepared_df = prepared_df.join(
    customer_avg_price, 
    prepared_df.customer_id == customer_avg_price.customer_id
).join(
    product_avg_price,
    prepared_df.product_id == product_avg_price.product_id
)

# 3. Create additional features for the model
# Normalize price ranges to 0-1
max_avg_price = enhanced_prepared_df.agg(F.max("customer_avg_price")).collect()[0][0]
enhanced_df = enhanced_prepared_df.withColumn(
    "normalized_customer_price", 
    F.col("customer_avg_price") / max_avg_price
)

max_product_price = enhanced_prepared_df.agg(F.max("product_avg_price")).collect()[0][0]
enhanced_df = enhanced_df.withColumn(
    "normalized_product_price",
    F.col("product_avg_price") / max_product_price
)

# 4. Adjust preference values based on price affinity
enhanced_df = enhanced_df.withColumn(
    "adjusted_preference",
    F.col("preference") * (1 + 0.2 * F.abs(
        F.col("normalized_customer_price") - F.col("normalized_product_price")
    ))
)

# 5. Create training data with the enhanced features
enhanced_als_data = enhanced_df.select(
    F.col("customerIndex").alias("userId"),
    F.col("productIndex").alias("itemId"),
    F.col("adjusted_preference").alias("rating")
)

# 6. Split the enhanced data
(enhanced_training, enhanced_test) = enhanced_als_data.randomSplit([0.8, 0.2], seed=42)

# 7. Train a model with additional regularization to address cold start
als_cold_start = ALS(
    maxIter=10,
    regParam=0.2,  # Higher regularization for cold start problems
    userCol="userId",
    itemCol="itemId",
    ratingCol="rating",
    coldStartStrategy="drop",
    implicitPrefs=True,
    alpha=3.0,  # Higher confidence for observed interactions
    nonnegative=True
)

# Create a Pipeline with the enhanced ALS model
enhanced_model_pipeline = Pipeline(stages=[als_cold_start])

# Train the enhanced model
enhanced_model = enhanced_model_pipeline.fit(enhanced_training)
enhanced_als_model = enhanced_model.stages[-1]

# 8. Generate recommendations and evaluate for cold start users specifically
enhanced_recommendations = enhanced_als_model.recommendForUserSubset(
    cold_start_user_indices,  # Focus on cold start users
    10
)

print("Enhanced recommendations for cold start users:")
enhanced_recommendations.show(5, truncate=False)

# 9. Compare with previous recommendations for cold start users
# This would require evaluating both models on the same test set
if cold_start_user_indices_list and cold_start_recs.count() > 0:
    # Extract scores from both recommendation sets
    original_cold_start_scores = cold_start_recs_exploded.select("score").agg(
        F.avg("score").alias("avg_score")
    ).collect()[0][0]
    
    enhanced_cold_start_scores = enhanced_recommendations.select(
        F.explode("recommendations").alias("recommendation")
    ).select(
        F.col("recommendation.rating").alias("score")
    ).agg(
        F.avg("score").alias("avg_score")
    ).collect()[0][0]
    
    print(f"Average score for cold start users (original model): {original_cold_start_scores:.4f}")
    print(f"Average score for cold start users (enhanced model): {enhanced_cold_start_scores:.4f}")
    print(f"Improvement: {(enhanced_cold_start_scores/original_cold_start_scores - 1) * 100:.2f}%")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Visualization and business insights

# CELL ********************

# Select a few customers to analyze their recommendations
sample_customers = final_recs.select("customer_id").distinct().limit(5).toPandas()["customer_id"].tolist()

# Get recommendations for these customers
sample_recommendations = final_recs.filter(F.col("customer_id").isin(sample_customers))

# Convert to pandas for easier visualization
sample_recommendations_pd = sample_recommendations.toPandas()

# Visualize recommendations for each customer
for customer in sample_customers:
    customer_recs = sample_recommendations_pd[sample_recommendations_pd["customer_id"] == customer]
    
    plt.figure(figsize=(10, 6))
    plt.barh(customer_recs["product_id"].astype(str), customer_recs["score"])
    plt.title(f"Top Product Recommendations for Customer {customer}")
    plt.xlabel("Recommendation Score")
    plt.ylabel("Product ID")
    plt.tight_layout()
    plt.show()

# Feature importance visualization (using latent factors)
# Extract user factors for visualization
user_factors = als_model.userFactors.toPandas()
user_factors_array = np.array([np.array(factors) for factors in user_factors["features"]])

# Calculate average magnitude of each factor to estimate importance
factor_importance = np.abs(user_factors_array).mean(axis=0)

# Visualize factor importance
plt.figure(figsize=(12, 6))
plt.bar(range(len(factor_importance)), factor_importance)
plt.title("Latent Factor Importance")
plt.xlabel("Factor Index")
plt.ylabel("Average Magnitude")
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate potential revenue impact of recommendations
# For each recommendation, estimate potential revenue

# Get average price per product
product_prices = orders_df.groupBy("product_id").agg(
    F.avg("price").alias("avg_price")
)

# Join recommendations with product prices
revenue_impact = final_recs.join(product_prices, "product_id")

# Calculate potential revenue per recommendation 
# (score represents likelihood of purchase, can be adjusted)
revenue_impact = revenue_impact.withColumn(
    "potential_revenue", 
    F.col("score") * F.col("avg_price") / 10  # Adjust formula as needed
)

# Aggregate by customer
customer_potential = revenue_impact.groupBy("customer_id").agg(
    F.sum("potential_revenue").alias("total_potential_revenue"),
    F.count("product_id").alias("recommendation_count")
)

# Show potential revenue impact
print("Potential Revenue Impact per Customer:")
customer_potential.orderBy(F.desc("total_potential_revenue")).show(10)

# Overall revenue potential
total_potential = customer_potential.agg(
    F.sum("total_potential_revenue")
).collect()[0][0]

print(f"Total estimated revenue potential from recommendations: ${total_potential:.2f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summarize findings
print("Recommendation System Summary:")
print(f"- Dataset: {unique_users} users, {unique_movies} movies, {total_ratings} ratings")
print(f"- Best model RMSE: {best_rmse:.3f}")
print(f"- Model parameters: rank={best_als.rank}, regParam={best_als.getRegParam()}, maxIter={best_als.getMaxIter()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
