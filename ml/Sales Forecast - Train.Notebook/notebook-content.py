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

# MARKDOWN ********************

# # Sales Forecast with AutoML

# CELL ********************

import pyspark.sql.functions as F
import pandas as pd
from flaml import AutoML
from datetime import datetime, timedelta
import mlflow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load and Pre-process data

# CELL ********************

orders = spark.table("Orders")
display(orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Cleaning and labelling

# CELL ********************

df = (orders
        # Exclude some none-standard orders 
        .filter(F.year(F.col("order_purchase_timestamp")) >= 2017)
        .filter(~((F.year(F.col("order_purchase_timestamp")) == 2018)&(F.month(F.col("order_purchase_timestamp"))==9)))
        
        # Only include purchases/sales which were delivered
        .filter(F.col("order_status") == 'delivered').select("price", "order_purchase_timestamp")
        .withColumnsRenamed({"price": "sales", "order_purchase_timestamp": "date"})
        )#.drop("order_id", "order_item_id", "sell"))
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Daily-frequency re-sampling and back/forward filling

# CELL ********************

# Re-sample the data for different forecast frequencies and back/forward-fill in missing sales data on a data/weekly/monthly
daily_time_horizon = 31
weekly_time_horizon = 4
monthly_time_horizon = 1

pdf = df.toPandas()

# Set the date column as the index
pdf.set_index('date', inplace=True)

# Resample the data to weekly and monthly frequency
daily_data = pdf.copy().resample('D').sum().bfill().ffill()  # makes sure there are no missing values
# # Last week is incomplete crossover to september
weekly_data = pdf.copy().resample('W').sum().bfill().ffill().head(-1)
# # monthly_data = pdf.resample('M').sum()
monthly_data = pdf.copy().resample('M').sum().bfill().ffill()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Optional: Install additional supported models


# CELL ********************

#!pip install prophet orbit pytorch_lightning -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Train model(s)
# #### - Defining a training function
#   - We split the data into a test set as well, to have some test data to validate the model on. In the pipeline, all of the data points should be included

# CELL ********************

# Define a function to train and forecast using FLAML AutoML
def train_forecast(data, freq, time_horizon):
    # Split the data into training and testing sets
    data = data.reset_index()
    num_samples = data.shape[0]
    split_idx = num_samples - time_horizon
    train_data = data.iloc[:split_idx]
    test_data = data.iloc[split_idx:]

    # Initialize FLAML AutoML
    automl = AutoML()

    # Define the settings for FLAML AutoML
    settings = {
        "time_budget": 0,  # Time budget in seconds
        "task": 'ts_forecast',  # Use 'ts_forecast' for time series forecasting
        "metric": 'mape',
        "eval_method": 'holdout',
        # Arima was found to be best during exploration
        # "estimator_list": ["arima"],# "sarimax", "xgboost"],
        "use_spark": True,
        # "log_file_name": f"{freq}_{time_horizon}_sales_forecast.log",
        "period": time_horizon,
        "label": "sales",
        "seed": 4629582 # Just a random seed for reproducebility
    }

    mlflow.set_experiment("Forecast_Sales")

    # Train with FLAML AutoML
    with mlflow.start_run(nested=True, run_name=f"univariate_{freq}_{time_horizon}") as run:
        # Train the model
        run_id = run.info.run_id
        print("Run ID:", run_id)
        automl.fit(dataframe=train_data, **settings)

    # Make predictions on the test set with the best model - Would not be used in production
    predictions = automl.predict(test_data)

    return automl, test_data, predictions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Train a Daily Sales Forecast Model

# CELL ********************

# # # Train and forecast for monthly data with a 1-month horizon
monthly_model, test_monthly, monthly_predictions = train_forecast(weekly_data, 'monthly', monthly_time_horizon)

# Train and forecast for weekly data with a 4-week horizon
weekly_model, test_weekly, weekly_predictions = train_forecast(weekly_data, 'weekly', weekly_time_horizon)

#Train and forecast for daily data with a 31-day horizon
daily_model, test_daily, daily_predictions = train_forecast(daily_data, 'daily', daily_time_horizon)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save best model
# Using the latest (nested run) and storing the run as a model. Method works only because the searched daily model is ran last

# CELL ********************

run = mlflow.last_active_run()
run

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# "model" is the artifact path within the experiment of the mlflow model
client = mlflow.MlflowClient()
model_uri = "runs:/{}/model".format(run.info.run_id)
model_name = "sales-forecast-daily"
# Will store or bump a nested model 
mv = mlflow.register_model(model_uri, model_name)

print("Name: {}".format(mv.name))
print("Version: {}".format(mv.version))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt

def plot_forecasts(train, pred_train, test, pred_test, freq):
    fig, axs = plt.subplots(1, 2, figsize=(14, 7), sharey=True)

    # Plot for training data
    axs[0].plot(train['date'], train['sales'], label='Actual level', color='blue')
    axs[0].plot(train['date'], pred_train, label='FLAML forecast', color='orange')
    axs[0].set_xlabel(f'Date ({freq}, train)')
    axs[0].set_ylabel(f'Sales ({freq}, train)')
    axs[0].legend()
    axs[0].set_title('Training Data')

    # Plot for testing data
    axs[1].plot(test['date'], test['sales'], label='Actual level', color='blue')
    axs[1].plot(test['date'], pred_test, label='FLAML forecast', color='orange')
    axs[1].set_xlabel(f'Date ({freq}, test)')
    axs[1].set_ylabel(f'Sales (Dai{freq}ly, test)')
    axs[1].legend()
    axs[1].set_title('Testing Data')

    plt.tight_layout()
    plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Daily

# CELL ********************

train_daily = daily_data.reset_index().iloc[:daily_data.shape[0] - daily_time_horizon]
pred_train_daily = daily_model.predict(train_daily)

plot_forecasts(train_daily, pred_train_daily, test_daily, daily_predictions, "Daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Weekly

# CELL ********************

train_weekly = weekly_data.reset_index().iloc[:weekly_data.shape[0] - weekly_time_horizon]
pred_train_weekly = weekly_model.predict(train_weekly)

plot_forecasts(train_weekly, pred_train_weekly, test_weekly, weekly_predictions, "Weekly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Monthly

# CELL ********************

pd.concat([test_monthly.set_index("date"), monthly_predictions.rename("pred_sales").to_frame()], axis=1)#.ffill()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_monthly#.merge(pred_train_monthly.to_frame(), left_index=True, right_index=True, suffixes=["_label", "_pred"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
