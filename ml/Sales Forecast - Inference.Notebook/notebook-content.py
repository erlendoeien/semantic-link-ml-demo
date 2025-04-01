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

# # Perform daily sales forecast
# #### 1. Load full dataset
# #### 2. Prepare dataset
# #### 2. Retrain model with AutoML on all of the data 
# #### 4. Predict next time horizon based on regression
# #### 5. Save predictions to Lakehouse

# CELL ********************

import logging
import warnings
 
logging.getLogger('synapse.ml').setLevel(logging.CRITICAL)
logging.getLogger('mlflow.utils').setLevel(logging.CRITICAL)
warnings.simplefilter('ignore', category=FutureWarning)
warnings.simplefilter('ignore', category=UserWarning)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
from datetime import datetime, timedelta, timezone
import pandas as pd
import mlflow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load and process Data

# CELL ********************

# Get latest dates with sales, 
# What the test data is depends on the type of model used
df = spark.sql("""
        SELECT 
            DATE(`Date[Date]`) as `date`,
            SUM(`Total Sales`) as sales            
        FROM 
            pbi.`Sales & Leads`.`_Metrics`
        WHERE
            `Date[Date]` BETWEEN MAKE_DATE(2017, 01, 01) AND MAKE_DATE(2018, 08, 30)
            AND `Orders[order_status]` IN ('delivered')
        GROUP BY `Date[Date]`
        ORDER BY `Date[Date]` ASC
    """)

df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Alternative using spark
# df = spark.sql("SELECT * FROM pbi.`Sales & Leads`.`Orders`")
# pdf = df
# time_horizon = 31
# test_data_raw = (pdf
#                 .filter(F.col("order_status") == 'delivered').select("price", "order_purchase_timestamp")
#                 .filter(F.year(F.col("order_purchase_timestamp")) >= 2017)
#                 .filter(~((F.year(F.col("order_purchase_timestamp")) == 2018)&(F.month(F.col("order_purchase_timestamp"))==9)))
#                 .withColumnsRenamed({"price": "sales", "order_purchase_timestamp": "date"})
#                 .select("date", "sales")
#                 .groupBy(F.col("date").cast("date").alias("date")).sum().alias("sales")
#                 .sort(F.col("date").desc())
#                 .limit(time_horizon)
#                 .withColumn("date", F.to_date("date"))
#                 .toPandas()
#                 .set_index("date"))
# # test_data
# # test_data.set_index('date', inplace=True)
# test_data = test_data_raw.copy().bfill().ffill().reset_index()[["date"]]#.resample('D').sum()[["date"]] # makes sure there are no missing values
# test_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Requries some overlap due to the regression of arima
# also backfill due to limitation of spark native connector seemingly
df_backfilled = df.set_index("date").bfill().ffill().reset_index()
time_horizon = 31
test_df = df_backfilled.orderBy("date", ascending=False).limit(time_horizon).sort("date").drop("sales").toPandas()
test_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

models = mlflow.search_registered_models()
model_name = "sales-forecast-daily"
registered_model = next(x for x in models if x.name == model_name)
latest_version = max(registered_model.latest_versions, key=lambda x:x.version) #.latest_versions
registered_model, latest_version

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
from synapse.ml.predict import MLFlowTransformer
spark.conf.set("spark.synapse.ml.predict.enabled", "true")


model = MLFlowTransformer(
    inputCols=["date"], # Your input columns here
    outputCol="sales_", # Your new column name here
    modelName=model_name,
    # modelName=mlflow.get_registry_uri()+"sales-forecast-daily/arima",#"models:/sales-forecast-daily", # Your model name here
    # trackingUri="sds",
    # trackingUri=None,#latest_version.source,
    modelVersion=latest_version.version, # Your model version here  
)

# display(model.transform(spark.createDataFrame(test_df)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_uri = f"{latest_version.source}"
model_uri

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_default = mlflow.pyfunc.load_model(model_uri=model_uri)
model_default

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions_raw = model_default.predict(test_df)
display(predictions_raw.reset_index())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions = predictions.rename("predicted_sales").reset_index().rename(columns={"index": "date"}).assign(
        prediction_ts=datetime.now(timezone.utc),
        datekey = predictions.index.strftime('%Y%m%d').astype(int)
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# save predictions
lakehouse = notebookutils.lakehouse.get("ML")
spark.createDataFrame(predictions).write.format("delta").mode("append").save(f"{lakehouse['properties']['abfsPath']}/Tables/Sales_Forecast")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
