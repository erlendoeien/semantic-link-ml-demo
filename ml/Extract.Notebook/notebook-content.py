# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Export Sales-Semantic Model to OneLake
# - Can do it "manually" through spark-connector or sempy, or exporting the model, e.g. using sempy-labs
# - `pdf` indicate `FabricDataFrame`s or `Pandas DataFrame`s, while df indicate Spark Dataframes


# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": 
# MAGIC         "name": "Gold",
# MAGIC     },
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install semantic-link-labs -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sempy import fabric
import sempy_labs as labs
import sempy
import pyspark.sql.functions as F
sempy.__version__

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Retrieve relevant tables to export (for Semantic Link)
# - Not required if using OneLake Delta Table Integration

# CELL ********************

semantic_model = "Sales & Leads"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_I = fabric.list_items()
semantic_models_pdf = df_I[df_I["Type"] == "SemanticModel"]
lakehouses_pdf = df_I[df_I["Type"] == "Lakehouse"]
# Exclude default semantic models
excluded_default_sem_models_pdf = semantic_models_pdf[~semantic_models_pdf["Display Name"].isin(lakehouses_pdf["Display Name"])]
excluded_default_sem_models_pdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Alt 1: Spark Native Connector

# CELL ********************

# Spark
# Requires restart of session to catch renamed tables
# May contain DateTableTemplates/LocalDate-tables if not disabled
semantic_model_tables = (
                        spark.sql("SHOW TABLES FROM pbi")
                            .filter(F.col("namespace") == semantic_model)
                            .drop("namespace", "isTemporary")
                            .withColumnRenamed("tableName", "Name")
                            .withColumns(
                                {
                                    "Description": F.lit(None),
                                    "Hidden": F.lit(None),
                                    "Data Category": F.lit(None),
                                    "Type": F.lit(None),

                            })
                        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Alt 2: Semantic Link (Python/Pandas)

# CELL ********************

# pandas
# Cleaner and more metadata
semantic_model_tables = fabric.list_tables(semantic_model)

display(semantic_model_tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Visualise relationships

# CELL ********************

relationships = fabric.list_relationships(semantic_model)
sempy.relationships.plot_relationship_metadata(relationships)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Make Semantic Model Data Available

# MARKDOWN ********************

# ## OneLake Delta Table Integration 
# ### Export semantic model tables with Sempy Labs (tmsl)
# - Requires Large storage format 

# CELL ********************

# Update storage mode as is required if not available
# also availble as notebookutils.PBIClient
pbi_client = fabric.PowerBIRestClient()
sem_model_id = labs.resolve_dataset_id(semantic_model)
response = pbi_client.get(f"/v1.0/myorg/datasets/{sem_model_id}").json()
print(*response.items(), sep="\n")
if response["targetStorageMode"] != 'PremiumFiles':
    print("Updating storage file format")
    update_response = pbi_client.patch(
                f"/v1.0/myorg/datasets/{sem_model_id}",
                data={"targetStorageMode": "PremiumFiles"})
    if not update_response.ok:
        print("Failed to update")
        print(update_response.__dict__)
    assert pbi_client.get(f"/v1.0/myorg/datasets/{sem_model_id}").json()["targetStorageMode"] == 'PremiumFiles'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Default workspace of notebook
lakehouse_name = "Gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# assign destination_lakehouse if shortcuts are wanted
# Assumes Large storage format dataset and OneLake integration enabled
# The tables are stored within the Semantic Model, using its Id,
# format https://api.onelake.fabric.microsoft.com/<workspace_id>/<semantic_model_id>/Tables/<semantic_model_table_name>
# but I've not been able to load it into spark programatically.
# Have not verified if fabric.read_table uses the table or the endpoint 
labs.export_model_to_onelake(semantic_model, destination_lakehouse=lakehouse_name, workspace=None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Semantic Link approach
# Will not stay in sync -> Will be a part of the ELT/MLOps-pipeline.
# Exporting metrics will fail if not present

# CELL ********************

lakehouse_id = labs.resolve_lakehouse_id(lakehouse_name)
workspace_id = fabric.resolve_workspace_id()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Alt 1: Spark Native Connector

# CELL ********************

for idx, row in semantic_model_tables.iterrows():
    table = row['Name']
    print("Exporting", table)
    try:
        # spark with potential predicate push down
        df = spark.table(f"pbi.`{semantic_model}`.`{table}`")
        df = df.withColumnsRenamed({col: col.replace(" ", "_") for col in df.columns})
        #df = spark.sql("SELECT <AGG> FROM pbi.`{semantic_model}`.`{table}` GROUP BY <col> HAVING <expr> WHERE <expr>")

        dst_path = labs.create_abfss_path(lakehouse_id, workspace_id, f"{table}")
        df.write.format("delta").mode("overwrite").save(dst_path)
        print("\t\t🟢\t[Success]")
    except Exception as err:
        print(f"\t\t🔴\t[Fail] Not able to export {table} from  {semantic_model}")
        print(err)
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extracting table for Low-code ML (sales forecasting)
ml_sales_df = spark.sql("""
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

#
# dst_path = labs.create_abfss_path(lakehouse_id, workspace_id, "ML_Sales")
spark.conf.set("spark.sql.caseSensitive", "true")
ml_sales_df.write.mode("overwrite").saveAsTable("Gold.ML_Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Alt 2: Semantic Link (Python/Pandas)

# CELL ********************

for idx, row in semantic_model_tables.iterrows():
    table = row['Name']
    print("Exporting", table)
    try:
        df = fabric.read_table(semantic_model, table)

        # # "Summarize" using measures and simple filter IN-statements
        # df = fabric.evaluate_measure(semantic_model, 
        #                                 measures = ["Measures[<Measure1>]", "Measures[<Measure2>]"],
        #                                 group_by_columns = ["Dim_1[<Attribute>]"],
        #                                 filters = {"Dim_2[<Attribute>]": ["<val1>", "<val2>"]}
        #                     )

        # # Evalute any DAX and return the result
        # df = fabric.evaluate_dax("<DAX query>")
        
        # Create the storage path and save the tables 
        dst_path = labs.create_abfss_path(lakehouse_id, workspace_id, f"{table}")
        df.write.format("delta").mode("overwrite").save(dst_path)
        print("\t\t🟢\t[Success]")
    except Exception as err:
        print(f"\t\t🔴\t[Fail] Not able to export {table} from  {semantic_model}")
        print(err)
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
