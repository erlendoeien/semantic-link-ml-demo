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

# MARKDOWN ********************

# # Setup notebook
# 1. Get the relevant capacity Id
# 2. Creates the required workspaces
# 2. Creates the initial lakehouses
# 3. Install kaggle
# 4. Manually attach the Raw-lakehouse to have a valid path to download to
# 5. Download and unzip Olist E-commerce dataset
# 7. Load the CSV-files as Delta tables into Raw-lakehouse

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "Raw",
# MAGIC     }
# MAGIC 
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install kaggle semantic-link-labs deltalake -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sempy import fabric
client = fabric.FabricRestClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. Get Capacity Id

# CELL ********************

capacities = client.get("v1/capacities").json()
trial_capacity_id = [capacity["id"] for capacity in capacities["value"] if 'Trial' in capacity["displayName"]][0]
trial_capacity_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Setup workspaces

# CELL ********************

ws_raw_name= "FabCon_Raw"
ws_ml_name= "FabCon_ML"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Fabric workspaces using trial capacity
raw_ws = fabric.create_workspace(ws_raw_name, capacity_id=trial_capacity_id)
ml_ws = fabric.create_workspace(ws_ml_name, capacity_id=trial_capacity_id)
raw_ws, ml_ws

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_ws= fabric.resolve_workspace_id(ws_raw_name)
ml_ws = fabric.resolve_workspace_id(ws_ml_name)
raw_ws, ml_ws

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Setup Lakehouses

# CELL ********************

# Setup lakehouses for initial raw lakehouse and the eventual Semantic Link Lake
raw_lh_id = fabric.create_lakehouse("Raw", workspace=raw_ws)
fabric.create_lakehouse("Translated", workspace=raw_ws)

gold_lh_id = fabric.create_lakehouse("Gold", workspace=ml_ws)
ml_lh_id = fabric.create_lakehouse("ML", workspace=ml_ws)
audit_lh_id = fabric.create_lakehouse("Audit", workspace=ml_ws)
platinum_lh_id = fabric.create_lakehouse("Platinum", workspace=ml_ws)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_lh_id = fabric.resolve_item_id("Raw", type="Lakehouse")
gold_lh_id = fabric.resolve_item_id("Gold", type="Lakehouse", workspace=ml_ws)
platinum_lh_id = fabric.resolve_item_id("Platinum", type="Lakehouse", workspace=ml_ws)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Install and setup authentication for Kaggle API

# CELL ********************

import os
# Required to import Kaggle and use the API
os.environ['KAGGLE_USERNAME'] = "<username>"
os.environ['KAGGLE_KEY'] = "<api_key>"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###  5. Manually Add/attach the "Raw"-lakehouse

# CELL ********************

mount_point = "/lakehouse/default"
notebookutils.fs.mount(f"abfss://{raw_ws}@onelake.dfs.fabric.microsoft.com/{raw_lh_id}", mount_point)
raw_local_lh_path  = next((mp["localPath"] for mp in notebookutils.fs.mounts() if mp["mountPoint"] == mount_point), None)
raw_local_lh_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Download Olist e-commerce Dataset

# CELL ********************

import kaggle
olist_raw_path = f'{raw_local_lh_path}/Files/olist_raw'
olist_marketing_raw_path = f'{raw_local_lh_path}/Files/olist_marketing_raw'
olist_raw_path, olist_marketing_raw_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kaggle.api.dataset_download_files(dataset = "olistbr/brazilian-ecommerce", 
                                path=olist_raw_path, #f{raw_lakehouse["properties"]["abfsPath"]}/Files/olist_raw', 
                                force=False,
                                unzip=True)
kaggle.api.dataset_download_files(dataset = "olistbr/marketing-funnel-olist",
                                path=olist_raw_path, #f{raw_lakehouse["properties"]["abfsPath"]}/Files/olist_raw', 
                                force=False,
                                unzip=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7. Load the CSV-files as Delta tables into `Raw`-Lakehouse

# CELL ********************

# Convert to delta tables
raw_lakehouse_path = notebookutils.lakehouse.get("Raw", workspaceId=raw_ws)["properties"]["abfsPath"]
raw_lakehouse_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import deltalake as dl
import pyarrow.csv as pv
import pyarrow as pa
import os
import pathlib
import deltalake as dl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# files = notebookutils.fs.ls(f'{raw_lakehouse_path}/Files/olist_raw')
files = list([f for f in pathlib.Path(olist_raw_path).iterdir() if not f.is_dir()])
for file in files:
    print("Converting", file.name, "to Delta")
    table_name = "_".join(file.name.split("_")[1:-1])
    (spark
        .read
        .option("inferSchema", "true")
        .csv(f"Files/olist_raw/{file.name}", header=True)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{raw_lakehouse_path}/Tables/{table_name}")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# files = notebookutils.fs.ls(f'{raw_lakehouse_path}/Files/olist_marketing_raw')
files = list([f for f in pathlib.Path(olist_marketing_raw_path).iterdir() if not f.is_dir()])
for file in files:
    print("Converting", file.name, "to Delta")
    table_name = "_".join(file.name.split("_")[1:-1])
    (spark
        .read
        .option("inferSchema", "true")
        .csv(f"Files/olist_marketing_raw/{file.name}", header=True)
        .write
        .format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f"{raw_lakehouse_path}/Tables/{table_name}")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Non-working alternative with delta-rs due to timestamp conversion

# CELL ********************

# Convert all timestamp columns to milliseconds
def convert_timestamps_to_ms(table: pa.Table):
    fields = []
    for field in table.schema:
        if pa.types.is_timestamp(field.type):
            fields.append(pa.field(field.name, pa.timestamp('ms', 'utc')))
        else:
            fields.append(field)
    return table.cast(pa.schema(fields))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # List files in the directory
# files = list([f for f in pathlib.Path(olist_raw_path).iterdir() if not f.is_dir()])

# for file in files:
#     print("Converting", file, "to Delta")
#     table_name = "_".join(file.stem.split("_")[1:-1])

#     pv_table = pv.read_csv(f"{olist_raw_path}/{file.name}")#.to_pandas()

#     print(pv_table.schema)
#     table_with_compatible_schema = convert_timestamps_to_ms(pv_table)
#     print(pv_table_compatible_schema.schema.field("won_date"))
#     dl_schema = dl.Schema.from_pyarrow(pv_table_compatible_schema.schema)
#     print(dl_schema)
#     break
    
#     # # Write the DataFrame to a Delta table
#     # dl.write_deltalake(f"{raw_local_lh_path}/Tables/{table_name}", 
#     #             pdf, mode='overwrite', scema_mode="overwrite"
#     #             engine='rust', storage_options={"allow_unsafe_rename":"true"}
#     #             )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # List files in the directory
# files = list([f for f in pathlib.Path(olist_marketing_raw_path).iterdir() if not f.is_dir()])

# for file in files:
#     table_name = file.stem
#     print("Converting", table_name, "to Delta")
#     table = pv.read_csv(f"{olist_raw_path}/{file.name}")
    
#     # Write the DataFrame to a Delta table
#     dl.write_deltalake(f"{raw_local_lh_path}/Tables/{table_name}", 
#                 table, mode='overwrite',
#                 engine='rust', 
#                 storage_options={"allow_unsafe_rename":"true"}
#                 )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
notebookutils.fs.unmount(mount_point)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
