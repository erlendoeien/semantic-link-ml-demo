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

# MAGIC %%configure -f
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
# MAGIC         "name": "Translated"
# MAGIC     },
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "Translated"
# MAGIC     },
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Install fixed version of packages
%pip install -q openai==1.30 semantic-link-labs
%pip install -q --force-reinstall httpx==0.27.0

# Install latest version of SynapseML-core
%pip install -q --force-reinstall https://mmlspark.blob.core.windows.net/pip/1.0.9/synapseml_core-1.0.9-py2.py3-none-any.whl

# Install SynapseML-Internal .whl with AI functions library from blob storage:
%pip install -q --force-reinstall https://mmlspark.blob.core.windows.net/pip/1.0.10.0-spark3.4-6-0cb46b55-SNAPSHOT/synapseml_internal-1.0.10.0.dev1-py2.py3-none-any.whl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pyspark AI Functions
from synapse.ml.services.openai import OpenAIDefaults
from synapse.ml.spark.aifunc.DataFrameExtensions import AIFunctions

# Required imports Pandas AI Functions
import synapse.ml.aifunc as aifunc
import openai

# Optional import for progress bars
from tqdm.auto import tqdm
tqdm.pandas()

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

# Other imports
import numpy as np 
import pandas as pd 
import pyspark.sql.functions as F
import sempy_labs
from sempy import fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fabric.create_lakehouse("Translated")
raw_ws_id = fabric.resolve_workspace_id("FabCon_Raw")
raw_lh_id = fabric.resolve_item_id("Raw", type="Lakehouse", workspace=raw_ws_id)
translated_lh_id = fabric.resolve_item_id("Translated", type="Lakehouse", workspace=raw_ws_id)
raw_lakehouse_path = sempy_labs.create_abfss_path(raw_ws_id, raw_lh_id)
translated_lakehouse_path = sempy_labs.create_abfss_path(raw_ws_id, translated_lh_id)
raw_lakehouse_path, translated_lakehouse_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_to_translate = ["order_reviews", "category_name"]
raw_tables_df = sempy_labs.lakehouse.get_lakehouse_tables("Raw", workspace="FabCon_Raw")
shortcut_tables_df = raw_tables_df[~raw_tables_df["Table Name"].isin(tables_to_translate)]
translate_tables_df = raw_tables_df[raw_tables_df["Table Name"].isin(tables_to_translate)]
display(translate_tables_df)
display(shortcut_tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

existing_shortcuts = sempy_labs.lakehouse.list_shortcuts(translated_lh_id, raw_ws_id)
not_already_shortcutted = shortcut_tables_df[~shortcut_tables_df["Table Name"].isin(existing_shortcuts["Shortcut Name"])]
for idx, table_row in not_already_shortcutted.iterrows():
    sempy_labs.lakehouse.create_shortcut_onelake(
        table_name = table_row["Table Name"], 
        source_lakehouse = table_row["Lakehouse Name"], 
        source_workspace = table_row["Workspace Name"], 
        destination_lakehouse = "Translated", 
        destination_workspace = table_row["Workspace Name"], 
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Translate product category names which are missing from the translation
# Add missing categories
product_category_translations_df = spark.read.format("delta").load(translate_tables_df.iloc[0]["Location"])
missing_categories = spark.createDataFrame([("pc_gamer", "gaming_pc"), ("portateis_cozinha_e_preparadores_de_alimentos", "portable_kitchen_and_food_preperators")], ["product_category_name", "product_category_name_english"])
df_category_extended = product_category_translations_df.union(missing_categories)
df_category_extended.write.format("delta").mode("overwrite").saveAsTable("Translated.category_name")

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

# MARKDOWN ********************

# # Translate using AI functions

# CELL ********************

defaults = OpenAIDefaults()

defaults.set_deployment_name(AZURE_OPENAI_MODEL)
defaults.set_subscription_key(AZURE_OPENAI_KEY)
defaults.set_URL(AZURE_OPENAI_ENDPOINT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_translate = {
    "olist_order_reviews_dataset": 
        {
            "key": "review_id",
            "columns": ["review_comment_title", "review_comment_message"]
        }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

translated_tables = {}
import pyspark.sql.functions as F
def blank_as_null(x):
    return F.when(F.trim(x) != "", F.col(x)).otherwise(None)

for idx, row in translate_tables_df.iterrows():
    table = row["Table Name"]
    location = row["Location"]
    trans_conf = columns_to_translate.get(table, list())
    # Map empty strings to null to avoid translate calls
    trans_conf = columns_to_translate.get(table, list())
    rel_cols = trans_conf["columns"]
    df = spark.read.format("delta").load(location).withColumns({
        col: blank_as_null(col) for col in rel_cols
    })
    print("Will translate", rel_cols)
    for col_to_translate in rel_cols:
        df_filtered = df.filter(F.col(col_to_translate).isNotNull())
        df_translated = df_filtered.ai.translate(
                to_lang="english", 
                input_col=col_to_translate, 
                output_col=f"{col_to_translate}_en"
                )
        trans_cols = [trans_conf["key"], f"{col_to_translate}_en", f"{col_to_translate}_translate_error"]
        translated_tables[table] = df.join(df_translated.select(trans_cols), how="left", on=trans_conf["key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for translated_table, df in translated_tables.items():
    print("Translating and saving", translated_table)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Translated.{translated_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Using pandas

# CELL ********************

from openai import AzureOpenAI

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=aifunc.session.api_version, 
    max_retries=aifunc.session.max_retries,
)

aifunc.default_conf.model_deployment_name = AZURE_OPENAI_MODEL
aifunc.default_conf.embedding_deployment_name = AZURE_OPENAI_EMBEDDING
aifunc.default_conf.concurrency = 1000
aifunc.setup(client)  # Set the client for all functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

translated_tables = {}
def blank_as_null(x):
    return F.when(F.trim(x) != "", F.col(x)).otherwise(None)

for idx, row in translate_tables_df.iterrows():
    table = row["Table Name"]
    location = row["Location"]
    trans_conf = columns_to_translate.get(table, list())
    # Map empty strings to null to avoid translate calls
    trans_conf = columns_to_translate.get(table, list())
    rel_cols = trans_conf["columns"]
    df = spark.read.format("delta").load(location).withColumns({
        col: blank_as_null(col) for col in rel_cols
    })
    pdf = df.toPandas()

    print("Translating", rel_cols)
    for col_to_translate in rel_cols:
        pdf_filtered = pdf.dropna(subset=[col_to_translate]).head(100)#.filter(F.col(col_to_translate).isNotNull())
        pdf_filtered[f"{col_to_translate}_en"] = pdf[col_to_translate].ai.translate("english")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for translated_table, df in translated_tables.items():
    print("Translating and saving", translated_table)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Translated.{translated_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Translate using Azure AI Translator

# CELL ********************

from pyspark.sql import DataFrame, functions as F
from tqdm.notebook import tqdm
import time, uuid, requests

def batch_inputs(rows: list):
    """Batch messages to optimize API"""
    # Initialize variables
    sublists = []
    sublist = []
    sublist_char_count = 0

    # Iterate over each tuple in the list
    for tup in rows:
        # Unpack the tuple into id and string
        idx, string = tup
        # If adding the current string would exceed the limits
        if (sublist_char_count + len(string)) > 45_000 or len(sublist) == 900:
            # Add the current sublist to the list of sublists
            sublists.append(sublist)
            # Start a new sublist and character count
            sublist = [(idx, {"text": string})]
            sublist_char_count = len(string)
        else:
            # Add the current tuple to the current sublist and update the character count
            sublist.append((idx, {"text": string}))
            sublist_char_count += len(string)

    # Add the last sublist if it's not empty
    if sublist:
        sublists.append(sublist)

    return sublists

def _translate(texts_body: list, **kwargs):
    endpoint = AZURE_AI_TRANSLATOR_ENDPOINT + "/translate"
    headers = kwargs.pop("headers", {})
    headers['X-ClientTraceId'] =  str(uuid.uuid4())
    params = kwargs
    return requests.post(endpoint, json=texts_body, params=params, headers=headers)

def batch_translate(text_elements: list, **translate_kwargs)->list:
    """Contains an id and the text column"""
    idxs, texts = zip(*text_elements)
    
    try:

        # response = text_translator.translate(body = texts, **translate_kwargs)
        response = _translate(texts, **translate_kwargs) #text_translator.translate(body = texts, **translate_kwargs)
        response.raise_for_status()
        translations = response.json()
        tranlated_texts = [txt_obj["translations"][0]["text"] for txt_obj in translations]
        return list(zip(idxs, [text["text"] for text in texts], tranlated_texts))

    except requests.HTTPError as exception:
        print(exception)
        return None


def blank_as_null(x):
    return F.when(F.trim(x) != "", F.col(x)).otherwise(None)

def batch_translate_df(df, id_col:str, txt_col: str, **translate_kwargs) -> tuple:
    df_emtpy_as_null = df.withColumn(txt_col, blank_as_null(txt_col))
    txt_list = df.dropna(subset=[txt_col]).select(id_col, txt_col).distinct().collect()
    txt_batches = batch_inputs(txt_list)

    translated = []
    for idx, batch in tqdm(enumerate(txt_batches, start=1), desc="Batch translations", total=len(txt_batches)):
        # print("\tTranslated batch", idx, "out of", len(txt_batches))
        translated_batch = batch_translate(batch, **translate_kwargs)
        while translated_batch is None:
            time.sleep(30)
            print("\tRetry batch", idx)
            translated_batch = batch_translate(batch, **translate_kwargs)

        translated.append(translated_batch)
    return txt_batches, translated

def add_translation(df, batch_translations:list, id_col:str, txt_col:str) -> DataFrame:
    flattened_translations = [record for batch in batch_translations for record in batch]
    translated_df = spark.createDataFrame(flattened_translations, [id_col, txt_col, f"{txt_col}_{TO_LANGUAGE}"])
    return df.drop(txt_col).join(translated_df.alias("translated"),
                                on=id_col,
                                how="left"
                            )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FROM_LANGUAGE = "pt"
TO_LANGUAGE = "en"
translate_kwargs = {
        "from" :FROM_LANGUAGE, 
        "to": [TO_LANGUAGE],
        "api-version": 3.0,
        "headers": {
            'Ocp-Apim-Subscription-Key': AZURE_OPENAI_KEY,
            'Ocp-Apim-Subscription-Region': AZURE_AI_TRANSLATOR_REGION,
            'Content-type': 'application/json',
        }
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

translate_tables_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_reviews = spark.read.format("delta").load(translate_tables_df["Location"].iloc[-1])
df_reviews

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Translating review titles")
title_batches, title_translations = batch_translate_df(df_reviews, "review_id", "review_comment_title", **translate_kwargs)
title_translated_df = add_translation(df_reviews, title_translations, "review_id", "review_comment_title").cache()
title_translated_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("Translated.order_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Translating review messages")
message_batches, message_translations = batch_translate_df(title_translated_df, "review_id", "review_comment_message", **translate_kwargs)
message_translated_df = add_translation(title_translated_df, message_translations, "review_id", "review_comment_message")
message_translated_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("Translated.order_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_title_translations = title_translated_df.withColumn("review_comment_title_en", blank_as_null("review_comment_title_en")).dropna(subset=["review_comment_title_en"]).count()
num_title_translations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_message_translations = message_translated_df.withColumn("review_comment_message_en", blank_as_null("review_comment_message_en")).dropna(subset=["review_comment_message_en"]).count()
num_message_translations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
