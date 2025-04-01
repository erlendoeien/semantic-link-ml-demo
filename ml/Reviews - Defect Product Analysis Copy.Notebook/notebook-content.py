# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e711cb39-e6cc-4f99-98e2-225ba302ff38",
# META       "default_lakehouse_name": "Translated",
# META       "default_lakehouse_workspace_id": "082d6063-1af7-4f12-931b-ec064fcbed1f"
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
# MAGIC         "name": "ML"
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

from pyspark.sql import Row, functions as F
from synapse.ml.services.openai import OpenAIChatCompletion
from pyspark.sql.types import *
import json
from pyspark import StorageLevel 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("Gold.Reviews").dropna(subset="review_comment_message").dropDuplicates(subset=["review_id"])

# spark.sql("SELECT DISTINCT(* FROM Gold.reviews where review_comment_message_en is not null")
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deployment_name = "gpt-4"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

system_message_content = "You are an AI system that helps a business understand customer product reviews."

# Define the instruction for analyzing reviews in a batch
batch_review_instruction = (
    "You will receive a JSON array of product reviews. For each review in the array, "
    "assign a 'defected' score between 0.0 and 1.0 based on whether the product is reported as defective. "
    "If the product is fine or the complaint is about something else (like delivery), give a score of 0.0."
    "If you suspect that there could be a case of a defect product, but are not 100% sure, assign a score between 0.0 and 1.0."
    "However, if you are very sure it is a defect product always give a score of 1.0."
    "Provide a list of defect scores in the same order as the reviews appear in the array."
    "DO NOT WRITE ANYTHING ELSE, ONLY ANSWER MY MESSAGE WITH THE LIST OF DEFECT SCORES IN CORRECT ORDER."
)

batch_review_instruction = "You will receive a JSON array of product reviews. For each review in the array, assign a 'defected' score between 0.0 and 1.0 based on whether the product is reported as defective. If the product is fine or the complaint is about something else (like delivery, wrong colour, different product than the one ordered, never arrived, too expensive etc), GIVE A SCORE OF 0.0. Give a score higher than 0.0 ONLY when you suspect that there could be a case of a defect product. However, if you are very sure it is a defect product always give a score of 1.0.Provide a list of defect scores in the same order as the reviews appear in the array.DO NOT WRITE ANYTHING ELSE, ONLY ANSWER MY MESSAGE WITH THE LIST OF DEFECT SCORES IN CORRECT ORDER."

batch_review_instruction = (
    "You will receive a JSON array of product reviews. For each review in the array, "
    "assign a 'defected' score between 0.0 and 1.0 based on whether the product is reported as defective. "
    "If the product is fine or the complaint is about something else (like delivery, wrong colour, different "
    "product than the one ordered, never arrived, too expensive etc), GIVE A SCORE OF 0.0. Give a score higher "
    "than 0.0 ONLY when you suspect that there could be a case of a defect product. However, if you are very sure "
    "it is a defect product always give a score of 1.0. Provide a list of defect scores in the same order as the "
    "reviews appear in the array. DO NOT WRITE ANYTHING ELSE, ONLY ANSWER MY MESSAGE WITH THE LIST OF DEFECT SCORES "
    "IN CORRECT ORDER."
)

# Function to create message Rows
def make_message(role, content):
    return Row(role=role, content=content, name=role)

# Function to create the batch message for chat completion
def create_batch_chat_row(reviews):
    # Create the system message
    system_message = make_message("system", system_message_content)
    
    # Extract only the review text for JSON serialization
    reviews_text = [review['review_comment_message_en'] for review in reviews]
    
    # Convert the list of reviews into a JSON-formatted string
    stringified_reviews_array = json.dumps(reviews_text)
    
    # Create a single user message with the JSON string and the instruction
    user_message_content = f"{batch_review_instruction} Here is the JSON array of reviews: {stringified_reviews_array}"
    user_message = make_message("user", user_message_content)
    
    # Return the combined messages: first the system message, then the user message
    return [system_message, user_message]

# Collect all reviews with their IDs into a list
reviews_list = df.select("review_id", "review_comment_message_en").rdd.map(lambda row: {"review_id": row.review_id, "review_comment_message_en": row.review_comment_message_en}).collect()

# Chunk the reviews into batches (adjust batch_size as needed)
batch_size = 20
batches = [reviews_list[i:i + batch_size] for i in range(0, len(reviews_list), batch_size)]

# Transform the batches into a DataFrame suitable for the API
batch_chat_rows = [create_batch_chat_row(batch) for batch in batches]
chat_df = spark.createDataFrame([(row,) for row in batch_chat_rows], ["messages"])

# Display the input chat_df
#display(chat_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(chat_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the OpenAIChatCompletion object
chat_completion = (
    OpenAIChatCompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMessagesCol("messages")
    .setErrorCol("error")
    .setOutputCol("chat_completions")
    .setTemperature(0.1)
)

# Apply the transformation and get defect_score for each review
result_df = chat_completion.transform(chat_df)

result_df.persist(StorageLevel.MEMORY_AND_DISK)

result_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling of errounous responses**

# CELL ********************


# Parse the JSON responses from the batch completions
parsed_results = result_df.select("messages", "chat_completions.choices.message.content").rdd.map(
    lambda row: json.loads(row['content'][0] if isinstance(row['content'], list) and row['content'][0] not in [None, "null"] else 'null')
).collect()

# Flatten the parsed results into a single list of tuples (review_id, review, score)
flattened_results = []
for batch_index, batch in enumerate(batches):
    # Check if batch_index is within the parsed_results range
    if batch_index >= len(parsed_results):
        print(f"Warning: batch_index {batch_index} out of range for parsed_results")
        continue

    # Handle cases where the result is [null]
    if parsed_results[batch_index] is None or parsed_results[batch_index] == [None]:
        defect_scores = [None] * len(batch)  # Assign a score of 0.0 to all reviews in the batch
    else:
        # Parse the JSON to get defect scores
        #print(parsed_results[batch_index])
        defect_scores = parsed_results[batch_index]
    
    # Ensure the number of reviews matches the number of defect scores
    if len(batch) != len(defect_scores):
        print(f"Warning: Number of reviews in batch {batch_index} does not match number of defect scores.")
        print(f"Number of reviews: {len(batch)}, Number of defect scores: {len(defect_scores)}")
        print(f"Batch {batch_index} reviews:", batch)
        print(f"Batch {batch_index} defect scores:", defect_scores)
        continue

    # Append the review and corresponding defect score to flattened_results
    for review_index, review in enumerate(batch):
        defect_score = defect_scores[review_index]
        flattened_results.append((review["review_id"], review["review_comment_message_en"], defect_score))

# Create a DataFrame from the flattened results
schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("review", StringType(), True),
    StructField("defect_score", FloatType(), True)
])

reviews_with_scores_df = spark.createDataFrame(flattened_results, schema)

# Display the resulting DataFrame
display(reviews_with_scores_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse = notebookutils.lakehouse.get("ML")
reviews_with_scores_df.write.format("delta").mode("overwrite").save(f"{lakehouse['properties']['abfsPath']}/Tables/Reviews_Defect")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
