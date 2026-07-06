# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "23fb6285-b11d-4d7b-a9af-78c761ce904e",
# META       "default_lakehouse_name": "RetailLakehouse",
# META       "default_lakehouse_workspace_id": "f30d77e2-708e-4748-8a9b-00c23037a869",
# META       "known_lakehouses": [
# META         {
# META           "id": "23fb6285-b11d-4d7b-a9af-78c761ce904e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01 - Bronze Ingestion
# **Bronze = Raw.** This layer is an unmodified copy of the source file, landed as Delta so it can be queried with
# SQL/Spark and versioned/time-travelled like any other Delta table. It intentionally contains **no business logic
# and no data cleansing** — no dedup, no trimming, no filtering, no type casting. That responsibility belongs to
# Silver, so that Bronze always reflects exactly what the source system sent, which is essential for auditing and
# for re-processing if a downstream rule turns out to be wrong.
# **Source:** `Files/raw/retail/superstore.csv` in the attached **RetailLakehouse**.
# **Output:** `bronze.bronze_superstore` (Delta table).
# > Before running this notebook, attach **RetailLakehouse** as the default lakehouse (View menu ▸ Lakehouse explorer ▸ Add).

# MARKDOWN ********************

# ## Step 1 — Read the raw CSV
# The file has a header row. We let Spark infer the schema since this is a one-time raw ingestion and no
# business rules have been applied yet.

# CELL ********************

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/raw/retail/superstore.csv")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Inspect the raw data
# Display the inferred schema, the row count, and a sample of records. This is purely observational — nothing here
# changes the data.

# CELL ********************

df_raw.printSchema()

raw_row_count = df_raw.count()
print(f"Raw row count: {raw_row_count}")

display(df_raw.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Write to the Bronze Delta table
# Written as-is, with no cleansing, into a managed Delta table `bronze.bronze_superstore`. Each run fully overwrites
# the table, which keeps this learning project simple — a production pipeline would typically use incremental/merge
# loading.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze.bronze_superstore")

print("bronze.bronze_superstore written successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
