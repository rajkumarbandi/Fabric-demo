# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 01 - Bronze Ingestion
#
# **Layer purpose:** land the raw `superstore.csv` file into a Delta table exactly as it arrives, applying only the
# minimal cleanup needed to make it safely queryable (no business logic, no renaming, no reshaping).
#
# **Source:** `Files/raw/retail/superstore.csv` in the attached **RetailLakehouse**.
#
# **Output:** `bronze.bronze_superstore` (Delta table).
#
# > Before running this notebook, attach **RetailLakehouse** as the default lakehouse (View menu ▸ Lakehouse explorer ▸ Add).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read the raw CSV
#
# The file has a header row. We let Spark infer the schema since this is a one-time raw ingestion and no
# business rules have been applied yet.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

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
#
# Display the inferred schema, the row count, and a sample of records before any changes are made. This gives us a
# baseline to compare against after cleanup.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

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

# ## Step 3 — Minimal validation
#
# Bronze only performs the minimum cleanup required to have a trustworthy raw layer:
#
# - Remove exact duplicate rows.
# - Trim leading/trailing whitespace on every string column (common issue in CSV exports).
# - Drop rows with a missing `Order ID`, since it is the natural key of the dataset.
#
# No columns are renamed, cast, split, or dropped here — that belongs in Silver.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType

# Remove exact duplicate rows
df_clean = df_raw.dropDuplicates()

# Trim whitespace on every string column
string_columns = [f.name for f in df_clean.schema.fields if isinstance(f.dataType, StringType)]
for column_name in string_columns:
    df_clean = df_clean.withColumn(column_name, trim(col(column_name)))

# Drop rows where the natural key (Order ID) is missing
df_clean = df_clean.filter(col("Order ID").isNotNull())

print(f"Row count after cleanup: {df_clean.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Write to the Bronze Delta table
#
# The result is written as a managed Delta table `bronze.bronze_superstore`. Each run fully overwrites the table,
# which keeps this learning project simple — a production pipeline would typically use incremental/merge loading.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

df_clean.write.format("delta").mode("overwrite").saveAsTable("bronze.bronze_superstore")

print("bronze.bronze_superstore written successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
