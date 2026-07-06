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
# **Bronze = Raw.** This layer is an unmodified copy of the source file, landed as Delta so it can be queried with
# SQL/Spark and versioned/time-travelled like any other Delta table. It intentionally contains **no business logic
# and no data cleansing** — no dedup, no trimming, no filtering, no value-level type casting. That responsibility
# belongs to Silver, so that Bronze always reflects exactly what the source system sent, which is essential for
# auditing and for re-processing if a downstream rule turns out to be wrong. The one exception is column-name
# standardization — see Step 2 for why that happens here instead of Silver.
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

# ## Step 2 — Standardize column names
#
# The source file's headers arrive as `Customer.ID`, `Order.Date`, `Sub.Category`, etc. — dot-separated rather than
# space-separated (a common artifact of exports produced through R/Power Query-style tools). This is standardized
# during ingestion, immediately after reading, rather than in Silver, for a few reasons that make it a best
# practice in Spark and Microsoft Fabric projects:
#
# - **Dots are unsafe in Spark column references.** `col("Customer.ID")` is parsed as *table `Customer`, column
#   `ID`* (nested/qualified field access), not as a literal column name — it fails to resolve even though the
#   column genuinely exists. Renaming once, up front, removes this trap for every notebook that reads this data.
# - **One fix point instead of many.** If the standardization lived in Silver instead, every other consumer of
#   `bronze.bronze_superstore` (ad hoc SQL, other notebooks, Power BI DirectLake queries) would have to
#   rediscover and work around the same quoting problem independently.
# - **It's a structural fix, not a business rule.** No rows are added, removed, deduplicated, or reshaped, and no
#   cell values change — only the column headers are rewritten. Bronze therefore stays a faithful, byte-for-byte
#   equivalent of the source *values*, while becoming safe to query.
#
# Rule applied to every column name: trim leading/trailing whitespace, then replace `.`, ` ` (space), `-`, and `/`
# with `_`. Original casing is preserved (e.g. `Customer.ID` → `Customer_ID`).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def standardize_column_name(name):
    cleaned = name.strip()
    for character in (".", " ", "-", "/"):
        cleaned = cleaned.replace(character, "_")
    return cleaned

standardized_columns = [standardize_column_name(c) for c in df_raw.columns]
df_raw = df_raw.toDF(*standardized_columns)

print("Standardized columns:", df_raw.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Inspect the raw data
#
# Display the schema, the row count, and a sample of records. This is purely observational — nothing here changes
# the data.

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

# ## Step 4 — Write to the Bronze Delta table
#
# Written with standardized column names but no other cleansing, into a managed Delta table
# `bronze.bronze_superstore`. Each run fully overwrites the table, which keeps this learning project simple — a
# production pipeline would typically use incremental/merge loading.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze.bronze_superstore")

print("bronze.bronze_superstore written successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
