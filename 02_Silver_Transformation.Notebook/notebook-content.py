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

# # 02 - Silver Transformation
# **Silver = Cleansed + Standardized.** Bronze is a byte-for-byte raw copy with no changes of any kind, so every
# standardization rule — column names, data types, and value-level cleansing — lives here instead. This notebook
# reads `bronze.bronze_superstore`, standardizes it, and splits it into a set of conformed tables — one per
# business entity — each with a surrogate key. This is a normalization step, not the final star schema (that
# happens in Gold).
# **Source:** `bronze.bronze_superstore`.
# **Output tables:** `silver.segment`, `silver.market`, `silver.ship_mode`, `silver.category`, `silver.geography`,
# `silver.date`, `silver.customer`, `silver.product`, `silver.sales`.

# CELL ********************

from pyspark.sql.functions import col, trim, monotonically_increasing_id, to_date, dayofmonth, month, year, quarter, date_format
from pyspark.sql.types import StringType, DoubleType, IntegerType

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read Bronze (raw, untouched)

# CELL ********************

bronze_raw = spark.table("bronze.bronze_superstore")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Standardize column names
# The source file's headers arrive as `Customer.ID`, `Order.Date`, `Sub.Category`, etc. — dot-separated rather than
# space-separated (a common artifact of exports produced through R/Power Query-style tools). A dot is especially
# unsafe in Spark: `col("Customer.ID")` is parsed as *table `Customer`, column `ID`* (a qualified field reference),
# not a literal column name, so it fails to resolve even though the column genuinely exists.
# Every column name is normalized once, immediately after reading Bronze and before any other step: trim leading
# and trailing whitespace, then replace `.`, ` ` (space), `-`, and `/` with `_`. Original casing is preserved
# (`Customer.ID` → `Customer_ID`). Everything downstream in this notebook then only ever deals with safe,
# predictable names.

# CELL ********************

def standardize_column_name(name):
    cleaned = name.strip()
    for character in (".", " ", "-", "/"):
        cleaned = cleaned.replace(character, "_")
    return cleaned

standardized_columns = [standardize_column_name(c) for c in bronze_raw.columns]
bronze_df = bronze_raw.toDF(*standardized_columns)

print("Standardized columns:", bronze_df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Data quality cleansing
# All value-level cleansing logic lives here:
# - Trim leading/trailing whitespace on every string column (done before dedup, so two rows that only differ by
#   whitespace are correctly treated as duplicates).
# - Remove exact duplicate rows.
# - Drop rows where `Order_ID` — the natural key of the dataset — is missing.

# CELL ********************

string_columns = [f.name for f in bronze_df.schema.fields if isinstance(f.dataType, StringType)]

df_trimmed = bronze_df
for column_name in string_columns:
    df_trimmed = df_trimmed.withColumn(column_name, trim(col(column_name)))

df_deduped = df_trimmed.dropDuplicates()

df_valid = df_deduped.filter(col("Order_ID").isNotNull())

print(f"Rows after cleansing: {df_valid.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Standardize data types
# `Order_Date` and `Ship_Date` arrive as text. Before parsing them, a handful of raw sample values are displayed so
# the date format assumption (`dd-MM-yyyy`, the standard Global Superstore export) can be verified against your
# actual file — adjust the format string below if the samples don't match.
# Numeric measures are explicitly cast rather than relying on CSV schema inference, and `Postal_Code` is cast to
# string to safely preserve values with leading zeros.

# CELL ********************

display(df_valid.select("Order_Date", "Ship_Date").limit(5))

silver_base = (
    df_valid
    .withColumn("order_date", to_date(col("Order_Date"), "dd-MM-yyyy"))
    .withColumn("ship_date", to_date(col("Ship_Date"), "dd-MM-yyyy"))
    .withColumn("Sales", col("Sales").cast(DoubleType()))
    .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    .withColumn("Discount", col("Discount").cast(DoubleType()))
    .withColumn("Profit", col("Profit").cast(DoubleType()))
    .withColumn("Shipping_Cost", col("Shipping_Cost").cast(DoubleType()))
    .withColumn("Postal_Code", col("Postal_Code").cast(StringType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Independent lookup tables
# These entities don't depend on any other dimension, so they're built first: `segment`, `market`, `ship_mode`, and
# `category` (which combines `Category` and `Sub_Category` from the source).
# > **Surrogate keys:** this notebook uses `monotonically_increasing_id()` to generate surrogate keys.
# > In production, surrogate keys are generally generated using sequences, identity columns, or maintained through
# > merge/SCD logic. `monotonically_increasing_id()` is used here only because this is a simple demo project — it
# > guarantees unique values but not contiguous or ordered ones.

# CELL ********************

# silver.segment
segment_df = (
    silver_base.select(col("Segment").alias("segment_name"))
    .filter(col("segment_name").isNotNull())
    .distinct()
)
silver_segment = segment_df.withColumn("segment_sk", monotonically_increasing_id()) \
    .select("segment_sk", "segment_name")
silver_segment.write.format("delta").mode("overwrite").saveAsTable("silver.segment")

# silver.market
market_df = (
    silver_base.select(col("Market").alias("market_name"))
    .filter(col("market_name").isNotNull())
    .distinct()
)
silver_market = market_df.withColumn("market_sk", monotonically_increasing_id()) \
    .select("market_sk", "market_name")
silver_market.write.format("delta").mode("overwrite").saveAsTable("silver.market")

# silver.ship_mode
ship_mode_df = (
    silver_base.select(col("Ship_Mode").alias("ship_mode_name"))
    .filter(col("ship_mode_name").isNotNull())
    .distinct()
)
silver_ship_mode = ship_mode_df.withColumn("ship_mode_sk", monotonically_increasing_id()) \
    .select("ship_mode_sk", "ship_mode_name")
silver_ship_mode.write.format("delta").mode("overwrite").saveAsTable("silver.ship_mode")

# silver.category (Category + Sub_Category)
category_df = (
    silver_base.select(col("Category").alias("category"), col("Sub_Category").alias("sub_category"))
    .filter(col("category").isNotNull())
    .distinct()
)
silver_category = category_df.withColumn(
    "category_sk", monotonically_increasing_id()
).select("category_sk", "category", "sub_category")
silver_category.write.format("delta").mode("overwrite").saveAsTable("silver.category")

print("segment, market, ship_mode, category written.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Geography and Date
# `geography` groups the location columns (`Country`, `State`, `City`, `Postal_Code`, `Region`). Postal_Code is
# frequently `NULL` outside the US in this dataset, so a null-safe key is used further down when resolving foreign
# keys for the sales table.
# `date` is a standard calendar dimension built from every distinct date found in **either** `Order_Date` or
# `Ship_Date`, so a single table can serve both roles.

# CELL ********************

# silver.geography
geography_df = (
    silver_base.select(
        col("Country").alias("country"),
        col("State").alias("state"),
        col("City").alias("city"),
        col("Postal_Code").alias("postal_code"),
        col("Region").alias("region"),
    )
    .filter(col("country").isNotNull())
    .distinct()
)
silver_geography = geography_df.withColumn(
    "geography_sk", monotonically_increasing_id()
).select("geography_sk", "country", "state", "city", "postal_code", "region")
silver_geography.write.format("delta").mode("overwrite").saveAsTable("silver.geography")

# silver.date
calendar_dates = (
    silver_base.select(col("order_date").alias("calendar_date"))
    .union(silver_base.select(col("ship_date").alias("calendar_date")))
    .filter(col("calendar_date").isNotNull())
    .distinct()
)
silver_date = (
    calendar_dates
    .withColumn("date_sk", monotonically_increasing_id())
    .withColumn("day", dayofmonth("calendar_date"))
    .withColumn("month", month("calendar_date"))
    .withColumn("month_name", date_format("calendar_date", "MMMM"))
    .withColumn("quarter", quarter("calendar_date"))
    .withColumn("year", year("calendar_date"))
    .withColumn("day_of_week", date_format("calendar_date", "EEEE"))
    .select("date_sk", "calendar_date", "day", "month", "month_name", "quarter", "year", "day_of_week")
)
silver_date.write.format("delta").mode("overwrite").saveAsTable("silver.date")

print("geography, date written.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — Dependent lookup tables
# `customer` references `segment`, and `product` references `category`. Each is joined to its parent lookup to
# resolve the surrogate key before being written.

# CELL ********************

# silver.customer (references silver.segment)
customer_base = (
    silver_base.select(
        col("Customer_ID").alias("customer_id"),
        col("Customer_Name").alias("customer_name"),
        col("Segment").alias("segment_name"),
    )
    .filter(col("customer_id").isNotNull())
    .distinct()
)
customer_with_fk = customer_base.join(silver_segment, on="segment_name", how="left")
silver_customer = customer_with_fk.withColumn(
    "customer_sk", monotonically_increasing_id()
).select("customer_sk", "customer_id", "customer_name", "segment_sk")
silver_customer.write.format("delta").mode("overwrite").saveAsTable("silver.customer")

# silver.product (references silver.category)
product_base = (
    silver_base.select(
        col("Product_ID").alias("product_id"),
        col("Product_Name").alias("product_name"),
        col("Category").alias("category"),
        col("Sub_Category").alias("sub_category"),
    )
    .filter(col("product_id").isNotNull())
    .distinct()
)
product_with_fk = product_base.join(silver_category, on=["category", "sub_category"], how="left")
silver_product = product_with_fk.withColumn(
    "product_sk", monotonically_increasing_id()
).select("product_sk", "product_id", "product_name", "category_sk")
silver_product.write.format("delta").mode("overwrite").saveAsTable("silver.product")

print("customer, product written.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8 — Sales (transaction grain)
# One row per source record, with every business attribute replaced by the surrogate key of its matching Silver
# lookup table. This preserves referential integrity: every foreign key in `silver.sales` has a matching row in its
# parent table.
# The geography join uses null-safe equality (`eqNullSafe`) because `postal_code` can legitimately be `NULL`.

# CELL ********************

customer_lookup = silver_customer.select("customer_id", "customer_sk")
product_lookup = silver_product.select("product_id", "product_sk")
geography_lookup = silver_geography.select("country", "state", "city", "postal_code", "geography_sk")
market_lookup = silver_market.select("market_name", "market_sk")
ship_mode_lookup = silver_ship_mode.select("ship_mode_name", "ship_mode_sk")
order_date_lookup = silver_date.select(col("calendar_date").alias("order_date"), col("date_sk").alias("order_date_sk"))
ship_date_lookup = silver_date.select(col("calendar_date").alias("ship_date"), col("date_sk").alias("ship_date_sk"))

silver_sales = (
    silver_base
    .join(customer_lookup, silver_base["Customer_ID"] == customer_lookup["customer_id"], "left")
    .join(product_lookup, silver_base["Product_ID"] == product_lookup["product_id"], "left")
    .join(
        geography_lookup,
        silver_base["Country"].eqNullSafe(geography_lookup["country"])
        & silver_base["State"].eqNullSafe(geography_lookup["state"])
        & silver_base["City"].eqNullSafe(geography_lookup["city"])
        & silver_base["Postal_Code"].eqNullSafe(geography_lookup["postal_code"]),
        "left",
    )
    .join(market_lookup, silver_base["Market"] == market_lookup["market_name"], "left")
    .join(ship_mode_lookup, silver_base["Ship_Mode"] == ship_mode_lookup["ship_mode_name"], "left")
    .join(order_date_lookup, on="order_date", how="left")
    .join(ship_date_lookup, on="ship_date", how="left")
    .select(
        col("Order_ID").alias("order_id"),
        "customer_sk",
        "product_sk",
        "geography_sk",
        "market_sk",
        "ship_mode_sk",
        "order_date_sk",
        "ship_date_sk",
        col("Sales").alias("sales"),
        col("Quantity").alias("quantity"),
        col("Discount").alias("discount"),
        col("Profit").alias("profit"),
        col("Shipping_Cost").alias("shipping_cost"),
        col("Order_Priority").alias("order_priority"),
    )
    .dropDuplicates()
)

silver_sales.write.format("delta").mode("overwrite").saveAsTable("silver.sales")

print(f"silver.sales written: {silver_sales.count()} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
