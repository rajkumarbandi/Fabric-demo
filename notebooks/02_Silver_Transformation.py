# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 02 - Silver Transformation
#
# **Layer purpose:** turn the single flat `bronze.bronze_superstore` table into a set of standardized, conformed
# tables — one per business entity — each with a meaningful surrogate key. This is a normalization step, not the
# final star schema (that happens in Gold).
#
# **Source:** `bronze.bronze_superstore`.
#
# **Output tables:** `silver.segment`, `silver.market`, `silver.ship_mode`, `silver.category`, `silver.geography`,
# `silver.date`, `silver.customer`, `silver.product`, `silver.sales`.
#
# Surrogate keys are generated with `row_number()` over a deterministic ordering, which keeps them stable across
# repeated full-overwrite runs without needing merge/SCD logic.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, row_number, to_date, dayofmonth, month, year, quarter, date_format
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read Bronze and parse dates
#
# `Order Date` and `Ship Date` arrive as text (Global Superstore export format `dd-MM-yyyy`). They are parsed into
# native `date` columns once here, so every downstream table uses the same values.
#
# > Adjust the date format string below if your source file uses a different date pattern.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = (
    spark.table("bronze.bronze_superstore")
    .withColumn("order_date", to_date(col("Order Date"), "dd-MM-yyyy"))
    .withColumn("ship_date", to_date(col("Ship Date"), "dd-MM-yyyy"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Independent lookup tables
#
# These entities don't depend on any other dimension, so they're built first: `segment`, `market`, `ship_mode`, and
# `category` (which combines `Category` and `Sub-Category` from the source).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# silver.segment
segment_df = (
    bronze_df.select(col("Segment").alias("segment_name"))
    .filter(col("segment_name").isNotNull())
    .distinct()
)
silver_segment = segment_df.withColumn("segment_sk", row_number().over(Window.orderBy("segment_name"))) \
    .select("segment_sk", "segment_name")
silver_segment.write.format("delta").mode("overwrite").saveAsTable("silver.segment")

# silver.market
market_df = (
    bronze_df.select(col("Market").alias("market_name"))
    .filter(col("market_name").isNotNull())
    .distinct()
)
silver_market = market_df.withColumn("market_sk", row_number().over(Window.orderBy("market_name"))) \
    .select("market_sk", "market_name")
silver_market.write.format("delta").mode("overwrite").saveAsTable("silver.market")

# silver.ship_mode
ship_mode_df = (
    bronze_df.select(col("Ship Mode").alias("ship_mode_name"))
    .filter(col("ship_mode_name").isNotNull())
    .distinct()
)
silver_ship_mode = ship_mode_df.withColumn("ship_mode_sk", row_number().over(Window.orderBy("ship_mode_name"))) \
    .select("ship_mode_sk", "ship_mode_name")
silver_ship_mode.write.format("delta").mode("overwrite").saveAsTable("silver.ship_mode")

# silver.category (Category + Sub-Category)
category_df = (
    bronze_df.select(col("Category").alias("category"), col("Sub-Category").alias("sub_category"))
    .filter(col("category").isNotNull())
    .distinct()
)
silver_category = category_df.withColumn(
    "category_sk", row_number().over(Window.orderBy("category", "sub_category"))
).select("category_sk", "category", "sub_category")
silver_category.write.format("delta").mode("overwrite").saveAsTable("silver.category")

print("segment, market, ship_mode, category written.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Geography and Date
#
# `geography` groups the location columns (`Country`, `State`, `City`, `Postal Code`, `Region`). Postal Code is
# frequently `NULL` outside the US in this dataset, so a null-safe key is used further down when resolving foreign
# keys for the sales table.
#
# `date` is a standard calendar dimension built from every distinct date found in **either** `Order Date` or
# `Ship Date`, so a single table can serve both roles.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# silver.geography
geography_df = (
    bronze_df.select(
        col("Country").alias("country"),
        col("State").alias("state"),
        col("City").alias("city"),
        col("Postal Code").alias("postal_code"),
        col("Region").alias("region"),
    )
    .filter(col("country").isNotNull())
    .distinct()
)
silver_geography = geography_df.withColumn(
    "geography_sk", row_number().over(Window.orderBy("country", "state", "city", "postal_code"))
).select("geography_sk", "country", "state", "city", "postal_code", "region")
silver_geography.write.format("delta").mode("overwrite").saveAsTable("silver.geography")

# silver.date
calendar_dates = (
    bronze_df.select(col("order_date").alias("calendar_date"))
    .union(bronze_df.select(col("ship_date").alias("calendar_date")))
    .filter(col("calendar_date").isNotNull())
    .distinct()
)
silver_date = (
    calendar_dates
    .withColumn("date_sk", row_number().over(Window.orderBy("calendar_date")))
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

# ## Step 4 — Dependent lookup tables
#
# `customer` references `segment`, and `product` references `category`. Each is joined to its parent lookup to
# resolve the surrogate key before being written.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# silver.customer (references silver.segment)
customer_base = (
    bronze_df.select(
        col("Customer ID").alias("customer_id"),
        col("Customer Name").alias("customer_name"),
        col("Segment").alias("segment_name"),
    )
    .filter(col("customer_id").isNotNull())
    .distinct()
)
customer_with_fk = customer_base.join(silver_segment, on="segment_name", how="left")
silver_customer = customer_with_fk.withColumn(
    "customer_sk", row_number().over(Window.orderBy("customer_id"))
).select("customer_sk", "customer_id", "customer_name", "segment_sk")
silver_customer.write.format("delta").mode("overwrite").saveAsTable("silver.customer")

# silver.product (references silver.category)
product_base = (
    bronze_df.select(
        col("Product ID").alias("product_id"),
        col("Product Name").alias("product_name"),
        col("Category").alias("category"),
        col("Sub-Category").alias("sub_category"),
    )
    .filter(col("product_id").isNotNull())
    .distinct()
)
product_with_fk = product_base.join(silver_category, on=["category", "sub_category"], how="left")
silver_product = product_with_fk.withColumn(
    "product_sk", row_number().over(Window.orderBy("product_id"))
).select("product_sk", "product_id", "product_name", "category_sk")
silver_product.write.format("delta").mode("overwrite").saveAsTable("silver.product")

print("customer, product written.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Sales (transaction grain)
#
# One row per source record, with every business attribute replaced by the surrogate key of its matching Silver
# lookup table. This preserves referential integrity: every foreign key in `silver.sales` has a matching row in its
# parent table.
#
# The geography join uses null-safe equality (`eqNullSafe`) because `postal_code` can legitimately be `NULL`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_lookup = silver_customer.select("customer_id", "customer_sk")
product_lookup = silver_product.select("product_id", "product_sk")
geography_lookup = silver_geography.select("country", "state", "city", "postal_code", "geography_sk")
market_lookup = silver_market.select("market_name", "market_sk")
ship_mode_lookup = silver_ship_mode.select("ship_mode_name", "ship_mode_sk")
order_date_lookup = silver_date.select(col("calendar_date").alias("order_date"), col("date_sk").alias("order_date_sk"))
ship_date_lookup = silver_date.select(col("calendar_date").alias("ship_date"), col("date_sk").alias("ship_date_sk"))

silver_sales = (
    bronze_df
    .join(customer_lookup, bronze_df["Customer ID"] == customer_lookup["customer_id"], "left")
    .join(product_lookup, bronze_df["Product ID"] == product_lookup["product_id"], "left")
    .join(
        geography_lookup,
        bronze_df["Country"].eqNullSafe(geography_lookup["country"])
        & bronze_df["State"].eqNullSafe(geography_lookup["state"])
        & bronze_df["City"].eqNullSafe(geography_lookup["city"])
        & bronze_df["Postal Code"].eqNullSafe(geography_lookup["postal_code"]),
        "left",
    )
    .join(market_lookup, bronze_df["Market"] == market_lookup["market_name"], "left")
    .join(ship_mode_lookup, bronze_df["Ship Mode"] == ship_mode_lookup["ship_mode_name"], "left")
    .join(order_date_lookup, on="order_date", how="left")
    .join(ship_date_lookup, on="ship_date", how="left")
    .select(
        col("Order ID").alias("order_id"),
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
        col("Shipping Cost").alias("shipping_cost"),
        col("Order Priority").alias("order_priority"),
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
