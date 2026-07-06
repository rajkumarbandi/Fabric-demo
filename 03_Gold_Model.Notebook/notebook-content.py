# Fabric notebook source


# MARKDOWN ********************

# # 03 - Gold Model
# # **Layer purpose:** present the Silver tables as a proper star schema for the Power BI semantic model — one fact
# table surrounded by single-purpose dimensions, each referenced directly from the fact by surrogate key.
# # **Source:** `silver.*` tables.
# # **Output tables:** `gold.dim_customer`, `gold.dim_product`, `gold.dim_category`, `gold.dim_segment`,
# `gold.dim_market`, `gold.dim_shipmode`, `gold.dim_geography`, `gold.dim_date`, `gold.fact_sales`.
# # Dimensions are kept pure — each holds only its own attributes, with no attributes borrowed from another entity
# (for example, `dim_customer` does not carry the customer's segment). Instead, `fact_sales` links to every
# dimension directly, including `dim_category` and `dim_segment`, so no dimension needs to be denormalized.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Dimensions
# # Each dimension is a direct, pure copy of its Silver lookup table — the surrogate keys generated in Silver are
# reused as-is, since Silver already normalized these entities correctly.

# CELL ********************

gold_dim_customer = spark.table("silver.customer").select("customer_sk", "customer_id", "customer_name")
gold_dim_customer.write.format("delta").mode("overwrite").saveAsTable("gold.dim_customer")

gold_dim_product = spark.table("silver.product").select("product_sk", "product_id", "product_name")
gold_dim_product.write.format("delta").mode("overwrite").saveAsTable("gold.dim_product")

gold_dim_category = spark.table("silver.category").select("category_sk", "category", "sub_category")
gold_dim_category.write.format("delta").mode("overwrite").saveAsTable("gold.dim_category")

gold_dim_segment = spark.table("silver.segment").select("segment_sk", "segment_name")
gold_dim_segment.write.format("delta").mode("overwrite").saveAsTable("gold.dim_segment")

gold_dim_market = spark.table("silver.market").select("market_sk", "market_name")
gold_dim_market.write.format("delta").mode("overwrite").saveAsTable("gold.dim_market")

gold_dim_shipmode = spark.table("silver.ship_mode").select("ship_mode_sk", "ship_mode_name")
gold_dim_shipmode.write.format("delta").mode("overwrite").saveAsTable("gold.dim_shipmode")

gold_dim_geography = spark.table("silver.geography") \
    .select("geography_sk", "country", "state", "city", "postal_code", "region")
gold_dim_geography.write.format("delta").mode("overwrite").saveAsTable("gold.dim_geography")

gold_dim_date = spark.table("silver.date") \
    .select("date_sk", "calendar_date", "day", "month", "month_name", "quarter", "year", "day_of_week")
gold_dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

print("All dimension tables written.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Fact table
# # `silver.sales` already carries `customer_sk` and `product_sk`. To let the fact table reference **every** dimension
# directly (rather than only reaching category/segment indirectly through product/customer), `category_sk` and
# `segment_sk` are pulled in here via a lookup join to `silver.product` and `silver.customer`.
# # `order_date_sk` and `ship_date_sk` both point at the same `gold.dim_date` table — this is a role-playing
# dimension. In Power BI, keep the relationship on `order_date_sk` active and mark the `ship_date_sk` relationship
# inactive (or use `USERELATIONSHIP` in DAX measures that need it).

# CELL ********************

silver_sales = spark.table("silver.sales")
customer_segment = spark.table("silver.customer").select("customer_sk", "segment_sk")
product_category = spark.table("silver.product").select("product_sk", "category_sk")

gold_fact_sales = (
    silver_sales
    .join(customer_segment, on="customer_sk", how="left")
    .join(product_category, on="product_sk", how="left")
    .select(
        "order_id",
        "customer_sk",
        "product_sk",
        "category_sk",
        "segment_sk",
        "geography_sk",
        "market_sk",
        "ship_mode_sk",
        "order_date_sk",
        "ship_date_sk",
        "sales",
        "quantity",
        "discount",
        "profit",
        "shipping_cost",
        "order_priority",
    )
)

gold_fact_sales.write.format("delta").mode("overwrite").saveAsTable("gold.fact_sales")

print(f"gold.fact_sales written: {gold_fact_sales.count()} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Star schema summary
# # ```
#                        dim_customer
#                        dim_segment
#                        dim_product
#                        dim_category      \
#                        dim_geography      >---  fact_sales
#                        dim_market        /
#                        dim_shipmode
#                        dim_date (order_date_sk, ship_date_sk)
# ```
# # `gold.fact_sales` is now ready to be used as the source for the Power BI semantic model (Task 4).
