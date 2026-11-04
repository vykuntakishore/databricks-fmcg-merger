# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


#  %run /Workspace/Consolidated_pipeline/1_setup/utilities

print(bronze_schema, silver_schema, gold_schema)


dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://databricks-fmcg01/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path:", base_path)
print("Landing Path:", landing_path)
print("Processed Path:", processed_path)

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"


df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows:", df.count())
df.show(5)


display(df.limit(20))


df.write\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.mode("append")\
.saveAsTable(bronze_table)


files = dbutils.fs.ls(landing_path)

for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )


df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.show(2)


# Kepp only rows where order_qty is present
df = df_orders.filter(F.col("order_qty").isNotNull())
 
# Clean customer_id : keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# Remove weekday name from the date text
# "Tuesday, July 01, 2025" → "July 01, 2025"

df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# Drop duplicates
df_orders = df_orders.dropDuplicates(
    ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]
)

# Convert product_id to string
df_orders = df_orders.withColumn('product_id', F.col('product_id').cast('string'))


# Check what's the maximum and minimum date
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()


display(df_orders.limit(20))


df_products = spark.table("fmcg.silver.products")

display(df_products.limit(5))


df_joined = df_orders.join(
    df_products,
    on="product_id",
    how="inner"
).select(
    df_orders["*"],
    df_products["product_code"]
)

display(df_joined.limit(10))


if not spark.catalog.tableExists(silver_table):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)

else:
    silver_delta = DeltaTable.forName(spark, silver_table)

    silver_delta.alias("silver").merge(
        df_joined.alias("bronze"),
        """
        silver.order_placement_date = bronze.order_placement_date
        AND silver.order_id = bronze.order_id
        AND silver.product_code = bronze.product_code
        AND silver.customer_id = bronze.customer_id
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# %md
# #Gold



df_gold = spark.sql(f"""
    SELECT
        order_id,
        order_placement_date AS date,
        customer_id AS customer_code,
        product_code,
        product_id,
        order_qty AS sold_quantity
    FROM {silver_table}
""")

df_gold.show(3)


gold_table


if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)

else:
    gold_delta = DeltaTable.forName(spark, gold_table)

    gold_delta.alias("source").merge(
        df_gold.alias("gold"),
        "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# %md
# ### Merge with Parent Company


df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")

df_child.show(10)


df_child.count()


# first change the date to first day of month. 
# 2025-07-12 -->  2025-07-1
# 2025-07-13 -->  2025-07-1


df_monthly = (
    df_child
    # 1. Get month start date (2025-11-30 -> 2025-11-01)
    .withColumn("month_start", F.trunc("date", "MM"))   # or F.date_trunc("month", "date").cast("date")

    # 2. Group at monthly grain by month_start + product_code + customer_code
    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )
    # 3. Rename month_start back to 'date' to match your target schema
    .withColumnRenamed("month_start", "date")
)

display(df_monthly.limit(10))

df_monthly.count()

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

(
gold_parent_delta.alias("parent_gold")
    .merge(df_monthly.alias("child_gold"), """parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code""")
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
)
