# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable



# DBTITLE 1,Run utilities notebook
#  %run /Consolidated_pipeline/1_setup/utilities


print(bronze_schema, silver_schema, gold_schema)



dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")


catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
print(catalog, data_source)

base_path = f's3://databricks-fmcg01/{data_source}/*.csv'
print(base_path)



df = spark.read.format("csv").load(base_path)
display(df.limit(10))



df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)
display(df.limit(10))


df.printSchema()

bronze_schema = "bronze"
df.write\
.format("delta")\
.option("delta.enableChangeDataFeed", "true") \
.mode("overwrite")\
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")



#  %md
# MAGIC ### Silver Processing
# 


df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)


df_bronze.printSchema()


df_duplicates = df_bronze.groupBy("customer_id").count().filter("count > 1")
display(df_duplicates)


### Removing the Duplicates
print('Rows before duplicates droped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates droped: ', df_silver.count())



### Removing the unwanted Spaces between the words 
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)


df_silver = df_silver.withColumn("customer_name", F.trim(F.col("customer_name")))


display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

df_silver.select('city').distinct().show()


# fixing the typos
city_mapping = {
    'Bangalore': 'Bengaluru',
    'Bangaluruu': 'Bengaluru',
   
    'NewDelhee': 'New Delhi',
    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',

    'Hyderbad': 'Hyderabad',
    'Hyderabadd': 'Hyderabad',
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(None)
    )
)

df_silver.select('city').distinct().show()


df_silver.select('customer_name').distinct().show()

# Title Case Fixing, making Customer anme 1st letter Capital

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap("customer_name"))
)

df_silver.select('customer_name').distinct().show()


df_silver.filter(F.col("city").isNull()).show(truncate=False)


# Sanity Check
null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)


customer_city_fix = {
    #Sprintx Nutrition
    789403: "New Delhi",
    #Zenathlete Foods
    789420: "Bengaluru",
    #Primefuel Nutrition
    789521: "Hyderabad",
    #Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)


df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   #Replacing null with Fixed City
    )
    .drop("fixed_city")
)

display(df_silver)

# Sanity Check
null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)


df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

df_silver = (
    df_silver
    # BUild final customer column: "CustomerName-City" or "customerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )

    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

display(df_silver.limit(5))


df_silver.write\
    .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")


## Gold Processing


df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source}")


# We will take required cols only
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# Merginng the dim_customers and sb_dim_customers
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)


# NOw we will do Upsert operation to customer_code
# if the customer code in target table matches in the parent then update, if it doesn't have a code in target table, then insert
delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()