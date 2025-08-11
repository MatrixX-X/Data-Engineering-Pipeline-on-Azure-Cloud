# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Spark Config for ADLS Gen2

# COMMAND ----------

# Step 1: Spark Config for ADLS Gen2
storage_account = "oliststorageaccountdbda"
application_id = ""
directory_id = ""
client_secret = ""

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")


# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Read CSVs from ADLS Bronze Layer

# COMMAND ----------


base_path = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/bronze/"

orders_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_orders_dataset.csv")
payments_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_order_payments_dataset.csv")
reviews_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_order_reviews_dataset.csv")
items_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_order_items_dataset.csv")
customers_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_customers_dataset.csv")
sellers_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_sellers_dataset.csv")
geolocation_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_geolocation_dataset.csv")
products_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(base_path + "olist_products_dataset.csv")


# COMMAND ----------

display(orders_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: Clean DataFrames

# COMMAND ----------

def clean_dataframe(df, name):
    print(f"Cleaning {name}")
    return df.dropDuplicates().na.drop("all")

orders_df = clean_dataframe(orders_df, "orders")
payments_df = clean_dataframe(payments_df, "payments")
items_df = clean_dataframe(items_df, "items")
customers_df = clean_dataframe(customers_df, "customers")
products_df = clean_dataframe(products_df, "products")
sellers_df = clean_dataframe(sellers_df, "sellers")
reviews_df = clean_dataframe(reviews_df, "reviews")


# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4: Enrich orders_df with time calculations

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, when

orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

orders_df = orders_df.withColumn("actual_delivery_time", datediff("order_delivered_customer_date", "order_purchase_timestamp"))\
    .withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp"))\
    .withColumn("Delay Time", col("actual_delivery_time") - col("estimated_delivery_time"))\
    .withColumn("is_late", when(col("Delay Time") > 0, 1).otherwise(0))


# COMMAND ----------

display(orders_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5: Read MongoDB Enrichment Data (product_categories)

# COMMAND ----------


from pymongo import MongoClient
import pandas as pd

hostname = "jy73wr.h.filess.io"
database = "olistdatanosqldbda_recallflag"
port = "27018"
username = "olistdatanosqldbda_recallflag"
password = ""
uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"

client = MongoClient(uri)
collection = client[database]['product_categories']
mongo_data = pd.DataFrame(list(collection.find()))
mongo_data.drop('_id', axis=1, inplace=True)

mongo_spark_df = spark.createDataFrame(mongo_data)


# COMMAND ----------

display(mongo_spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6: Join Tables

# COMMAND ----------

# Clean and prepare each DataFrame
orders_df = orders_df.dropDuplicates()
customers_df = customers_df.dropDuplicates()
payments_df = payments_df.dropDuplicates()
items_df = items_df.dropDuplicates()
products_df = products_df.dropDuplicates()
sellers_df = sellers_df.dropDuplicates()
reviews_df = reviews_df.dropDuplicates()
mongo_spark_df = mongo_spark_df.dropDuplicates()

# Join 1: Orders + Customers
orders_customers_df = orders_df.join(customers_df, on="customer_id", how="left")

# Join 2: + Payments
orders_payments_df = orders_customers_df.join(payments_df, on="order_id", how="left")

# Join 3: + Items
orders_items_df = orders_payments_df.join(items_df, on="order_id", how="left")

# Join 4: + Products
orders_products_df = orders_items_df.join(products_df, on="product_id", how="left")

# Join 5: + Sellers
orders_sellers_df = orders_products_df.join(sellers_df, on="seller_id", how="left")

# Join 6: + Reviews
orders_reviews_df = orders_sellers_df.join(reviews_df, on="order_id", how="left")

# Join 7: + Product Category (from MongoDB)
final_df = orders_reviews_df.join(mongo_spark_df, on="product_category_name", how="left")



# COMMAND ----------

display(final_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extracted Date For Parquet Partitioning

# COMMAND ----------

from pyspark.sql.functions import to_date

final_df = final_df.withColumn("order_date", to_date("order_purchase_timestamp"))



# COMMAND ----------

display(final_df.limit(5).select("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 7: Save to Silver Layer (Partitioned Parquet)

# COMMAND ----------


final_df.write.mode("overwrite")\
    .partitionBy("order_date")\
    .parquet(f"abfss://olistdata@{storage_account}.dfs.core.windows.net/silver/olist_orders_partitioned")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Queries

# COMMAND ----------

from pyspark.sql.functions import count, avg

# Average delivery delay per state
delivery_insight = final_df.groupBy("customer_state") \
                           .agg(avg("Delay Time").alias("avg_delay"))
display(delivery_insight)
