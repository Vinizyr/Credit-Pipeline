from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, lower, trim
from src.schemas.orders_schema import orders_schema
from src.schemas.customers_schema import customers_schema

def landing_to_bronze(spark):

    df = (
        spark.read
        .format("csv")
        .schema(orders_schema)
        .option("header", "true")
        .load("data-lake/landing/olist_orders_dataset.csv")
    )

    df_bronze = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit("olist_orders_dataset.csv"))
    )

    (
        df_bronze
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/bronze/orders")
    )

def bronze_to_silver(spark):
    df = spark.read.parquet("data-lake/bronze/orders")

    df_silver = (
        df
        .withColumn("order_purchase_timestamp",
                    to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",
                    to_timestamp("order_approved_at"))
        .withColumn("order_status",
                    lower(trim(col("order_status"))))
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
    )

    (
        df_silver
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/silver/orders")
    )

def landing_customers_to_bronze(spark):

    df = (
        spark.read
        .format("csv")
        .schema(customers_schema)
        .option("header", "true")
        .load("data-lake/landing/olist_customers_dataset.csv")
    )

    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/bronze/customers")
    )

def bronze_customers_to_silver(spark):

    df = spark.read.parquet("data-lake/bronze/customers")

    df_silver = (
        df
        .dropDuplicates(["customer_id"])
        .filter(df.customer_id.isNotNull())
    )

    (
        df_silver
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/silver/customers")
    )