from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col, year, month, dayofmonth

def create_dim_customers(spark):

    df = spark.read.parquet("data-lake/silver/customers")

    dim_customers = (
        df
        .withColumn("customer_sk", monotonically_increasing_id())
        .select(
            "customer_sk",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        )
    )

    (
        dim_customers
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/gold/dim_customers")
    )

def create_dim_date(spark):

    df = spark.read.parquet("data-lake/silver/orders")

    dim_date = (
        df
        .select("order_purchase_timestamp")
        .distinct()
        .withColumn("year", year(col("order_purchase_timestamp")))
        .withColumn("month", month(col("order_purchase_timestamp")))
        .withColumn("day", dayofmonth(col("order_purchase_timestamp")))
        .withColumnRenamed("order_purchase_timestamp", "date")
    )

    (
        dim_date
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/gold/dim_date")
    )

def create_fact_orders(spark):

    orders = spark.read.parquet("data-lake/silver/orders")
    dim_customers = spark.read.parquet("data-lake/gold/dim_customers")

    fact_orders = (
        orders
        .join(dim_customers,
              orders.customer_id == dim_customers.customer_id,
              "left")
        .select(
            col("order_id"),
            col("customer_sk"),
            col("order_purchase_timestamp").alias("purchase_date"),
            col("order_status")
        )
    )

    (
        fact_orders
        .write
        .mode("overwrite")
        .format("parquet")
        .save("data-lake/gold/fact_orders")
    )