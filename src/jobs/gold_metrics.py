from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

def generate_metrics(spark):
    orders = spark.read.parquet("data-lake/gold/fact_orders")

    total_orders = orders.count()

    cancel_rate = orders.agg(
        (
            F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0))
            / F.count("*")
        ).alias("cancel_rate")
    ).collect()[0]["cancel_rate"]

    recurring_customers = (
        orders.groupBy("customer_sk")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Criando DataFrame único de métricas
    metrics_df = spark.createDataFrame([
        Row(
            reference_date=datetime.now(),
            total_orders=total_orders,
            cancel_rate=float(cancel_rate),
            recurring_customers=recurring_customers
        )
    ])

    # Salvando na Gold
    (
        metrics_df
        .write
        .mode("overwrite")
        .parquet("data-lake/gold/order_metrics")
    )

    print("Métricas salvas com sucesso em gold/order_metrics")
