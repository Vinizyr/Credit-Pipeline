from src.jobs.gold_metrics import generate_metrics
from src.utils.spark_session import get_spark
from src.jobs.bronze_to_silver import (
    bronze_customers_to_silver, 
    landing_customers_to_bronze, 
    landing_to_bronze, 
    bronze_to_silver
)
from src.jobs.silver_to_gold import (
    create_dim_customers,
    create_dim_date,
    create_fact_orders
)

def main():

    spark = get_spark()

    landing_to_bronze(spark)
    bronze_to_silver(spark)
    landing_customers_to_bronze(spark)
    bronze_customers_to_silver(spark)
    create_dim_customers(spark)
    create_dim_date(spark)
    create_fact_orders(spark)
    generate_metrics(spark)

    spark.stop()

if __name__ == "__main__":
    main()
