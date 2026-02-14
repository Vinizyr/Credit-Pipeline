from src.utils.spark_session import get_spark
from src.jobs.bronze_to_silver import landing_to_bronze, bronze_to_silver

def main():

    spark = get_spark()

    landing_to_bronze(spark)
    bronze_to_silver(spark)

    spark.stop()

if __name__ == "__main__":
    main()
