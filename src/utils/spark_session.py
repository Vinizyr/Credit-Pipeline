from pyspark.sql import SparkSession

def get_spark():
    spark = (
        SparkSession.builder
        .appName("Credit-Pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    return spark
