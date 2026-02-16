import os
from pyspark.sql import SparkSession

def get_spark(app_name="CreditPipeline"):

    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
    os.environ["SPARK_HOME"] = r"C:\spark\spark-3.5.8-bin-hadoop3"
    os.environ["HADOOP_HOME"] = r"C:\hadoop"

    os.environ["PYSPARK_PYTHON"] = r"C:\Users\marcos.araujo\AppData\Local\Programs\Python\Python310\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\marcos.araujo\AppData\Local\Programs\Python\Python310\python.exe"

    os.environ["PATH"] = (
        os.environ["JAVA_HOME"] + r"\bin;" +
        os.environ["SPARK_HOME"] + r"\bin;" +
        os.environ["HADOOP_HOME"] + r"\bin;" +
        os.environ["PATH"]
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    return spark
