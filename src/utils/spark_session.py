import os
from pyspark.sql import SparkSession

def get_spark(app_name="CreditPipeline"):
    # Configurações de ambiente
    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
    os.environ["SPARK_HOME"] = r"C:\spark\spark-3.5.8-bin-hadoop3"
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + \
                        os.environ["SPARK_HOME"] + r"\bin;" + \
                        os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

    # Cria e retorna SparkSession
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
