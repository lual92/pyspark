from pyspark.sql import SparkSession

def create_session():
    NUMERO_MAXIMO_CPUS = 3
    SPARK_MASTER = "local[" + str(NUMERO_MAXIMO_CPUS) + "]"

    spark = SparkSession \
        .builder \
        .master(SPARK_MASTER) \
        .appName("App") \
        .getOrCreate()
    return spark
