from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[3]") \
        .getOrCreate()

    df_1 = spark.read.format("csv").\
        option("header", "true").\
        option("inferSchema", "true").\
        load("data/sample.csv")

    df_1.show(5)

    spark.stop()

