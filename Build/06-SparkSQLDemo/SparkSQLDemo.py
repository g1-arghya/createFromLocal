from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSQLDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Start Processing")

    flightTimeParquetDF = \
        spark.read \
            .format("parquet") \
            .load("dataSource/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # flightTimeParquetDF.write \
    #     .mode("overwrite") \
    #     .partitionBy("ORIGIN", "OP_CARRIER") \
    #     .saveAsTable("flight_data_tbl")

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
