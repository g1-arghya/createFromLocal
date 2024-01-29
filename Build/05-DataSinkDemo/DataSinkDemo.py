from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("DataSinkDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Start Processing")

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    flightTimeParquetDF.show(5)

    # flightTimeParquetDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    # flightTimeParquetDF.write \
    #     .format("csv") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("path", "dataSink/json/") \
        .option("maxRecordsPerFile", 10000) \
        .save()

    logger.info("Num Partitions Before : " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("Num Partitions After : " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()


