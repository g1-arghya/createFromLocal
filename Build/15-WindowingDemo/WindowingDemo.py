from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("WindowingDemo") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Start Processing")

    summaryDF = spark.read \
        .format("parquet") \
        .load("data/sum*.parquet")
    # summaryDF.show(5)
    running_tot_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDF.withColumn("RunningTotal", \
                         f.round(f.sum("InvoiceValue").over(running_tot_window), 2)
                         ) \
        .sort("Country", "WeekNumber") \
        .show(10)


