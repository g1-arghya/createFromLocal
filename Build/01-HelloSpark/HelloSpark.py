import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    # spark = SparkSession.builder \
    # .appName("HelloSpark") \
    # .master("local[3]") \
    # .getOrCreate()

    # conf = SparkConf()
    # conf.set("spark.app.name", "HelloSpark")
    # conf.set("spark.master", "local[3]")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage : HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    # Get conf data
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    # Data Processing

    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)
    count_survey_df = count_by_country(partitioned_survey_df)
    count_survey_df.show()
    #logger.info(count_survey_df.collect())

    logger.info("Ending HelloSpark")

    #For debugging execution plan
    #input("Press Enter ")
    #spark.stop()
