import re
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logger import Log4j


def parse_gender(gender):
    female_pattern = r'^f$|f.m|w.m'
    male_pattern = r'^m$|m.l|ma'

    if re.search(female_pattern, gender.lower()):
        return 'Female'
    elif re.search(male_pattern, gender.lower()):
        return 'Male'
    else:
        return 'Unknown'


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("LogFileDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    surveyDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/survey.csv")

    surveyDF.show(10)
    # udf method only converts to a dataframe udf and serializes it for executors.
    # It doesn't register the function in the catalog.
    parse_gender_udf = udf(parse_gender)
    logger.info("Catalog Entry Before: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    surveyDF2 = surveyDF.withColumn("Gender", parse_gender_udf("Gender"))
    surveyDF2.show(10)

    # to use the function as a sql function, need to register the udf in the catalog
    spark.udf.register("parse_gender_udf", parse_gender)
    logger.info("Catalog Entry After: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    surveyDF3 = surveyDF.withColumn("Gender", expr("parse_gender_udf(Gender)"))
