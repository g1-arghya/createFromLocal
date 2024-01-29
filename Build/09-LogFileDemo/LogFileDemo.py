from pyspark.sql import *
from pyspark.sql.functions import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("LogFileDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    fileDF = spark.read \
        .format("text") \
        .load("data/apache_logs.txt")

    # fileDF.show(5)

    # log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^")*)'
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    logsDF = fileDF.select(regexp_extract('value', log_reg, 1).alias("ip"),
                           regexp_extract('value', log_reg, 4).alias("date"),
                           regexp_extract('value', log_reg, 6).alias("request"),
                           regexp_extract('value', log_reg, 10).alias("referrer")
                           )

    logsDF.printSchema()

    logsDF \
        .where("trim(referrer) != '-' ") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)
