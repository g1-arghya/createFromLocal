import configparser
from pyspark import SparkConf

'''
    This function will read the spark config file 
    and return a spark conf object 
'''


def get_spark_app_config():
    spark_conf = SparkConf()  # Create a Spark config object
    config = configparser.ConfigParser()  # Read the configs from the file
    config.read("spark.conf")

    # Loop through the config and set it to spark_conf
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf


def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)

def count_by_country(survey_df):
    return survey_df.where("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


