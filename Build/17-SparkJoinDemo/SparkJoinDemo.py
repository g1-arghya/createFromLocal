from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, coalesce

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkJoinDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Start Processing")

    ordersList = [("01", "02", 350, 1),
                  ("01", "04", 580, 1),
                  ("01", "07", 320, 2),
                  ("02", "03", 450, 1),
                  ("02", "06", 220, 1),
                  ("03", "01", 195, 1),
                  ("04", "09", 270, 3),
                  ("04", "08", 410, 2),
                  ("05", "02", 350, 1)]
    orderDF = spark.createDataFrame(ordersList).toDF("orderID", "prodID", "UnitPrice", "qty")

    productList = [("01", "Scroll Mouse", 250, 20),
                   ("02", "Optical Mouse", 350, 20),
                   ("03", "Wireless Mouse", 450, 20),
                   ("04", "Wireless Keyboard", 580, 20),
                   ("05", "Standard Keyboard", 360, 20),
                   ("06", "16 GB Flash Storage", 240, 20),
                   ("07", "32 GB Flash Storage", 320, 20),
                   ("08", "64 GB Flash Storage", 430, 20)]
    prodDF = spark.createDataFrame(productList).toDF("prodID", "prodName", "listPrice", "qty")

    joinExpr = orderDF.prodID == prodDF.prodID
    orderDF.join(prodDF, on=joinExpr, how="left") \
        .select(orderDF.orderID, orderDF.prodID, prodDF.prodName, orderDF.UnitPrice, prodDF.listPrice, orderDF.qty) \
        .withColumn("prodName", expr("coalesce(prodName,'NA')")) \
        .sort(orderDF.orderID) \
        .show()

