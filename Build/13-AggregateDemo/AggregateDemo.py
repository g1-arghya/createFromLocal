from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("AggregateDemo") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Start Processing")

    invoicesDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/inv*")

    #invoicesDF.show(10)

    # invoicesDF.select(f.count("*").alias("Count 1"),
    #                   f.count("StockCode").alias("Count StockCode"),
    #                   f.sum("Quantity").alias("TotalQuantity"),
    #                   f.avg("UnitPrice").alias("AvgPrice"),
    #                   f.countDistinct("InvoiceNo")).show()

    invoicesDF.createOrReplaceTempView("invoicesVW")

    # summarySQL = spark.sql("""
    # select Country, InvoiceNo,
    #         sum(Quantity) as TotalQuantity,
    #         round(sum(Quantity * UnitPrice), 2) as InvoiceValue
    # from invoicesVW
    # group by Country, InvoiceNo""")

    # summarySQL.show()

    # Using Column Expressions
    # summaryDF = invoicesDF \
    #     .groupBy("Country", "InvoiceNo") \
    #     .agg(f.sum("Quantity").alias("TotalQuantity"),
    #          f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))

    # Exercise

    # exerciseSQL = spark.sql("""
    #     Select Country, WeekNumber,
    #            countDistinct(InvoiceNo) as NumInvoices,
    #            sum(Quantity) as TotalQuantity,
    #            round(sum(Quantity * UnitPrice), 2) as InvoiceValue
    #         from (Select Country, InvoiceNo, Quantity, UnitPrice,
    #                     weekofyear(to_date(InvoiceDate,"dd-MM-yyyy H.mm")) as WeekNumber
    #                from invoicesVW)
    #         group by Country, WeekNumber
    #     """)

    exerciseSQL = spark.sql("""
        Select Country, WeekNumber,
               count(distinct InvoiceNo) as NumInvoices,
               sum(Quantity) as TotalQuantity,
               round(sum(Quantity * UnitPrice), 2) as InvoiceValue 
               from (Select Country, InvoiceNo, Quantity, UnitPrice,
                        weekofyear(to_date(InvoiceDate,"dd-MM-yyyy H.mm")) as WeekNumber
                   from invoicesVW
                   where year(to_date(InvoiceDate,"dd-MM-yyyy H.mm")) = 2010)
               group by Country, WeekNumber
        """)

    exerciseSQL.sort("Country", "WeekNumber").show(10)
    # numInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    # totalQuantity = f.sum("Quantity").alias("TotalQuantity")
    # invoiceValue = f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValue")

    # exerciseDF = invoicesDF \
    #     .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
    #     .where("year(InvoiceDate) == 2010") \
    #     .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
    #     .groupBy("Country", "WeekNumber") \
    #     .agg(numInvoices, totalQuantity, invoiceValue)

    exerciseSQL.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")

    # exerciseDF.sort("Country", "WeekNumber").show(10)
