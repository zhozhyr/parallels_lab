from pyspark.sql import SparkSession, functions as F, types as T

spark = (
    SparkSession.builder
    .appName("Lab 2 (PySpark)")
    .getOrCreate()
)

# Загрузка
DATA_PATH = "data.csv"

schema = T.StructType([
    T.StructField("InvoiceNo",   T.StringType(),  True),
    T.StructField("StockCode",   T.StringType(),  True),
    T.StructField("Description", T.StringType(),  True),
    T.StructField("Quantity",    T.IntegerType(), True),
    T.StructField("InvoiceDate", T.StringType(),  True),
    T.StructField("UnitPrice",   T.DoubleType(),  True),
    T.StructField("CustomerID",  T.IntegerType(),  True),
    T.StructField("Country",     T.StringType(),  True),
])

df_raw = (
    spark.read
         .option("header", True)
         .schema(schema)
         .csv(DATA_PATH)
)

df = (
    df_raw
    .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "dd/MM/yyyy HH:mm"))
    .withColumn("TotalPrice", F.col("Quantity") * F.col("UnitPrice"))
)


clean = (
    df.filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
)

# Топ-5 товаров по проданным единицам
top5_products = (
    clean.groupBy("StockCode", "Description")
         .agg(F.sum("Quantity").alias("units_sold"))
         .orderBy(F.desc("units_sold"))
         .limit(5)
)


# Метрики по каждому клиенту
orders = (
    clean.filter(F.col("CustomerID").isNotNull())
         .groupBy("CustomerID", "InvoiceNo")
         .agg(F.sum("TotalPrice").alias("order_total"))
)

per_customer = (
    orders.groupBy("CustomerID")
          .agg(
              F.countDistinct("InvoiceNo").alias("orders_count"),
              F.sum("order_total").alias("total_spent")
          )
          .withColumn("avg_check",
                      F.round(F.col("total_spent") / F.col("orders_count"), 2))
          .orderBy(F.desc("total_spent"))
)


top5_products.write.mode("overwrite").option("header", True).csv("/tmp/out/top5_products_csv")
per_customer.write.mode("overwrite").option("header", True).csv("/tmp/out/per_customer_csv")


spark.stop()
