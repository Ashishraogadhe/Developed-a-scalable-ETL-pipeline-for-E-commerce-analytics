from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, upper, trim
from config import paths

spark = SparkSession.builder.appName("TransformOrdersJoinCustomers").getOrCreate()

bronze_orders = spark.read.format("delta").load(paths.bronze_orders)
bronze_customers = spark.read.format("delta").load(paths.bronze_customers)

orders_clean = (
    bronze_orders
    .withColumn("order_ts", to_timestamp(col("order_datetime")))
    .withColumn("currency", upper(trim(col("currency"))))
    .dropDuplicates(["order_id"])
)

customers_clean = (
    bronze_customers
    .withColumn("country", upper(trim(col("country"))))
    .dropDuplicates(["customer_id"])
)

fact_orders = (
    orders_clean.alias("o")
    .join(
        customers_clean.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .select(
        col("o.order_id"),
        col("o.order_ts"),
        col("o.customer_id"),
        col("c.country").alias("customer_country"),
        col("o.total_amount"),
        col("o.currency"),
    )
)

(
    fact_orders.write.mode("overwrite")
    .format("delta")
    .partitionBy("customer_country")
    .save(paths.gold_fact_orders)
)
