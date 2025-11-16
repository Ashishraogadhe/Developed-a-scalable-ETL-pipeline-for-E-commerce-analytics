import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from config import paths

spark = SparkSession.builder.appName("ExtractOrdersAPI").getOrCreate()

ORDERS_API_URL = "https://api.example.com/orders"

api_key = dbutils.secrets.get("ecommerce-scope", "orders-api-key")

resp = requests.get(
    ORDERS_API_URL,
    headers={"Authorization": f"Bearer {api_key}"}
)
resp.raise_for_status()
orders = resp.json()  # assume list[dict]

df_orders = spark.createDataFrame(orders).withColumn(
    "ingested_at_utc", current_timestamp()
)

(
    df_orders.write.mode("append")
    .format("delta")
    .save(paths.bronze_orders)
)
