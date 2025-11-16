from dataclasses import dataclass

@dataclass
class Paths:
    base: str = "abfss://ecommerce@yourstorageaccount.dfs.core.windows.net"
    bronze_orders: str = f"{base}/bronze/orders"
    bronze_customers: str = f"{base}/bronze/customers"
    silver_orders: str = f"{base}/silver/orders"
    silver_customers: str = f"{base}/silver/customers"
    gold_fact_orders: str = f"{base}/gold/fact_orders"

paths = Paths()
