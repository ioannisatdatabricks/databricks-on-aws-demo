# Databricks notebook source
# MAGIC %md
# MAGIC # Setup & Data Generation
# MAGIC
# MAGIC This notebook creates the catalog/schema/volume structure and generates synthetic
# MAGIC e-commerce data (orders, customers, products, clickstream) used throughout the demo.
# MAGIC
# MAGIC **Run this notebook once before starting the demo.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Create Catalog / Schema / Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}
  COMMENT 'Raw landing zone for AWS webinar demo data'
""")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
print(f"Volume path: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Synthetic Data Generation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random, string
from datetime import datetime, timedelta

NUM_CUSTOMERS = 5_000
NUM_PRODUCTS  = 200
NUM_ORDERS    = 50_000
NUM_CLICKS    = 200_000

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md ### 2a — Customers (CDC / Parquet)

# COMMAND ----------

import pandas as pd
import numpy as np

np.random.seed(42)

countries = ["US", "US", "US", "US", "GB", "DE", "FR", "CA", "AU", "JP"]
segments  = ["Premium", "Standard", "Budget"]

customers_pd = pd.DataFrame({
    "customer_id":    [f"C{i:05d}" for i in range(1, NUM_CUSTOMERS + 1)],
    "first_name":     np.random.choice(["Alice","Bob","Carol","David","Eve","Frank","Grace","Hank","Iris","Jack"], NUM_CUSTOMERS),
    "last_name":      ["".join(random.choices(string.ascii_uppercase, k=6)) for _ in range(NUM_CUSTOMERS)],
    "email":          [f"user{i}@shopnow.example.com" for i in range(1, NUM_CUSTOMERS + 1)],
    "country":        np.random.choice(countries, NUM_CUSTOMERS),
    "segment":        np.random.choice(segments, NUM_CUSTOMERS, p=[0.2, 0.5, 0.3]),
    "signup_date":    pd.date_range("2022-01-01", periods=NUM_CUSTOMERS, freq="1h").strftime("%Y-%m-%d"),
    "is_active":      np.random.choice([True, True, True, False], NUM_CUSTOMERS),
    "_change_type":   "insert",
    "_commit_version": 1,
})

customers_df = spark.createDataFrame(customers_pd)
customers_df.write.mode("overwrite").parquet(f"{volume_path}/customers/")
print(f"Wrote {NUM_CUSTOMERS} customer records")

# COMMAND ----------

# MAGIC %md ### 2b — Products (JSON)

# COMMAND ----------

categories = {
    "Electronics":  (50,  1500),
    "Clothing":     (10,  200),
    "Home & Garden":(15,  500),
    "Sports":       (20,  800),
    "Books":        (5,   60),
}

products_rows = []
for pid in range(1, NUM_PRODUCTS + 1):
    cat  = random.choice(list(categories.keys()))
    lo, hi = categories[cat]
    products_rows.append({
        "product_id":   f"P{pid:04d}",
        "name":         f"{cat} Item {pid}",
        "category":     cat,
        "brand":        random.choice(["BrandA","BrandB","BrandC","BrandD","BrandE"]),
        "price":        round(random.uniform(lo, hi), 2),
        "cost":         round(random.uniform(lo * 0.4, hi * 0.6), 2),
        "stock_qty":    random.randint(0, 500),
        "rating":       round(random.uniform(3.0, 5.0), 1),
    })

products_df = spark.createDataFrame(products_rows)
products_df.write.mode("overwrite").json(f"{volume_path}/products/")
print(f"Wrote {NUM_PRODUCTS} product records")

# COMMAND ----------

# MAGIC %md ### 2c — Orders (CSV, incremental batches)

# COMMAND ----------

statuses = ["completed","completed","completed","returned","cancelled","pending"]
cids = [f"C{i:05d}" for i in range(1, NUM_CUSTOMERS + 1)]
pids = [f"P{i:04d}" for i in range(1, NUM_PRODUCTS + 1)]

# Date range: one year ending today so the last possible order date is current date
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
date_range_sec = (end_date - start_date).total_seconds()

orders_rows = []
for oid in range(1, NUM_ORDERS + 1):
    qty   = random.randint(1, 5)
    price = round(random.uniform(5, 1500), 2)
    # Spread orders uniformly from start_date to end_date (last order ≈ current date)
    offset_sec = date_range_sec * (oid - 1) / max(1, NUM_ORDERS - 1)
    order_dt = start_date + timedelta(seconds=offset_sec)
    orders_rows.append({
        "order_id":    f"O{oid:07d}",
        "customer_id": random.choice(cids),
        "product_id":  random.choice(pids),
        "quantity":    qty,
        "unit_price":  price,
        "total_amount":round(qty * price, 2),
        "status":      random.choice(statuses),
        "order_date":  order_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "ship_country":random.choice(countries),
    })

orders_df = spark.createDataFrame(orders_rows)
# split into two batches to simulate incremental arrival
batch1 = orders_df.filter(F.col("order_id") <= f"O{NUM_ORDERS // 2:07d}")
batch2 = orders_df.filter(F.col("order_id") >  f"O{NUM_ORDERS // 2:07d}")
batch1.write.mode("overwrite").option("header", True).csv(f"{volume_path}/orders/batch1/")
batch2.write.mode("overwrite").option("header", True).csv(f"{volume_path}/orders/batch2/")
print(f"Wrote {NUM_ORDERS} order records across 2 batches")

# COMMAND ----------

# MAGIC %md ### 2d — Clickstream (JSON streaming source)

# COMMAND ----------

events = ["view_product","add_to_cart","remove_from_cart","checkout","purchase","search","home_page"]
clicks_rows = []
for i in range(1, NUM_CLICKS + 1):
    # Spread clickstream events uniformly from start_date to end_date (last event ≈ current date)
    offset_sec = date_range_sec * (i - 1) / max(1, NUM_CLICKS - 1)
    ts_dt = start_date + timedelta(seconds=offset_sec)
    clicks_rows.append({
        "event_id":   f"E{i:08d}",
        "customer_id":random.choice(cids),
        "product_id": random.choice(pids + [None] * 20),
        "event_type": random.choice(events),
        "session_id": f"S{random.randint(1,20000):06d}",
        "device":     random.choice(["mobile","desktop","tablet"]),
        "ts":         ts_dt.strftime("%Y-%m-%d %H:%M:%S"),
    })

clicks_df = spark.createDataFrame(clicks_rows)
clicks_df.write.mode("overwrite").json(f"{volume_path}/clickstream/")
print(f"Wrote {NUM_CLICKS} clickstream events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC
# MAGIC Data written to:
# MAGIC | Dataset | Format | Path |
# MAGIC |---------|--------|------|
# MAGIC | Customers | Parquet (CDC) | `<volume>/customers/` |
# MAGIC | Products | JSON | `<volume>/products/` |
# MAGIC | Orders | CSV (2 batches) | `<volume>/orders/` |
# MAGIC | Clickstream | JSON | `<volume>/clickstream/` |
# MAGIC
# MAGIC **Next step:** Open `../00-introduction` to start the demo!

print("\n✅ Setup complete — all data generated successfully!")
