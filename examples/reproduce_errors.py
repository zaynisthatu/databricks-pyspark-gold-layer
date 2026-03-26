"""Reproduces the two Spark errors met on Databricks, on the synthetic data.

Run:  python examples/reproduce_errors.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp
from pyspark.errors import AnalysisException

spark = (SparkSession.builder.master("local[2]").appName("errors")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.option("multiline", "true").json("data/posts_export_*.json")

print("--- Error 1: timestamp is not a top-level column (it lives inside feedPosts)")
try:
    df.withColumn("post_timestamp", to_timestamp(col("timestamp"))).collect()
except AnalysisException as e:
    print("   ", e.getCondition())
print("    fix: explode(feedPosts) first, then read post.timestamp")

print("--- Error 2: plain union of the two exploded arrays (the structs have different fields)")
feed = df.select(explode(col("feedPosts")).alias("post"))
scraped = df.select(explode(col("scrapedPosts")).alias("post"))
try:
    feed.union(scraped).collect()
except AnalysisException as e:
    print("   ", e.getCondition())
print("    fix: select the same flat columns from both arrays first, then unionByName")
spark.stop()
