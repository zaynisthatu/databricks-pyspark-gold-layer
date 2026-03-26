# Databricks notebook source
# MAGIC %md
# MAGIC # Posts: bronze -> gold (PySpark)
# MAGIC The cell below is the version that worked on Databricks Free Edition (serverless), 26 Mar 2026.
# MAGIC Replace `<YOUR_EMAIL>` and the paths with your own. The data used in the original run is not published.

# COMMAND ----------

# CELL 1: read the JSON, explode feedPosts and scrapedPosts, select the same flat columns FIRST,
# then unionByName and dedupe -> df_gold_layer
from pyspark.sql.functions import col, explode, to_timestamp, to_date

# Step 1: file path
file_path = "/Workspace/Users/<YOUR_EMAIL>/datasets/posts_export_*.json"
df_master = spark.read.option("multiline", "true").json(file_path)

# Step 2: take the posts out of feedPosts and select only the needed columns right away
df_feed = df_master.filter(col("feedPosts").isNotNull()).select(explode(col("feedPosts")).alias("post"))
df_feed_flat = df_feed.select(
    col("post.shortCode").alias("shortcode"),
    col("post.type").alias("post_type"),
    col("post.likesCount").alias("likes"),
    col("post.commentsCount").alias("comments"),
    col("post.videoViewCount").alias("views"),
    col("post.caption").alias("caption"),
    col("post.timestamp").alias("raw_timestamp"),
)

# Step 3: same for scrapedPosts, with exactly the same columns
df_fast = df_master.filter(col("scrapedPosts").isNotNull()).select(explode(col("scrapedPosts")).alias("post"))
df_fast_flat = df_fast.select(
    col("post.shortCode").alias("shortcode"),
    col("post.type").alias("post_type"),
    col("post.likesCount").alias("likes"),
    col("post.commentsCount").alias("comments"),
    col("post.videoViewCount").alias("views"),
    col("post.caption").alias("caption"),
    col("post.timestamp").alias("raw_timestamp"),
)

# Step 4: union (both tables are flat now and have the same structure)
df_all_posts = df_feed_flat.unionByName(df_fast_flat)

# Step 5: dates, duplicates, final clean table
df_gold_layer = (df_all_posts
                 .withColumn("post_date", to_date(to_timestamp(col("raw_timestamp"))))
                 .dropDuplicates(["shortcode"])
                 .filter(col("shortcode").isNotNull())
                 .drop("raw_timestamp"))

display(df_gold_layer)

