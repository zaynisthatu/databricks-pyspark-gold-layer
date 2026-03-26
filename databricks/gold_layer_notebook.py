# Databricks notebook source
# MAGIC %md
# MAGIC # Posts: bronze -> gold (PySpark)
# MAGIC The two cells below are the versions that worked on Databricks Free Edition (serverless), 26 Mar 2026.
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

# COMMAND ----------

# CELL 2: parse video_analysis_notes.txt with regex (plain Python), build df_vibe, LEFT JOIN on shortcode
# NOTE: many vibe/objects values stay null because the notes file does not cover every short code.
import re

file_path_txt = "/Workspace/Users/<YOUR_EMAIL>/datasets/video_analysis_notes.txt"

with open(file_path_txt, "r", encoding="utf-8") as f:
    content = f.read()

extracted_data = []
blocks = content.split("\U0001F4C2 POST PATH:")

for block in blocks:
    shortcode_match = re.search(r"\U0001F539 Shortcode:\s*([a-zA-Z0-9_-]+)", block)
    vibe_match = re.search(r"Vibe:\s*(.*?)(?:\n|\U0001F399\uFE0F|$)", block)
    objects_match = re.search(r"Objects:\s*(.*?)\s*\|", block)

    if shortcode_match and shortcode_match.group(1) != "Unknown":
        extracted_data.append({
            "shortcode": shortcode_match.group(1),
            "vibe": vibe_match.group(1).strip() if vibe_match else "No Vibe",
            "objects": objects_match.group(1).strip() if objects_match else "No Objects",
        })

df_vibe = spark.createDataFrame(extracted_data)
df_final_master = df_gold_layer.join(df_vibe, on="shortcode", how="left")

display(df_final_master)
