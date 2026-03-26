"""Bronze -> gold with PySpark, local version of the Databricks notebook.

Step 1: read the JSON exports, explode feedPosts and scrapedPosts, pick the SAME flat columns from both,
        union them by name, parse the timestamp, drop duplicate short codes -> df_gold_layer
Step 2: parse a plain-text notes file with regular expressions, left-join it on the short code
        -> df_final_master

Run:  python gold_layer.py
"""
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date

DATA_GLOB = "data/posts_export_*.json"
NOTES_FILE = "data/video_analysis_notes.txt"

spark = (SparkSession.builder.master("local[2]").appName("gold-layer")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---- Step 1: JSON -> flat columns -> union -> dedupe -------------------------------------------
df_master = spark.read.option("multiline", "true").json(DATA_GLOB)


def flat(array_column):
    """Explode one array of post structs and select the same flat columns right away."""
    return (df_master.filter(col(array_column).isNotNull())
            .select(explode(col(array_column)).alias("post"))
            .select(col("post.shortCode").alias("shortcode"),
                    col("post.type").alias("post_type"),
                    col("post.likesCount").alias("likes"),
                    col("post.commentsCount").alias("comments"),
                    col("post.videoViewCount").alias("views"),
                    col("post.caption").alias("caption"),
                    col("post.timestamp").alias("raw_timestamp")))


df_all_posts = flat("feedPosts").unionByName(flat("scrapedPosts"))

df_gold_layer = (df_all_posts
                 .withColumn("post_date", to_date(to_timestamp(col("raw_timestamp"))))
                 .dropDuplicates(["shortcode"])
                 .filter(col("shortcode").isNotNull())
                 .drop("raw_timestamp"))

# ---- Step 2: parse the notes text file with regex, left join ------------------------------------
with open(NOTES_FILE, "r", encoding="utf-8") as f:
    content = f.read()

extracted = []
for block in content.split("\U0001F4C2 POST PATH:"):
    shortcode_match = re.search(r"\U0001F539 Shortcode:\s*([a-zA-Z0-9_-]+)", block)
    vibe_match = re.search(r"Vibe:\s*(.*?)(?:\n|\U0001F399\uFE0F|$)", block)
    objects_match = re.search(r"Objects:\s*(.*?)\s*\|", block)
    if shortcode_match and shortcode_match.group(1) != "Unknown":
        extracted.append({"shortcode": shortcode_match.group(1),
                          "vibe": vibe_match.group(1).strip() if vibe_match else "No Vibe",
                          "objects": objects_match.group(1).strip() if objects_match else "No Objects"})

df_vibe = spark.createDataFrame(extracted)
df_final_master = df_gold_layer.join(df_vibe, on="shortcode", how="left")

# ---- Checks -------------------------------------------------------------------------------------
n_raw = df_all_posts.count()
n_gold = df_gold_layer.count()
n_dup_codes = df_all_posts.filter(col("shortcode").isNotNull()).count() - n_gold
n_null_code = df_all_posts.filter(col("shortcode").isNull()).count()
n_matched = df_final_master.filter(col("vibe").isNotNull()).count()

print("posts after union:            ", n_raw)
print("rows with null short code:    ", n_null_code, "(dropped)")
print("duplicate short codes removed:", n_dup_codes)
print("rows in df_gold_layer:        ", n_gold)
print("rows with vibe/objects:       ", n_matched, "of", n_gold, "(the notes file does not cover every post)")
print()
df_gold_layer.orderBy("post_date").show(5, truncate=False)
df_final_master.orderBy("post_date").show(8, truncate=False)
spark.stop()
