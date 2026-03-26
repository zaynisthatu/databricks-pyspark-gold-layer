# databricks-pyspark-gold-layer

A PySpark bronze-to-gold pipeline for social-media post exports. It merges post records from two export sources with different schemas into one deduplicated table of post metrics, then enriches each post with content tags (vibe and detected objects) parsed from a video-analysis notes file.

Developed on Databricks Free Edition (serverless compute) in March 2026. The repository contains the original Databricks notebook, a version that runs locally with plain PySpark, a generator for sample input data, and a record of the Spark errors met while building it.

## What the pipeline does

1. **Read.** Loads several multiline JSON exports with `spark.read.json`. Each file holds two arrays of post structs, `feedPosts` and `scrapedPosts`, whose fields differ.
2. **Flatten.** Explodes each array and selects the same seven columns from both: `shortcode`, `post_type`, `likes`, `comments`, `views`, `caption` and `raw_timestamp`.
3. **Merge and clean.** `unionByName` combines the two sets, the timestamp becomes `post_date`, and duplicate and null short codes are dropped. The result is `df_gold_layer`.
4. **Enrich.** `video_analysis_notes.txt` is free text with one block per post. Regular expressions extract `shortcode`, `vibe` and `objects`, and a left join on `shortcode` produces `df_final_master`. Posts that the notes do not cover keep null `vibe` and `objects`.

## Result on the sample data

```
posts after union:             78
rows with null short code:     3 (dropped)
duplicate short codes removed: 15
rows in df_gold_layer:         60
rows with vibe/objects:        36 of 60 (the notes file does not cover every post)
```

Full output: `output/sample_run.txt`. Run on the original data in Databricks, the gold table held 187 posts.

## Run it locally

```bash
pip install -r requirements.txt     # PySpark needs Java 17 or 21
python generate_synthetic_data.py   # optional, the files are already in data/
python gold_layer.py
python examples/reproduce_errors.py
```

## Repository layout

| Path | Contents |
|---|---|
| `databricks/gold_layer_notebook.py` | The two Databricks cells, with placeholders for paths |
| `gold_layer.py` | The same pipeline with a local `SparkSession`, `.show()` instead of `display()`, and summary counts |
| `generate_synthetic_data.py`, `data/` | Sample input: three JSON exports and a notes file |
| `examples/reproduce_errors.py` | Reproduces the two Spark errors on the sample data |
| `docs/errors_and_fixes.md` | The attempts made on Databricks and the two errors, with fixes |
| `output/` | Saved output of the runs |

## Sample data

`generate_synthetic_data.py` writes `data/` with the same shape as the original exports: two arrays of post structs with different fields, 15 short codes present in both arrays, three posts without a short code, and a notes file that covers 36 of the 60 distinct posts.

## License

MIT
