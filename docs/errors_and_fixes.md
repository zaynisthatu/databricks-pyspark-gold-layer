# Errors and fixes (Databricks, 26 Mar 2026)

How the notebook reached its working form on Databricks. Each version was run, and the error or output decided the next change.

| Attempt | What was run | Result |
|---|---|---|
| 1 | Convert `timestamp` with `to_timestamp(col("timestamp"))` on the top-level DataFrame | **Failed:** `UNRESOLVED_COLUMN`. `timestamp` is not a top-level column, it lives inside the post structs. |
| 2 | `explode(feedPosts)`, then read `post.timestamp` | Worked for `feedPosts`. |
| 3 | Explode `feedPosts` and `scrapedPosts` and use a plain `union` | **Failed:** `INCOMPATIBLE_COLUMN_TYPE`. The two post structs have different fields. |
| 4 | Select the same flat columns from both arrays first, then `unionByName`, `dropDuplicates(["shortcode"])` | **Worked.** The final table had 187 posts on the original data. |
| 5a | Read the notes text file with `spark.read.csv` | Dropped: the file is free text, not CSV. |
| 5b | Read the file in plain Python, extract `shortcode`, `vibe` and `objects` with regular expressions, build a DataFrame, left join | **Ran.** Many `vibe`/`objects` values stay null because the notes file does not cover every post. |

## Takeaways

- With nested JSON, look at the schema (`df.printSchema()`) before choosing column names.
- Two arrays of structs can look alike and still not union. Flatten to the same columns first, then use `unionByName`.
- A left join on a partial lookup table gives nulls. Count them and report them.
