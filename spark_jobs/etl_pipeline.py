
#!/usr/bin/env python3
"""
PySpark ETL Pipeline (Windows-friendly)

What it demonstrates:
- Reading CSV with an explicit schema
- Cleaning + derived columns
- Transformations vs actions (lazy evaluation)
- groupBy aggregation (shuffle)
- repartitioning
- Optional write step (disabled by default on Windows via --no_write)
"""

import argparse
from pyspark.sql import SparkSession, functions as F, types as T


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # reasonable local defaults; can be overridden in cluster env
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="PySpark ETL pipeline example.")
    parser.add_argument("--input", required=True, help="Path to input CSV (e.g., data/events.csv)")
    parser.add_argument("--output", required=True, help="Path to output folder (e.g., out/curated_events)")
    parser.add_argument("--repartition", type=int, default=4, help="Number of partitions for downstream processing")
    parser.add_argument(
        "--no_write",
        action="store_true",
        help="Skip writing output (recommended on Windows unless Hadoop winutils is configured).",
    )
    args = parser.parse_args()

    spark = build_spark("pyspark-etl-pipeline")

    schema = T.StructType([
        T.StructField("event_id", T.StringType(), True),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("event_ts", T.StringType(), True),  # parse to timestamp below
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("country", T.StringType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(args.input)
    )

    # Clean + enrich
    df_clean = (
        df
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount") < 10, F.lit("LOW"))
             .when(F.col("amount") < 100, F.lit("MED"))
             .otherwise(F.lit("HIGH")),
        )
    )

    # Cache is useful if we reuse df_clean multiple times (common pattern in assignments)
    df_clean = df_clean.cache()

    # A simple profiling aggregation (forces an action and highlights shuffle behavior)
    counts_by_type = (
        df_clean.groupBy("event_type")
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
    )
    counts_by_type.show(50, truncate=False)

    # Repartition to demonstrate controlling parallelism / downstream layout
    df_out = df_clean.repartition(args.repartition, "event_date")

    # Preview output rows (useful for TA demos)
    df_out.select(
        "event_id", "user_id", "event_type", "event_ts", "event_date", "amount", "amount_bucket", "country"
    ).show(10, truncate=False)

    if args.no_write:
        print("Skipping write step (--no_write). Pipeline completed successfully.")
        spark.stop()
        return 0

    # NOTE: On Windows, writing to local filesystem may require Hadoop winutils.exe.
    # On Linux/WSL/cloud, writing typically works out of the box.
    (
        df_out.write
        .mode("overwrite")
        .option("header", True)
        .csv(args.output)
    )

    print(f"Write completed: {args.output}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
