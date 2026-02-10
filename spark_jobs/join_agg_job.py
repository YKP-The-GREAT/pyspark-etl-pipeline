#!/usr/bin/env python3
"""
Join + Aggregation job (Windows-friendly)

Demonstrates:
- join (wide transformation)
- aggregation (shuffle-heavy)
- optional broadcast join optimization
- optional write step (use --no_write on Windows)
"""

import argparse
from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark join + aggregation example.")
    parser.add_argument("--events", required=True, help="Path to events CSV (data/events.csv)")
    parser.add_argument("--users", required=True, help="Path to users CSV (data/users.csv)")
    parser.add_argument("--output", required=True, help="Path to output folder (out/user_rollups)")
    parser.add_argument("--broadcast_users", action="store_true", help="Broadcast users table for join optimization")
    parser.add_argument(
        "--no_write",
        action="store_true",
        help="Skip writing output (recommended on Windows unless Hadoop winutils is configured).",
    )
    args = parser.parse_args()

    spark = build_spark("spark-join-agg")

    events = (
        spark.read.option("header", True).csv(args.events)
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )

    users = spark.read.option("header", True).csv(args.users)

    if args.broadcast_users:
        users = F.broadcast(users)

    joined = events.join(users, on="user_id", how="left")

    rollup = (
        joined.groupBy("user_id", "country")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.coalesce(F.col("amount"), F.lit(0.0))).alias("total_amount"),
            F.max("event_ts").alias("last_event_ts"),
        )
        .orderBy(F.desc("event_count"))
    )

    rollup.show(20, truncate=False)

    if args.no_write:
        print("Skipping write step (--no_write). Join + aggregation job completed successfully.")
        spark.stop()
        return 0

    # NOTE: On Windows, Spark local writes may require Hadoop winutils.exe / HADOOP_HOME.
    rollup.write.mode("overwrite").option("header", True).csv(args.output)

    print(f"Write completed: {args.output}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
