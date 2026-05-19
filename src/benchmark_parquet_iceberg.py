#!/usr/bin/env python3
"""Benchmark Spark + Parquet fallback vs Spark + Iceberg table on MinIO.

The comparison isolates the storage/table layer because both paths use Spark.
Local results must not be interpreted as production-scale proof.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLES = ["retail_transactions", "twitter_sentiment", "iot_sensor_events"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Parquet fallback vs Iceberg table paths.")
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument("--parquet-dir", default="data/processed/parquet_fallback")
    parser.add_argument("--results-file", default="benchmark/results/benchmark_results.csv")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warehouse", default="s3a://warehouse/iceberg")
    parser.add_argument("--catalog", default="unilake")
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--skip-iceberg", action="store_true")
    return parser.parse_args()


def create_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder
        .appName("UniLakeParquetIcebergBenchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{args.catalog}.warehouse", args.warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def input_path(input_dir: str, table: str) -> str:
    base = Path(input_dir)
    parquet = base / f"{table}.parquet"
    csv_path = base / f"{table}.csv"
    if parquet.exists():
        return str(parquet)
    if csv_path.exists():
        return str(csv_path)
    raise FileNotFoundError(f"No input file found for table {table} in {input_dir}")


def read_source(spark: SparkSession, path: str) -> DataFrame:
    if path.endswith(".csv"):
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    return spark.read.parquet(path)


def normalize(table: str, df: DataFrame) -> DataFrame:
    if table == "retail_transactions":
        return (
            df.withColumn("invoice_timestamp", F.to_timestamp("invoice_timestamp"))
              .withColumn("quantity", F.col("quantity").cast("int"))
              .withColumn("unit_price", F.col("unit_price").cast("double"))
              .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
              .filter(F.col("invoice_timestamp").isNotNull())
              .filter(F.col("quantity") > 0)
              .filter(F.col("unit_price") >= 0)
        )
    if table == "twitter_sentiment":
        return (
            df.withColumn("text_length", F.length(F.col("tweet_text")))
              .filter(F.col("sentiment").isNotNull())
              .filter(F.col("entity").isNotNull())
        )
    if table == "iot_sensor_events":
        return (
            df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
              .withColumn("event_hour", F.date_trunc("hour", F.col("event_timestamp")))
              .filter(F.col("event_timestamp").isNotNull())
        )
    raise ValueError(f"Unsupported table: {table}")


def query_table(spark: SparkSession, table: str, view_name: str) -> int:
    if table == "retail_transactions":
        q = f"""
        SELECT country, COUNT(DISTINCT invoice_no) AS invoice_count, SUM(revenue) AS total_revenue
        FROM {view_name}
        GROUP BY country
        ORDER BY total_revenue DESC
        LIMIT 10
        """
    elif table == "twitter_sentiment":
        q = f"""
        SELECT sentiment, COUNT(*) AS tweet_count
        FROM {view_name}
        GROUP BY sentiment
        ORDER BY tweet_count DESC
        """
    elif table == "iot_sensor_events":
        q = f"""
        SELECT device_id, event_hour, COUNT(*) AS event_count,
               AVG(temperature) AS avg_temperature,
               AVG(humidity) AS avg_humidity,
               SUM(CASE WHEN motion THEN 1 ELSE 0 END) AS motion_count
        FROM {view_name}
        GROUP BY device_id, event_hour
        ORDER BY event_hour, device_id
        LIMIT 100
        """
    else:
        raise ValueError(table)
    return len(spark.sql(q).collect())


def timed(func):
    start = time.perf_counter()
    result = func()
    end = time.perf_counter()
    return result, end - start


def add_record(rows: List[Dict[str, object]], run_id: int, table: str, storage_path: str,
               write_seconds: float | None, read_seconds: float | None,
               query_seconds: float | None, row_count: int | None,
               status: str, notes: str = "") -> None:
    rows.append({
        "run_id": run_id,
        "table": table,
        "storage_path": storage_path,
        "write_seconds": f"{write_seconds:.6f}" if write_seconds is not None else "",
        "read_seconds": f"{read_seconds:.6f}" if read_seconds is not None else "",
        "query_seconds": f"{query_seconds:.6f}" if query_seconds is not None else "",
        "row_count": row_count if row_count is not None else "",
        "status": status,
        "notes": notes,
    })


def write_rows(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "run_id",
        "table",
        "storage_path",
        "write_seconds",
        "read_seconds",
        "query_seconds",
        "row_count",
        "status",
        "notes",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(results_path: Path) -> None:
    import pandas as pd
    df = pd.read_csv(results_path)
    ok = df[df["status"] == "OK"].copy()
    if ok.empty:
        return
    long = ok.melt(
        id_vars=["table", "storage_path", "row_count"],
        value_vars=["write_seconds", "read_seconds", "query_seconds"],
        var_name="operation",
        value_name="seconds",
    ).dropna(subset=["seconds"])
    long["operation"] = long["operation"].str.replace("_seconds", "", regex=False)
    long["operation"] = long["operation"].replace({"read": "read_count"})
    summary = (
        long.groupby(["table", "storage_path", "operation"], as_index=False)
          .agg(
              runs=("seconds", "count"),
              avg_seconds=("seconds", "mean"),
              min_seconds=("seconds", "min"),
              max_seconds=("seconds", "max"),
              std_seconds=("seconds", "std"),
              row_count=("row_count", "max"),
          )
    )
    summary.to_csv(results_path.parent / "benchmark_summary.csv", index=False)


def main() -> None:
    args = parse_args()
    spark = create_spark(args)
    rows: List[Dict[str, object]] = []
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.default")

    for run_id in range(1, args.runs + 1):
        for table in TABLES:
            src_path = input_path(args.input_dir, table)
            source_df = normalize(table, read_source(spark, src_path)).cache()
            row_count = source_df.count()

            parquet_path = str(Path(args.parquet_dir) / table)
            try:
                _, write_seconds = timed(lambda: source_df.write.mode("overwrite").parquet(parquet_path))
                parquet_df = spark.read.parquet(parquet_path)
                counted, read_seconds = timed(lambda: parquet_df.count())
                parquet_df.createOrReplaceTempView(f"parquet_{table}")
                _, query_seconds = timed(lambda: query_table(spark, table, f"parquet_{table}"))
                add_record(
                    rows, run_id, table, "parquet_fallback",
                    write_seconds, read_seconds, query_seconds, counted, "OK"
                )
            except Exception as exc:
                add_record(rows, run_id, table, "parquet_fallback", None, None, None, row_count, "FAILED", str(exc)[:500])

            if args.skip_iceberg:
                add_record(rows, run_id, table, "iceberg_minio", None, None, None, row_count, "SKIPPED", "--skip-iceberg")
            else:
                iceberg_table = f"{args.catalog}.default.{table}"
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
                    _, write_seconds = timed(lambda: source_df.writeTo(iceberg_table).using("iceberg").create())
                    iceberg_df = spark.table(iceberg_table)
                    counted, read_seconds = timed(lambda: iceberg_df.count())
                    iceberg_df.createOrReplaceTempView(f"iceberg_{table}")
                    _, query_seconds = timed(lambda: query_table(spark, table, f"iceberg_{table}"))
                    add_record(
                        rows, run_id, table, "iceberg_minio",
                        write_seconds, read_seconds, query_seconds, counted, "OK"
                    )
                except Exception as exc:
                    add_record(rows, run_id, table, "iceberg_minio", None, None, None, row_count, "FAILED", str(exc)[:500])

            source_df.unpersist()

    results_path = Path(args.results_file)
    write_rows(results_path, rows)
    write_summary(results_path)
    print(f"Wrote benchmark results to {results_path}")
    print(f"Wrote summary to {results_path.parent / 'benchmark_summary.csv'}")
    spark.stop()


if __name__ == "__main__":
    main()
