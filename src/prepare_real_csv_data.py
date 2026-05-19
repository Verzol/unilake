#!/usr/bin/env python3
"""Prepare real CSV datasets for the Parquet vs Iceberg benchmark."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize project CSV datasets for Spark benchmark input.")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--output-dir", default="data/benchmark_input")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    return parser.parse_args()


def write_frame(df: pd.DataFrame, path_base: Path, fmt: str) -> Path:
    path_base.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        out = path_base.with_suffix(".parquet")
        df.to_parquet(out, index=False, coerce_timestamps="ms", allow_truncated_timestamps=True)
    else:
        out = path_base.with_suffix(".csv")
        df.to_csv(out, index=False)
    return out


def prepare_retail(raw_dir: Path) -> pd.DataFrame:
    path = raw_dir / "online_retail" / "Online Retail.csv"
    df = pd.read_csv(path, encoding="utf-8")
    retail = pd.DataFrame(
        {
            "invoice_no": df["InvoiceNo"].astype("string"),
            "stock_code": df["StockCode"].astype("string"),
            "description": df["Description"].astype("string"),
            "quantity": pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64"),
            "invoice_timestamp": pd.to_datetime(df["InvoiceDate"], errors="coerce"),
            "unit_price": pd.to_numeric(df["UnitPrice"], errors="coerce"),
            "customer_id": df["CustomerID"].astype("string"),
            "country": df["Country"].astype("string"),
        }
    )
    retail["revenue"] = retail["quantity"].astype("float64") * retail["unit_price"]
    return retail.dropna(subset=["invoice_timestamp", "quantity", "unit_price", "country"])


def prepare_sentiment(raw_dir: Path) -> pd.DataFrame:
    names = ["tweet_id", "entity", "sentiment", "tweet_text"]
    paths = [
        raw_dir / "twitter_sentiment" / "twitter_training.csv",
        raw_dir / "twitter_sentiment" / "twitter_validation.csv",
    ]
    frames = [pd.read_csv(path, header=None, names=names, encoding="utf-8") for path in paths]
    sentiment = pd.concat(frames, ignore_index=True)
    sentiment["tweet_id"] = sentiment["tweet_id"].astype("string")
    sentiment["entity"] = sentiment["entity"].astype("string")
    sentiment["sentiment"] = sentiment["sentiment"].astype("string")
    sentiment["tweet_text"] = sentiment["tweet_text"].fillna("").astype("string")
    sentiment["text_length"] = sentiment["tweet_text"].str.len()
    return sentiment.dropna(subset=["entity", "sentiment"])


def prepare_iot(raw_dir: Path) -> pd.DataFrame:
    path = raw_dir / "iot" / "iot_telemetry_data.csv"
    df = pd.read_csv(path, encoding="utf-8")
    iot = pd.DataFrame(
        {
            "event_timestamp": pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="s", errors="coerce"),
            "device_id": df["device"].astype("string"),
            "temperature": pd.to_numeric(df["temp"], errors="coerce"),
            "humidity": pd.to_numeric(df["humidity"], errors="coerce"),
            "co": pd.to_numeric(df["co"], errors="coerce"),
            "lpg": pd.to_numeric(df["lpg"], errors="coerce"),
            "smoke": pd.to_numeric(df["smoke"], errors="coerce"),
            "motion": df["motion"].astype("string").str.lower().eq("true"),
        }
    )
    return iot.dropna(subset=["event_timestamp", "device_id"])


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    output_dir = Path(args.output_dir)
    frames = {
        "retail_transactions": prepare_retail(raw_dir),
        "twitter_sentiment": prepare_sentiment(raw_dir),
        "iot_sensor_events": prepare_iot(raw_dir),
    }
    print("Prepared real CSV benchmark inputs:")
    for name, df in frames.items():
        path = write_frame(df, output_dir / name, args.format)
        print(f"- {name}: {len(df):,} rows -> {path}")


if __name__ == "__main__":
    main()
