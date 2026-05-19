#!/usr/bin/env python3
"""Generate reproducible sample data for UniLake Analytics."""

from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate UniLake sample datasets.")
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--retail-rows", type=int, default=200_000)
    parser.add_argument("--sentiment-rows", type=int, default=50_000)
    parser.add_argument("--iot-rows", type=int, default=150_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet")
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


def make_retail(rows: int, rng: np.random.Generator) -> pd.DataFrame:
    countries = np.array(["United Kingdom", "Netherlands", "EIRE", "Germany", "France", "Spain", "Australia"])
    stock_codes = np.array([f"SKU-{i:05d}" for i in range(1, 2001)])
    invoice_ids = np.array([f"INV-{i:07d}" for i in range(1, max(1000, rows // 8) + 1)])
    ts = pd.Timestamp("2023-01-01") + pd.to_timedelta(rng.integers(0, 120 * 24 * 3600, rows), unit="s")
    quantity = rng.integers(1, 10, rows)
    unit_price = np.round(rng.gamma(shape=2.0, scale=8.0, size=rows) + 0.5, 2)
    country = rng.choice(countries, rows, p=[0.74, 0.05, 0.05, 0.05, 0.05, 0.03, 0.03])
    return pd.DataFrame({
        "invoice_no": rng.choice(invoice_ids, rows),
        "stock_code": rng.choice(stock_codes, rows),
        "description": "Synthetic product",
        "quantity": quantity,
        "invoice_timestamp": ts,
        "unit_price": unit_price,
        "customer_id": [f"CUST-{x:06d}" for x in rng.integers(1, 50_000, rows)],
        "country": country,
        "revenue": quantity * unit_price,
    })


def make_sentiment(rows: int, rng: np.random.Generator) -> pd.DataFrame:
    entities = np.array(["Google", "Amazon", "Microsoft", "Apple", "Tesla", "Netflix"])
    sentiments = np.array(["Positive", "Neutral", "Negative", "Irrelevant"])
    labels = rng.choice(sentiments, rows, p=[0.30, 0.27, 0.28, 0.15])
    texts = np.array([f"Synthetic tweet about entity with {label.lower()} sentiment." for label in labels])
    return pd.DataFrame({
        "tweet_id": [f"TWEET-{i:08d}" for i in range(rows)],
        "entity": rng.choice(entities, rows),
        "sentiment": labels,
        "tweet_text": texts,
        "text_length": [len(x) for x in texts],
    })


def make_iot(rows: int, rng: np.random.Generator) -> pd.DataFrame:
    devices = np.array([f"device-{i:03d}" for i in range(1, 51)])
    ts = pd.Timestamp("2023-01-01") + pd.to_timedelta(rng.integers(0, 30 * 24 * 3600, rows), unit="s")
    return pd.DataFrame({
        "event_timestamp": ts,
        "device_id": rng.choice(devices, rows),
        "temperature": np.round(rng.normal(26.0, 4.0, rows), 2),
        "humidity": np.round(rng.normal(65.0, 10.0, rows), 2),
        "co": np.round(rng.gamma(2.0, 0.25, rows), 4),
        "lpg": np.round(rng.gamma(2.0, 0.20, rows), 4),
        "smoke": np.round(rng.gamma(2.0, 0.35, rows), 4),
        "motion": rng.choice([True, False], rows, p=[0.18, 0.82]),
    })


def main() -> None:
    args = parse_args()
    out_dir = Path(args.output_dir)
    rng = np.random.default_rng(args.seed)
    frames = {
        "retail_transactions": make_retail(args.retail_rows, rng),
        "twitter_sentiment": make_sentiment(args.sentiment_rows, rng),
        "iot_sensor_events": make_iot(args.iot_rows, rng),
    }
    print("Generated datasets:")
    for name, df in frames.items():
        path = write_frame(df, out_dir / name, args.format)
        print(f"- {name}: {len(df):,} rows -> {path}")


if __name__ == "__main__":
    main()
