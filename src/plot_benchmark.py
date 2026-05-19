#!/usr/bin/env python3
"""Create benchmark charts for the IEEE report."""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser(description="Plot UniLake benchmark results.")
    parser.add_argument("--summary", default="benchmark/results/benchmark_summary.csv")
    parser.add_argument("--figures-dir", default="reports/figures")
    parser.add_argument("--tables-dir", default="reports/tables")
    return parser.parse_args()


def plot_operation(summary: pd.DataFrame, operation: str, out_path: Path) -> bool:
    df = summary[summary["operation"] == operation].copy()
    if df.empty:
        return False
    pivot = df.pivot_table(index="table", columns="storage_path", values="avg_seconds", aggfunc="mean")
    ax = pivot.plot(kind="bar", figsize=(9, 5))
    ax.set_xlabel("Table")
    ax.set_ylabel("Average seconds")
    ax.set_title(f"{operation.replace('_', ' ').title()} latency comparison")
    ax.legend(title="Storage path")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()
    return True


def main():
    args = parse_args()
    summary_path = Path(args.summary)
    figures_dir = Path(args.figures_dir)
    tables_dir = Path(args.tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)
    if not summary_path.exists():
        raise FileNotFoundError(f"Summary file not found: {summary_path}")
    summary = pd.read_csv(summary_path)
    summary.to_csv(tables_dir / "benchmark_summary.csv", index=False)
    created = []
    mapping = {
        "write": "write_latency_comparison.png",
        "read_count": "read_latency_comparison.png",
        "query": "query_latency_comparison.png",
    }
    for operation, filename in mapping.items():
        out_path = figures_dir / filename
        if plot_operation(summary, operation, out_path):
            created.append((operation, out_path))
    captions = [
        "# Figure Captions",
        "",
        "Các biểu đồ sử dụng giá trị trung bình từ benchmark local. Chúng không đại diện cho hiệu năng production.",
        "",
    ]
    for operation, path in created:
        captions.append(f"- **{path.name}**: So sánh thời gian trung bình cho thao tác `{operation}` giữa Parquet fallback và Iceberg table trên cùng dữ liệu và cùng Spark engine.")
    (figures_dir / "figure_captions.md").write_text("\n".join(captions), encoding="utf-8")
    print("Created figures:")
    for _, path in created:
        print(f"- {path}")


if __name__ == "__main__":
    main()
