# Data Directory

This directory separates large raw datasets from small sample files used for local testing.

Large datasets should be placed under `data/raw/` and should not be committed. Small sample files for repeatable demos belong in `data/sample/`. Spark jobs write local fallback outputs under `data/processed/` when Iceberg is not available.

Current raw dataset layout:

```text
data/raw/online_retail/Online Retail.csv
data/raw/twitter_sentiment/twitter_training.csv
data/raw/twitter_sentiment/twitter_validation.csv
data/raw/iot/iot_telemetry_data.csv
```

The retail dataset is stored as CSV after converting the original UCI Excel workbook.
