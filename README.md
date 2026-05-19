# UniLake

UniLake là một nguyên mẫu phân tích dữ liệu theo hướng lakehouse, được xây dựng để so sánh hai đường xử lý dữ liệu trên cùng Apache Spark:

- **Parquet fallback**: Spark ghi/đọc dữ liệu dưới dạng file Parquet theo thư mục.
- **Iceberg trên MinIO**: Spark ghi/đọc dữ liệu dưới dạng Apache Iceberg table, lưu trên MinIO qua S3A.

Mục tiêu của dự án là hỗ trợ báo cáo IEEE-style về sự khác biệt giữa kiến trúc Big Data truyền thống và kiến trúc lakehouse hiện đại. Benchmark trong repo là benchmark cục bộ, không đại diện cho môi trường production-scale.

## Kiến Trúc Tổng Quan

Kiến trúc tham chiếu truyền thống:

```text
Kafka -> Spark -> HDFS/Parquet -> Presto/Trino -> Analytics
```

Kiến trúc hiện đại trong UniLake:

```text
Redpanda -> Spark -> MinIO + Apache Iceberg -> Spark SQL / StarRocks optional -> Analytics
```

Trong benchmark lõi, dự án tập trung vào:

```text
Spark + Parquet fallback
Spark + Apache Iceberg table trên MinIO
```

Parquet là **file format**, còn Iceberg là **table format**. Hai công nghệ này bổ sung cho nhau; báo cáo không kết luận Iceberg luôn nhanh hơn Parquet.

## Dữ Liệu Sử Dụng

Benchmark hiện tại dùng ba nhóm dữ liệu:

| Bảng | Ý nghĩa |
|---|---|
| `retail_transactions` | Giao dịch bán lẻ, dùng để truy vấn doanh thu theo quốc gia |
| `twitter_sentiment` | Dữ liệu văn bản có nhãn sentiment |
| `iot_sensor_events` | Dữ liệu cảm biến IoT theo thời gian |

Các CSV thật được đặt trong:

```text
data/raw/online_retail/Online Retail.csv
data/raw/twitter_sentiment/twitter_training.csv
data/raw/twitter_sentiment/twitter_validation.csv
data/raw/iot/iot_telemetry_data.csv
```

Script `src/prepare_real_csv_data.py` chuẩn hóa các CSV này thành input benchmark trong `data/benchmark_input/`.

## Yêu Cầu Cài Đặt

Khuyến nghị chạy trong **WSL2 Ubuntu** trên Windows.

Cần cài:

- Docker Desktop
- Docker Compose
- Git
- WSL2 Ubuntu

Không cần cài native Spark, Java, Hadoop hoặc `winutils.exe` trên Windows.

## Cài Đặt Và Chạy

### 1. Build container Spark

```bash
docker compose build
```

### 2. Khởi động MinIO

```bash
docker compose up -d minio mc
```

MinIO console:

```text
http://localhost:9001
username: minioadmin
password: minioadmin
```

### 3. Chuẩn hóa dữ liệu CSV thật

```bash
docker compose run --rm spark python3 src/prepare_real_csv_data.py \
  --raw-dir data/raw \
  --output-dir data/benchmark_input \
  --format parquet
```

Nếu không có CSV thật, có thể sinh dữ liệu mẫu:

```bash
docker compose run --rm spark python3 src/generate_sample_data.py \
  --retail-rows 200000 \
  --sentiment-rows 50000 \
  --iot-rows 150000 \
  --output-dir data/raw \
  --format parquet \
  --seed 42
```

### 4. Chạy benchmark Parquet vs Iceberg

```bash
docker compose run --rm spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  src/benchmark_parquet_iceberg.py \
  --input-dir data/benchmark_input \
  --parquet-dir data/processed/parquet_fallback \
  --results-file benchmark/results/benchmark_results.csv \
  --runs 3
```

Kết quả raw được ghi tại:

```text
benchmark/results/benchmark_results.csv
```

Kết quả tổng hợp được ghi tại:

```text
benchmark/results/benchmark_summary.csv
```

### 5. Tạo biểu đồ và bảng cho report

```bash
docker compose run --rm spark python3 src/plot_benchmark.py \
  --summary benchmark/results/benchmark_summary.csv \
  --figures-dir reports/figures \
  --tables-dir reports/tables
```

Các biểu đồ chính:

```text
reports/figures/write_latency_comparison.png
reports/figures/read_latency_comparison.png
reports/figures/query_latency_comparison.png
```

### 6. Dừng dịch vụ

```bash
docker compose down
```

Nếu muốn xóa cả volume MinIO:

```bash
docker compose down -v
```

## Kết Quả Benchmark Hiện Tại

Benchmark gần nhất chạy đủ 3 lần cho mỗi bảng và mỗi đường lưu trữ. Tất cả 18 dòng kết quả có trạng thái `OK`.

| Bảng | Đường lưu trữ | Ghi TB (s) | Đọc/đếm TB (s) | Truy vấn TB (s) | Số dòng |
|---|---|---:|---:|---:|---:|
| `retail_transactions` | `parquet_fallback` | 1.946694 | 0.216435 | 0.877848 | 531283 |
| `retail_transactions` | `iceberg_minio` | 1.801077 | 0.180938 | 0.682644 | 531283 |
| `twitter_sentiment` | `parquet_fallback` | 0.979131 | 0.179304 | 0.384057 | 75682 |
| `twitter_sentiment` | `iceberg_minio` | 0.556811 | 0.078838 | 0.256339 | 75682 |
| `iot_sensor_events` | `parquet_fallback` | 1.367450 | 0.102305 | 0.303978 | 405184 |
| `iot_sensor_events` | `iceberg_minio` | 0.708538 | 0.072717 | 0.281392 | 405184 |

Trong lần chạy cục bộ này, Iceberg trên MinIO có thời gian trung bình thấp hơn Parquet fallback ở các thao tác được đo. Tuy nhiên, kết quả này chỉ có giá trị trong môi trường local của dự án và không chứng minh Iceberg luôn nhanh hơn Parquet.

## Cấu Trúc Thư Mục

```text
.
├── benchmark/
│   └── results/                  # CSV kết quả benchmark
├── data/
│   ├── raw/                      # Dữ liệu CSV gốc, không nên commit
│   ├── benchmark_input/          # Dữ liệu đã chuẩn hóa cho benchmark
│   └── processed/                # Output Parquet fallback
├── docs/                         # Hướng dẫn chạy, kế hoạch, validation
├── reports/
│   ├── figures/                  # Biểu đồ benchmark
│   ├── tables/                   # Bảng summary cho report
│   └── ieee_report/              # Báo cáo LaTeX IEEE
├── src/
│   ├── prepare_real_csv_data.py
│   ├── generate_sample_data.py
│   ├── benchmark_parquet_iceberg.py
│   └── plot_benchmark.py
├── docker-compose.yml
├── Dockerfile.spark
└── requirements.txt
```

## Báo Cáo LaTeX

Báo cáo IEEE-style nằm trong:

```text
reports/ieee_report/
```

File chính:

```text
reports/ieee_report/main.tex
```

Nếu môi trường LaTeX đã sẵn sàng, có thể build bằng:

```bash
cd reports/ieee_report
pdflatex main.tex
bibtex main
pdflatex main.tex
pdflatex main.tex
```

## Lưu Ý Khi Diễn Giải

- Không được bịa số benchmark.
- Không viết “Iceberg luôn nhanh hơn Parquet”.
- Phải phân biệt rõ benchmark cục bộ, bằng chứng học thuật/công khai và lập luận kiến trúc.
- Nếu Iceberg hoặc MinIO lỗi, kết quả phải được ghi là `FAILED` hoặc `SKIPPED`.
- Kết quả hiện tại là kết quả Docker local, không phải kết quả production-scale.

## Tài Liệu Liên Quan

- `docs/docker_run_guide.md`: hướng dẫn chạy chi tiết.
- `docs/public_sources_matrix.md`: ma trận đối chiếu nguồn công khai.
- `docs/final_validation_report.md`: báo cáo validation cuối.
- `reports/report_insert_template.md`: đoạn chèn báo cáo tiếng Việt.
