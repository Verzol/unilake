# AGENTS.md

## Project Mission

This repository supports an IEEE-style Big Data report about **UniLake Analytics**, a Docker-based prototype that compares a file-based data lake path with a modern lakehouse table-format path.

The practical benchmark should compare:

```text
Framework / Path A: Spark + Parquet fallback
Framework / Path B: Spark + Apache Iceberg table on MinIO
```

The broader report should also compare the legacy Big Data architecture and the modern lakehouse architecture through academic papers, official documentation, and public technical sources.

```text
Legacy architecture:
Kafka → Spark → HDFS/Parquet → Presto/Trino → Analytics

Modern architecture:
Redpanda → Spark → MinIO + Iceberg → Spark SQL / StarRocks optional → Analytics
```

The benchmark is local and limited. Do not describe it as production-scale.

## Hard Rules

1. Never fabricate benchmark numbers.
2. Do not claim Iceberg is universally faster than Parquet.
3. Parquet is a file format. Iceberg is a table format. They are complementary, not direct replacements.
4. Any performance statement must be supported by `benchmark/results/benchmark_results.csv`, a generated figure, or a cited paper/documentation source.
5. If a service or benchmark fails, write the failure clearly and keep the result as `TBD` or `FAILED`.
6. Keep workloads equivalent: same source data, same Spark version, same query logic, same number of runs.
7. The report must distinguish local experimental evidence, architecture-level reasoning, and public/academic evidence.
8. Use formal academic Vietnamese for report writing.
9. Use Docker-first workflow for Windows users through WSL2 Ubuntu.
10. Do not require native Windows Spark, Java, Hadoop, or `winutils.exe`.

## Target Evidence Model for the Report

The final report should use **triangulation**:

```text
Local benchmark results
+ Academic / official sources
+ Careful architecture reasoning
= defensible conclusion
```

The report should answer:

1. What can the prototype prove?
2. What does the public literature claim?
3. Which public claims are supported by the local benchmark?
4. Which claims remain unverified due to local hardware and scope limitations?

## Required Final Files

At minimum, the repository should contain:

```text
AGENTS.md
docker-compose.yml
Dockerfile.spark
requirements.txt
src/generate_sample_data.py
src/benchmark_parquet_iceberg.py
src/plot_benchmark.py
docs/codex_stage_plan.md
docs/docker_run_guide.md
docs/public_sources_matrix.md
benchmark/results/benchmark_results.csv
reports/figures/read_latency_comparison.png
reports/figures/write_latency_comparison.png
reports/tables/benchmark_summary.csv
reports/report_insert_template.md
```

## Benchmark Scope

The benchmark compares **Spark reading/writing Parquet fallback** and **Spark reading/writing Iceberg tables**.

Datasets:

1. `retail_transactions`
2. `twitter_sentiment`
3. `iot_sensor_events`

Required metrics:

| Metric | Meaning |
|---|---|
| `write_seconds` | Time to write cleaned DataFrame to storage path |
| `read_seconds` | Time to read table/path and count rows |
| `query_seconds` | Time to run equivalent Spark SQL aggregate |
| `row_count` | Number of rows in the table |
| `run_id` | Benchmark repetition number |
| `storage_path` | `parquet_fallback` or `iceberg_minio` |
| `status` | `OK`, `FAILED`, or `SKIPPED` |
| `notes` | Useful warning or failure reason |

Run each benchmark at least 3 times. Save raw and summary results.

## Required Benchmark Tasks

For each table, run equivalent operations.

### Retail

1. Write cleaned data.
2. Read and count rows.
3. Query top countries by revenue.

### Twitter Sentiment

1. Write cleaned data.
2. Read and count rows.
3. Query sentiment count by label.

### IoT Sensor Events

1. Write cleaned data.
2. Read and count rows.
3. Query hourly/device sensor aggregation.

## Docker Rules

The user uses Windows, but all development commands should run inside WSL2 Ubuntu.

Use:

```bash
docker compose build
docker compose up -d minio
docker compose run --rm spark ...
```

Do not use:

```text
C:\Users\...
winutils.exe
native Windows Spark setup
```

Keep the core stack lightweight:

- `spark`
- `minio`
- optional `mc`

Redpanda, Trino, and StarRocks may be described in the report, but they should not block the core benchmark unless the user explicitly asks to run full stack.

## Report Structure

The report should include these sections:

1. Abstract
2. Introduction
3. Problem Definition
4. Related Work
5. Technology Background
6. Methodology
7. Proposed Architecture
8. Experimental Setup and Results
9. Public Evidence Validation
10. Discussion
11. Recommendation
12. Conclusion
13. References

## Section: Public Evidence Validation

Add a section named:

```text
Đối chiếu với nghiên cứu và nguồn công khai
```

This section must contain a table like:

| Public claim | Source type | Local observation | Supported? | Interpretation |
|---|---|---|---|---|
| Lakehouse unifies data lake and warehouse using open data formats | Academic paper | Prototype stores tables as Parquet with Iceberg metadata | Partially supported | Supported architecturally, not production-scale |
| Iceberg adds metadata, snapshots, and schema evolution | Official docs | Prototype uses Iceberg catalog tables | Supported | The project validates table-management workflow |
| Trino/StarRocks can serve interactive SQL on lake data | Official docs | Not implemented in core benchmark | Not verified | Future work |
| Local Iceberg read is faster than local Parquet in this run | Project benchmark | See benchmark table | Locally supported only | Not a general claim |

## Writing Rules for Performance Claims

Bad:

```text
Iceberg is faster than Parquet.
```

Good:

```text
Trong benchmark cục bộ của đề tài, đường đọc Iceberg ghi nhận thời gian thấp hơn Parquet fallback trên các bảng được kiểm tra. Tuy nhiên, kết quả này không chứng minh Iceberg luôn nhanh hơn Parquet, vì Iceberg vẫn lưu dữ liệu vật lý dưới dạng Parquet và kết quả local có thể chịu ảnh hưởng bởi cache, cấu hình file, metadata và môi trường Docker.
```

Bad:

```text
Modern stack replaces legacy stack.
```

Good:

```text
Modern lakehouse stack không thay thế tuyệt đối legacy stack. Nó phù hợp hơn với mục tiêu quản lý bảng, metadata, snapshot và tái lập local trong phạm vi đề tài, trong khi legacy stack vẫn có lợi thế về độ trưởng thành và hệ sinh thái production.
```

## Figure Requirements

Generate at least:

1. `reports/figures/read_latency_comparison.png`
2. `reports/figures/write_latency_comparison.png`
3. `reports/figures/query_latency_comparison.png`

Each chart should compare Parquet fallback and Iceberg using average runtime by table. Add captions to `reports/figures/figure_captions.md`.

## Code Quality Rules

Python scripts must:

1. Use `argparse`.
2. Use relative paths.
3. Use `time.perf_counter()`.
4. Write CSV outputs.
5. Fail gracefully when Iceberg or MinIO is unavailable.
6. Clearly label failed rows in CSV.
7. Avoid huge default datasets.
8. Use deterministic random seed for generated sample data.

## Codex Workflow

Follow these stages:

1. **Stage C0: Docker audit**, ensure Docker files exist and run.
2. **Stage C1: Data generator**, implement reproducible sample data generation.
3. **Stage C2: Benchmark**, implement Parquet vs Iceberg benchmark.
4. **Stage C3: Visualization**, generate benchmark charts and summary tables.
5. **Stage C4: Report insertion**, write a Vietnamese report section using real benchmark results.
6. **Stage C5: Validation**, check no fake results, no unsupported claims, no broken commands.

Always update documentation after changing commands.
