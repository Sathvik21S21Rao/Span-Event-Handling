# Span-Event-Handling

## Introduction

This repository contains an event-streaming experiment that generates and processes "span" events (call sessions and tower signals) to evaluate streaming query behaviour and correctness. The project includes implementations using Apache Spark and Apache Flink; each implementation consumes the same generated CSV input and runs equivalent queries to validate results.

## Project structure

- `spark/` — Spark streaming implementations and examples. See `spark/README.md` for detailed run instructions.
- `flink/` — Flink streaming implementations and examples. See `flink/README.md` for detailed run instructions.
- `stream.py` — Data generator that produces CSV files under `output_data/` (mono/bi/tower/bi_tower signals).
- `insert_query.sh` — Helper script to bulk-load generated CSVs into a PostgreSQL database (uses `psql` and `\COPY`).
- `scripts.sql` — DDL to create the example tables (`mono_signal`, `tower_signal`) used for SQL validation.
- `queries.sql` — (placeholder) SQL queries used for validation; the project includes SQL used to verify Spark and Flink results.

## How this repository is intended to be used

- Generate data using `stream.py` (this writes CSV files to `output_data/`).
- Optionally load the generated CSVs into a local PostgreSQL instance using `insert_query.sh` (useful for debugging or running SQL validations).
- Run the Spark and/or Flink jobs (see their respective `README.md` files). The jobs read CSVs from the `output_data/` folders and print results to console or sink them for validation.

## Scripts and utilities

- `stream.py` — Command-line generator. Example:

```bash
python stream.py <throughput> <iterations> <max_call_duration>
# e.g. python stream.py 100 60 300
```

- `insert_query.sh` — Loads CSV files from `output_data/` into PostgreSQL tables. Edit DB/USER/PASSWORD at the top if needed. Run from project root:

```bash
bash insert_query.sh
```

## Queries run (high level)

- **Query 2**: Return tower connections that overlap with a call session but are not fully contained within the call session. This query is implemented and validated in both Spark and Flink examples.
- **Query 10**: Calculate the time gap between consecutive calls of the same user (i.e., time between call end and next call start). This is implemented and validated across Spark and Flink examples.

The SQL files (found under `scripts.sql` and `queries.sql`) were used to create tables and validate the answers produced by the Spark and Flink jobs.

## Notes

- Each of `spark/` and `flink/` directories has its own README with specific build and run instructions; follow those for detailed commands (including `spark-submit` usage for Spark and the Flink CLI for Flink).
- The streaming examples use CSV files with timestamps formatted as `yyyy-MM-dd HH:mm:ss.SSS`. The generator (`stream.py`) writes files with millisecond precision.


