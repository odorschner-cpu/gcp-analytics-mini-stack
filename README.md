## Selected Technical Portfolio (Optional) — Mini Analytics Stack (Python, DuckDB, dbt)

### Overview
This repo demonstrates a small, production-minded analytics pipeline built locally:
- Python ingestion loads CSV events into DuckDB (raw layer) and writes an execution log.
- dbt builds a staging view and an analytics mart table (daily aggregates).
- Data quality checks run via dbt tests.

### Architecture
CSV → Python ingestion → DuckDB (raw.raw_events + ops.pipeline_run_log) → dbt (analytics.stg_events → analytics.fact_events_daily)

### Data Layers
- **raw.raw_events**: raw ingestion table (1 row per event)
- **ops.pipeline_run_log**: pipeline run log (status, file, row count)
- **analytics.stg_events**: cleaned/typed staging view
- **analytics.fact_events_daily**: daily aggregates by country and event_type

### How to Run
1) Ingest:
   - `python ingestion/load_to_duckdb.py`
2) Transform:
   - `cd dbt`
   - `dbt run`
   - `dbt test`

### Validation Snapshot (example)
- raw.raw_events: 8 rows
- ops.pipeline_run_log: 1 run logged
- analytics.fact_events_daily: 8 rows
