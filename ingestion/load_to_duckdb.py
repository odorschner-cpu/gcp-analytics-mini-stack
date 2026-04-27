from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import duckdb


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["event_timestamp"])
    # Normalisation simple (propre pour analytics)
    df["event_type"] = df["event_type"].astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip()
    return df


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops;")


def ensure_run_log_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
            run_id VARCHAR,
            run_utc TIMESTAMP,
            pipeline_name VARCHAR,
            status VARCHAR,
            input_file VARCHAR,
            row_count BIGINT,
            notes VARCHAR
        );
        """
    )


def write_raw_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    # Approche simple: on remplace la table à chaque run (idempotent)
    con.execute("DROP TABLE IF EXISTS raw.raw_events;")
    con.register("df_events", df)
    con.execute("CREATE TABLE raw.raw_events AS SELECT * FROM df_events;")
    row_count = con.execute("SELECT COUNT(*) FROM raw.raw_events;").fetchone()[0]
    return int(row_count)


def log_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    pipeline_name: str,
    status: str,
    input_file: str,
    row_count: int,
    notes: str = "",
) -> None:
    run_utc = datetime.now(timezone.utc).replace(tzinfo=None)  # DuckDB TIMESTAMP sans TZ
    con.execute(
        """
        INSERT INTO ops.pipeline_run_log
        (run_id, run_utc, pipeline_name, status, input_file, row_count, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        [run_id, run_utc, pipeline_name, status, input_file, row_count, notes],
    )


def main() -> None:
    root = project_root()
    csv_path = root / "data" / "events.csv"
    db_path = root / "warehouse" / "analytics.duckdb"

    pipeline_name = "csv_to_duckdb_raw_events"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    print(f"[INFO] Project root : {root}")
    print(f"[INFO] Input CSV    : {csv_path}")
    print(f"[INFO] DuckDB file  : {db_path}")

    df = load_csv(csv_path)
    print("[INFO] Preview:")
    print(df.head())

    con = duckdb.connect(str(db_path))
    try:
        ensure_schemas(con)
        ensure_run_log_table(con)

        row_count = write_raw_table(con, df)
        log_run(con, run_id, pipeline_name, "SUCCESS", str(csv_path), row_count, "Loaded CSV into raw.raw_events")

        print(f"[OK] Loaded {row_count} rows into raw.raw_events")
        print("[OK] Logged run in ops.pipeline_run_log")

        # mini validation visible
        sample = con.execute(
            "SELECT country, event_type, COUNT(*) AS n FROM raw.raw_events GROUP BY 1,2 ORDER BY n DESC;"
        ).fetchdf()
        print("[INFO] Aggregation sample:")
        print(sample)

    except Exception as e:
        # log l'échec si possible
        try:
            log_run(con, run_id, pipeline_name, "FAILED", str(csv_path), 0, f"Error: {e}")
        except Exception:
            pass
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
