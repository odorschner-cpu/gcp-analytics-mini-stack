import duckdb

DB_PATH = "warehouse/analytics.duckdb"

def fq_count(con, schema_name: str, table_name: str):
    # Trouve le bon catalog + schema + table dans information_schema
    row = con.execute(
        """
        select table_catalog, table_schema, table_name
        from information_schema.tables
        where table_schema = ? and table_name = ?
        limit 1
        """,
        [schema_name, table_name],
    ).fetchone()

    if row is None:
        raise RuntimeError(f"Table not found: {schema_name}.{table_name}")

    catalog, schema, table = row
    sql = f'SELECT COUNT(*) FROM "{catalog}"."{schema}"."{table}"'
    return con.execute(sql).fetchone()

def main():
    con = duckdb.connect(DB_PATH)

    print("DB:", DB_PATH)

    # Affiche catalog+schemas pour diagnostic (utile si besoin)
    print("\nCatalogs:")
    print(con.execute("select distinct catalog_name from information_schema.schemata order by 1").fetchall())

    print("\nSchemas:")
    print(con.execute("select catalog_name, schema_name from information_schema.schemata order by 1,2").fetchall())

    print("\nTables (raw/ops/analytics):")
    print(
        con.execute(
            """
            select table_catalog, table_schema, table_name
            from information_schema.tables
            where table_schema in ('raw','ops','analytics')
            order by 1,2,3
            """
        ).fetchall()
    )

    print("\nCounts:")
    print("raw.raw_events =", fq_count(con, "raw", "raw_events"))
    print("ops.pipeline_run_log =", fq_count(con, "ops", "pipeline_run_log"))
    print("analytics.fact_events_daily =", fq_count(con, "analytics", "fact_events_daily"))

    con.close()
    print("\nOK")

if __name__ == "__main__":
    main()
