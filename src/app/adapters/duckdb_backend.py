import duckdb
from pathlib import Path

def init_in_memory_db(
        parquet_glob: str = "data/events/*",
        ):
    """
    Creates a fresh in-memory DuckDB and exposes all Parquet files as a view named 'events'.
    Database always up to date with the Parquet files on disk.
    Zero copy, instant startup.
    Read only.
    """
    con = duckdb.connect(database=':memory:')

    # Ensure files exist (fail fast with a clear message)
    matches = list(Path().glob(parquet_glob))
    if not matches:
        raise FileNotFoundError(f"No files matched the glob pattern: {parquet_glob}")
    
    con.execute(f"""
        CREATE OR REPLACE VIEW events AS
        SELECT * FROM read_parquet('{parquet_glob}') WHERE user_pseudo_id IS NOT NULL;
    """)

    query = """
        WITH exploded AS (
        SELECT
            e.user_pseudo_id,
            e.event_timestamp,
            (ep).key AS k,
            (ep).value.string_value AS v
        FROM events AS e,
            UNNEST(e.event_params) AS u(ep)
        ),
        pivoted AS (
        SELECT
            user_pseudo_id,
            event_timestamp,
            MAX(CASE WHEN k = 'source' THEN v END)   AS source,
            MAX(CASE WHEN k = 'medium' THEN v END)   AS medium,
            MAX(CASE WHEN k = 'campaign' THEN v END) AS campaign
        FROM exploded
        GROUP BY user_pseudo_id, event_timestamp
        ),
        ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
        FROM pivoted
        WHERE source IS NOT NULL
        )
        SELECT user_pseudo_id, source, medium, campaign
        FROM ranked
        WHERE rn = 1

    """

    con.execute("CREATE OR REPLACE VIEW last_click_attributions AS (" + query + ");")

    return con




