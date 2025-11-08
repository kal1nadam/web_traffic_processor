from contextlib import contextmanager
import os
import duckdb
from pathlib import Path


DEFAULT_DB_PATH = os.getenv("DUCKDB_PATH", "data/local.duckdb")
DEFAULT_EVENTS_GLOB = os.getenv("EVENTS_GLOB", "data/events/*")

SCHEMA_SQL = f"""
    CREATE TABLE IF NOT EXISTS events (
        event_name VARCHAR,
        event_timestamp BIGINT,
        user_pseudo_id VARCHAR,
        hostname VARCHAR,
        event_params STRUCT(key VARCHAR, value STRUCT(string_value VARCHAR, int_value BIGINT, float_value DOUBLE, double_value DOUBLE))[],
        items STRUCT(item_id VARCHAR, item_name VARCHAR, item_brand VARCHAR, item_variant VARCHAR, item_category VARCHAR, item_category2 VARCHAR, item_category3 VARCHAR, item_category4 VARCHAR, item_category5 VARCHAR, price_in_usd DOUBLE, price DOUBLE, quantity BIGINT, item_revenue_in_usd DOUBLE, item_revenue DOUBLE, item_refund_in_usd DOUBLE, item_refund DOUBLE, coupon VARCHAR, affiliation VARCHAR, location_id VARCHAR, item_list_id VARCHAR, item_list_name VARCHAR, item_list_index VARCHAR, promotion_id VARCHAR, promotion_name VARCHAR, creative_name VARCHAR, creative_slot VARCHAR, item_params STRUCT("key" VARCHAR, "value" STRUCT(string_value VARCHAR, int_value BIGINT, float_value DOUBLE, double_value DOUBLE))[])[]
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id UUID PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS purchase_last_click_attributions (
        user_pseudo_id VARCHAR,
        event_timestamp BIGINT,
        hostname VARCHAR,
        source VARCHAR,
        medium VARCHAR,
        campaign VARCHAR
    );

    CREATE INDEX IF NOT EXISTS idx_events_ts ON events(event_timestamp);
    CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_pseudo_id);
    CREATE INDEX IF NOT EXISTS idx_events_hostname ON events(hostname);
"""

ATTRIBUTION_SQL = """
        WITH exploded AS (
        SELECT
            e.user_pseudo_id,
            e.event_timestamp,
            e.hostname,
            (ep).key AS k,
            (ep).value.string_value AS v
        FROM events AS e,
            UNNEST(e.event_params) AS u(ep)
        ),
        pivoted AS (
        SELECT
            user_pseudo_id,
            MAX(event_timestamp) AS event_timestamp,
            hostname,
            MAX(CASE WHEN k = 'source' THEN v END)   AS source,
            MAX(CASE WHEN k = 'medium' THEN v END)   AS medium,
            MAX(CASE WHEN k = 'campaign' THEN v END) AS campaign
        FROM exploded
        GROUP BY user_pseudo_id, hostname
        ),
        ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
        FROM pivoted
        WHERE source IS NOT NULL
        )
        SELECT user_pseudo_id, hostname, event_timestamp, source, medium, campaign
        FROM ranked
        WHERE rn = 1
    """

@contextmanager
def connect(db_path: str = DEFAULT_DB_PATH):
    con = duckdb.connect(db_path)
    try:
        yield con
    finally:
        con.close()

def init_db(db_path: str = DEFAULT_DB_PATH, events_glob: str = DEFAULT_EVENTS_GLOB):
    """
    Recreate the DB file, build schema, and seed events from parquet files.
    """

    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    if db_file.exists():
        db_file.unlink()

    with connect(db_path) as con:
        # Create schema
        con.execute(SCHEMA_SQL)

        # Seed events from parquet files
        matches = list(Path().glob(events_glob))
        if not matches:
            raise FileNotFoundError(f"No files matched the glob pattern: {events_glob}")

        con.execute(f"CREATE OR REPLACE VIEW events_seed AS SELECT * FROM read_parquet('{events_glob}');")
        con.execute("DELETE FROM events;")
        con.execute(
            "INSERT INTO events (event_name, event_timestamp, user_pseudo_id, hostname, event_params, items) "
            "SELECT event_name, event_timestamp, user_pseudo_id, device.web_info.hostname AS hostname, event_params, items FROM events_seed WHERE user_pseudo_id IS NOT NULL;"
        )
        con.execute("DROP VIEW events_seed;")



def calc_purchase_attributions(db_path: str = DEFAULT_DB_PATH):
    """
    Calculate last click attributions for purchases and store them in a table.
    """

    with connect(db_path) as con:
        # Populate the existing table with last-click attributions
        con.execute("DELETE FROM purchase_last_click_attributions;")
        con.execute(
            "INSERT INTO purchase_last_click_attributions (user_pseudo_id, hostname, event_timestamp, source, medium, campaign) "
            + ATTRIBUTION_SQL
        )

def get_connection_test(db_path: str = DEFAULT_DB_PATH):
    """
    Returns a DuckDB connection to the specified database path for testing purposes.
    """
    return duckdb.connect(db_path)