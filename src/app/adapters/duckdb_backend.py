from contextlib import contextmanager
import json
import os
import duckdb
from pathlib import Path

from app.domain.models import Order, Product


DEFAULT_DB_PATH = os.getenv("DUCKDB_PATH", "data/local.duckdb")
DEFAULT_EVENTS_GLOB = os.getenv("EVENTS_GLOB", "data/events/*")

SQL_DIR = Path(__file__).parent / "sql"

def load_sql(name: str) -> str:
    path = SQL_DIR / f"{name}.sql"
    return path.read_text(encoding="utf-8")

# Load SQL queries
INIT_SCHEMA_SQL = load_sql("init_schema")
CALC_ATTRIBUTION_SQL = load_sql("calc_attribution")
PROCESS_ORDERS_SQL = load_sql("process_orders")

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
        con.execute(INIT_SCHEMA_SQL)

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


def calc_attributions(db_path: str = DEFAULT_DB_PATH):
    """
    Calculate last click attributions for purchases and store them in a table.
    """

    with connect(db_path) as con:
        # Populate the existing table with last-click attributions
        con.execute("DELETE FROM purchase_last_click_attributions;")
        con.execute(
            "INSERT INTO purchase_last_click_attributions (user_pseudo_id, hostname, event_timestamp, source, medium, campaign) "
            + CALC_ATTRIBUTION_SQL
        )


def process_orders(db_path: str = DEFAULT_DB_PATH):
    """
    Process orders.
    Match purchase events from events table with last-click attribution data from purchase_last_click_attributions table.
    Return orders.
    """
    with connect(db_path) as con:
        orders_df = con.execute(PROCESS_ORDERS_SQL).fetchdf()

    orders = []
    for _, row in orders_df.iterrows():
        raw_products = row["products"]

        if isinstance(raw_products, str):
            products_data = json.loads(raw_products)
        else:
            products_data = raw_products

        products = []
        for i in (products_data if products_data is not None else []):
            price_raw = i.get("price")
            price = None if price_raw is None else float(price_raw)
            qty_raw = i.get("quantity")
            quantity = None if qty_raw is None else int(qty_raw)
            products.append(
            Product(
                id=i.get("item_id"),
                name=i.get("item_name"),
                price=price,
                quantity=quantity,
            )
        )

        order = Order(
            event_timestamp=row["event_timestamp"],
            hostname=row["hostname"],
            user_pseudo_id=row["user_pseudo_id"],
            currency=row["currency"],
            value=row["value"],
            source=row["source"],
            medium=row["medium"],
            campaign=row["campaign"],
            products=products,
        )

        orders.append(order)

    return orders




def get_connection_test(db_path: str = DEFAULT_DB_PATH):
    """
    Returns a DuckDB connection to the specified database path for testing purposes.
    """
    return duckdb.connect(db_path)