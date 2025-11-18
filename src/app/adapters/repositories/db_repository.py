
from app.adapters.duckdb_backend import DEFAULT_DB_PATH, connect
from app.domain.models import Order, OrderProduct, Product


class DbRepository:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path

    def get_products_by_hash(self, hashes: list[str]) -> list[Product]:
        with connect(self.db_path) as con:
            products_df = con.execute(
                f"SELECT * FROM products WHERE hash IN ({','.join(['?'] * len(hashes))})",
                hashes
            ).fetchdf()

        return [Product(**row) for row in products_df.to_dict(orient="records")]

    def get_orders_by_hash(self, hashes: list[str]) -> list[Order]:
        with connect(self.db_path) as con:
            orders_df = con.execute(
                f"SELECT * FROM orders WHERE hash IN ({','.join(['?'] * len(hashes))})",
                hashes
            ).fetchdf()

        return [Order(**row) for row in orders_df.to_dict(orient="records")]

    def add_orders(self, orders: list[Order]):
        with connect(self.db_path) as con:
            for order in orders:
                con.execute(
                    "INSERT INTO orders (id, event_timestamp, hostname, user_pseudo_id, currency, value, source, medium, campaign, hash) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        order.id,
                        order.event_timestamp,
                        order.hostname,
                        order.user_pseudo_id,
                        order.currency,
                        order.value,
                        order.source,
                        order.medium,
                        order.campaign,
                        order.hash,
                    )
                )

    def add_products(self, products: list[Product]):
        with connect(self.db_path) as con:
            for product in products:
                con.execute(
                    "INSERT INTO products (id, feed_id, name, hash) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        product.id,
                        product.feed_id,
                        product.name,
                        product.hash,
                    )
                )

    def add_order_products(self, order_products: list[OrderProduct]):
        with connect(self.db_path) as con:
            for op in order_products:
                con.execute(
                    "INSERT INTO order_to_products (order_id, product_id, price, quantity) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        op.order_id,
                        op.product_id,
                        op.price,
                        op.quantity,
                    )
                )

    def get_orders(self, page: int, page_size: int) -> list[Order]:
        with connect(self.db_path) as con:
            orders_df = con.execute(
                f"SELECT * FROM orders LIMIT {page_size} OFFSET {page * page_size}"
            ).fetchdf()

        return [Order(**row) for row in orders_df.to_dict(orient="records")]