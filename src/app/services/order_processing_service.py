from app.adapters.duckdb_backend import fetch_orders
from app.domain.models import Order, OrderProduct, Product
from app.mappers.order_mapper import map_order_dto_to_domain_order
from app.adapters.repositories.db_repository import DbRepository


def process_orders():
    # Fetch orders from the DuckDB backend
    print("Fetching orders...")
    fetched_orders = fetch_orders()

    # Map fetched orders to domain models
    print("Mapping orders to domain models...")
    mapped_orders = [map_order_dto_to_domain_order(order) for order in fetched_orders]

    # Repository instance
    repo = DbRepository()

    # Remove existing orders to avoid duplicates
    print("Removing existing orders...")
    mapped_orders = deduplicate_orders(repo, mapped_orders)

    if not mapped_orders:
        print("No new orders to process.")
        return

    # Remove existing products to avoid duplicates
    print("Removing existing products...")
    mapped_orders = deduplicate_products(repo, mapped_orders)

    # Save into the database
    print("Saving orders into the database...")
    repo.add_orders([order for order, _, _ in mapped_orders])
    print("Saving products into the database...")
    repo.add_products([product for _, products, _ in mapped_orders for product in products])
    print("Saving order-product relationships into the database...")
    repo.add_order_products([order_product for _, _, order_products in mapped_orders for order_product in order_products])

def deduplicate_orders(repo: DbRepository, orders: list[tuple[Order, list[Product], list[OrderProduct]]]) -> list[tuple[Order, list[Product], list[OrderProduct]]]:
    # Gather all hashes from the orders
    order_hashes = [order.hash for order, _, _ in orders]
    # Fetch existing orders from the database
    existing_orders = repo.get_orders_by_hash(order_hashes)
    # dict hash ; order
    existing_order_hashes = {order.hash: order for order in existing_orders}

    result: list[tuple[Order, list[Product], list[OrderProduct]]] = []

    seen = set(existing_order_hashes.keys())

    for order, products, order_products in orders:
        h = order.hash
        
        if h in seen:
            continue
        result.append((order, products, order_products))
        seen.add(h)

    return result

def deduplicate_products(repo: DbRepository, orders: list[tuple[Order, list[Product], list[OrderProduct]]]) -> list[tuple[Order, list[Product], list[OrderProduct]]]:
    # Gather all hashes from the products in the orders
    product_hashes = {product.hash for _, products, _ in orders for product in products}
    # Fetch existing products from the database
    existing_products: list[Product] = repo.get_products_by_hash(product_hashes)

    result: list[tuple[Order, list[Product], list[OrderProduct]]] = []

    for order, products, order_products in orders:
        result_products: list[Product] = []
        result_order_products: list[OrderProduct] = []

        for product in products:
            # Find all order_products related to this product
            order_products_to_update: list[OrderProduct] = [op for op in order_products if op.product_id == product.id]
            # Check if product exists in existing_products
            existing_product = next((p for p in existing_products if p.hash == product.hash), None)
            if existing_product:
                # found matching product in existing_products
                # Reference the existing product instead of adding a new one
                for op in order_products_to_update:
                    op.product_id = existing_product.id
                
                # Skip adding the product, just add the order products with updated product_id reference
                # Check for result_order_product with same product_id to avoid duplicates - only add up the quantity
                for op in order_products_to_update:
                    existing_op = next((r for r in result_order_products if r.product_id == op.product_id and r.order_id == op.order_id), None)
                    if existing_op:
                        existing_op.quantity += op.quantity
                    else:
                        result_order_products.append(op)
                continue
            else:
                # New product, keep it
                result_products.append(product)
                existing_products.append(product)  # Add to existing to avoid duplicates in this run
                result_order_products.extend(order_products_to_update)

        result.append((order, result_products, result_order_products))

    return result