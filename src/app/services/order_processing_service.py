from app.adapters.duckdb_backend import fetch_orders, get_products_by_feed_ids, add_orders, add_products, add_order_products
from app.domain.models import Order, OrderProduct, Product
from app.mappers.order_mapper import map_order_dto_to_domain_order


def process_orders():
    # Fetch orders from the DuckDB backend
    print("Fetching orders...")
    fetched_orders = fetch_orders()

    # Map fetched orders to domain models
    print("Mapping orders to domain models...")
    mapped_orders = [map_order_dto_to_domain_order(order) for order in fetched_orders]

    # Remove existing products to avoid duplicates
    print("Removing existing products...")
    cleaned_orders = remove_existing_products(mapped_orders)

    # Save into the database
    print("Saving orders into the database...")
    add_orders([order for order, _, _ in cleaned_orders])
    print("Saving products into the database...")
    add_products([product for _, products, _ in cleaned_orders for product in products])
    print("Saving order-product relationships into the database...")
    add_order_products([order_product for _, _, order_products in cleaned_orders for order_product in order_products])

# TODO remove_existing_orders

def remove_existing_products(orders: list[tuple[Order, list[Product], list[OrderProduct]]]) -> list[tuple[Order, list[Product], list[OrderProduct]]]:
    # Gather all feed_ids from the products in the orders
    feed_ids = {product.feed_id for _, products, _ in orders for product in products}
    # Fetch existing products from the database
    existing_products = get_products_by_feed_ids(feed_ids)

    result: list[tuple[Order, list[Product], list[OrderProduct]]] = []

    for order, products, order_products in orders:
        result_products: list[Product] = []
        result_order_products: list[OrderProduct] = []

        for product in products:
            # Find all order products related to this product
            order_products_to_update: list[OrderProduct] = [op for op in order_products if op.product_id == product.id]
            # Check if product exists in existing_products
            existing_product = next((p for p in existing_products if p.feed_id == product.feed_id and p.name == product.name), None)
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