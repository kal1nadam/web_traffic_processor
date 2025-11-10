from app.domain.models import Order, OrderProduct, Product
from app.dto.order_dto import OrderDTO
from datetime import datetime, timezone


def map_order_dto_to_domain_order(dto: OrderDTO) -> tuple[Order, list[Product], list[OrderProduct]]:
    # convert unix microseconds to UTC datetime
    event_dt = datetime.fromtimestamp(dto.event_timestamp / 1_000_000, tz=timezone.utc)

    # create the Order
    order = Order(
        event_timestamp=event_dt,
        hostname=dto.hostname,
        user_pseudo_id=dto.user_pseudo_id,
        currency=dto.currency,
        value=dto.value,
        source=dto.source,
        medium=dto.medium,
        campaign=dto.campaign,
    )

    products: list[Product] = []
    order_products: list[OrderProduct] = []

    # map each DTO product to a domain Product + junction entity
    for p in dto.products:
        product = Product(
            feed_id=p.feed_id,
            name=p.name,
        )
        products.append(product)

        order_products.append(
            OrderProduct(
                order_id=order.id,
                product_id=product.id,
                price=p.price,
                quantity=p.quantity,
            )
        )

    return order, products, order_products