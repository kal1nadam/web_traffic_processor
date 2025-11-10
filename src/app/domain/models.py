from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import uuid

@dataclass
class OrderProduct:
    order_id: str
    product_id: str
    price: Optional[float]
    quantity: Optional[int]

@dataclass
class Product:
    feed_id: str
    name: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class Order:
    event_timestamp: datetime
    hostname: str
    user_pseudo_id: str
    currency: Optional[str]
    value: Optional[float]
    source: Optional[str]
    medium: Optional[str]
    campaign: Optional[str]
    id: str = field(default_factory=lambda: str(uuid.uuid4()))