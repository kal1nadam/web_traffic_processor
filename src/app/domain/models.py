from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Product:
    id: str
    name: str
    price: Optional[float]
    quantity: Optional[int]

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
    products: list[Product] = field(default_factory=list)