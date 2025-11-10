from dataclasses import dataclass, field
from typing import Optional
from numpy import long

@dataclass
class ProductDTO:
    feed_id: str
    name: str
    price: Optional[float]
    quantity: Optional[int]

@dataclass
class OrderDTO:
    event_timestamp: long
    hostname: str
    user_pseudo_id: str
    currency: Optional[str]
    value: Optional[float]
    source: Optional[str]
    medium: Optional[str]
    campaign: Optional[str]
    products: list[ProductDTO] = field(default_factory=list)
