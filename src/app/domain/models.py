from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import uuid

from app.domain.services.normalization import normalize_float, normalize_string, normalize_timestamp
from app.utils.hash_utils import compute_hash

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
    hash: Optional[str] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self):
        self.hash = compute_hash(self.feed_id, self.name)

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
    hash: Optional[str] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self):
        if self.hash is None:
            self.hash = compute_hash(
                normalize_timestamp(self.event_timestamp),
                normalize_string(self.hostname),
                normalize_string(self.user_pseudo_id),
                normalize_string(self.currency),
                normalize_float(self.value),
                normalize_string(self.source),
                normalize_string(self.medium),
                normalize_string(self.campaign)
            )