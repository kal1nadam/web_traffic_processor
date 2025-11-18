from datetime import datetime
from typing import Optional

def normalize_timestamp(timestamp: Optional[datetime]) -> Optional[datetime]:
    if timestamp is None:
        return None
    if isinstance(timestamp, datetime):
        return timestamp.replace(tzinfo=None, microsecond=0).isoformat()
    s = str(timestamp).strip()
    if "." in s:
        s = s.split(".")[0]
    return s.replace(" ", "T")

def normalize_float(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return f"{float(value):.6f}"

def normalize_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.strip().lower()