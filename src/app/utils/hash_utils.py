import hashlib

def compute_hash(*args) -> str:
    """Compute a deterministic short SHA256 hash from multiple fields."""
    raw = ":".join(str(a).strip().lower() for a in args if a is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]