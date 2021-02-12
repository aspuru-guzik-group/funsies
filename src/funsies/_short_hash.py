"""Refer to objects by smaller hashes."""
from __future__ import annotations

# std

# external
from redis import Redis

# module
from .constants import hash_t


def shorten_hash(h: hash_t) -> hash_t:
    """Shorten a hash."""
    return h[:6]


def save_short_hash(db: Redis[bytes], hash: hash_t) -> None:
    """Save the shortened version of this hash for convenient retrieval."""
    return
