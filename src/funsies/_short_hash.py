"""Refer to objects by smaller hashes."""
from __future__ import annotations

# std

# external
from redis import Redis

# module
from .constants import HASH_INDEX, hash_t
from .logging import logger

# Constants
SHORT = 6  # Length of short hashes


def shorten_hash(h: hash_t) -> str:
    """Shorten a hash."""
    return h[:SHORT]


def hash_save(db: Redis[bytes], hash: hash_t) -> None:
    """Save the shortened version of this hash for convenient retrieval."""
    db.zadd(HASH_INDEX, {str(hash): "0"})


def hash_load(db: Redis[bytes], short_hash: str) -> list[hash_t]:
    """Save the shortened version of this hash for convenient retrieval."""
    if len(short_hash) > 64:
        raise AttributeError(f"hash {short_hash} has length {len(short_hash)} > 64")

    data = db.zrangebylex(HASH_INDEX, f"[{short_hash}", "+")  # type:ignore
    out = []
    for key in data:
        k = key.decode()
        if k[: len(short_hash)] == short_hash:
            out += [hash_t(k)]

    if len(out) == 0:
        logger.error(f"{short_hash} not found")
    elif len(out) > 1:
        logger.warning(f"{len(data)} possible values for {short_hash}")
    return out
