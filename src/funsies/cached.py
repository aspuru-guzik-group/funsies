"""Cached files."""
# std
from dataclasses import dataclass
from enum import Enum, unique
import logging
from typing import Optional, Union

# external
from redis import Redis


# ------------------------------------------------------------------------------
# Cached files
@unique
class FileType(Enum):
    """Types of cached files."""

    INP = 1
    OUT = 2
    CMD = 3


@dataclass(frozen=True)
class CachedFile:
    """A reference to a file on cache.

    CachedFile is the main class that provides a way to uniquely access files
    that are currently cached for writing and reading. Basically, Tasks have
    file inputs and outputs as well as stdouts and stderr. All of these are
    stored in the cache and are accessed using the associated CachedFiles as
    keys.

    """

    task_id: int
    type: FileType
    name: str

    def __str__(self: "CachedFile") -> str:
        """Return string representation."""
        return f"{self.task_id}|{self.type}|{self.name}"


def cache_file(
    cache: Redis,
    task_id: int,
    filetype: FileType,
    name: str,
    data: Optional[bytes],
) -> CachedFile:
    """Store a file in redis and return a CachedFile key for it."""
    key = CachedFile(task_id, FileType.OUT, name)
    if data is None:
        logging.warning(f"data for file {name} is absent")
        d = b""
    else:
        d = data

    cache.hset("funsies.files", str(key), d)
    return key
