"""Functional wrappers for commandline programs."""
# std
from dataclasses import dataclass
from enum import Enum, unique
import logging
from typing import Optional

# external
from diskcache import FanoutCache


# ------------------------------------------------------------------------------
# Cached files
@unique
class CachedFileType(Enum):
    """Types of cached files."""

    FILE_INPUT = 1
    FILE_OUTPUT = 2
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
    type: CachedFileType
    name: str
    cmd_id: int = -1


def __key(f: CachedFile) -> str:
    return f"{f.task_id}/{f.cmd_id}/{f.type}/{f.name}"


# -----------------------------------------------------------------------------------
# Get a file from Cache
def get_file(cache: FanoutCache, f: CachedFile) -> Optional[bytes]:
    """Pull a file from cache using a CachedFile object as identifier."""
    out = cache.get(__key(f))
    if out is None:
        logging.warning(f"file could not be found in cache: {f}")
        return None
    else:
        return bytes(out)


# -----------------------------------------------------------------------------------
# Send a file to Cache
def add_file(cache: FanoutCache, f: CachedFile, value: Optional[bytes]) -> CachedFile:
    """Store a file using a CachedFile object as identifier."""
    if value is None:
        value = b""
    code = cache.add(__key(f), value)
    if code:
        return f
    else:
        logging.warning(f"file already set in cache: {f}")
        return f
