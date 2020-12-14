"""Functional wrappers for commandline programs."""
# std
from dataclasses import dataclass
from enum import Enum, unique
import logging
from typing import Dict, Optional, Sequence, Tuple, Union, IO, Literal, overload

# external
from diskcache import FanoutCache

# module
from .constants import _AnyPath


# ------------------------------------------------------------------------------
# Cached files
@unique
class FileType(Enum):
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
    type: FileType
    name: str
    cmd_id: int = -1


def __key(f: CachedFile) -> str:
    return f"{f.task_id}/{f.cmd_id}/{f.type}/{f.name}"


# Get a file from Cache
@overload
def get_file(cache: FanoutCache, f: CachedFile, handle: Literal[True]) -> IO[bytes]:
    ...


@overload
def get_file(cache: FanoutCache, f: CachedFile, handle: Literal[False]) -> bytes:
    ...


def get_file(
    cache: FanoutCache, f: CachedFile, handle: bool = False
) -> Union[bytes, IO[bytes]]:
    out = cache.get(__key(f), read=handle)
    if out is None:
        # TODO handle properly
        raise RuntimeError("Could not set file value")
    else:
        return out


# Send a file to Cache
def set_file(cache: FanoutCache, f: CachedFile, value: bytes) -> CachedFile:
    code = cache.set(__key(f), value)
    if code:
        return f
    else:
        # TODO handle properly
        raise RuntimeError("Could not set file value")
