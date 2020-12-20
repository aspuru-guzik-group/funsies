"""Cached files."""
# std
import logging
from typing import Literal, Optional, overload

# external
from redis import Redis

# module
from .constants import __FILES, __OBJECTS
from .types import FilePtr, get_hash_id, pull


# ------------------------------------------------------------------------------
# Routine to set file *values* at locations of *pointers*
def put_file(cache: Redis, fid: FilePtr, data: Optional[bytes]) -> FilePtr:
    """Set value of a FilePtr."""
    if data is None:
        logging.warning(f"data for file {fid.name} is absent")
        d = b""
    else:
        d = data

    cache.hset(__FILES, str(fid), d)
    return fid


def pull_file(cache: Redis, fid: FilePtr) -> Optional[bytes]:
    """Extract value of a FilePtr."""
    out = cache.hget(__FILES, str(fid))
    return out


# Note: register file EITHER takes a value OR a comefrom. Importantly, it is
# not called with both or neither. In addition, both value and comefrom *must*
# be keyword arguments. Getting mypy to enfore this is the reason for all the
# fancy overloading below.

# fmt: off
@overload
def register_file(db: Redis, filename: str, *, value: bytes, comefrom: Literal[None] = None) -> FilePtr: ...  # noqa
@overload
def register_file(db: Redis, filename: str, *, comefrom: str, value: Literal[None] = None) -> FilePtr: ...  # noqa
# fmt: on


def register_file(
    db: Redis,
    filename: str,
    *,
    value: Optional[bytes] = None,
    comefrom: Optional[str] = None,
) -> FilePtr:
    """Register a new file pointer into the database."""
    invariants = b""
    if comefrom is not None:
        cf = comefrom
        invariants += comefrom.encode()
    else:
        cf = ""
        assert value is not None
        invariants += value
    invariants += filename.encode()
    task_id = get_hash_id(invariants)

    if db.hexists(__OBJECTS, task_id):
        logging.debug("file already exists, return cached version.")
        out = pull(db, task_id, which="FilePtr")
        if out is None:
            logging.error("Tried to extract FilePtr but failed! recomputing...")
        else:
            return out

    # output object
    out = FilePtr(task_id, filename, cf)

    # Save
    # TODO catch errors
    db.hset(__OBJECTS, out.task_id, out.pack())

    # If this file has an explicit value, we store it.
    # TODO different IDs for files tasks and transformers?
    if value is not None:
        put_file(db, out, value)

    return out
