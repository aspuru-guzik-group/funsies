"""Cached files."""
# std
import logging
from typing import cast, Literal, Optional, overload

# external
from redis import Redis

# module
from .constants import __FILES, __IDS, __OBJECTS, __TASK_ID
from .types import FilePtr, pull


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
    if value is not None:
        cf = "0"
        key = b"file contents:" + value
        if db.hexists(__IDS, key):
            logging.debug("file already exists, return cached version.")
            # if it does, get task id
            tmp = db.hget(__IDS, key)
            # pull the id from the db
            assert tmp is not None
            out = pull(db, tmp.decode(), which="FilePtr")
            if out is None:
                logging.error("Tried to extract RTask but failed! recomputing...")
            else:
                return out

    else:
        assert comefrom is not None
        cf = comefrom

    # If it doesn't exist, we make the FilePtr.
    # grab a new id
    task_id = cast(str, str(db.incrby(__TASK_ID, 1)))  # type:ignore

    # output object
    out = FilePtr(task_id, filename, cf)

    # Save
    # TODO catch errors
    db.hset(__OBJECTS, out.task_id, out.pack())

    # If this file has an explicit value, we store it.
    # TODO different IDs for files tasks and transformers?
    if value is not None:
        put_file(db, out, value)
        db.hset(__IDS, key, task_id)

    return out
