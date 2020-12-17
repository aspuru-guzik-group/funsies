"""Cached files."""
# std
from dataclasses import asdict, dataclass
import logging
from typing import cast, Optional, Type

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from .constants import __FILES, __IDS, __OBJECTS, __TASK_ID


# ------------------------------------------------------------------------------
# Pointers for file in redis
@dataclass(frozen=True)
class FilePtr:
    """A pointer to a file on cache.

    FilePtr is the main class that provides a way to uniquely access files
    that are currently cached for writing and reading. Basically, Tasks have
    file inputs and outputs as well as stdouts and stderr. All of these are
    stored in the cache and are accessed using the associated CachedFiles as
    keys.

    """

    task_id: str
    name: str

    def __str__(self: "FilePtr") -> str:
        """Return string representation."""
        return f"{str(self.task_id)}::{self.name}"

    def pack(self: "FilePtr") -> bytes:
        """Pack a FilePtr."""
        d = asdict(self)
        return packb(d)

    @classmethod
    def unpack(cls: Type["FilePtr"], inp: bytes) -> "FilePtr":
        """Build a transformer from packed form."""
        d = unpackb(inp)

        return FilePtr(
            task_id=d["task_id"],
            name=d["name"],
        )


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


def pull_fileptr(cache: Redis, fid: bytes) -> Optional[FilePtr]:
    """Extract a FilePtr from its id."""
    out = cache.hget(__OBJECTS, fid)
    if out is not None:
        return FilePtr.unpack(out)
    else:
        return None


def register_file(db: Redis, filename: str, value: Optional[bytes] = None) -> FilePtr:
    """Register a new file pointer into the database."""
    if value is not None:
        key = b"file contents:" + value
        if db.hexists(__IDS, key):
            logging.debug("file already exists, return cached version.")
            # if it does, get task id
            tmp = db.hget(__IDS, key)
            # pull the id from the db
            assert tmp is not None
            out = pull_fileptr(db, tmp)
            assert out is not None
            # done
            return out

    # If it doesn't exist, we make the FilePtr.
    # grab a new id
    task_id = cast(Optional[bytes], db.incrby(__TASK_ID, 1))  # type:ignore
    assert task_id is not None  # TODO fix

    # output object
    out = FilePtr(task_id, filename)

    # Save
    # TODO catch errors
    db.hset(__OBJECTS, out.task_id, out.pack())

    # If this file has an explicit value, we store it.
    # TODO different IDs for files tasks and transformers?
    if value is not None:
        put_file(db, out, value)
        db.hset(__IDS, key, task_id)

    return out
