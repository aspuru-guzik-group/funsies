"""Functions for data storage and retrieval."""
from __future__ import annotations

# std
from dataclasses import dataclass
import io
import traceback

# external
from redis import Redis

# module
from ._constants import ARTEFACTS, BLOCK_SIZE, hash_t, join
from .errors import Error, ErrorKind, Result

# Max redis value size in bytes
MIB = 1024 * 1024
MAX_VALUE_SIZE = 512 * MIB


def _set_block_size(n: int) -> None:
    """Change block size."""
    global BLOCK_SIZE
    BLOCK_SIZE = n


@dataclass
class Location:
    key: hash_t

    def get_bytes(self: Location) -> Result[bytes]:
        raise NotImplementedError("called get_bytes on baseclass")

    def set_bytes(self: Location, value: bytes) -> Result[None]:
        raise NotImplementedError("called set_bytes on baseclass")


@dataclass
class RedisLocation(Location):
    db: Redis[bytes]

    def get_bytes(self: RedisLocation) -> Result[bytes]:
        key = join(ARTEFACTS, self.key, "data")
        if not self.db.exists(key):
            return Error(
                kind=ErrorKind.Mismatch,
                details="expected data was not found",
            )

        return b"".join(self.db.lrange(key, 0, -1))

    def set_bytes(self: RedisLocation, value: bytes) -> Result[None]:
        key = join(ARTEFACTS, self.key, "data")
        first = True  # this workaround is to make sure that writing no data is ok.
        pipe = self.db.pipeline(transaction=True)
        pipe.delete(key)
        buf = io.BytesIO(value)
        while True:
            try:
                dat = buf.read(BLOCK_SIZE)
            except Exception:
                tb_exc = traceback.format_exc()
                return Error(
                    kind=ErrorKind.ExceptionRaised,
                    details=tb_exc,
                )

            if len(dat) == 0 and not first:
                break
            else:
                pipe.rpush(key, dat)
            first = False
        pipe.execute()
        return None


def get_location(db: Redis[bytes], key: hash_t) -> Location:

    # get the backend for this data
    return RedisLocation(key, db)


# def _set_bytes(
#     store: Redis[bytes],
#     loc: StorageUnit,
#     value: bytes,
# ) -> Result[None]:
#     """Set bytes corresponding to an artefact."""
#     # write
