"""Engines for storing and retrieving artefacts."""
from __future__ import annotations

# std
from io import BytesIO
import os
import traceback
from typing import cast, NewType, Optional

# external
from redis import Redis

# module
from ._constants import _AnyPath, ARTEFACTS, hash_t, join
from ._logging import logger
from .errors import Error, ErrorKind, Result

descr_t = NewType("descr_t", str)


class StorageEngine:
    """Baseclass implementing the artefact storage protocol."""

    def get_key(self: StorageEngine, hash: hash_t) -> descr_t:
        """Return the key for a given hash."""
        raise NotImplementedError("Baseclass used where derived class is required.")

    def take(self: StorageEngine, key: descr_t) -> Result[BytesIO]:
        """Return a bytes stream for a given key.

        Note: It is the caller's responsibility to .close() the stream.
        """
        raise NotImplementedError("Baseclass used where derived class is required.")

    def put(self: StorageEngine, key: descr_t, data: BytesIO) -> Optional[Error]:
        """Write stream data to a given key.

        Note: this does not .close() the stream.
        """
        raise NotImplementedError("Baseclass used where derived class is required.")


# --------------------------------------------------------------------------
# Storage engine configurations
# --------------------------------------------------------------------------

# Data storage on disk
class DiskStorage(StorageEngine):
    """Storage engine that puts artefact data on disk."""

    def __init__(self: DiskStorage, path: _AnyPath) -> None:
        """Return a storage engine that saves to a given path."""
        self.path = os.path.abspath(path)
        logger.info(f"saving artefacts to {self.path}")
        os.makedirs(self.path, exist_ok=True)
        self.buffer_length = 16 * 1024

    def get_key(self: DiskStorage, hash: hash_t) -> descr_t:
        """Return the key for a given hash."""
        dir = os.path.join(self.path, hash[:2])
        os.makedirs(dir, exist_ok=True)
        key = os.path.join(dir, hash[2:])
        return descr_t(str(key))

    def take(self: DiskStorage, key: descr_t) -> Result[BytesIO]:
        """Return a bytes stream for a given key.

        Note: It is the caller's responsibility to .close() the stream.
        """
        try:
            fdst = cast(BytesIO, open(key, "rb"))
        except Exception:
            tb_exc = traceback.format_exc()
            return Error(
                kind=ErrorKind.ExceptionRaised,
                details=tb_exc,
            )

        return fdst

    def put(self: DiskStorage, key: descr_t, data: BytesIO) -> Optional[Error]:
        """Write stream data to a given key.

        Note: this function does not .close() the stream.
        """
        try:
            with open(key, "wb") as fdst:
                while True:
                    buf = data.read(self.buffer_length)
                    if not buf:
                        break
                    fdst.write(buf)

        except Exception:
            tb_exc = traceback.format_exc()
            return Error(
                kind=ErrorKind.ExceptionRaised,
                details=tb_exc,
            )
        return None


_DEFAULT_BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB


class RedisStorage(StorageEngine):
    """Storage engine that puts artefact data in Redis."""

    def __init__(
        self: RedisStorage,
        conn: Redis[bytes],
        block_size: int = _DEFAULT_BLOCK_SIZE,
    ) -> None:
        """Returns a storage engine that saves to a Redis instance."""
        self.instance = conn
        self.block_size = block_size

    def get_key(self: RedisStorage, hash: hash_t) -> descr_t:
        """Return the key for a given hash."""
        key = join(ARTEFACTS, hash, "data")
        return descr_t(key)

    def take(self: RedisStorage, key: descr_t) -> Result[BytesIO]:
        """Return a bytes stream for a given key.

        Note: It is the caller's responsibility to .close() the stream.
        """
        # TODO: stream it?
        # TODO: check for truncation
        out = self.instance.lrange(key, 0, -1)
        if len(out) == 0:
            return Error(
                ErrorKind.DataNotFound,
                details=f"No data at address {key} in redis instance.",
            )
        else:
            return BytesIO(b"".join(out))

    def put(self: RedisStorage, key: descr_t, data: BytesIO) -> Optional[Error]:
        """Write stream data to a given key.

        Note: this function does not .close() the stream.
        """
        first = True  # this workaround is to make sure that writing no data is ok.
        pipe = self.instance.pipeline(transaction=True)
        pipe.delete(key)
        while True:
            try:
                dat = data.read(self.block_size)
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
