"""Engines for storing and retrieving artefacts."""
from __future__ import annotations

# std
from io import BytesIO
from typing import NewType, Optional

# module
from ._constants import hash_t
from .errors import Error, Result

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
