"""Engines for storing and retrieving artefacts."""
from __future__ import annotations

# std
from enum import Enum
from typing import Optional

# external
from redis import Redis

# module
from ._constants import hash_t
from .errors import Error, Result


class StorageEngine:
    """Baseclass implementing the artefact storage protocol."""

    def get_key(self, hash: hash_t) -> Optional[str]:
        raise NotImplementedError("Baseclass used where derived class is required.")

    def take(self, key: str) -> Result[bytes]:
        raise NotImplementedError("Baseclass used where derived class is required.")

    def put(self, key: str, data: bytes) -> Optional[Error]:
        raise NotImplementedError("Baseclass used where derived class is required.")
