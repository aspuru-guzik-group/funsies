"""Engines for storing and retrieving artefacts."""
from __future__ import annotations

# std
from typing import Optional
from enum import Enum

# external
from redis import Redis

# funsies
from ._constants import hash_t

class StorageEngine:
    """Baseclass implementing the artefact storage protocol."""

    def get_key(self, hash: hash_t)->Optional[str]:
        raise NotImplementedError("Baseclass used where derived class is required.")

    def take(self, redis: Redis[bytes]):
        raise NotImplementedError("Baseclass used where derived class is required.")

    def put(self, redis: Redis[bytes]):
        raise NotImplementedError("Baseclass used where derived class is required.")

# Max size of continuous data
# BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB

# def get_bytes(
#     store: Redis[bytes],
#     source: Artefact[Any],
#     carry_error: Optional[hash_t] = None,
#     do_resolve_link: bool = True,
# ) -> Result[bytes]:
#     """Retrieve bytes corresponding to an artefact."""
#     raw = match(
#         __get_data_loc(store, source, carry_error, do_resolve_link),
#         some=lambda x: b"".join(store.lrange(x, 0, -1)),
#         none=lambda x: x,
#     )
#     return raw

# in the set_data function
# key = join(ARTEFACTS, address, "data")

# # write
# first = True  # this workaround is to make sure that writing no data is ok.
# pipe = store.pipeline(transaction=True)
# pipe.delete(key)
# while True:
#     try:
#         dat = buf.read(BLOCK_SIZE)
#     except Exception:
#         tb_exc = traceback.format_exc()
#         mark_error(
#             store,
#             address,
#             error=Error(
#                 kind=ErrorKind.ExceptionRaised,
#                 source=address,
#                 details=tb_exc,
#             ),
#         )
#         return

#     if len(dat) == 0 and not first:
#         break
#     else:
#         pipe.rpush(key, dat)
#     first = False
# set_status(pipe, address, status)
# pipe.execute()

class RedisStore(StorageEngine):
    pass

class StorageType(Enum):
    """An enum for the various kinds of storage engines"""
    RedisOnly = "RedisOnly"

def store_dispatch(storage_type:StorageType)->StorageEngine:
    if storage_type == StorageType.RedisOnly:
        return RedisStore()
    else:
        raise NotImplementedError(f"No engine of type {storage_type}.")

