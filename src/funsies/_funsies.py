"""Main data structures."""
from __future__ import annotations

# std
from dataclasses import asdict, dataclass, field
from enum import IntEnum
import hashlib
from typing import Dict, List, Mapping, Optional, Tuple, Type

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from ._short_hash import hash_save
from .constants import FUNSIES, hash_t, join
from .errors import Error, ErrorKind, Result
from .logging import logger


def _to_list(inp: dict[bytes, bytes]) -> list[str]:
    out = []
    for i in range(len(inp)):
        out += [inp[f"{i}".encode()].decode()]
    return out


# --------------------------------------------------------------------------------
class FunsieHow(IntEnum):
    """Kinds of funsies.

    This enum contains the various kinds of funsies: python code, shell code,
    etc.
    """

    python = 0
    shell = 1


@dataclass
class Funsie:
    """A funsie is a wrapped command that can be backed up to the KV store.

    A Funsie has a "how" (an integer that defines how it is to be executed), a
    "what" (a string that identifies the funsie, such as a function name
    or shell commands) and input and output artefact names. All of
    these are used to generate the hash of the Funsie instance.

    Funsies also have an "extra" field that include auxiliary data that is not
    to be used in hashing the funsie, but is useful for executing it, such as
    a cloudpickled python function.

    """

    how: FunsieHow
    what: str
    inp: list[str]
    out: list[str]
    extra: dict[str, bytes]
    error_tolerant: int = 0
    hash: hash_t = field(init=False)

    def put(self: "Funsie", db: Redis[bytes]):
        """Save a Funsie to Redis."""
        db.hset(  # type:ignore
            join(FUNSIES, self.hash),
            mapping={
                "hash": self.hash,
                "how": int(self.how),
                "what": self.what,
                "error_tolerant": self.error_tolerant,
            },
        )
        if self.inp:
            db.hset(  # type:ignore
                join(FUNSIES, self.hash, "inp"), mapping=dict(list(enumerate(self.inp)))
            )
        if self.out:
            db.hset(  # type:ignore
                join(FUNSIES, self.hash, "out"), mapping=dict(list(enumerate(self.out)))
            )
        if self.extra:
            db.hset(  # type:ignore
                join(FUNSIES, self.hash, "extra"), mapping=self.extra  # type:ignore
            )

        # Save the hash in the quickhash db
        hash_save(db, self.hash)

    @classmethod
    def grab(cls: Type["Funsie"], db: Redis[bytes], hash: hash_t) -> "Funsie":
        """Grab a Funsie from the Redis store."""
        if not db.exists(join(FUNSIES, hash)):
            raise RuntimeError(f"No funsie at {hash}")

        metadata = db.hgetall(join(FUNSIES, hash))
        inp = db.hgetall(join(FUNSIES, hash, "inp"))
        out = db.hgetall(join(FUNSIES, hash, "out"))
        extra = db.hgetall(join(FUNSIES, hash, "extra"))
        return Funsie(
            how=FunsieHow(int(metadata[b"how"].decode())),
            what=metadata[b"what"].decode(),
            error_tolerant=int(metadata[b"error_tolerant"].decode()),
            inp=_to_list(inp),
            out=_to_list(out),
            extra=dict([(k.decode(), v) for k, v in extra.items()]),
        )

    def __str__(self: "Funsie") -> str:
        """Get the string representation of a funsie."""
        # ==============================================================
        #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
        # --------------------------------------------------------------
        # When hashes change, previous databases become deprecated. This
        # (will) require a change in version number!
        out = f"how={self.how}\n" + f"what={self.what}\n"
        for key in sorted(self.inp):
            out += f"input:{key}\n"
        for key in sorted(self.out):
            out += f"output:{key}\n"
        out += f"error tolerant:{self.error_tolerant}\n"
        # ==============================================================
        return out

    def __post_init__(self: "Funsie") -> None:
        """Calculate the hash."""
        # ==============================================================
        #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
        # --------------------------------------------------------------
        # When hashes change, previous databases become deprecated. This
        # (will) require a change in version number!
        m = hashlib.sha256()
        # header
        m.update(b"funsie")
        # funsie
        m.update(str(self).encode())
        self.hash = hash_t(m.hexdigest())
        # ==============================================================
