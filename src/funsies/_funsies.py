"""Main data structures."""
from __future__ import annotations

# std
from dataclasses import asdict, dataclass
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
    "what" (a bytestring that identifies the funsie, such as a function name
    or packed shell commands) and input and output artefact names. All of
    these are used to generate the hash of the Funsie instance.

    Funsies also have an aux field that include auxiliary data that is not to
    be used in key-ing the funsie, but is useful for executing it, such as a
    cloudpickled python function.

    """

    how: FunsieHow
    what: bytes
    inp: list[str]
    out: list[str]
    aux: bytes = b""
    error_tolerant: bool = False

    def put(self: "Funsie", db: Redis[bytes]):
        """Save a Funsie to Redis."""
        db.hset(  # type:ignore
            join(FUNSIES, self.hash),
            mapping={
                "how": int(self.how),
                "what": self.what,
                "aux": self.aux,
                "error_tolerant": bytes(self.error_tolerant),
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
        return Funsie(
            how=FunsieHow(int(metadata[b"how"].decode())),
            what=metadata[b"what"],
            aux=metadata[b"aux"],
            error_tolerant=bool(metadata[b"error_tolerant"]),
            inp=_to_list(inp),
            out=_to_list(out),
        )

    def __str__(self: "Funsie") -> str:
        """Get the string representation of a funsie."""
        # ==============================================================
        #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
        # --------------------------------------------------------------
        # When hashes change, previous databases become deprecated. This
        # (will) require a change in version number!
        out = f"Funsie[\n  how={self.how}" + f"\n  what={self.what!r}" + "\n  inputs\n"
        for key in sorted(self.inp):
            out += f"    {key}\n"
        out += "  outputs\n"
        for key in sorted(self.out):
            out += f"    {key}\n"

        out += f"    error tolerant: {self.error_tolerant}\n"
        out += "  ]"
        # ==============================================================
        return out

    def check_inputs(
        self: "Funsie", actual: Mapping[str, Result[bytes]]
    ) -> Tuple[Dict[str, bytes], Dict[str, Error]]:
        """Match actual inputs of funsie with expected inputs."""
        output = {}
        errors = {}
        for key in self.inp:
            if key in actual:
                val = actual[key]
                if isinstance(val, Error):
                    if self.error_tolerant:
                        errors[key] = val
                    else:
                        logger.error(f"{key} is in error.")
                        raise RuntimeError()
                else:
                    # all good: value exists, and is not Error
                    output[key] = val
            else:
                logger.error(f"{key} not found.")
                if self.error_tolerant:
                    errors[key] = Error(
                        kind=ErrorKind.MissingInput,
                        source=self.hash,
                        details="missing input in funsie.",
                    )
                else:
                    raise RuntimeError()
        return output, errors

    @property
    def hash(self: "Funsie") -> hash_t:
        """Hash of a funsie."""
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
        return hash_t(m.hexdigest())
        # ==============================================================
