"""Main data structures."""
from __future__ import annotations

# std
from dataclasses import dataclass, field
from enum import IntEnum
import hashlib
from io import BytesIO
from typing import cast, Mapping, Type

# external
from redis import Redis
from redis.client import Pipeline

# module
from . import _serdes
from ._constants import Encoding, FUNSIES, hash_t, join
from ._short_hash import hash_save
from .errors import Error, Result


def _artefacts(inp: dict[bytes, bytes]) -> dict[str, Encoding]:
    out = {}
    for key, val in inp.items():
        out[key.decode()] = Encoding(val.decode())
    return out


# --------------------------------------------------------------------------------
class FunsieHow(IntEnum):
    """Kinds of funsies.

    This enum contains the various kinds of funsies: python code, shell code,
    etc.
    """

    python = 0
    shell = 1
    subdag = 2


@dataclass
class Funsie:
    """A funsie is a wrapped command that can be backed up to the KV store.

    A Funsie has a `how`, a `what` (a string that identifies the funsie, such
    as a function name or shell commands) and input and output artefact names.
    All of these are used to generate the hash of the Funsie instance.

    Funsies also have an `extra` field that include auxiliary data that is not
    to be used in hashing the funsie, but is useful for executing it, such as
    a cloudpickled python function.

    """

    how: FunsieHow
    what: str
    inp: dict[str, Encoding]
    out: dict[str, Encoding]
    extra: dict[str, bytes]
    error_tolerant: int = 0
    hash: hash_t = field(init=False)

    def decode(
        self: Funsie, input_data: Mapping[str, Result[BytesIO]]
    ) -> dict[str, Result[object]]:
        """Decode input data according to `inp`."""
        out = {}
        for key, enc in self.inp.items():
            element = input_data[key]
            if isinstance(element, Error):
                out[key] = cast(Result, element)
            else:
                out[key] = _serdes.decode(enc, element.read())

            if self.error_tolerant == 0 and isinstance(out[key], Error):
                raise RuntimeError(
                    f"Decoding of input data {key} failed:\n{out[key].details}"
                )
        return out

    def put(self: Funsie, db: Redis[bytes]) -> None:
        """Save a Funsie to Redis."""
        db.hset(
            join(FUNSIES, self.hash),
            mapping={
                "hash": self.hash,
                "how": int(self.how),
                "what": self.what,
                "error_tolerant": self.error_tolerant,
            },
        )
        if self.inp:
            db.hset(
                join(FUNSIES, self.hash, "inp"),
                mapping=dict([(k, v.value) for k, v in self.inp.items()]),
            )
        if self.out:
            db.hset(
                join(FUNSIES, self.hash, "out"),
                mapping=dict([(k, v.value) for k, v in self.out.items()]),
            )
        if self.extra:
            db.hset(
                join(FUNSIES, self.hash, "extra"), mapping=self.extra  # type:ignore
            )

        # Save the hash in the quickhash db
        hash_save(db, self.hash)

    @classmethod
    def grab(cls: Type[Funsie], db: Redis[bytes], hash: hash_t) -> "Funsie":
        """Grab a Funsie from the Redis store."""
        pipe: Pipeline = db.pipeline(transaction=False)
        pipe.exists(join(FUNSIES, hash))
        pipe.hgetall(join(FUNSIES, hash))
        pipe.hgetall(join(FUNSIES, hash, "inp"))
        pipe.hgetall(join(FUNSIES, hash, "out"))
        pipe.hgetall(join(FUNSIES, hash, "extra"))
        exists, metadata, inp, out, extra = pipe.execute()

        if not exists:
            raise RuntimeError(f"No funsie at {hash}")

        return Funsie(
            how=FunsieHow(int(metadata[b"how"].decode())),
            what=metadata[b"what"].decode(),
            error_tolerant=int(metadata[b"error_tolerant"].decode()),
            inp=_artefacts(inp),
            out=_artefacts(out),
            extra=dict([(k.decode(), v) for k, v in extra.items()]),
        )

    def __str__(self: Funsie) -> str:
        """Get the string representation of a funsie."""
        # ==============================================================
        #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
        # --------------------------------------------------------------
        # When hashes change, previous databases become deprecated. This
        # (will) require a change in version number!
        out = f"how={self.how}\n" + f"what={self.what}\n"
        for key in sorted(self.inp.keys()):
            out += f"input:{key} -> {self.inp[key].value}\n"
        for key in sorted(self.out.keys()):
            out += f"output:{key} -> {self.out[key].value}\n"
        out += f"error tolerant:{self.error_tolerant}\n"
        # ==============================================================
        return out

    def __post_init__(self: Funsie) -> None:
        """Calculate the hash."""
        # ==============================================================
        #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
        # --------------------------------------------------------------
        # When hashes change, previous databases become deprecated. This
        # (will) require a change in version number!
        m = hashlib.sha1()
        # header
        m.update(b"funsie")
        # funsie
        m.update(str(self).encode())
        self.hash = hash_t(m.hexdigest())
        # ==============================================================
