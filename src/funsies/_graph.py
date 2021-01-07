"""Functions for describing redis-backed DAGs."""
# std
from dataclasses import asdict, dataclass
from enum import IntEnum
import hashlib
import logging
from typing import Dict, Optional, Type

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from ._funsies import Funsie, store_funsie
from .constants import (
    ARTEFACTS,
    hash_t,
    OPERATIONS,
    SREADY,
    SRUNNING,
    STORE,
)


# --------------------------------------------------------------------------------
# Artefacts
class ArtefactStatus(IntEnum):
    """Status of data associated with an artefact."""

    # > 0 -> ready

    none = 0
    constant = 1
    good = 2


@dataclass(frozen=True)
class Artefact:
    """An instantiated artefact."""

    hash: hash_t
    parent: str
    status: ArtefactStatus

    def pack(self: "Artefact") -> bytes:
        """Pack an Artefact to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Artefact"], data: bytes) -> "Artefact":
        """Unpack an Artefact from a byte string."""
        args = unpackb(data)
        # args["status"] = ArtefactStatus(args["status"])
        return Artefact(**args)


def get_data(store: Redis, artefact: Artefact) -> Optional[bytes]:
    """Retrieve data corresponding to an artefact."""
    valb = store.hget(STORE, artefact.hash)
    if valb is None:
        logging.warning("Attempted to retrieve missing data.")
        return None
    else:
        return valb


def update_generated_data(
    store: Redis, artefact: Artefact, value: Optional[bytes]
) -> Artefact:
    """Update an artefact with a value."""
    if artefact.status == ArtefactStatus.constant:
        raise AttributeError(
            "Attempted to update a constant artefact."
            + "\nA new artefact should be generated instead."
        )

    if value is not None:
        _ = store.hset(
            STORE,
            artefact.hash,
            value,
        )
    # value of None means that the data was not obtained but is actually
    # "ready" so to speak.
    new_artefact = Artefact(artefact.hash, artefact.parent, ArtefactStatus.good)

    # overwrite the artefact
    _ = store.hset(
        ARTEFACTS,
        new_artefact.hash,
        new_artefact.pack(),
    )

    return new_artefact


def store_const(store: Redis, value: bytes) -> Artefact:
    """Store an artefact with a defined value."""
    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha256()
    m.update(b"artefact\n")
    m.update(b"explicit\n")
    m.update(value)
    h = m.hexdigest()
    # ==============================================================

    node = Artefact(hash=h, status=ArtefactStatus.constant, parent="")

    # store the artefact
    val = store.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )

    if val != 1:
        logging.debug(f'Artefact with value "{value!r}" already exists.')

    # store the artefact
    _ = store.hset(
        STORE,
        node.hash,
        value,
    )
    return node


def store_generated_artefact(store: Redis, parent_hash: str, name: str) -> Artefact:
    """Store an artefact with a generated value."""
    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha256()
    m.update(b"artefact\n")
    m.update(b"generated\n")
    m.update(f"parent:{parent_hash}\n".encode())
    m.update(f"name:{name}\n".encode())
    h = m.hexdigest()
    # ==============================================================
    node = Artefact(hash=h, status=ArtefactStatus.none, parent=parent_hash)

    # store the artefact
    val = store.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )
    if val != 1:
        logging.debug(
            f'Artefact with parent {parent_hash} and name "{name}" already exists.'
        )
    return node


def get_artefact(store: Redis, hash: str) -> Artefact:
    """Pull an artefact from the Redis store."""
    # store the artefact
    out = store.hget(ARTEFACTS, hash)
    if out is None:
        raise RuntimeError(f"Artefact at {hash} could not be found.")
    return Artefact.unpack(out)


# --------------------------------------------------------------------------------
# Operations
@dataclass(frozen=True)
class Operation:
    """An instantiated Funsie."""

    hash: hash_t
    funsie: hash_t
    inp: Dict[str, hash_t]
    out: Dict[str, hash_t]

    def pack(self: "Operation") -> bytes:
        """Pack an Operation to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Operation"], data: bytes) -> "Operation":
        """Unpack an Operation from a byte string."""
        return Operation(**unpackb(data))


def make_op(store: Redis, funsie: Funsie, inp: Dict[str, Artefact]) -> Operation:
    """Store an artefact with a defined value."""
    # Setup the input artefacts.
    inp_art = {}
    dependencies = set()
    for key in funsie.inp:
        if key not in inp:
            raise AttributeError(f"Missing {key} from inputs required by funsie.")
        else:
            inp_art[key] = inp[key].hash
            dependencies.add(inp[key].parent)

    # save funsie
    store_funsie(store, funsie)

    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha256()
    m.update(b"op")
    m.update(funsie.hash.encode())
    for key, val in sorted(inp.items(), key=lambda x: x[0]):
        m.update(f"file={key}, hash={val}".encode())
    ophash = m.hexdigest()
    # ==============================================================

    # Setup the output artefacts.
    out_art = {}
    for key in funsie.out:
        out_art[key] = store_generated_artefact(store, ophash, key).hash

    # Make the node
    node = Operation(ophash, funsie.hash, inp_art, out_art)

    # store the node
    store.hset(
        OPERATIONS,
        ophash,
        node.pack(),
    )

    # Add to the ready list and remove from the running list if it was
    # previously aborted.
    store.srem(SRUNNING, ophash)
    store.sadd(SREADY, ophash)  # type:ignore
    return node


def get_op(store: Redis, hash: str) -> Operation:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.hget(OPERATIONS, hash)
    if out is None:
        raise RuntimeError(f"Operation at {hash} could not be found.")

    return Operation.unpack(out)
