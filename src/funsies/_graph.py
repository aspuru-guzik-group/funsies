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
    DATA_STATUS,
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

    absent = 0
    done = 1


def get_status(db: Redis, address: hash_t) -> ArtefactStatus:
    """Get the status of a given operation or artefact."""
    val = db.hget(DATA_STATUS, address)
    if val is None:
        return ArtefactStatus.absent
    else:
        return ArtefactStatus(int(val))


def set_status(db: Redis, address: hash_t, stat: ArtefactStatus) -> None:
    """Set the status of a given operation or artefact."""
    _ = db.hset(DATA_STATUS, address, int(stat))


@dataclass(frozen=True)
class Artefact:
    """An instantiated artefact."""

    hash: hash_t
    parent: str

    def pack(self: "Artefact") -> bytes:
        """Pack an Artefact to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Artefact"], data: bytes) -> "Artefact":
        """Unpack an Artefact from a byte string."""
        return Artefact(**unpackb(data))


def get_data(store: Redis, artefact: Artefact) -> Optional[bytes]:
    """Retrieve data corresponding to an artefact."""
    valb = store.hget(STORE, artefact.hash)
    if valb is None:
        logging.warning("Attempted to retrieve missing data.")
        return None
    else:
        return valb


def set_data(store: Redis, artefact: Artefact, value: Optional[bytes]) -> None:
    """Update an artefact with a value."""
    if value is not None:
        _ = store.hset(
            STORE,
            artefact.hash,
            value,
        )

    # value of None means that the data was not obtained but is actually
    # "ready" so to speak.
    set_status(store, artefact.hash, ArtefactStatus.done)


def store_explicit_artefact(store: Redis, value: bytes) -> Artefact:
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

    node = Artefact(hash=h, parent="root")

    # store the artefact
    val = store.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )

    if val != 1:
        logging.debug(f'Artefact with value "{value!r}" already exists.')

    # store the artefact
    set_data(store, node, value)

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
    node = Artefact(hash=h, parent=parent_hash)

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
