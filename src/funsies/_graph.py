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
    TAGS,
    TAGS_SET,
)
from .errors import Error, ErrorKind, get_error, Result, set_error


# --------------------------------------------------------------------------------
# Artefact status
class ArtefactStatus(IntEnum):
    """Status of data associated with an artefact."""

    no_data = 0
    # > absent  =  artefact has been computed
    done = 1
    const = 2
    error = 3


def get_status(db: Redis, address: hash_t) -> ArtefactStatus:
    """Get the status of a given operation or artefact."""
    val = db.hget(DATA_STATUS, address)
    if val is None:
        return ArtefactStatus.no_data
    else:
        return ArtefactStatus(int(val))


# --------------------------------------------------------------------------------
# Artefact
@dataclass(frozen=True)
class Artefact:
    """An instantiated artefact."""

    hash: hash_t
    parent: hash_t

    def pack(self: "Artefact") -> bytes:
        """Pack an Artefact to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Artefact"], data: bytes) -> "Artefact":
        """Unpack an Artefact from a byte string."""
        return Artefact(**unpackb(data))


def is_artefact(db: Redis, address: hash_t) -> bool:
    """Check whether a hash corresponds to an artefact."""
    return db.hexists(ARTEFACTS, address)


# Mark artefacts
def mark_done(db: Redis, address: hash_t) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == 2:
        logging.warning("attempted to mark done a const artefact.")
    elif old == 0:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.done))


def mark_error(db: Redis, address: hash_t, error: Error) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == 2:
        logging.warning("attempted to mark in error a const artefact.")
    elif old == 0:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.error))
        set_error(db, address, error)


# Tag artefacts
def tag_artefact(db: Redis, address: hash_t, tag: str) -> None:
    """Set the status of a given operation or artefact."""
    _ = db.sadd(TAGS + tag, address)  # type:ignore
    _ = db.sadd(TAGS_SET, tag)  # type:ignore


# Save and load artefacts
def get_data(
    store: Redis, artefact: Artefact, source: Optional[hash_t] = None
) -> Result[bytes]:
    """Retrieve data corresponding to an artefact."""
    # First check the status
    stat = get_status(store, artefact.hash)

    if stat == ArtefactStatus.error:
        return get_error(store, artefact.hash)
    elif stat <= ArtefactStatus.no_data:
        return Error(
            kind=ErrorKind.NotFound,
            details="No data associated with artefact.",
            source=source,
        )
    else:
        valb = store.hget(STORE, artefact.hash)
        if valb is None:
            return Error(
                kind=ErrorKind.Mismatch,
                details="expected data was not found",
                source=source,
            )
        return valb


def set_data(store: Redis, artefact: Artefact, value: bytes) -> None:
    """Update an artefact with a value."""
    stat = get_status(store, artefact.hash)
    if stat == ArtefactStatus.const:
        raise TypeError("Attempted to set data to a const artefact.")

    _ = store.hset(
        STORE,
        artefact.hash,
        value,
    )
    # value of None means that the data was not obtained but is actually
    # "ready" so to speak.
    mark_done(store, artefact.hash)


def constant_artefact(store: Redis, value: bytes) -> Artefact:
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
    h = hash_t(m.hexdigest())
    # ==============================================================

    node = Artefact(hash=h, parent=hash_t("root"))
    # store the artefact
    val = store.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )
    if val != 1:
        logging.debug(f"Const artefact at {h} already exists.")
    # store the artefact data
    _ = store.hset(
        STORE,
        h,
        value,
    )
    # mark the artefact as const
    _ = store.hset(DATA_STATUS, h, int(ArtefactStatus.const))
    return node


def variable_artefact(store: Redis, parent_hash: hash_t, name: str) -> Artefact:
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
    h = hash_t(m.hexdigest())
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


def get_artefact(store: Redis, hash: hash_t) -> Artefact:
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
    ophash = hash_t(m.hexdigest())
    # ==============================================================

    # Setup the output artefacts.
    out_art = {}
    for key in funsie.out:
        out_art[key] = variable_artefact(store, ophash, key).hash

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
