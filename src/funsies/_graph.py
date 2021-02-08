"""Functions for describing redis-backed DAGs."""
# std
from dataclasses import asdict, dataclass
from enum import IntEnum
import hashlib
from typing import Dict, Optional, Type

# external
from msgpack import packb, unpackb
from redis import Redis
from redis.client import Pipeline

# module
from ._funsies import Funsie, store_funsie
from .config import Options
from .constants import (
    ARTEFACTS,
    BLOCK_SIZE,
    DATA_STATUS,
    hash_t,
    OPERATIONS,
    OPTIONS,
    SREADY,
    SRUNNING,
    STORE,
    TAGS,
    TAGS_SET,
)
from .errors import Error, ErrorKind, get_error, Result, set_error
from .logging import logger

# Max redis value size in bytes
MIB = 1024 * 1024
MAX_VALUE_SIZE = 512 * MIB


# --------------------------------------------------------------------------------
# Artefact status
class ArtefactStatus(IntEnum):
    """Status of data associated with an artefact."""

    deleted = -10
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
    if old == ArtefactStatus.const:
        logger.error("attempted to mark done a const artefact.")
    else:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.done))


def mark_deleted(db: Redis, address: hash_t) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error("attempted to mark deleted a const artefact.")
    else:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.deleted))


def mark_error(db: Redis, address: hash_t, error: Error) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error("attempted to mark in error a const artefact.")
    else:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.error))
        set_error(db, address, error)


# Tag artefacts
def tag_artefact(db: Redis, address: hash_t, tag: str) -> None:
    """Set the status of a given operation or artefact."""
    _ = db.sadd(TAGS + tag, address)  # type:ignore
    _ = db.sadd(TAGS_SET, tag)  # type:ignore


def _set_block_size(n: int) -> None:
    """Change block size."""
    global BLOCK_SIZE
    BLOCK_SIZE = n


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
            details=f"No data associated with artefact: {stat}.",
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

        count = 1
        while True:
            # Load more data if it is split
            nval = store.hget(STORE, artefact.hash + f"_{count}")
            if nval is None:
                break
            else:
                valb += nval
                count = count + 1

        return valb


def set_data(store: Redis, artefact: Artefact, value: bytes) -> None:
    """Update an artefact with a value."""
    stat = get_status(store, artefact.hash)
    if stat == ArtefactStatus.const:
        raise TypeError("Attempted to set data to a const artefact.")

    # delete previous data
    count = 0
    while True:
        if count > 0:
            where = artefact.hash + f"_{count}"
        else:
            where = artefact.hash
        i = store.hdel(STORE, where)
        if not i:
            break
        count += 1

    count = 0
    k = 0
    # Split data in blocks if need be.
    while True:
        nextk = min(k + BLOCK_SIZE, len(value))
        val = value[k:nextk]
        if count > 0:
            where = artefact.hash + f"_{count}"
        else:
            where = artefact.hash

        if len(val) > MAX_VALUE_SIZE:
            raise RuntimeError(
                f"Data too large to save in db, size={len(val)/MIB} MiB."
            )

        _ = store.hset(
            STORE,
            where,
            val,
        )

        count = count + 1
        if nextk == len(value):
            break
        else:
            k = nextk

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

    pipe: Pipeline = store.pipeline(transaction=False)  # type:ignore
    # store the artefact
    val = pipe.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )
    if val != 1:
        logger.debug(f"Const artefact at {h} already exists.")
    # store the artefact data
    _ = pipe.hset(
        STORE,
        h,
        value,
    )
    # mark the artefact as const
    _ = pipe.hset(DATA_STATUS, h, int(ArtefactStatus.const))
    pipe.execute()  # type:ignore
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
        logger.debug(
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


def make_op(
    store: Redis, funsie: Funsie, inp: Dict[str, Artefact], opt: Options
) -> Operation:
    """Store an artefact with a defined value."""
    # Setup the input artefacts.
    inp_art = {}
    dependencies = set()
    for key in inp:
        if key not in funsie.inp:
            raise AttributeError(f"Extra key {key} passed to funsie.")

    for key in funsie.inp:
        if key not in inp:
            raise AttributeError(f"Missing {key} from inputs required by funsie.")
        else:
            inp_art[key] = inp[key].hash
            dependencies.add(inp[key].parent)

    # Setup a buffered command pipeline for performance
    pipe: Pipeline = store.pipeline(transaction=False)  # type:ignore

    # save funsie
    store_funsie(pipe, funsie)

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
        out_art[key] = variable_artefact(pipe, ophash, key).hash

    # Make the node
    node = Operation(ophash, funsie.hash, inp_art, out_art)

    # store the runtime options for the node
    pipe.hset(
        OPTIONS,
        ophash,
        opt.pack(),
    )

    # store the node
    pipe.hset(
        OPERATIONS,
        ophash,
        node.pack(),
    )

    # Add to the ready list and remove from the running list if it was
    # previously aborted.
    pipe.srem(SRUNNING, ophash)
    pipe.sadd(SREADY, ophash)  # type:ignore

    # Execute the full transaction
    pipe.execute()  # type:ignore
    return node


def get_op(store: Redis, hash: hash_t) -> Operation:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.hget(OPERATIONS, hash)
    if out is None:
        raise RuntimeError(f"Operation at {hash} could not be found.")

    return Operation.unpack(out)


def get_op_options(store: Redis, hash: hash_t) -> Options:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.hget(OPTIONS, hash)
    if out is None:
        raise RuntimeError(f"Options for operation at {hash} could not be found.")

    return Options.unpack(out)


def reset_locks(db: Redis) -> None:
    """Reset all the operation locks."""
    # store the node
    keys = db.hkeys(OPERATIONS)
    pipe = db.pipeline(transaction=True)  # type:ignore
    for key in keys:
        # Add to the ready list and remove from the running list if it was
        # previously aborted.
        pipe.srem(SRUNNING, key)
        pipe.sadd(SREADY, key)
