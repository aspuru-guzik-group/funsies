"""Functions for describing redis-backed DAGs."""
from __future__ import annotations

# std
from dataclasses import asdict, dataclass
from enum import IntEnum
import hashlib
from typing import cast, Optional, Type

# external
from msgpack import packb, unpackb
from redis import Redis
from redis.client import Pipeline

# module
from ._funsies import Funsie, store_funsie
from ._short_hash import hash_save
from .config import Options
from .constants import (
    ARTEFACTS,
    BLOCK_SIZE,
    DAG_CHILDREN,
    DAG_PARENTS,
    DATA_STATUS,
    hash_t,
    OPERATIONS,
    OPTIONS,
    STORE,
    TAGS,
    TAGS_SET,
    join,
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

    deleted = -2
    no_data = 0
    # > absent  =  artefact has been computed
    done = 1
    const = 2
    error = 3


def get_status(db: Redis[bytes], address: hash_t) -> ArtefactStatus:
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


def is_artefact(db: Redis[bytes], address: hash_t) -> bool:
    """Check whether a hash corresponds to an artefact."""
    return db.hexists(ARTEFACTS, address)


# Mark artefacts
def mark_done(db: Redis[bytes], address: hash_t) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error("attempted to mark done a const artefact.")
    else:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.done))


def mark_error(db: Redis[bytes], address: hash_t, error: Error) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error("attempted to mark in error a const artefact.")
    else:
        _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.error))
        set_error(db, address, error)


# Tag artefacts
def tag_artefact(db: Redis[bytes], address: hash_t, tag: str) -> None:
    """Set the status of a given operation or artefact."""
    _ = db.sadd(TAGS + tag, address)
    _ = db.sadd(TAGS_SET, tag)


# Delete artefact
def delete_artefact(db: Redis[bytes], address: hash_t) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error(f"attempted to delete const artefact {address[:6]}.")
        logger.error("you should set it to a different value instead")
        return

    # mark as deleted
    _ = db.hset(DATA_STATUS, address, int(ArtefactStatus.deleted))

    # delete previous data
    count = 0
    while True:
        if count > 0:
            where = address + f"_{count}"
        else:
            where = address
        i = db.hdel(STORE, where)
        if not i:
            break
        count += 1


def _set_block_size(n: int) -> None:
    """Change block size."""
    global BLOCK_SIZE
    BLOCK_SIZE = n


# Save and load artefacts
def get_data(
    store: Redis[bytes], artefact: Artefact, source: Optional[hash_t] = None
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


def set_data(store: Redis[bytes], artefact: Artefact, value: bytes) -> None:
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


def constant_artefact(store: Redis[bytes], value: bytes) -> Artefact:
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

    pipe: Pipeline = store.pipeline(transaction=False)
    # store the artefact
    val = pipe.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )
    hash_save(pipe, h)
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
    pipe.execute()
    return node


def variable_artefact(store: Redis[bytes], parent_hash: hash_t, name: str) -> Artefact:
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
    hash_save(store, h)

    if val != 1:
        logger.debug(
            f'Artefact with parent {parent_hash} and name "{name}" already exists.'
        )
    return node


def get_artefact(store: Redis[bytes], hash: hash_t) -> Artefact:
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
    inp: dict[str, hash_t]
    out: dict[str, hash_t]

    def put(self: "Operation", db: Redis[bytes]):
        """Save an operation to Redis."""
        if self.inp:
            db.hset(join(OPERATIONS, self.hash, "inp"), mapping=self.inp)  # type:ignore
        if self.out:
            db.hset(join(OPERATIONS, self.hash, "out"), mapping=self.out)  # type:ignore
        db.hset(
            join(OPERATIONS, self.hash, "metadata"),
            mapping={"funsie": self.funsie, "hash": self.hash},
        )
        db.set(join(OPERATIONS, self.hash), "1")

    @classmethod
    def grab(cls: Type["Operation"], db: Redis[bytes], hash: hash_t) -> "Operation":
        """Grab an operation from the Redis store."""
        if not db.exists(join(OPERATIONS, hash)):
            raise RuntimeError(f"No operation at {hash}")

        metadata = db.hgetall(join(OPERATIONS, hash, "metadata"))
        inp = db.hgetall(join(OPERATIONS, hash, "inp"))
        out = db.hgetall(join(OPERATIONS, hash, "out"))
        return Operation(
            hash=hash_t(metadata[b"hash"].decode()),
            funsie=hash_t(metadata[b"funsie"].decode()),
            inp=dict([(k.decode(), hash_t(v.decode())) for k, v in inp.items()]),
            out=dict([(k.decode(), hash_t(v.decode())) for k, v in out.items()]),
        )


def make_op(
    store: Redis[bytes], funsie: Funsie, inp: dict[str, Artefact], opt: Options
) -> Operation:
    """Store an artefact with a defined value."""
    # Setup the input artefacts.
    inp_art = {}
    for key in inp:
        if key not in funsie.inp:
            raise AttributeError(f"Extra key {key} passed to funsie.")

    for key in funsie.inp:
        if key not in inp:
            raise AttributeError(f"Missing {key} from inputs required by funsie.")
        else:
            inp_art[key] = inp[key].hash

    # Setup a buffered command pipeline for performance
    pipe: Pipeline = store.pipeline(transaction=False)

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
    node.put(pipe)

    # Save the hash in the quickhash db
    hash_save(pipe, ophash)

    # Add parents
    root = True
    for k in inp_art.keys():
        v = inp[k]
        if v.parent != "root":
            pipe.sadd(DAG_PARENTS + ophash, v.parent)
            pipe.sadd(DAG_CHILDREN + v.parent, ophash)
            root = False
    if root:
        # This dag has no dependencies
        pipe.sadd(DAG_PARENTS + ophash, "root")
        pipe.sadd(DAG_CHILDREN + "root", ophash)

    # Execute the full transaction
    pipe.execute()
    return node


def get_op_options(store: Redis[bytes], hash: hash_t) -> Options:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.hget(OPTIONS, hash)
    if out is None:
        raise RuntimeError(f"Options for operation at {hash} could not be found.")

    return Options.unpack(out)
