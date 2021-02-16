"""Functions for describing redis-backed DAGs."""
from __future__ import annotations

# std
from dataclasses import dataclass
from enum import IntEnum
import hashlib
import io
from typing import Optional, Type, Union

# external
from redis import Redis
from redis.client import Pipeline

# module
from ._funsies import Funsie
from ._short_hash import hash_save
from .config import Options
from .constants import (
    ARTEFACTS,
    BLOCK_SIZE,
    hash_t,
    join,
    OPERATIONS,
)
from .errors import Error, ErrorKind, Result
from .logging import logger

# Max redis value size in bytes
MIB = 1024 * 1024
MAX_VALUE_SIZE = 512 * MIB


# --------------------------------------------------------------------------------
# Artefact status
class ArtefactStatus(IntEnum):
    """Status of data associated with an artefact."""

    deleted = -2
    not_found = -1
    no_data = 0
    # > absent  =  artefact has been computed
    done = 1
    const = 2
    error = 3


def get_status(db: Redis[bytes], address: hash_t) -> ArtefactStatus:
    """Get the status of an artefact."""
    val = db.get(join(ARTEFACTS, address, "status"))
    if val is None:
        return ArtefactStatus.not_found
    else:
        return ArtefactStatus(int(val))


def set_status(db: Redis[bytes], address: hash_t, stat: ArtefactStatus) -> None:
    """Set the status of an artefact."""
    _ = db.set(join(ARTEFACTS, address, "status"), int(stat))


def set_status_nx(db: Redis[bytes], address: hash_t, stat: ArtefactStatus) -> None:
    """Set the status of an artefact iff it has no status."""
    _ = db.setnx(join(ARTEFACTS, address, "status"), int(stat))  # type:ignore


# --------------------------------------------------------------------------------
# Artefact
@dataclass(frozen=True)
class Artefact:
    """An instantiated artefact."""

    hash: hash_t
    parent: hash_t

    def put(self: "Artefact", db: Redis[bytes]) -> None:
        """Save an artefact to Redis."""
        data = dict(hash=self.hash, parent=self.parent)
        db.hset(  # type:ignore
            join(ARTEFACTS, self.hash),
            mapping=data,  # type:ignore
        )
        # Save the hash in the quickhash db
        hash_save(db, self.hash)

    @classmethod
    def grab(cls: Type["Artefact"], db: Redis[bytes], hash: hash_t) -> "Artefact":
        """Grab an artefact from the Redis store."""
        if not db.exists(join(ARTEFACTS, hash)):
            raise RuntimeError(f"No artefact at {hash}")

        data = db.hgetall(join(ARTEFACTS, hash))
        return Artefact(
            hash=hash_t(data[b"hash"].decode()),
            parent=hash_t(data[b"parent"].decode()),
        )


def is_artefact(db: Redis[bytes], address: hash_t) -> bool:
    """Check whether a hash corresponds to an artefact."""
    return bool(db.exists(join(ARTEFACTS, address)))


# Mark artefacts
def mark_done(db: Redis[bytes], address: hash_t) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error("attempted to mark done a const artefact.")
    else:
        set_status(db, address, ArtefactStatus.done)


def mark_error(db: Redis[bytes], address: hash_t, error: Error) -> None:
    """Set the status of a given operation or artefact."""
    assert is_artefact(db, address)
    old = get_status(db, address)
    if old == ArtefactStatus.const:
        logger.error("attempted to mark in error a const artefact.")
    else:
        set_status(db, address, ArtefactStatus.error)
        error.put(db, address)


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
    set_status(db, address, ArtefactStatus.deleted)

    # delete data
    db.delete(join(ARTEFACTS, address, "data"))


def _set_block_size(n: int) -> None:
    """Change block size."""
    global BLOCK_SIZE
    BLOCK_SIZE = n


# Save and load artefacts
def get_data(
    store: Redis[bytes],
    where: Union[hash_t, Artefact],
    carry_error: Optional[hash_t] = None,
) -> Result[bytes]:
    """Retrieve data corresponding to an artefact."""
    if isinstance(where, Artefact):
        address = where.hash
    else:
        address = where

    # First check the status
    stat = get_status(store, address)
    if stat == ArtefactStatus.error:
        return Error.grab(store, address)
    elif stat <= ArtefactStatus.no_data:
        return Error(
            kind=ErrorKind.NotFound,
            details=f"No data associated with artefact: {stat}.",
            source=carry_error,
        )
    else:
        key = join(ARTEFACTS, address, "data")
        if not store.exists(key):
            return Error(
                kind=ErrorKind.Mismatch,
                details="expected data was not found",
                source=carry_error,
            )
        else:
            return b"".join(store.lrange(key, 0, -1))


def set_data(
    store: Redis[bytes],
    address: hash_t,
    value: Union[bytes, io.BytesIO],
    status: ArtefactStatus,
) -> None:
    """Update an artefact with a value."""
    if get_status(store, address) == ArtefactStatus.const:
        if status == ArtefactStatus.const:
            pass
        else:
            raise TypeError("Attempted to set data to a const artefact.")

    key = join(ARTEFACTS, address, "data")

    if isinstance(value, bytes):
        buf = io.BytesIO(value)
    else:
        buf = value

    # write
    first = True  # this workaround is to make sure that writing no data is ok.
    pipe = store.pipeline(transaction=True)
    pipe.delete(key)
    while True:
        dat = buf.read(BLOCK_SIZE)
        if len(dat) == 0 and not first:
            break
        else:
            pipe.lpush(key, dat)
        first = False
    set_status(pipe, address, status)
    pipe.execute()


def constant_artefact(store: Redis[bytes], value: bytes) -> Artefact:
    """Store an artefact with a defined value."""
    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha1()
    m.update(b"artefact\n")
    m.update(b"constant\n")
    m.update(value)
    h = hash_t(m.hexdigest())
    # ==============================================================

    node = Artefact(hash=h, parent=hash_t("root"))
    node.put(store)
    set_data(store, node.hash, value, status=ArtefactStatus.const)
    return node


def variable_artefact(store: Redis[bytes], parent_hash: hash_t, name: str) -> Artefact:
    """Store an artefact with a generated value."""
    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha1()
    m.update(b"artefact\n")
    m.update(b"variable\n")
    m.update(f"parent:{parent_hash}\n".encode())
    m.update(f"name:{name}\n".encode())
    h = hash_t(m.hexdigest())
    # ==============================================================
    node = Artefact(hash=h, parent=parent_hash)
    pipe: Pipeline = store.pipeline(transaction=False)
    node.put(pipe)
    set_status_nx(pipe, node.hash, ArtefactStatus.no_data)
    pipe.execute()
    return node


# --------------------------------------------------------------------------------
# Operations
@dataclass(frozen=True)
class Operation:
    """An operation on data in the graph."""

    hash: hash_t
    funsie: hash_t
    inp: dict[str, hash_t]
    out: dict[str, hash_t]
    options: Optional[Options] = None

    def put(self: "Operation", db: Redis[bytes]) -> None:
        """Save an operation to Redis."""
        if self.inp:
            db.hset(join(OPERATIONS, self.hash, "inp"), mapping=self.inp)  # type:ignore
        if self.out:
            db.hset(join(OPERATIONS, self.hash, "out"), mapping=self.out)  # type:ignore
        if self.options:
            db.set(join(OPERATIONS, self.hash, "options"), self.options.pack())
        db.hset(  # type:ignore
            join(OPERATIONS, self.hash),
            mapping={"funsie": self.funsie, "hash": self.hash},
        )

        # Save the hash in the quickhash db
        hash_save(db, self.hash)

    @classmethod
    def grab(cls: Type["Operation"], db: Redis[bytes], hash: hash_t) -> "Operation":
        """Grab an operation from the Redis store."""
        if not db.exists(join(OPERATIONS, hash)):
            raise RuntimeError(f"No operation at {hash}")

        metadata = db.hgetall(join(OPERATIONS, hash))
        inp = db.hgetall(join(OPERATIONS, hash, "inp"))
        out = db.hgetall(join(OPERATIONS, hash, "out"))

        tmp = db.get(join(OPERATIONS, hash, "options"))
        if tmp is not None:
            options: Optional[Options] = Options.unpack(tmp.decode())
        else:
            options = None

        return Operation(
            hash=hash_t(metadata[b"hash"].decode()),
            funsie=hash_t(metadata[b"funsie"].decode()),
            inp=dict([(k.decode(), hash_t(v.decode())) for k, v in inp.items()]),
            out=dict([(k.decode(), hash_t(v.decode())) for k, v in out.items()]),
            options=options,
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
    funsie.put(pipe)

    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha1()
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
    node = Operation(ophash, funsie.hash, inp_art, out_art, opt)

    # store the node
    node.put(pipe)

    # Add parents
    root = True
    for k in inp_art.keys():
        v = inp[k]
        if v.parent != "root":
            pipe.sadd(join(OPERATIONS, ophash, "parents"), v.parent)
            pipe.sadd(join(OPERATIONS, v.parent, "children"), ophash)
            root = False
    if root:
        # This dag has no dependencies
        pipe.sadd(join(OPERATIONS, ophash, "parents"), "root")
        pipe.sadd(join(OPERATIONS, hash_t("root"), "children"), ophash)

    # Execute the full transaction
    pipe.execute()
    return node


def get_op_options(store: Redis[bytes], hash: hash_t) -> Options:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.get(join(OPERATIONS, hash, "options"))
    if out is None:
        raise RuntimeError(f"Options for operation at {hash} could not be found.")
    return Options.unpack(out.decode())
