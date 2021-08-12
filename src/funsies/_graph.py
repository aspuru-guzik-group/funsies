"""Functions for describing redis-backed DAGs."""
from __future__ import annotations

# std
from dataclasses import dataclass
from enum import IntEnum
import hashlib
import io
from typing import Any, Generic, Mapping, Optional, Type, TypeVar

# external
from redis import Redis
from redis.client import Pipeline

# module
from . import _serdes
from ._constants import _Data, ARTEFACTS, Encoding, hash_t, join, OPERATIONS
from ._funsies import Funsie
from ._logging import logger
from ._short_hash import hash_save
from ._storage import descr_t, StorageEngine
from .config import Options
from .errors import Error, ErrorKind, match, Result

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
    linked = 4


def get_status(
    db: Redis[bytes], address: hash_t, resolve_links: bool = False
) -> ArtefactStatus:
    """Get the status of an artefact."""
    if resolve_links:
        address = resolve_link(db, address)

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
    _ = db.setnx(join(ARTEFACTS, address, "status"), int(stat))


# --------------------------------------------------------------------------------
# Generic Artefacts
T = TypeVar("T")
Tdata = TypeVar("Tdata", bound=_Data)


@dataclass(frozen=True)
class Artefact(Generic[T]):
    """Artefacts are the main data structure."""

    hash: hash_t
    parent: hash_t
    kind: Encoding

    def put(self: Artefact[Any], db: Redis[bytes]) -> None:
        """Save an artefact to Redis."""
        data = dict(hash=self.hash, parent=self.parent, kind=self.kind.value)
        db.hset(
            join(ARTEFACTS, self.hash),
            mapping=data,  # type:ignore
        )
        # Save the hash in the quickhash db
        hash_save(db, self.hash)

    @classmethod
    def grab(cls: Type[Artefact[T]], db: Redis[bytes], hash: hash_t) -> Artefact[T]:
        """Grab an artefact from the Redis store."""
        pipe: Pipeline = db.pipeline(transaction=False)
        pipe.exists(join(ARTEFACTS, hash))
        pipe.hgetall(join(ARTEFACTS, hash))
        exists, data = pipe.execute()

        if not exists:
            raise RuntimeError(f"No artefact at {hash}")

        return Artefact[T](
            hash=hash_t(data[b"hash"].decode()),
            parent=hash_t(data[b"parent"].decode()),
            kind=Encoding(data[b"kind"].decode()),
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


# Make an artefact point to another artefact
def create_link(db: Redis[bytes], afrom: hash_t, ato: hash_t) -> None:
    """Set the status of a given operation or artefact."""
    # TODO do not link to artefact of different type!
    assert is_artefact(db, afrom)
    assert is_artefact(db, ato)
    old = get_status(db, afrom)
    if old == ArtefactStatus.const:
        logger.error("attempted to link a const artefact to something else.")
    else:
        set_status(db, afrom, ArtefactStatus.linked)
        key = join(ARTEFACTS, afrom, "links_to")
        db.set(key, ato)


def resolve_link(db: Redis[bytes], address: hash_t) -> hash_t:
    """Resolve any link recursively."""
    key = join(ARTEFACTS, address, "links_to")
    link = db.get(key)
    if link is None:
        return address
    else:
        out = hash_t(link.decode())
        return resolve_link(db, out)


def __get_data_loc(
    db: Redis[bytes],
    store: StorageEngine,
    address: hash_t,
    carry_error: Optional[hash_t] = None,
    do_resolve_link: bool = True,
) -> Result[descr_t]:
    """Perform all the prior step before actually retrieving data."""
    if do_resolve_link:
        address = resolve_link(db, address)

    # First check the status
    stat = get_status(db, address)

    # if it's a link, we move over to the link
    if stat == ArtefactStatus.linked:
        return Error(
            kind=ErrorKind.UnresolvedLink,
            details=f"artefact at {address} is a link and thus has no data",
            source=carry_error,
        )
    elif stat == ArtefactStatus.error:
        return Error.grab(db, address)
    elif stat <= ArtefactStatus.no_data:
        return Error(
            kind=ErrorKind.NotFound,
            details=f"No data associated with artefact: {stat}.",
            source=carry_error,
        )
    else:
        key = store.get_key(address)
        return key


# Save and load artefacts
def get_stream(
    db: Redis[bytes],
    store: StorageEngine,
    source: hash_t,
    carry_error: Optional[hash_t] = None,
    do_resolve_link: bool = True,
) -> Result[io.BytesIO]:
    """Retrieve data corresponding to an artefact."""
    key = __get_data_loc(db, store, source, carry_error, do_resolve_link)
    if isinstance(key, Error):
        return key

    stream = store.take(key)

    if isinstance(stream, Error):
        return Error(
            kind=stream.kind,
            details=stream.details,
            source=carry_error,
        )

    return stream


def get_data(
    db: Redis[bytes],
    store: StorageEngine,
    source: Artefact[T],
    carry_error: Optional[hash_t] = None,
    do_resolve_link: bool = True,
) -> Result[T]:
    """Retrieve data corresponding to an artefact."""
    stream = get_stream(db, store, source.hash, carry_error, do_resolve_link)
    if isinstance(stream, Error):
        return stream
    else:
        raw = stream.read()
        stream.close()
        return _serdes.decode(source.kind, raw, carry_error=carry_error)


def set_stream(
    db: Redis[bytes],
    store: StorageEngine,
    address: hash_t,
    value: Result[io.BytesIO],
    status: ArtefactStatus,
) -> None:
    """Update an artefact with a stream of bytes."""
    if get_status(db, address) == ArtefactStatus.const:
        if status == ArtefactStatus.const:
            pass
        else:
            raise TypeError("Attempted to set data to a const artefact.")

    if isinstance(value, Error):
        # fail gracefully
        mark_error(db, address, error=value)
        return

    key = store.get_key(address)
    stat = store.put(key, value)

    if stat is not None:
        mark_error(db, address, error=stat)
    else:
        set_status(db, address, status)


def set_data(
    db: Redis[bytes],
    store: StorageEngine,
    address: hash_t,
    value: Result[bytes],
    status: ArtefactStatus,
) -> None:
    """Update an artefact with a value."""
    # This function is just a wrapper around set_stream.
    buf = match(value, lambda x: io.BytesIO(x), lambda x: x)
    set_stream(db, store, address, buf, status)


# not pipeline-able (because of set_data)
def constant_artefact(
    db: Redis[bytes], store: StorageEngine, value: Tdata
) -> Artefact[Tdata]:
    """Db an artefact with a defined value."""
    kind = _serdes.kind(value)
    data = _serdes.encode(kind, value)
    if isinstance(data, Error):
        raise TypeError(f"constant artefact could not be encoded:\n{data}")

    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha1()
    m.update(b"artefact\n")
    m.update(b"constant\n")
    m.update(f"kind:{str(kind)}\n".encode())
    m.update(data)
    h = hash_t(m.hexdigest())
    # ==============================================================
    node = Artefact[Tdata](hash=h, parent=hash_t("root"), kind=kind)
    pipe: Pipeline = db.pipeline(transaction=False)
    node.put(pipe)
    pipe.execute()
    set_data(db, store, h, data, status=ArtefactStatus.const)
    return node


# pipeline-able
def variable_artefact(
    store: Redis[bytes],
    parent_hash: hash_t,
    name: str,
    kind: Encoding,
) -> Artefact[Any]:
    """Store an artefact with a generated value."""
    # ==============================================================
    #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
    # --------------------------------------------------------------
    # When hashes change, previous databases become deprecated. This
    # (will) require a change in version number!
    m = hashlib.sha1()
    m.update(b"artefact\n")
    m.update(b"variable\n")
    m.update(f"kind:{str(kind)}\n".encode())
    m.update(f"parent:{parent_hash}\n".encode())
    m.update(f"name:{name}\n".encode())
    h = hash_t(m.hexdigest())
    # ==============================================================
    node = Artefact[T](hash=h, parent=parent_hash, kind=kind)
    node.put(store)
    set_status_nx(store, node.hash, ArtefactStatus.no_data)
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

    # pipeline-able
    def put(self: "Operation", db: Redis[bytes]) -> None:
        """Save an operation to Redis."""
        if self.inp:
            db.hset(join(OPERATIONS, self.hash, "inp"), mapping=self.inp)  # type:ignore
        if self.out:
            db.hset(join(OPERATIONS, self.hash, "out"), mapping=self.out)  # type:ignore
        if self.options:
            db.set(join(OPERATIONS, self.hash, "options"), self.options.pack())
        db.hset(
            join(OPERATIONS, self.hash),
            mapping={"funsie": self.funsie, "hash": self.hash},
        )

        # Save the hash in the quickhash db
        hash_save(db, self.hash)

    # pipelined
    @classmethod
    def grab(cls: Type["Operation"], db: Redis[bytes], hash: hash_t) -> "Operation":
        """Grab an operation from the Redis store."""
        if not db.exists(join(OPERATIONS, hash)):
            raise RuntimeError(f"No operation at {hash}")

        pipe: Pipeline = db.pipeline(transaction=False)
        pipe.hgetall(join(OPERATIONS, hash))
        pipe.hgetall(join(OPERATIONS, hash, "inp"))
        pipe.hgetall(join(OPERATIONS, hash, "out"))
        pipe.get(join(OPERATIONS, hash, "options"))
        metadata, inp, out, tmp = pipe.execute()

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


# pipelined
def make_op(
    store: Redis[bytes], funsie: Funsie, inp: Mapping[str, Artefact[Any]], opt: Options
) -> Operation:
    """Store an artefact with a defined value."""
    # Setup the input artefacts.
    inp_art = {}
    for key in inp:
        if key not in funsie.inp:
            raise AttributeError(f"Extra key {key} passed to funsie.")
        if inp[key].kind != funsie.inp[key]:
            raise TypeError(
                f"Key {key} has type {inp[key].kind} but funsie"
                + f" expects {funsie.inp[key]}"
            )

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
    for key, type in funsie.out.items():
        out_art[key] = variable_artefact(pipe, ophash, key, type).hash

    # Make the node
    node = Operation(ophash, funsie.hash, inp_art, out_art, opt)

    # store the node
    node.put(pipe)

    # Add parents
    root = True
    for k in inp_art.keys():
        v = inp[k]
        # register op as dependent of artefacts
        pipe.sadd(join(ARTEFACTS, v.hash, "dependents"), ophash)

        if v.parent != "root":
            # register ops as dependents of other ops
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
