"""Functions for describing redis-backed DAGs."""
# std
from dataclasses import asdict, dataclass
import hashlib
import logging
from typing import Dict, Optional, Type

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from ._funsies import Funsie
from .constants import ARTEFACTS, hash_t, OPERATIONS, STORE


@dataclass(frozen=True)
class Artefact:
    """An instantiated artefact."""

    hash: hash_t
    parent: Optional[str] = None

    def pack(self: "Artefact") -> bytes:
        """Pack an Artefact to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Artefact"], data: bytes) -> "Artefact":
        """Unpack an Artefact from a byte string."""
        return Artefact(**unpackb(data))


@dataclass(frozen=True)
class Operation:
    """An instantiated Funsie."""

    hash: hash_t
    funsie: Funsie
    inp: Dict[str, hash_t]
    out: Dict[str, hash_t]

    def pack(self: "Operation") -> bytes:
        """Pack an Operation to a bytestring."""
        return packb(
            dict(hash=self.hash, funsie=self.funsie.pack(), inp=self.inp, out=self.out)
        )

    @classmethod
    def unpack(cls: Type["Operation"], data: bytes) -> "Operation":
        """Unpack an Operation from a byte string."""
        out = unpackb(data)
        return Operation(
            hash=out["hash"],
            funsie=Funsie.unpack(out["funsie"]),
            inp=out["inp"],
            out=out["out"],
        )


# --------------------------------------------------------------------------------
# Artefacts
def get_data(store: Redis, artefact: Artefact) -> Optional[bytes]:
    """Retrieve data corresponding to an artefact."""
    valb = store.hget(STORE, artefact.hash)
    if valb is None:
        logging.warning("Attempted to retrieve missing data.")
        return None
    else:
        return valb


def set_data(store: Redis, artefact: Artefact, value: bytes) -> None:
    """Update an artefact with a value."""
    _ = store.hset(
        STORE,
        artefact.hash,
        value,
    )


def store_explicit_artefact(store: Redis, value: bytes) -> Artefact:
    """Store an artefact with a defined value."""
    m = hashlib.sha256()
    m.update(b"artefact\n")
    m.update(b"explicit\n")
    m.update(value)
    h = m.hexdigest()
    node = Artefact(hash=h)

    # store the artefact
    val = store.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )

    if val != 1:
        logging.warning(f'Artefact with value "{value!r}" already exists.')

    # store the artefact
    set_data(store, node, value)

    return node


def store_generated_artefact(store: Redis, parent_hash: str, name: str) -> Artefact:
    """Store an artefact with a generated value."""
    m = hashlib.sha256()
    m.update(b"artefact\n")
    m.update(b"generated\n")
    m.update(f"parent:{parent_hash}\n".encode())
    m.update(f"name:{name}\n".encode())
    h = m.hexdigest()
    node = Artefact(hash=h, parent=parent_hash)

    # store the artefact
    val = store.hset(
        ARTEFACTS,
        h,
        node.pack(),
    )

    if val != 1:
        logging.warning(
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


def make_op(store: Redis, funsie: Funsie, inp: Dict[str, Artefact]) -> Operation:
    """Store an artefact with a defined value."""
    # Setup the input artefacts.
    inp_art = {}
    for key in funsie.inp:
        if key not in inp:
            raise AttributeError(f"Missing {key} from inputs required by funsie.")
        else:
            inp_art[key] = inp[key].hash

    # Compute the operation's hash
    m = hashlib.sha256()
    # header
    m.update(b"op")
    # funsie
    m.update(str(funsie).encode())
    # input hashes
    for key, val in sorted(inp.items(), key=lambda x: x[0]):
        m.update(f"file={key}, hash={val}".encode())
    ophash = m.hexdigest()

    # Setup the output artefacts.
    out_art = {}
    for key in funsie.out:
        out_art[key] = store_generated_artefact(store, ophash, key).hash

    # Make the node
    node = Operation(ophash, funsie, inp_art, out_art)

    # store the node
    store.hset(
        OPERATIONS,
        ophash,
        node.pack(),
    )
    return node


def get_op(store: Redis, hash: str) -> Operation:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.hget(OPERATIONS, hash)
    if out is None:
        raise RuntimeError(f"Operation at {hash} could not be found.")

    return Operation.unpack(out)
