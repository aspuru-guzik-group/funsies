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
from ._funsies import ART_TYPES, Funsie
from ._locations import _ARTEFACTS, _OPERATIONS, _STORE


@dataclass(frozen=True)
class Artefact:
    """An instantiated artefact."""

    hash: str
    kind: str
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

    hash: str
    funsie: Funsie
    inp: Dict[str, str]
    out: Dict[str, str]

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
# Graph generation and operations
def store_explicit_artefact(store: Redis, value: ART_TYPES) -> Artefact:
    """Store an artefact with a defined value."""
    # TODO: explicit type check of value?
    what = type(value).__name__
    bvalue = packb(value)
    m = hashlib.sha256()
    m.update(b"artefact\n")
    m.update(b"explicit\n")
    m.update(bvalue)
    h = m.hexdigest()
    node = Artefact(hash=h, kind=what)

    # store the artefact
    val = store.hset(
        _ARTEFACTS,
        h,
        node.pack(),
    )

    if val != 1:
        logging.warning(f'Artefact with value "{value!r}" already exists.')

    # store the artefact
    val = store.hset(
        _STORE,
        h,
        packb(value),
    )

    if val != 1:
        logging.warning(f'Artefact with value "{value!r}" already exists in store.')

    return node


def get_data(store: Redis, artefact: Artefact) -> Optional[ART_TYPES]:
    """Retrieve data corresponding to an artefact."""
    valb = store.hget(_STORE, artefact.hash)
    if valb is None:
        logging.warning("Attempted to retrieve missing data.")
        return None
    else:
        val = unpackb(valb)
        what = type(val).__name__
        if artefact.kind != what:
            raise TypeError(f"expected {artefact.kind}, got {what}")
        return val  # type:ignore


def update_artefact(store: Redis, artefact: Artefact, value: ART_TYPES) -> None:
    """Update an artefact with a value."""
    what = type(value).__name__
    if artefact.kind != what:
        raise TypeError(f"expected {artefact.kind}, got {what}")

    # update the artefact
    val = store.hset(
        _STORE,
        artefact.hash,
        packb(value),
    )

    if val != 1:
        logging.warning(f'Artefact with value "{value!r}" already exists.')


def store_generated_artefact(
    store: Redis, parent_hash: str, name: str, kind: str
) -> Artefact:
    """Store an artefact with a generated value."""
    m = hashlib.sha256()
    m.update(b"artefact\n")
    m.update(b"generated\n")
    m.update(f"parent:{parent_hash}\n".encode())
    m.update(f"name:{name}\n".encode())
    h = m.hexdigest()
    node = Artefact(hash=h, kind=kind, parent=parent_hash)

    # store the artefact
    val = store.hset(
        _ARTEFACTS,
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
    out = store.hget(_ARTEFACTS, hash)
    if out is None:
        raise RuntimeError(f"Artefact at {hash} could not be found.")

    return Artefact.unpack(out)


def make_op(store: Redis, funsie: Funsie, inp: Dict[str, Artefact]) -> Operation:
    """Store an artefact with a defined value."""
    # Setup the input artefacts.
    inp_art = {}
    for key, val in funsie.inp.items():
        if key not in inp:
            raise AttributeError(f"Missing {key} from inputs required by funsie.")
        elif val != inp[key].kind:
            raise TypeError(
                f"Expected input {key} of type {inp[key].kind} but got type {val} instead"
            )
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
    for key, val in funsie.out.items():
        out_art[key] = store_generated_artefact(store, ophash, key, val).hash

    # Make the node
    node = Operation(ophash, funsie, inp_art, out_art)

    # store the node
    store.hset(
        _OPERATIONS,
        ophash,
        node.pack(),
    )
    return node


def get_op(store: Redis, hash: str) -> Operation:
    """Load an operation from Redis store."""
    # store the artefact
    out = store.hget(_OPERATIONS, hash)
    if out is None:
        raise RuntimeError(f"Operation at {hash} could not be found.")

    return Operation.unpack(out)
