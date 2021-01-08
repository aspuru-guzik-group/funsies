"""Error for artefact results."""
# std
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional, TypeVar, Union

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from .constants import ERRORS, hash_t


class ErrorKind(str, Enum):
    """Kinds of errors."""

    NotFound = "NotFound"
    Mismatch = "Mismatch"
    MissingOutput = "RunError"
    ExceptionRaised = "ExceptionRaised"


@dataclass
class Error:
    """An Error value for artefacts."""

    kind: ErrorKind
    source: Optional[hash_t] = None
    details: Optional[str] = None


# An Option type
T = TypeVar("T")
Option = Union[Error, T]


def set_error(db: Redis, address: hash_t, error: Error) -> None:
    """Save an Error to redis."""
    _ = db.hset(ERRORS, address, packb(asdict(error)))


def get_error(db: Redis, address: hash_t) -> Error:
    """Load an Error from redis."""
    val = db.hget(ERRORS, address)
    assert val is not None  # TODO:fix
    out = unpackb(val)
    return Error(**out)


def unwrap(dat: Option[T]) -> T:
    """Unwrap an option type."""
    if isinstance(dat, Error):
        raise RuntimeError(
            "data is errored: kind={dat.ErrorKind}"
            + "\ndetails={dat.details}"
            + "\nsource={dat.source}"
        )
    else:
        return dat
