"""Error for artefact results."""
from __future__ import annotations

# std
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional, TypeVar, Union

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from .constants import ERRORS, hash_t


class UnwrapError(Exception):
    """Exception thrown when unwrapping an error."""

    pass


class ErrorKind(str, Enum):
    """Kinds of errors."""

    NotFound = "NotFound"
    Mismatch = "Mismatch"
    MissingOutput = "MissingOutput"
    MissingInput = "MissingInput"
    ExceptionRaised = "ExceptionRaised"
    NoErrorData = "NoErrorData"


@dataclass
class Error:
    """An Error value for artefacts."""

    kind: ErrorKind
    source: Optional[hash_t] = None
    details: Optional[str] = None
    data: Optional[bytes] = None


def set_error(db: Redis[bytes], address: hash_t, error: Error) -> None:
    """Save an Error to redis."""
    _ = db.hset(ERRORS, address, packb(asdict(error)))


def get_error(db: Redis[bytes], address: hash_t) -> Error:
    """Load an Error from redis."""
    val = db.hget(ERRORS, address)
    if val is None:
        return Error(ErrorKind.NoErrorData)
    out = unpackb(val)
    return Error(**out)


# A simple mypy result type
T = TypeVar("T")
Result = Union[Error, T]


def unwrap(dat: Result[T]) -> T:
    """Unwrap an option type."""
    if isinstance(dat, Error):
        raise UnwrapError(
            f"data is errored: kind={dat.kind}"
            + f"\nsource={dat.source}"
            + f"\ndetails={dat.details}"
        )
    else:
        return dat
