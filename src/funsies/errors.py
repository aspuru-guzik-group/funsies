"""Error for artefact results."""
from __future__ import annotations

# std
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Type, TypeVar, Union

# external
from redis import Redis

# module
from ._constants import ARTEFACTS, hash_t, join


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

    def put(self: "Error", db: Redis[bytes], hash: hash_t) -> None:
        """Save an Error to Redis."""
        data = dict(kind=self.kind.name)
        if self.source:
            data["source"] = str(self.source)
        if self.details:
            data["details"] = str(self.details)
        db.hset(  # type:ignore
            join(ARTEFACTS, hash, "error"),
            mapping=data,  # type:ignore
        )

    @classmethod
    def grab(cls: Type["Error"], db: Redis[bytes], hash: hash_t) -> "Error":
        """Grab an Error from the Redis store."""
        if not join(ARTEFACTS, hash, "error"):
            raise RuntimeError(f"No error for artefact at {hash}")

        data = db.hgetall(join(ARTEFACTS, hash, "error"))
        kind = ErrorKind(data[b"kind"].decode())

        # Sometimes the python boilerplate is really freaking annoying...
        tmp = data.get(b"source", None)
        if tmp is not None:
            source: Optional[hash_t] = hash_t(tmp.decode())
        else:
            source = None
        tmp = data.get(b"details", None)
        if tmp is not None:
            details: Optional[str] = tmp.decode()
        else:
            details = None

        return Error(kind, source=source, details=details)


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
