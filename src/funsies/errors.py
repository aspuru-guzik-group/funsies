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
    JobTimedOut = "JobTimedOut"
    NoErrorData = "NoErrorData"
    KilledBySignal = "KilledBySignal"


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
"""Result contains either a value or an `Error` instance.

`Result[T]` is a type hint that corresponds to `Union[T, Error]`. This is only
a `typing` abstraction: at runtime, a `Result[T]` is just `T` or `Error` and
does not exist as a class or instance etc.
"""


def unwrap(it: Result[T]) -> T:
    """Unwrap a `Result` type.

    Unwrap `Result[T]` and return `T`. If `Result[T]` is of type `Error`, this
    function raises.

    Args:
        it: `Result[T]`

    Returns:
        `T`

    Raises:
        UnwrapError: `Result[T]` is an `Error` instance.
    """
    if isinstance(it, Error):
        raise UnwrapError(
            f"data is errored: kind={it.kind}"
            + f"\nsource={it.source}"
            + f"\ndetails={it.details}"
        )
    else:
        return it
