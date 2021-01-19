"""Some useful functions for workflows."""
# std
from typing import Callable, List, Optional, Sequence, TypeVar, Union

# external
from redis import Redis

# module
from ._graph import Artefact
from .errors import Error, Result
from .ui import reduce

Tin = TypeVar("Tin")
Tout1 = TypeVar("Tout1")
Tout2 = TypeVar("Tout2")


def match_results(
    results: Sequence[Result[Tin]],
    some: Callable[[Tin], Tout1],
    error: Optional[Callable[[Error], Tout2]] = None,
) -> List[Union[Tout1, Tout2]]:
    """Match on result errors."""
    out: List[Union[Tout1, Tout2]] = []
    for el in results:
        if isinstance(el, Error):
            if error is not None:
                out += [error(el)]
        else:
            out += [some(el)]
    return out


def concat(
    *inp: Union[Artefact, str, bytes],
    join: Union[Artefact, str, bytes] = b"",
    strict: bool = True,
    connection: Optional[Redis] = None
) -> Artefact:
    """Concatenate artefacts."""

    def concatenation(joiner: bytes, *args: Result[bytes]) -> bytes:
        lines = match_results(args, lambda x: x)
        out = b""
        for i, l in enumerate(lines):
            out += l
            if i != len(lines) - 1:
                out += joiner
        return out

    return reduce(concatenation, join, *inp, strict=strict, connection=connection)
