"""Some useful functions for workflows."""
from __future__ import annotations

# std
import pickle
from typing import Any, Callable, Optional, Sequence, TypeVar, Union

# external
from redis import Redis

# module
from ._graph import Artefact
from .config import Options
from .errors import Error, Result
from .ui import reduce

Tin = TypeVar("Tin")
Tout1 = TypeVar("Tout1")
Tout2 = TypeVar("Tout2")


def match_results(
    results: Sequence[Result[Tin]],
    some: Callable[[Tin], Tout1],
    error: Optional[Callable[[Error], Tout2]] = None,
) -> list[Union[Tout1, Tout2]]:
    """Match on result errors."""
    out: list[Union[Tout1, Tout2]] = []
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
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
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

    return reduce(
        concatenation, join, *inp, strict=strict, connection=connection, opt=opt
    )


def stop_if(
    fun: Callable[[bytes], bool],
    inp: Union[Artefact, str, bytes],
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Stop execution if a condition holds."""

    def __stop_if(inp: bytes) -> bytes:
        if fun(inp):
            raise RuntimeError("Data triggered stop.")
        else:
            return inp

    fun_name = f"stop_if:{fun.__qualname__}"
    return reduce(
        __stop_if, inp, name=fun_name, strict=False, connection=connection, opt=opt
    )


def pickled(fun: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a function so that args and return value are automatically pickled."""

    def pickled_fun(*inp: bytes) -> Any:
        unpickled = []
        for i in inp:
            try:
                unpickled += [pickle.loads(i)]
            except pickle.PickleError:
                unpickled += [i]
        out = fun(*unpickled)
        return pickle.dumps(out)

    pickled_fun.__qualname__ = fun.__qualname__ + "_pickled"
    return pickled_fun
