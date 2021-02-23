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
from .ui import mapping, morph, reduce

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
    strip: bool = False,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Concatenate artefacts."""

    def concatenation(joiner: bytes, strip_flag: bytes, *args: Result[bytes]) -> bytes:
        do_strip = strip_flag.decode() == "1"
        lines = match_results(args, lambda x: x)
        out = b""
        for i, l in enumerate(lines):
            if strip:
                out += l.strip()
            else:
                out += l

            if i != len(lines) - 1:
                out += joiner
        return out

    if strip:
        sflag = "1"
    else:
        sflag = "0"
    return reduce(
        concatenation, join, sflag, *inp, strict=strict, connection=connection, opt=opt
    )


def truncate(
    inp: Union[Artefact, str, bytes],
    top: int = 0,
    bottom: int = 0,
    separator: Union[Artefact, str, bytes] = b"\n",
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Truncate an artefact."""

    def __truncate(inp: bytes, top: bytes, bottom: bytes, sep: bytes) -> bytes:
        data = inp.split(sep)
        i = int(top.decode())
        j = len(data) - int(bottom.decode())
        return sep.join(data[i:j])

    return reduce(
        __truncate,
        inp,
        f"{top}".encode(),
        f"{bottom}".encode(),
        separator,
        name="truncate",
        strict=strict,
        opt=opt,
        connection=connection,
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
        __stop_if, inp, name=fun_name, strict=True, connection=connection, opt=opt
    )


def not_empty(
    inp: Union[Artefact, str, bytes],
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Stop DAG on empty files."""

    def __not_empty(inp: bytes) -> bytes:
        if len(inp):
            return inp
        else:
            raise RuntimeError("")

    fun_name = "not an empty file"
    return morph(
        __not_empty, inp, name=fun_name, strict=True, connection=connection, opt=opt
    )


def pickled(fun: Callable[..., Any], noutputs: int = 1) -> Callable[..., Any]:
    """Wrap a function so that args and return value are automatically pickled."""

    def pickled_fun(*inp: bytes) -> Any:
        unpickled = []
        for i in inp:
            try:
                unpickled += [pickle.loads(i)]
            except pickle.PickleError:
                unpickled += [i]

        out = fun(*unpickled)
        if noutputs == 1:
            return pickle.dumps(out)
        else:
            return tuple(pickle.dumps(o) for o in out)

    pickled_fun.__qualname__ = fun.__qualname__ + "_pickled"
    return pickled_fun


def identity(
    *inp: Union[Artefact, str, bytes],
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> tuple[Artefact, ...]:
    """Add a no-op on the call graph."""

    def __I(*inp: Result[bytes]) -> bytes:
        return inp

    return mapping(__I, *inp, noutputs=len(inp), name="no op", strict=strict, opt=opt)
