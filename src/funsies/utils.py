"""Some useful functions for workflows."""
from __future__ import annotations

# std
from typing import Callable, Optional, overload, Sequence, TypeVar, Union

# external
from redis import Redis

# module
from ._constants import Encoding
from ._graph import Artefact
from .config import Options
from .errors import Error, Result
from .ui import _Target, py

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
    *inp: _Target,
    join: Union[str, bytes] = b"",
    strip: bool = False,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Concatenate artefacts."""

    def concatenation(joiner: bytes, do_strip: bool, *args: Result[bytes]) -> bytes:
        lines = match_results(args, lambda x: x)
        out = b""
        for i, l in enumerate(lines):
            if do_strip:
                out += l.strip()
            else:
                out += l

            if i != len(lines) - 1:
                out += joiner
        return out

    # type convert str to bytes
    if isinstance(join, str):
        join = join.encode()
    inputs = [k.encode() if isinstance(k, str) else k for k in inp]

    return py(
        concatenation,
        join,
        strip,
        *inputs,
        out=[Encoding.blob],
        strict=strict,
        connection=connection,
        opt=opt,
    )[0]


def truncate(
    inp: _Target,
    top: int = 0,
    bottom: int = 0,
    separator: Union[str, bytes] = b"\n",
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Truncate an artefact."""

    def __truncate(inp: bytes, top: int, bottom: int, sep: bytes) -> bytes:
        data = inp.split(sep)
        i = top
        j = len(data) - bottom
        return sep.join(data[i:j])

    # type convert str to bytes
    if isinstance(inp, str):
        inp = inp.encode()
    if isinstance(separator, str):
        separator = separator.encode()

    return py(
        __truncate,
        inp,
        top,
        bottom,
        separator,
        out=[Encoding.blob],
        name="truncate",
        strict=strict,
        opt=opt,
        connection=connection,
    )[0]


def stop_if(
    fun: Callable[[bytes], bool],
    inp: _Target,
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
    return py(
        __stop_if, inp, name=fun_name, strict=True, connection=connection, opt=opt
    )[0]


def not_empty(
    inp: _Target,
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
    return py(
        __not_empty, inp, name=fun_name, strict=True, connection=connection, opt=opt
    )[0]


@overload
def identity(
    __inp: Artefact,
    *,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    ...


@overload
def identity(
    __inp: Artefact,
    __inp2: Artefact,
    *,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> tuple[Artefact, Artefact]:
    ...


@overload
def identity(
    __inp: Artefact,
    __inp2: Artefact,
    __inp3: Artefact,
    *,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> tuple[Artefact, Artefact, Artefact]:
    ...


def identity(
    *inp: Artefact,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Union[Artefact, tuple[Artefact, ...]]:
    """Add a no-op on the call graph."""

    def __I(*inp: Result[bytes]) -> tuple[Result[bytes], ...]:
        return inp

    out = py(__I, *inp, out=[i.kind for i in inp], name="no op", strict=strict, opt=opt)
    if len(out) == 1:
        return out[0]
    else:
        return out
