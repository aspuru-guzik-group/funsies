"""Some useful functions for workflows."""
from __future__ import annotations

# std
from typing import Any, Callable, Optional, Sequence, TypeVar, Union

# external
from redis import Redis

# module
from ._constants import _Data, Encoding
from ._graph import Artefact
from ._shell import ShellOutput
from .config import Options
from .errors import Error, Result
from .fp import morph, py, reduce
from .ui import execute, wait_for

_TargetBytes = Union[Artefact[bytes], bytes]

Tin = TypeVar("Tin")
Tout1 = TypeVar("Tout1")
Tout2 = TypeVar("Tout2")
Td = TypeVar("Td", bound=_Data)


def execute_all(
    things: Sequence[Union[Artefact[Any], ShellOutput]],
    block: bool = True,
    timeout: Optional[float] = None,
    *,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact[bytes]:
    """Execute a sequence of artefacts or shell commands.

    `execute_all()` executes a sequence of artefact in arbitrary, parallel
    order. It's equivalent to the following simple program,

    ```python
    for el in things:
        funsies.execute(el)

    for el in things:
        funsies.wait_for(el)
    ```

    The return value is `funsies.errors.Result` containing an empty bytestring
    or `funsies.errors.Error` if any of `things` is `funsies.errors.Error`.

    Args:
        things: A bunch of Artefact or ShellOutput.
        block: Whether to block until all artefacts have been executed. Defaults to True.
        timeout: `timeout=` argument for `funsies.wait_for()` when blocking.
        connection: An explicit Redis connection. Not required if called
            within a `funsies.Fun()` context.
        opt: An `funsies.config.Options` instance generated from
            `funsies.options()`. Not required if called within a
            `funsies.Fun()` context.

    Returns:
        An `funsies.types.Artefact[bytes]` object that resolves to `b""`.
    """
    things2 = [t if isinstance(t, Artefact) else t.stdout for t in things]
    out = reduce(
        lambda *x: b"", *things2, out=Encoding.blob, opt=opt, connection=connection
    )
    execute(out, connection=connection)
    if block:
        wait_for(out, timeout=timeout, connection=connection)
    return out


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
    *inp: _TargetBytes,
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

    return py(  # type:ignore
        concatenation,
        join,
        strip,
        *inp,
        out=[Encoding.blob],
        strict=strict,
        connection=connection,
        opt=opt,
    )


def truncate(
    inp: _TargetBytes,
    top: int = 0,
    bottom: int = 0,
    separator: Union[str, bytes] = b"\n",
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
    if isinstance(separator, str):
        separator = separator.encode()

    return reduce(
        __truncate,
        inp,
        top,
        bottom,
        separator,
        out=Encoding.blob,
        name="truncate",
        strict=True,
        opt=opt,
        connection=connection,
    )


def stop_if(
    fun: Callable[[Td], bool],
    inp: Union[Td, Artefact[Td]],
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact[Td]:
    """Stop execution if a condition holds."""

    def __stop_if(inp: Td) -> Td:
        if fun(inp):
            raise RuntimeError("Data triggered stop.")
        else:
            return inp

    fun_name = f"stop_if:{fun.__qualname__}"
    return morph(
        __stop_if, inp, name=fun_name, strict=True, connection=connection, opt=opt
    )


def not_empty(
    inp: _TargetBytes,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Stop DAG on empty files."""

    def __not_empty(inp: bytes) -> bytes:
        if len(inp):
            return inp
        else:
            raise RuntimeError("This file is empty.")

    fun_name = "not an empty file"
    return py(
        __not_empty, inp, name=fun_name, strict=True, connection=connection, opt=opt
    )
