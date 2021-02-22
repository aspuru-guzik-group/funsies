"""Run DAGs from DAGs using funsies."""
from __future__ import annotations

# std
import time
from typing import Callable, Dict, Mapping, Optional, overload, Sequence, Union

# external
import cloudpickle

# python 3.7 imports Literal from typing_extensions
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type:ignore

# module
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact
from ._logging import logger
from .errors import Result


# types
subdag_t = Callable[[Dict[str, bytes]], Dict[str, Artefact]]
subdag_opt_t = Callable[[Dict[str, Result[bytes]]], Dict[str, Artefact]]
subdag_opt_map_t = Callable[[Mapping[str, Result[bytes]]], Dict[str, Artefact]]


# strict overload
# fmt:off
@overload
def subdag_funsie(fun: Union[subdag_opt_t, subdag_t], inputs: Sequence[str], outputs: Sequence[str], name: Optional[str] = None, strict: Literal[False] = False) -> Funsie:  # noqa
    ...


@overload
def subdag_funsie(fun: subdag_t, inputs: Sequence[str], outputs: Sequence[str], name: Optional[str] = None, strict: Literal[True] = True) -> Funsie:  # noqa
    ...
# fmt:on


def subdag_funsie(
    fun: Union[subdag_t, subdag_opt_t],
    inputs: Sequence[str],
    outputs: Sequence[str],
    name: Optional[str] = None,
    strict: bool = True,
) -> Funsie:
    """Wrap a python function that creates a DAG.

    The callable should have the following signature:

        f(dict[str, Artefact]) -> dict[str, Artefact]

    This is similar to python_funsie in _pyfunc.py

    Args:
        fun: a Python callable f(inp)->out.
        inputs: Keys in the input dictionary inp.
        outputs: Keys in the output dictionary out.
        name (optional): name of callable. If not given, the qualified name of
            the callable is used instead.
        strict (optional): If True (default), the function will not run if any
            of its inputs are Error-ed.

    Returns:
        A Funsie instance.
    """
    if name is None:
        name = callable.__qualname__

    ierr = 1
    if strict:
        ierr = 0

    return Funsie(
        how=FunsieHow.subdag,
        what=name,
        inp=list(inputs),
        out=list(outputs),
        error_tolerant=ierr,
        extra={"pickled function": cloudpickle.dumps(fun)},
    )


def run_subdag_funsie(
    funsie: Funsie, input_values: Mapping[str, Result[bytes]]
) -> dict[str, Optional[Artefact]]:
    """Execute a subdag generator."""
    logger.info("subdag generator")
    fun: subdag_opt_map_t = cloudpickle.loads(funsie.extra["pickled function"])
    name = funsie.what
    logger.info(f"$> {name} subdag generator")

    # run
    t1 = time.time()
    outfun = fun(input_values)
    t2 = time.time()

    logger.info(f"done 1/1 \t\tduration: {t2-t1:.2f}s")
    out: dict[str, Optional[Artefact]] = {}
    for output in funsie.out:
        if output in outfun:
            out[output] = outfun[output]
        else:
            logger.warning(f"missing expected output {output}")
            out[output] = None

    return out
