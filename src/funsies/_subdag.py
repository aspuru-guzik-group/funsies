"""Run DAGs from DAGs using funsies."""
from __future__ import annotations

# std
from io import BytesIO
import time
from typing import Any, Callable, Mapping, Optional

# external
import cloudpickle

# module
from ._constants import Encoding
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact
from ._logging import logger
from .errors import Result

# types
subdag_t = Callable[..., Any]


def subdag_funsie(
    fun: subdag_t,
    inputs: dict[str, Encoding],
    outputs: dict[str, Encoding],
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
        name:
            Name of callable. If not given, the qualified name of the callable
            is used instead.
        strict:
            If True (default), the function will not run if any of its inputs
            are Error-ed.

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
        inp=inputs,
        out=outputs,
        error_tolerant=ierr,
        extra={"pickled function": cloudpickle.dumps(fun)},
    )


def run_subdag_funsie(
    funsie: Funsie, input_values: Mapping[str, Result[BytesIO]]
) -> dict[str, Optional[Artefact[Any]]]:
    """Execute a subdag generator."""
    logger.info("subdag generator")
    fun: subdag_t = cloudpickle.loads(funsie.extra["pickled function"])
    name = funsie.what
    logger.info(f"$> {name} subdag generator")

    decoded = funsie.decode(input_values)

    # run
    t1 = time.time()
    outfun = fun(decoded)
    t2 = time.time()

    logger.info(f"done 1/1 \t\tduration: {t2-t1:.2f}s")
    out: dict[str, Optional[Artefact[Any]]] = {}
    for output in funsie.out:
        if output in outfun:
            out[output] = outfun[output]
        else:
            logger.warning(f"missing expected output {output}")
            out[output] = None

    return out
