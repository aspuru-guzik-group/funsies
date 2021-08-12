"""Run pythonf functions using funsies."""
from __future__ import annotations

# std
from io import BytesIO
import time
from typing import Any, Callable, Mapping, Optional

# external
import cloudpickle

# module
from ._constants import _Data, Encoding
from ._funsies import Funsie, FunsieHow
from ._logging import logger
from .errors import Result

# types
pyfunc_t = Callable[..., Any]


def python_funsie(
    fun: pyfunc_t,
    inputs: dict[str, Encoding],
    outputs: dict[str, Encoding],
    name: Optional[str] = None,
    strict: bool = True,
) -> Funsie:
    """Wrap a python function.

    The callable should have the following signature:

        f(dict[str, bytes or Result[bytes]]) -> dict[str, bytes]

    This is done like this because there is no way to ensure that a python
    function will return a specific number of arguments at runtime. This
    structure is both the most robust and most general.

    The input type can be made to accept Result by setting strict = False.

    Args:
        fun: a Python callable f(inp)->out.
        inputs: Keys in the input dictionary inp.
        outputs: Keys in the output dictionary out.
        name:
            Name of callable. If not given, the qualified name of the callable
            is used instead.
        strict: If true, the function accepts Result.

    Returns:
        A Funsie instance.
    """
    if name is None:
        name = callable.__qualname__

    ierr = 1
    if strict:
        ierr = 0

    return Funsie(
        how=FunsieHow.python,
        what=name,
        inp=inputs,
        out=outputs,
        error_tolerant=ierr,
        extra={"pickled function": cloudpickle.dumps(fun)},
    )


def run_python_funsie(
    funsie: Funsie, input_values: Mapping[str, Result[BytesIO]]
) -> dict[str, Optional[_Data]]:
    """Execute a python function."""
    logger.info("python function")
    fun: pyfunc_t = cloudpickle.loads(funsie.extra["pickled function"])
    name = funsie.what
    logger.info(f"$> {name}(*args)")

    decoded = funsie.decode(input_values)

    # run
    t1 = time.time()
    outfun = fun(decoded)
    t2 = time.time()

    logger.info(f"done 1/1 \t\tduration: {t2-t1:.2f}s")
    out: dict[str, Optional[_Data]] = {}
    for output in funsie.out.keys():
        if output in outfun:
            out[output] = outfun[output]
        else:
            logger.warning(f"missing expected output {output}")
            out[output] = None

    return out
