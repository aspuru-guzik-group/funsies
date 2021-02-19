"""Run shell commands using funsies."""
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
from ._logging import logger
from .errors import Result

# types
pyfunc_t = Callable[[Dict[str, bytes]], Dict[str, bytes]]
pyfunc_opt_t = Callable[[Dict[str, Result[bytes]]], Dict[str, bytes]]
pyfunc_opt_map_t = Callable[[Mapping[str, Result[bytes]]], Dict[str, bytes]]


# strict overload
# fmt:off
@overload
def python_funsie(fun: Union[pyfunc_opt_t, pyfunc_t], inputs: Sequence[str], outputs: Sequence[str], name: Optional[str] = None, strict: Literal[False] = False) -> Funsie:  # noqa
    ...


@overload
def python_funsie(fun: pyfunc_t, inputs: Sequence[str], outputs: Sequence[str], name: Optional[str] = None, strict: Literal[True] = True) -> Funsie:  # noqa
    ...
# fmt:on


def python_funsie(
    fun: Union[pyfunc_t, pyfunc_opt_t],
    inputs: Sequence[str],
    outputs: Sequence[str],
    name: Optional[str] = None,
    strict: bool = True,
) -> Funsie:
    """Wrap a python function.

    The callable should have the following signature:

        f(dict[str, bytes or Option]) -> dict[str, bytes]

    This is done like this because there is no way to ensure that a python
    function will return a specific number of arguments at runtime. This
    structure is both the most robust and most general.

    The input type can be made to accept Options by setting strict = False.

    Args:
        fun: a Python callable f(inp)->out.
        inputs: Keys in the input dictionary inp.
        outputs: Keys in the output dictionary out.
        name: (optional) name of callable. If not given, the qualified name of
            the callable is used instead.
        strict: (optional) whether the function accepts Option.

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
        inp=list(inputs),
        out=list(outputs),
        error_tolerant=ierr,
        extra={"pickled function": cloudpickle.dumps(fun)},
    )


def run_python_funsie(
    funsie: Funsie, input_values: Mapping[str, Result[bytes]]
) -> dict[str, Optional[bytes]]:
    """Execute a python function."""
    logger.info("python function")
    fun: pyfunc_opt_map_t = cloudpickle.loads(funsie.extra["pickled function"])
    name = funsie.what
    logger.info(f"$> {name}(*args)")

    # run
    t1 = time.time()
    outfun = fun(input_values)
    t2 = time.time()

    logger.info(f"done 1/1 \t\tduration: {t2-t1:.2f}s")
    out: dict[str, Optional[bytes]] = {}
    for output in funsie.out:
        if output in outfun:
            out[output] = outfun[output]
        else:
            logger.warning(f"missing expected output {output}")
            out[output] = None

    return out
