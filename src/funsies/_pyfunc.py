"""Run shell commands using funsies."""
# std
import time
from typing import Callable, Dict, Literal, Mapping, Optional, overload, Sequence, Union

# external
import cloudpickle

# module
from ._funsies import Funsie, FunsieHow
from .errors import Result
from .logging import logger

# types
pyfunc_t = Callable[[Dict[str, bytes]], Dict[str, bytes]]
pyfunc_opt_t = Callable[[Dict[str, Result[bytes]]], Dict[str, bytes]]


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

        f(Dict[str, bytes or Option]) -> Dict[str, bytes]

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

    return Funsie(
        how=FunsieHow.python,
        what=name.encode(),
        inp=list(inputs),
        out=list(outputs),
        aux=cloudpickle.dumps(fun),
        error_tolerant=not strict,
    )


def run_python_funsie(
    funsie: Funsie, input_values: Mapping[str, Result[bytes]]
) -> Dict[str, Optional[bytes]]:
    """Execute a python function."""
    logger.info("python function")
    fun: pyfunc_t = cloudpickle.loads(funsie.aux)
    name = funsie.what.decode()
    inps, errs = funsie.check_inputs(input_values)
    inps.update(errs)  # type:ignore

    logger.info(f"$> {name}(*args)")
    t1 = time.time()
    outfun = fun(inps)
    t2 = time.time()
    logger.info(f"done 1/1 \t\tduration: {t2-t1:.2f}s")
    out: Dict[str, Optional[bytes]] = {}
    for output in funsie.out:
        if output in outfun:
            out[output] = outfun[output]
        else:
            logger.warning(f"missing expected output {output}")
            out[output] = None

    return out
