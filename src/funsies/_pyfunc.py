"""Run shell commands using funsies."""
# std
import logging
from typing import Dict, Mapping, Optional, Sequence

# external
import cloudpickle

# module
from ._funsies import Funsie, FunsieHow
from .constants import pyfunc_t


def python_funsie(
    fun: pyfunc_t,
    inputs: Sequence[str],
    outputs: Sequence[str],
    name: Optional[str] = None,
) -> Funsie:
    """Wrap a python function.

    The callable should have the following signature:

        f(Dict[str, Optional[bytes]]) -> Dict[str, Optional[bytes]]

    This is done like this because there is no way to ensure that a python
    function will return a specific number of arguments at runtime. This
    structure is both the most robust and most general.

    Args:
        fun: a Python callable f(inp)->out.
        inputs: Keys in the input dictionary inp.
        outputs: Keys in the output dictionary out.
        name: (optional) name of callable. If not given, the qualified name of
            the callable is used instead.

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
    )


def run_python_funsie(
    funsie: Funsie,
    input_values: Mapping[str, Optional[bytes]],
) -> Dict[str, Optional[bytes]]:
    """Execute a python function."""
    fun: pyfunc_t = cloudpickle.loads(funsie.aux)
    name = funsie.what.decode()

    inps = {}
    for fn in funsie.inp:
        if fn not in input_values:
            logging.error(f"expected input {fn} not passed to function {name}.")
            val = None
        else:
            val = input_values[fn]

        val = input_values[fn]
        if val is not None:
            inps[fn] = val
        else:
            logging.warning(f"file {fn} not present.")

    outfun = fun(inps)
    out: Dict[str, Optional[bytes]] = {}
    for output in funsie.out:
        if output in outfun:
            out[output] = outfun[output]
        else:
            logging.error(f"expected output {output} not returned by function {name}.")
            out[output] = None

    return out
