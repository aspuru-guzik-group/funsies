"""Dynamic DAG generation."""
from __future__ import annotations

# std
import time
from typing import (
    Callable,
    Iterable,
    Mapping,
    Optional,
    overload,
    Union,
)

# external
from redis import Redis

# python 3.7 imports Literal from typing_extensions
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type:ignore

# module
from ._constants import _AnyPath, hash_t
from ._context import get_db, get_options
from ._dag import descendants, start_dag_execution
from ._graph import (
    Artefact,
    constant_artefact,
    delete_artefact,
    get_data,
    get_status,
    make_op,
    Operation,
)
from ._logging import logger
from ._pyfunc import python_funsie
from ._run import is_it_cached
from ._shell import shell_funsie, ShellOutput
from ._short_hash import shorten_hash
from .config import Options
from .errors import Error, Result, unwrap
from .ui import put

# Types
_INP_FILES = Optional[Mapping[_AnyPath, Union[Artefact, str, bytes]]]
_OUT_FILES = Optional[Iterable[_AnyPath]]


# --------------------------------------------------------------------------------
# Map-Reduce
def map_reduce(  # noqa:C901
    split: Callable,
    apply: Callable,
    combine: Callable,
    *inp: Union[Artefact, str, bytes],
    noutputs: int = 1,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> tuple[Artefact, ...]:
    """Perform a map-reduce using three functions."""
    opt = get_options(opt)
    db = get_db(connection)
    arg_names = []
    inputs = {}
    for k, arg in enumerate(inp):
        arg_names += [f"in{k}"]
        if isinstance(arg, Artefact):
            inputs[arg_names[-1]] = arg
        else:
            inputs[arg_names[-1]] = put(arg, connection=db)

    # output slots
    outputs = [f"out{k}" for k in range(noutputs)]

    if name is not None:
        fun_name = name
    else:
        fun_name = (
            f"mapreduce_{len(inputs)}:{split.__qualname__}_{apply.__qualname__}"
            + "_{combine.__qualname__}:{noutputs}"
        )

    def map_reduce(inpd: dict[str, bytes]) -> dict[str, _graph.Artefact]:
        """Perform the map reduce."""
        args = [inpd[key] for key in arg_names]
        inp_data = split(args)
        out_artefacts = []
        for el in inp_data:
            artefact = put(el)
            out_artefacts += [apply(artefact)]
        return {"out": combine(out_artefacts)}

    # Generate the subdag operations
    cmd = subdag_funsie(map_reduce, arg_names, ["out"], name=fun_name, strict=strict)
    operation = make_op(db, cmd, inp, opt)
    out = Artefact.grab(db, operation.out["out"])
    return out
