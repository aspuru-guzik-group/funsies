"""Dynamic DAG generation."""
from __future__ import annotations

# std
from typing import Any, Callable, Optional, Sequence, TypeVar

# module
from ._constants import _Data, Encoding
from ._context import Connection, get_connection, get_options
from ._graph import Artefact, constant_artefact, make_op
from ._subdag import subdag_funsie
from .config import Options
from .ui import _Target, put

# --------------------------------------------------------------------------------
Tin = TypeVar("Tin", bound=_Data)
T1 = TypeVar("T1", bound=_Data)
T2 = TypeVar("T2", bound=_Data)
T3 = TypeVar("T3", bound=_Data)


def sac(
    split_fun: Callable[..., Sequence[T1]],
    apply_fun: Callable[[Artefact[T1]], Artefact[T2]],
    combine_fun: Callable[[Sequence[Artefact[T2]]], Artefact[T3]],
    *inp: _Target,
    out: Encoding,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Connection = None,
) -> Artefact[T3]:
    """Perform a split/apply/combine dynamic DAG."""
    opt = get_options(opt)
    db, store = get_connection(connection)

    inputs: dict[str, Artefact] = {}
    # Parse input  -------------------------------------
    inputs = {}
    arg_names = []
    for k, arg in enumerate(inp):
        arg_names += [f"in{k}"]
        if isinstance(arg, Artefact):
            inputs[arg_names[-1]] = arg
        else:
            inputs[arg_names[-1]] = put(arg, connection=db)
    inp_types = dict([(k, val.kind) for k, val in inputs.items()])

    if name is not None:
        fun_name = name
    else:
        fun_name = (
            f"SAC|{split_fun.__qualname__}|{apply_fun.__qualname__}"
            + f"|{combine_fun.__qualname__}"
        )

    def __sac(inpd: dict[str, Any]) -> dict[str, Artefact[T3]]:
        """Perform the map reduce."""
        db, store = get_connection()
        args = [inpd[k] for k in arg_names]
        split_data = [constant_artefact(db, store, d) for d in split_fun(*args)]
        apply_data = [apply_fun(d) for d in split_data]
        combine_data = combine_fun(apply_data)
        return dict(out=combine_data)

    # Generate the subdag operations
    cmd = subdag_funsie(__sac, inp_types, {"out": out}, name=fun_name, strict=strict)
    operation = make_op(db, cmd, inputs, opt)
    return Artefact.grab(db, operation.out["out"])
