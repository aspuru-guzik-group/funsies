"""Dynamic DAG generation."""
from __future__ import annotations

# std
from typing import (
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Union,
)

# external
from redis import Redis

# module
from ._constants import _AnyPath
from ._context import get_db, get_options
from ._graph import (
    Artefact,
    make_op,
)
from ._subdag import subdag_funsie
from .config import Options
from .ui import put

# Types
_INP_FILES = Optional[Mapping[_AnyPath, Union[Artefact, str, bytes]]]
_OUT_FILES = Iterable[_AnyPath]


# --------------------------------------------------------------------------------
# Split-Apply-Combine
def sac(  # noqa:C901
    split: Callable[[Dict[str, bytes]], Sequence[Dict[str, bytes]]],
    apply: Callable[[Dict[str, Artefact]], Dict[str, Artefact]],
    combine: Callable[[list[Dict[str, Artefact]]], Dict[str, Artefact]],
    out: _OUT_FILES,
    inp: _INP_FILES = None,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Dict[str, Artefact]:
    """Perform a generic split/apply/combine dynamic DAG."""
    opt = get_options(opt)
    db = get_db(connection)

    inputs: dict[str, Artefact] = {}
    # Parse input files -------------------------------------
    if inp is None:
        pass
    # multiple input files as a mapping
    elif isinstance(inp, Mapping):
        for key, val in inp.items():
            skey = str(key)
            if isinstance(val, Artefact):
                inputs[skey] = val
            else:
                inputs[skey] = put(val, connection=db)
    else:
        raise TypeError(f"{inp} not a valid file input")

    outputs = [str(o) for o in out]

    if name is not None:
        fun_name = name
    else:
        fun_name = (
            f"mapreduce_{len(inputs)}:{split.__qualname__}_{apply.__qualname__}"
            + "_{combine.__qualname__}:{noutputs}"
        )

    def map_reduce(inpd: dict[str, bytes]) -> dict[str, Artefact]:
        """Perform the map reduce."""
        split_data = split(inpd)
        out_data = []
        for onedict in split_data:
            inp2 = {}
            for key, val in onedict.items():
                inp2[key] = put(val)
            out_data += [apply(inp2)]
        return combine(out_data)

    # Generate the subdag operations
    cmd = subdag_funsie(
        map_reduce, list(inputs.keys()), outputs, name=fun_name, strict=True
    )
    operation = make_op(db, cmd, inputs, opt)
    output_artefacts = {}
    for key, val in operation.out.items():
        output_artefacts[key] = Artefact.grab(db, val)
    return output_artefacts
