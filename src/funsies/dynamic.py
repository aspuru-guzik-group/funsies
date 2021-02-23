"""Dynamic DAG generation."""
from __future__ import annotations

# std
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
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

# Function signatures
split_sig = Callable[[Dict[str, bytes]], Sequence[Dict[str, bytes]]]
apply_sig = Callable[[Dict[str, Artefact]], Dict[str, Artefact]]
combine_sig = Callable[[List[Dict[str, Artefact]]], Dict[str, Artefact]]


# --------------------------------------------------------------------------------
# Split-Apply-Combine
def sac(  # noqa:C901
    split: split_sig,
    apply: apply_sig,
    combine: combine_sig,
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
            f"SAC|{split.__qualname__}|{apply.__qualname__}"
            + f"|{combine.__qualname__}"
        )

    def __sac(inpd: dict[str, bytes]) -> dict[str, Artefact]:
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
    cmd = subdag_funsie(__sac, list(inputs.keys()), outputs, name=fun_name, strict=True)
    operation = make_op(db, cmd, inputs, opt)
    output_artefacts = {}
    for key, val in operation.out.items():
        output_artefacts[key] = Artefact.grab(db, val)
    return output_artefacts


def map_reduce(
    split: Callable[[bytes], list[bytes]],
    apply: Callable[[Artefact], Artefact],
    combine: Callable[[list[Artefact]], Artefact],
    inp: Union[Artefact, str, bytes],
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Perform a simple one-to-one map reduce."""

    def __split(inp: dict[str, bytes]) -> list[dict[str, bytes]]:
        return [{"inner": v} for v in split(inp["in"])]

    def __apply(inp: dict[str, Artefact]) -> dict[str, Artefact]:
        return {"inner": apply(inp["inner"])}

    def __combine(inp: list[dict[str, Artefact]]) -> dict[str, Artefact]:
        data = [el["inner"] for el in inp]
        return {"out": combine(data)}

    if name is not None:
        fun_name = name
    else:
        fun_name = (
            f"map_reduce|{split.__qualname__}|{apply.__qualname__}"
            + f"|{combine.__qualname__}"
        )

    out = sac(
        __split,
        __apply,
        __combine,
        inp={"in": inp},
        out=["out"],
        name=fun_name,
        strict=strict,
        opt=opt,
        connection=connection,
    )
    return out["out"]
