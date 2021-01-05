"""User-friendly interfaces to funsies functionality."""
# std
import os
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

# external
from redis import Redis

# module
from ._funsies import ART_TYPES
from ._graph import Artefact, get_artefact, get_data, make_op, store_explicit_artefact
from ._shell import shell_funsie, ShellOutput

# Types
_AnyPath = Union[str, os.PathLike]
_INP_FILES = Optional[
    Union[Mapping[_AnyPath, Union[Artefact, str, bytes]], Iterable[_AnyPath]]
]
_OUT_FILES = Optional[Iterable[_AnyPath]]
T = TypeVar("T")


def shell(  # noqa:C901
    db: Redis,
    *args: str,
    inp: _INP_FILES = None,
    out: _OUT_FILES = None,
    env: Optional[Dict[str, str]] = None,
):
    """Make a shell command.

    Make a shell operationfor running with run(). This is a more user-friendly
    interface than the direct constructor in ._shell, and it is much more
    lenient on types.

    Arguments:
        db: Redis instance.
        *args: Shell commands.
        inp: Input files for task.
        out: Output files for task.
        env: Environment variables to be set.

    Returns:
        A Task object.

    Raises:
        TypeError: when types of arguments are wrong.

    """
    if not isinstance(db, Redis):
        raise TypeError("First argument is not a Redis connection.")

    # Parse args --------------------------------------------
    cmds: List[str] = []
    inputs: Dict[str, Artefact] = {}

    for arg in args:
        if isinstance(arg, str):
            cmds += [arg]
        else:
            raise TypeError(f"argument {arg} not str.")

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
                inputs[skey] = store(db, val)

    # multiple input files as a list of paths
    elif isinstance(inp, Iterable):
        for el in inp:
            with open(el, "rb") as f:
                skey = str(os.path.basename(el))
                inputs[skey] = store(db, f.read())
    else:
        raise TypeError(f"{inp} not a valid file input")

    if out is None:
        outputs = []
    else:
        outputs = [str(o) for o in out]

    funsie = shell_funsie(cmds, inputs.keys(), outputs, env)
    operation = make_op(db, funsie, inputs)
    return ShellOutput(operation, len(cmds))


def store(
    db: Redis,
    value: Union[bytes, str],
) -> Artefact:
    """Make and register a FilePtr."""
    if isinstance(value, str):
        return store_explicit_artefact(db, value.encode())
    elif isinstance(value, bytes):
        return store_explicit_artefact(db, value)
    else:
        raise TypeError("value of {name_or_path} not bytes or string")


def grab(
    db: Redis,
    where: Union[Artefact, str],
) -> Optional[ART_TYPES]:
    """Make and register a FilePtr."""
    if isinstance(where, Artefact):
        obj = where
    else:
        obj = get_artefact(db, where)
        if obj is None:
            raise RuntimeError(f"Address {where} does not point to a valid artefact.")

    dat = get_data(db, obj)
    return dat


# def pyfunc(
#     db: Redis,
#     fun: _TransformerFun,
#     inp: Optional[Iterable[FilePtr]] = None,
#     noutputs: int = 1,
# ) -> RTransformer:
#     """Make and register a Transformer."""
#     # todo guess noutputs from type hint if possible
#     inputs = []
#     if inp is None:
#         pass
#     else:
#         inputs = list(inp)

#     return register_transformer(db, fun, inputs, noutputs)


# def file(
#     db: Redis,
#     name: _AnyPath,
#     value: Union[bytes, str],
# ) -> FilePtr:
#     """Make and register a FilePtr."""
#     skey = str(name)
#     if isinstance(value, str):
#         return register_file(db, skey, value=value.encode())
#     elif isinstance(value, bytes):
#         return register_file(db, skey, value=value)
#     else:
#         raise TypeError("value of {name_or_path} not bytes or string")
