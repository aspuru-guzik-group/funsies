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
from .cached import register_file
from .constants import _AnyPath, _TransformerFun
from .rtask import register_task
from .rtransformer import register_transformer
from .types import FilePtr, RTask, RTransformer


# Types
_INP_FILES = Optional[
    Union[Mapping[_AnyPath, Union[FilePtr, str, bytes]], Iterable[_AnyPath]]
]
_OUT_FILES = Optional[Iterable[_AnyPath]]
T = TypeVar("T")


def __split(arg: Iterable[T]) -> Tuple[T, List[T]]:
    this_arg = list(arg)
    if len(this_arg) > 1:
        rest = this_arg[1:]
    else:
        rest = []
    return this_arg[0], rest


def shell(  # noqa:C901
    db: Redis,
    *args: str,
    inp: _INP_FILES = None,
    out: _OUT_FILES = None,
    env: Optional[Dict[str, str]] = None,
) -> RTask:
    """Make a Task.

    Make a Task structure for running with run(). This is a more user-friendly
    interface than the direct Task() constructor, and it is much more lenient
    on types.

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
    inputs: Dict[str, FilePtr] = {}

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
            if isinstance(val, str):
                inputs[skey] = register_file(db, skey, value=val.encode())
            elif isinstance(val, bytes):
                inputs[skey] = register_file(db, skey, value=val)
            elif isinstance(val, FilePtr):
                inputs[skey] = val
            else:
                raise TypeError(
                    f"{val} of key {skey} is of an invalid type to be a file."
                )

    # multiple input files as a list of paths
    elif isinstance(inp, Iterable):
        for el in inp:
            with open(el, "rb") as f:
                skey = str(os.path.basename(el))
                inputs[skey] = register_file(db, skey, value=f.read())
    else:
        raise TypeError(f"{inp} not a valid file input")

    # Parse outputs -----------------------------------------
    myoutputs = []
    if out is None:
        pass
    else:
        myoutputs = [str(c) for c in out]
    rtask = register_task(db, cmds, inputs, myoutputs, env)
    return rtask


def pyfunc(
    db: Redis,
    fun: _TransformerFun,
    inp: Optional[Iterable[FilePtr]] = None,
    noutputs: int = 1,
) -> RTransformer:
    """Make and register a Transformer."""
    # todo guess noutputs from type hint if possible
    inputs = []
    if inp is None:
        pass
    else:
        inputs = list(inp)

    return register_transformer(db, fun, inputs, noutputs)


def file(
    db: Redis,
    name: _AnyPath,
    value: Union[bytes, str],
) -> FilePtr:
    """Make and register a FilePtr."""
    skey = str(name)
    if isinstance(value, str):
        return register_file(db, skey, value=value.encode())
    elif isinstance(value, bytes):
        return register_file(db, skey, value=value)
    else:
        raise TypeError("value of {name_or_path} not bytes or string")
