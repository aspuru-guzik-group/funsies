"""User-friendly interfaces to funsies core functionality."""
# std
import os
import shlex
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
from .cached import CachedFile, put_file
from .command import Command
from .constants import _AnyPath
from .rtask import register_task, RTask, UnregisteredTask


# Types
_ARGS = Union[str, Iterable[str]]
_INP_FILES = Optional[
    Union[Mapping[_AnyPath, Union[CachedFile, str, bytes]], Iterable[_AnyPath]]
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


def task(  # noqa:C901
    db: Redis,
    *args: _ARGS,
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
    cmds: List[Command] = []
    inputs: Dict[str, Union[CachedFile, str]] = {}
    outputs: List[str] = []

    for arg in args:
        if isinstance(arg, str):
            cmds += [Command(*__split(shlex.split(arg)))]
        elif isinstance(arg, Iterable):
            cmds += [Command(*__split(arg))]
        else:
            raise TypeError(f"argument {arg} not str or iterable")

    # Parse input files -------------------------------------
    if inp is None:
        pass
    # multiple input files as a mapping
    elif isinstance(inp, Mapping):
        for key, val in inp.items():
            skey = str(key)
            if isinstance(val, str):
                inputs[skey] = val
            elif isinstance(val, bytes):
                inputs[skey] = val.decode()
            elif isinstance(val, CachedFile):
                inputs[skey] = val
            else:
                raise TypeError(f"{val} invalid value for a file.")

    # multiple input files as a list of paths
    elif isinstance(inp, Iterable):
        for el in inp:
            with open(el, "rb") as f:
                skey = str(os.path.basename(el))
                inputs[skey] = put_file(db, CachedFile(skey), f.read())
    else:
        raise TypeError(f"{inp} not a valid file input")

    # Parse outputs -----------------------------------------
    if out is None:
        pass
    elif isinstance(out, Iterable):
        outputs = [str(k) for k in out]
    else:
        raise TypeError(f"{out} not a valid file output")

    utask = UnregisteredTask(cmds, inputs, outputs, env)
    rtask = register_task(db, utask)
    return rtask
