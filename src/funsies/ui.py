"""User-friendly interfaces to funsies core functionality."""
# std
import os
import shlex
from typing import Dict, Iterable, List, Mapping, Optional, Tuple, TypeVar, Union

# external
from redis import Redis

# module
from .cached import CachedFile, put_file
from .constants import _AnyPath
from .core import (
    Command,
    Task,
)


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
    input_files: _INP_FILES = None,
    output_files: _OUT_FILES = None,
    env: Optional[Dict[str, str]] = None,
) -> Task:
    """Make a Task.

    Make a Task structure for running with run(). This is a more user-friendly
    interface than the direct Task() constructor, and it is much more lenient
    on types.

    Arguments:
        db: Redis instance.
        *args: Shell commands.
        input_files: Input files for task.
        output_files: Output files for task.
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
    for arg in args:
        if isinstance(arg, str):
            cmds += [Command(*__split(shlex.split(arg)))]
        elif isinstance(arg, Iterable):
            cmds += [Command(*__split(arg))]
        else:
            raise TypeError(f"argument {arg} not str or iterable")

    # Parse input files -------------------------------------
    # single input file
    inputs: Dict[str, CachedFile] = {}
    if input_files is None:
        pass
    # multiple input files as a mapping
    elif isinstance(input_files, Mapping):
        for key, val in input_files.items():
            skey = str(key)
            if isinstance(val, str):
                inputs[skey] = put_file(db, CachedFile(skey), val.encode())
            elif isinstance(val, bytes):
                inputs[skey] = put_file(db, CachedFile(skey), val)
            elif isinstance(val, CachedFile):
                inputs[skey] = val
            else:
                raise TypeError(f"{val} invalid value for a file.")

    # multiple input files as a list of paths
    elif isinstance(input_files, Iterable):
        for el in input_files:
            with open(el, "rb") as f:
                skey = str(os.path.basename(el))
                inputs[skey] = put_file(db, CachedFile(skey), f.read())
    else:
        raise TypeError(f"{input_files} not a valid file input")

    # Parse outputs -----------------------------------------
    outputs: List[str] = []
    if output_files is None:
        pass
    elif isinstance(output_files, Iterable):
        outputs = [str(k) for k in output_files]
    else:
        raise TypeError(f"{output_files} not a valid file output")

    return Task(cmds, inputs, outputs, env)
