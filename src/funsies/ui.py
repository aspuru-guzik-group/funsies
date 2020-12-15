"""User-friendly interfaces to funsies core functionality."""
# std
import os
import shlex
from typing import Dict, Iterable, List, Mapping, Optional, Tuple, TypeVar, Union

# module
from .cached import CachedFile
from .constants import _AnyPath
from .core import (
    Command,
    CommandOutput,
    CachedCommandOutput,
    Task,
    TaskOutput,
)


# Types
_ARGS = Union[str, Iterable[str]]
_INP_FILES = Optional[Union[Mapping[_AnyPath, Union[str, bytes]], Iterable[_AnyPath]]]
_OUT_FILES = Optional[Iterable[_AnyPath]]
T = TypeVar("T")


def __split(arg: Iterable[T]) -> Tuple[T, Tuple[T, ...]]:
    this_arg = list(arg)
    if len(this_arg) > 1:
        rest = tuple(this_arg[1:])
    else:
        rest = ()
    return this_arg[0], rest


def task(  # noqa:C901
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
        *args: Commandline arguments.

    """

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
    inputs: Dict[_AnyPath, bytes] = {}
    if input_files is None:
        pass
    # multiple input files as a mapping
    elif isinstance(input_files, Mapping):
        for key, val in input_files.items():
            if isinstance(val, str):
                inputs[key] = val.encode()
            else:
                inputs[key] = val

    # multiple input files as a list of paths
    elif isinstance(input_files, Iterable):
        for el in input_files:
            with open(el, "rb") as f:
                inputs[os.path.basename(el)] = f.read()
    else:
        raise TypeError(f"{input_files} not a valid file input")

    # Parse outputs -----------------------------------------
    outputs: List[_AnyPath] = []
    if output_files is None:
        pass
    elif isinstance(output_files, Iterable):
        outputs = [k for k in output_files]
    else:
        raise TypeError(f"{output_files} not a valid file output")

    return Task(cmds, inputs, outputs, env)
