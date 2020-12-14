"""User-friendly interfaces to funsies core functionality."""
# std
import shlex
from typing import Dict, Iterable, Optional, TypeVar, Tuple, Union, List
from collections.abc import Mapping

# module
from .core import Task, _AnyPath, Command


# Types
_ARGS = Union[str, Iterable[str]]
_INP_FILES = Optional[
    Union[_AnyPath, Mapping[_AnyPath, Union[str, bytes]], Iterable[_AnyPath]]
]
_OUT_FILES = Optional[Union[_AnyPath, Iterable[_AnyPath]]]
T = TypeVar("T")


def __split(arg: Iterable[T]) -> Tuple[T, Tuple[T, ...]]:
    this_arg = list(arg)
    if len(this_arg) > 1:
        rest = tuple(this_arg[1:])
    else:
        rest = ()
    return this_arg[0], rest


def make(
    *args: _ARGS,
    input_files: _INP_FILES = None,
    output_files: _OUT_FILES = None,
) -> Task:
    """Make a Task.

    Make a Task structure for running with run(). This is a more user-friendly
    interface than the direct Task() constructor, and it is more lenient on
    types.

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
                inputs[el] = f.read()

    # We assume it's a single path
    else:
        with open(input_files, "rb") as f:
            inputs[input_files] = f.read()

    # Parse outputs -----------------------------------------
    outputs = []
    if output_files is None:
        pass

    return Task(cmds, inputs, outputs, env)

    # elif isinstance(args, Iterable):
    #     for el in args:
