"""User-friendly interfaces to funsies core functionality."""
# std
import os
import shlex
from typing import Dict, Iterable, List, Mapping, Optional, Tuple, TypeVar, Union

# module
from .cached import CachedFile, CacheSpec, get_file
from .constants import _AnyPath
from .core import (
    Command,
    CommandOutput,
    CachedCommandOutput,
    Task,
    TaskOutput,
    open_cache,
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


def task(
    *args: _ARGS,
    input_files: _INP_FILES = None,
    output_files: _OUT_FILES = None,
    env: Optional[Dict[str, str]] = None,
) -> Task:  # noqa:C901
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


class Cache:
    """A user-friendly overarching class for caches."""

    def __init__(
        self: "Cache", path: _AnyPath, shards: int = 1, timeout: float = 1.0
    ) -> None:
        """Setup a Cache."""
        # setup cachespec
        self.spec = CacheSpec(path, shards, timeout)
        # open the cache
        self.cache = open_cache(self.spec)
        assert self.cache is not None

    def unwrap_file(self: "Cache", file: Optional[CachedFile]) -> bytes:
        """Load a file from the cache in a pythonic way."""
        assert file is not None

        out = get_file(self.cache, file)
        if out is None:
            raise IOError("Error retrieving file from Cache")
        else:
            return out

    def unwrap_command(
        self: "Cache", cmd: Optional[CachedCommandOutput]
    ) -> CommandOutput:
        """Load a file from the cache in a pythonic way."""
        assert cmd is not None
        stdout = self.unwrap_file(cmd.stdout)
        stderr = self.unwrap_file(cmd.stderr)
        return CommandOutput(cmd.returncode, stdout, stderr, cmd.raises)
