"""Funsies is a transparently-memoized worfklow engine."""
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, Operation
from .constants import hash_t
from .context import Fun
from .dag import execute
from .errors import Error, ErrorKind, Result, unwrap, UnwrapError
from .run import run_op, RUNNERS, RunStatus
from .ui import (
    mapping,
    morph,
    put,
    reduce,
    rm,
    shell,
    ShellOutput,
    tag,
    take,
    takeout,
    wait_for,
)

__all__ = [
    # ui
    "shell",
    "morph",
    "reduce",
    "mapping",
    "put",
    "rm",
    "tag",
    "take",
    "takeout",
    "wait_for",
    # context,
    "Fun",
    # dag
    "execute",
    # error
    "unwrap",
    # run
    "run_op",
    "RUNNERS",
    # types
    "hash_t",
    "Artefact",
    "Operation",
    "ShellOutput",
    "UnwrapError",
    "Result",
    "Error",
    "ErrorKind",
    "RunStatus",
    "Funsie",
    "FunsieHow",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
