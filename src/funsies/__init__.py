"""Funsies is a transparently-memoized worfklow engine."""
from ._funsies import Funsie, FunsieHow
from .constants import hash_t
from .context import Fun
from .dag import execute
from .errors import Error, ErrorKind, Result, unwrap, UnwrapError
from .run import run_op, RUNNERS, RunStatus
from .ui import mapping, morph, put, reduce, shell, tag, take, takeout, wait_for

__all__ = [
    # funsie
    "Funsie",
    "FunsieHow",
    # ui
    "shell",
    "morph",
    "reduce",
    "mapping",
    "put",
    "tag",
    "take",
    "takeout",
    "wait_for",
    # run
    "run_op",
    "RUNNERS",
    "RunStatus",
    # types
    "hash_t",
    # context,
    "Fun",
    # dag
    "execute",
    # error
    "UnwrapError",
    "unwrap",
    "Result",
    "Error",
    "ErrorKind",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
