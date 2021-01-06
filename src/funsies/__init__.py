"""Funsies is a transparently-memoized worfklow engine."""
from ._funsies import Funsie, FunsieHow
from .constants import hash_t, pyfunc_t
from .dag import execute
from .run import run_op, RUNNERS, RunStatus
from .ui import morph, put, reduce, shell, take

__all__ = [
    # funsie
    "Funsie",
    "FunsieHow",
    # ui
    "shell",
    "morph",
    "reduce",
    "put",
    "take",
    # run
    "run_op",
    "RUNNERS",
    "RunStatus",
    # types
    "pyfunc_t",
    "hash_t",
    # dag
    "execute",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
