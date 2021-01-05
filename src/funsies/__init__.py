"""Funsies is a transparently-memoized worfklow engine."""
from ._funsies import Funsie, FunsieHow
from .run import run_op, RUNNERS
from .ui import shell

__all__ = [
    # funsie
    "Funsie",
    "FunsieHow",
    # ui
    "shell",
    # run
    "run_op",
    "RUNNERS",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
