"""Funsies is a functional wrapper for terminal commands."""
from .cached import pull_file, put_file
from .core import run, runall
from .types import pull
from .ui import file, pyfunc, shell

__all__ = [
    # core
    "run",
    "runall",
    # cached
    "pull_file",
    "put_file",
    # types
    "pull",
    # ui
    "file",
    "shell",
    "pyfunc",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
