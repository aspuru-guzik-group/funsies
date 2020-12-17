"""Funsies is a functional wrapper for terminal commands."""
from .cached import pull_file, put_file
from .core import pull, run
from .ui import file, task, transformer

__all__ = [
    # core
    "run",
    "pull",
    # cached
    "pull_file",
    "put_file",
    # ui
    "file",
    "task",
    "transformer",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
