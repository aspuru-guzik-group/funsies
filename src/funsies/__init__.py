"""Funsies is a functional wrapper for terminal commands."""
from .cached import pull_file, put_file
from .core import run
from .rtransformer import transformer
from .ui import task

__all__ = [
    # core
    "run",
    # transformer
    "transformer",
    # cached
    "pull_file",
    "put_file",
    # ui
    "task",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
