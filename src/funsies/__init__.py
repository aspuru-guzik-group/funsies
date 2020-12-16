"""Funsies is a functional wrapper for terminal commands."""
from .cached import pull_file, put_file  # noqa:F401
from .core import run  # noqa:F401
from .ui import task

__all__ = [
    # core
    "run",
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
