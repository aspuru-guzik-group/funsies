"""Funsies is a functional wrapper for terminal commands."""
from .core import (  # noqa:F401
    CacheSettings,
    Command,
    CommandOutput,
    Context,
    run,
    run_command,
    Task,
    TaskOutput,
)
from .ui import make

__all__ = [
    # core
    "CacheSettings",
    "Command",
    "CommandOutput",
    "Context",
    "run",
    "run_command",
    "Task",
    "TaskOutput",
    # ui
    "make",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
