"""Funsies is a functional wrapper for terminal commands."""
from .cached import CachedFile, FileType, pull_file, put_file  # noqa:F401
from .core import (  # noqa:F401
    Command,
    CachedCommandOutput,
    CommandOutput,
    pull_task,
    put_task,
    register,
    run,
    run_command,
    blabla,
    RTask,
)
from .ui import task

__all__ = [
    # core
    "Command",
    "CachedCommandOutput",
    "CommandOutput",
    "pull_task",
    "put_task",
    "register",
    "run",
    "run_command",
    "Task",
    "TaskOutput",
    # cached
    "CachedFile",
    "FileType",
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
