"""Funsies is a functional wrapper for terminal commands."""
from .cached import (
    add_file,
    CachedFile,
    CachedFileType,
    CacheSpec,
    get_file,
)  # noqa:F401
from .core import (  # noqa:F401
    Command,
    CachedCommandOutput,
    CommandOutput,
    run,
    run_command,
    Task,
    open_cache,
    TaskOutput,
)
from .ui import Cache, task

__all__ = [
    # core
    "CacheSpec",
    "Command",
    "CachedCommandOutput",
    "CommandOutput",
    "run",
    "run_command",
    "Task",
    "open_cache",
    "TaskOutput",
    # cached
    "add_file",
    "CachedFile",
    "CachedFileType",
    "get_file",
    # ui
    "task",
    "Cache",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
