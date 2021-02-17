"""Funsies is a lightweight workflow engine 🔧.

.. include:: documentation.md
"""
from . import debug
from . import types
from . import utils
from ._context import Fun, ManagedFun, options
from ._getter import get
from .errors import unwrap
from .ui import (
    execute,
    mapping,
    morph,
    put,
    reduce,
    reset,
    shell,
    take,
    takeout,
    wait_for,
)


__all__ = [
    # ui
    "shell",
    "reduce",
    "morph",
    "mapping",
    "take",
    "takeout",
    "put",
    "execute",
    "wait_for",
    "reset",
    "get",
    "Fun",
    "ManagedFun",
    "unwrap",
    "options",
    "utils",
    "debug",
    "types",
]

# versioning information
from importlib import metadata  # noqa

__version__ = metadata.version("funsies")
