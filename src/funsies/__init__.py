"""Funsies is a lightweight workflow engine 🔧.

.. include:: documentation.md
"""
# module
from . import debug, dynamic, parametric, types, utils
from ._context import Fun, ManagedFun, options
from ._getter import get
from .errors import unwrap
from .fp import morph, py, reduce
from .template import template
from .ui import execute, put, reset, shell, take, takeout, wait_for

__all__ = [
    # shell
    "shell",
    # fp
    "py",
    "reduce",
    "morph",
    # template
    "template",
    # artefact manipulation
    "take",
    "takeout",
    "put",
    "execute",
    "wait_for",
    "reset",
    "get",
    # contexts
    "Fun",
    "ManagedFun",
    "options",
    # Error handling and types
    "unwrap",
    "types",
    "debug",
    # utility
    "utils",
    "dynamic",
    "parametric",
]


# Version information
# We grab it from setup.py so that we don't have to bump versions in multiple
# places.
try:
    # std
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    # external
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
