"""Funsies is a transparently-memoized worfklow engine."""
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
    "execute",
    "shell",
    "morph",
    "reduce",
    "mapping",
    "put",
    "reset",
    "take",
    "takeout",
    "wait_for",
    # context,
    "Fun",
    "ManagedFun",
    # dag
    "execute",
    # error
    "unwrap",
    # getter
    "get",
    # options
    "options",
    # submodules
    "utils",
    "debug",
    "types",
]

# versioning information
from importlib import metadata  # noqa

__version__ = metadata.version("funsies")
