# noqa
"""
`funsies` is a lightweight, distributed, transparently cached and
incrementally computed workflow engine. 🔧

.. include:: ./documentation.md

"""
# qa
from . import debug
from . import t
from . import utils
from .context import Fun, ManagedFun, options
from .errors import unwrap
from .getter import get
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


# Note: order here is the order in documentation
__all__ = [
    "shell",
    "morph",
    "reduce",
    "mapping",
    "execute",
    "wait_for",
    "put",
    "take",
    "takeout",
    "reset",
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
    "t",
]

# versioning information
from importlib import metadata  # noqa

__version__ = metadata.version("funsies")
