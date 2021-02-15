"""Names of stuff in the key value store."""
from __future__ import annotations

from os import PathLike
from typing import NewType, Union


def join(prefix: str, address: hash_t, *suffix: str) -> str:
    """Make a redis identifier."""
    return ":".join([prefix, str(address)] + list(suffix))


# Some types
_AnyPath = Union[str, PathLike]
hash_t = NewType("hash_t", str)


# Some locations
ARTEFACTS = "funsies.artefacts"
FUNSIES = "funsies.funsies"
OPERATIONS = "funsies.ops"
HASH_INDEX = "funsies.hash_index"

# Data associated with operations
OPTIONS = "funsies.runtime_options"

# Data associated with artefacts
STORE = "funsies.store"
ERRORS = "funsies.errors"

# DAGs
DAG_STORE = "funsies.dags.run."
DAG_INDEX = "funsies.dags.index"
DAG_PARENTS = "funsies.dags.parents."
DAG_CHILDREN = "funsies.dags.children."

# Max size of continuous data
BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB
