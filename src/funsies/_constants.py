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
PARAMETRIC = "funsies.parametric"
HASH_INDEX = "funsies.hash_index"

# DAGs
DAG_OPERATIONS = "funsies.dags.operations"
DAG_STATUS = "funsies.dags.status.done"
DAG_INDEX = "funsies.dags.index"

# Max size of continuous data
BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB
