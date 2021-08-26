"""Names of stuff in the key value store."""
from __future__ import annotations

# std
from enum import Enum
from os import PathLike
from typing import NewType, Union

JsonData = Union[str, int, float, bool, None, dict, list]
"""Can be converted to JSON."""

_Data = Union[bytes, JsonData]
"""All output data types."""

# Some types
_AnyPath = Union[str, PathLike]
hash_t = NewType("hash_t", str)


class Encoding(str, Enum):
    """Types for data objects.

    Funsies does not support a full-blown type system. For this, we defer to
    json's encoding of various data structures.
    """

    json = "json"
    blob = "blob"


# Utility functions
def join(prefix: str, address: hash_t, *suffix: str) -> str:
    """Make a redis identifier."""
    return ":".join([prefix, str(address)] + list(suffix))


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
