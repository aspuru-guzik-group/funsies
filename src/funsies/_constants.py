"""Names of stuff in the key value store."""
from __future__ import annotations

from enum import Enum
from os import PathLike
from typing import Any, Dict, List, NewType, Union

# from https://github.com/python/typing/issues/182
# fmt:off
_JSONType_0 = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
_JSONType_1 = Union[str, int, float, bool, None, Dict[str, _JSONType_0], List[_JSONType_0]]
_JSONType_2 = Union[str, int, float, bool, None, Dict[str, _JSONType_1], List[_JSONType_1]]
_JSONType_3 = Union[str, int, float, bool, None, Dict[str, _JSONType_2], List[_JSONType_2]]
JsonData =  Union[str, int, float, bool, None, Dict[str, _JSONType_3], List[_JSONType_3]]
"""Can be converted to JSON."""
# fmt:on

# Some types
_AnyPath = Union[str, PathLike]
hash_t = NewType("hash_t", str)


_Data = Union[bytes, JsonData]
"""All output data types."""


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

# Max size of continuous data
BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB
