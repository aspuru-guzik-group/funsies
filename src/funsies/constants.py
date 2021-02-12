"""Names of stuff in the key value store."""
from os import PathLike
from typing import NewType, Union


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

TAGS = "funsies.tags."
TAGS_SET = "funsies.tags"

# job status repos
DATA_STATUS = "funsies.data.status"
SREADY = "funsies.jobs.ready."
SRUNNING = "funsies.jobs.running."

# job dags
DAG_STORE = "funsies.dags."
DAG_INDEX = "funsies.dags.index"

# Max size of continuous data
BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB
