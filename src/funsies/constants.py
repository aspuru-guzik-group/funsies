"""Names of stuff in the key value store."""
from typing import NewType

# Some types
hash_t = NewType("hash_t", str)

# Some locations
ARTEFACTS = "funsies.artefacts"
FUNSIES = "funsies.funsies"
OPERATIONS = "funsies.ops"
OPTIONS = "funsies.runtime_options"
STORE = "funsies.store"
ERRORS = "funsies.errors"

TAGS = "funsies.tags."
TAGS_SET = "funsies.tags"

# job status repos
DATA_STATUS = "funsies.data.status"
SREADY = "funsies.jobs.ready"
SRUNNING = "funsies.jobs.running"

# job dags
DAG_STORE = "funsies.dags."
DAG_INDEX = "funsies.dags.index"

# Max size of continuous data
BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB
