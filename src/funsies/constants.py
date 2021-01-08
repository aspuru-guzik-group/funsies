"""Names of stuff in the key value store."""
from typing import Any, Callable, Dict

# Some types
hash_t = str
pyfunc_t = Callable[[Dict[str, bytes]], Dict[str, bytes]]

# Some locations
ARTEFACTS = "funsies.artefacts"
FUNSIES = "funsies.funsies"
OPERATIONS = "funsies.ops"
STORE = "funsies.store"

# job status repos
DATA_STATUS = "funsies.data.status"
SREADY = "funsies.jobs.ready"
SRUNNING = "funsies.jobs.running"

# job descendants
DAG_STORE = "funsies.dags."

# RQ defaults
RQ_JOB_DEFAULTS: Dict[str, Any] = dict()
RQ_QUEUE_DEFAULTS: Dict[str, Any] = dict()
