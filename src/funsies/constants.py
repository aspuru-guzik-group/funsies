"""Names of stuff in the key value store."""
from typing import Callable, Dict

# Some types
hash_t = str
pyfunc_t = Callable[[Dict[str, bytes]], Dict[str, bytes]]

# Some locations
ARTEFACTS = "funsies.artefacts"
FUNSIES = "funsies.funsies"
OPERATIONS = "funsies.ops"
STORE = "funsies.data_store"
