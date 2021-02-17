"""Object types."""
from ._constants import hash_t
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, Operation
from ._run import RunStatus
from ._shell import ShellOutput
from .config import Options
from .errors import Error, ErrorKind, Result, UnwrapError

__all__ = [
    "Funsie",
    "FunsieHow",
    "Artefact",
    "Operation",
    "ShellOutput",
    "hash_t",
    "Error",
    "ErrorKind",
    "Result",
    "UnwrapError",
    "RunStatus",
    "Options",
]
