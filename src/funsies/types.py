"""Object types."""
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, Operation
from ._shell import ShellOutput
from .constants import hash_t
from .errors import Error, ErrorKind, Result, UnwrapError
from .run import RunStatus

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
]
