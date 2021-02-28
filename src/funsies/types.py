"""Object types."""
from ._constants import DataType, hash_t
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, ArtefactStatus, Operation
from ._run import RunStatus
from ._shell import ShellOutput
from .config import Options
from .errors import Error, ErrorKind, Result, UnwrapError

# A simple mypy result type
Result = Result
"""see `funsies.errors.Result`."""


__all__ = [
    "Funsie",
    "FunsieHow",
    "Artefact",
    "ArtefactStatus",
    "DataType",
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
