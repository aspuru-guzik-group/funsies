"""Some general constants."""
from os import PathLike
from typing import Callable, Dict, IO, Union

# Redis tables
__IDS = "funsies.ids"
__DATA = "funsies.data"
__FILES = "funsies.files"
__TASK_ID = "funsies.current"

# Type for paths
_AnyPath = Union[str, PathLike]

# A transformer is a function that takes as argument inputs and outputs
# dictionaries of file handles, and outputs nothing.
_Transformer = Callable[[Dict[str, IO[str]], Dict[str, IO[str]]], None]
