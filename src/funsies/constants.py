"""Some general constants."""
from os import PathLike
from typing import Union

# Redis tables
__IDS = "funsies.ids"
__DATA = "funsies.data"
__FILES = "funsies.files"
__TASK_ID = "funsies.current"
__TASK_DONE = "funsies.done"

# Type for paths
_AnyPath = Union[str, PathLike]
