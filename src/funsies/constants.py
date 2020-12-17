"""Some general constants."""
from os import PathLike
from typing import Callable, Tuple, Union


# Redis tables
__IDS = "funsies.ids"
__OBJECTS = "funsies.objects"
__FILES = "funsies.files"
__TASK_ID = "funsies.current"
__DONE = "funsies.done_jobs"


# Type for paths
_AnyPath = Union[str, PathLike]

# TODO
_TransformerFun = Callable[..., Union[Tuple[bytes, ...], bytes]]  # type:ignore
