"""Main type definitions."""
# stdlib
from dataclasses import asdict, dataclass, field
import logging
from typing import Any, Dict, List, Literal, Optional, overload, Type, Union

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from .constants import __OBJECTS


# --------------------------------- FILEPTR -------------------------------------
@dataclass(frozen=True)
class FilePtr:
    """A pointer to a file on cache.

    FilePtr is the main class that provides a way to uniquely access files
    that are currently cached for writing and reading. Basically, Tasks have
    file inputs and outputs as well as stdouts and stderr. All of these are
    stored in the cache and are accessed using the associated CachedFiles as
    keys.

    """

    task_id: str
    name: str
    comefrom: str

    def __str__(self: "FilePtr") -> str:
        """Return string representation."""
        return f"{str(self.task_id)}::{self.name}"

    def pack(self: "FilePtr") -> bytes:
        """Pack self to bytes."""
        return packb(("FilePtr", packb(asdict(self))))

    @classmethod
    def unpack(cls: Type["FilePtr"], inp: bytes) -> "FilePtr":
        """Build a FilePtr from packed form."""
        d = unpackb(inp)

        return FilePtr(
            task_id=d["task_id"],
            name=d["name"],
            comefrom=d["comefrom"],
        )


# --------------------------------- TRANSFORMER -------------------------------------
@dataclass(frozen=True)
class RTransformer:
    """Holds a registered transformer."""

    # task info
    task_id: str

    # The transformation function
    fun: bytes

    # input & output files
    inputs: List[FilePtr]
    outputs: List[FilePtr]

    def pack(self: "RTransformer") -> bytes:
        """Pack self to bytes."""
        return packb(("RTransformer", packb(asdict(self))))

    @classmethod
    def unpack(cls: Type["RTransformer"], inp: bytes) -> "RTransformer":
        """Build a transformer from packed form."""
        d = unpackb(inp)

        return RTransformer(
            task_id=d["task_id"],
            fun=d["fun"],
            inputs=[FilePtr(**v) for v in d["inputs"]],
            outputs=[FilePtr(**v) for v in d["outputs"]],
        )


# -------------------------  CLI COMMANDS --------------------------------------
# TODO REFACTOR
@dataclass
class Command:
    """A shell command executed by a task."""

    executable: str
    args: List[str] = field(default_factory=list)

    def __repr__(self: "Command") -> str:
        """Return command as a string."""
        return self.executable + " " + " ".join([a for a in self.args])


@dataclass
class CommandOutput:
    """Holds the result of running a command."""

    returncode: int
    stdout: Optional[bytes]
    stderr: Optional[bytes]
    raises: Optional[Exception] = None


@dataclass
class SavedCommand:
    """Holds the result of running a command, with its stdout and err cached.."""

    returncode: int
    executable: str
    args: List[str]
    stdout: FilePtr
    stderr: FilePtr

    # Maybe we just want to get rid of these classes altogether.
    @classmethod
    def from_dict(  # type:ignore
        cls: Type["SavedCommand"], c: Dict[str, Any]
    ) -> "SavedCommand":
        """Populate from a dictionary."""
        return SavedCommand(
            returncode=c["returncode"],
            executable=c["executable"],
            args=c["args"],
            stdout=FilePtr(**c["stdout"]),
            stderr=FilePtr(**c["stderr"]),
        )


# --------------------------------- TASK -------------------------------------
@dataclass
class RTask:
    """Holds a registered task."""

    # task info
    task_id: str

    # commands and outputs
    commands: List[SavedCommand]
    env: Optional[Dict[str, str]]

    # input & output files
    inputs: Dict[str, FilePtr]
    outputs: Dict[str, FilePtr]

    def pack(self: "RTask") -> bytes:
        """Pack self to bytes."""
        return packb(("RTask", packb(asdict(self))))

    @classmethod
    def unpack(cls: Type["RTask"], inp: bytes) -> "RTask":
        """Build a RTask from packed form."""
        d = unpackb(inp)

        return RTask(
            task_id=d["task_id"],
            commands=[SavedCommand.from_dict(c) for c in d["commands"]],
            env=d["env"],
            inputs=dict((k, FilePtr(**v)) for k, v in d["inputs"].items()),
            outputs=dict((k, FilePtr(**v)) for k, v in d["outputs"].items()),
        )


# -----------------------------------------------------------------------------------
# Important function: pull objects from db
__TYPES = {
    "RTask": RTask,
    "RTransformer": RTransformer,
    "FilePtr": FilePtr,
}


# fmt:off
@overload
def pull(cache: Redis, task_id: str, which: Literal["RTask"]) -> Optional[RTask]: ...  # noqa
@overload
def pull(cache: Redis, task_id: str, which: Literal["RTransformer"]) -> Optional[RTransformer]: ...  # noqa
@overload
def pull(cache: Redis, task_id: str, which: Literal["FilePtr"]) -> Optional[FilePtr]: ...  # noqa
@overload
def pull(cache: Redis, task_id: str, which: Literal[None]=None) -> Optional[Union[FilePtr, RTransformer, RTask]]: ...  # noqa
# fmt:on


def pull(
    cache: Redis, task_id: str, which: Optional[str] = None
) -> Union[None, RTask, FilePtr, RTransformer]:
    """Return object associated with a given id."""
    out = cache.hget(__OBJECTS, task_id)
    if out is not None:
        typ, val = unpackb(out)
        if which is not None and typ != which:
            logging.critical(f"attempted to pull object at {task_id}")
            logging.critical(f"expected to find {which}, got {typ}")
            logging.critical("database is probably very messed up.")
            raise RuntimeError("critical database issue.")

        if typ not in __TYPES:
            logging.critical(f"attempted to pull object at {task_id}")
            logging.critical(f"got {typ} not in {__TYPES.keys()}")
            logging.critical("database is probably very messed up.")
            raise RuntimeError("critical database issue.")

        return __TYPES[typ].unpack(val)  # type:ignore
    else:
        # not object found
        return None
