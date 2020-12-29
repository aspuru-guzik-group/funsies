"""Main type definitions."""
# stdlib
from dataclasses import asdict, dataclass
import hashlib
import logging
from typing import Any, Dict, List, Literal, Optional, overload, Type, TypeVar, Union

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from .constants import __OBJECTS

T = TypeVar("T")


@dataclass(frozen=True)
class Thing:
    """Represent a calculation step registered to the database."""

    task_id: str

    def pack(self: "Thing") -> bytes:
        """Pack a thing into bytes."""
        out = {}
        for k, v in asdict(self).items():
            if isinstance(v, Thing):
                out[k] = v.pack()
            else:
                out[k] = v
        return packb((type(self).__name__, packb(out)))


# --------------------------------- FILEPTR -------------------------------------
@dataclass(frozen=True)
class FilePtr(Thing):
    """A pointer to a file on cache.

    FilePtr is the main class that provides a way to uniquely access files
    that are currently cached for writing and reading. Basically, Tasks have
    file inputs and outputs as well as stdouts and stderr. All of these are
    stored in the cache and are accessed using the associated CachedFiles as
    keys.

    """

    name: str
    comefrom: str

    def __str__(self: "FilePtr") -> str:
        """Return string representation."""
        return f"{str(self.task_id)}::{self.name}"

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
class RTransformer(Thing):
    """Holds a registered transformer."""

    # The transformation function
    fun: bytes

    # input & output files
    inp: List[FilePtr]
    out: List[FilePtr]

    @classmethod
    def unpack(cls: Type["RTransformer"], inp: bytes) -> "RTransformer":
        """Build a transformer from packed form."""
        d = unpackb(inp)

        return RTransformer(
            task_id=d["task_id"],
            fun=d["fun"],
            inp=[FilePtr(**v) for v in d["inp"]],
            out=[FilePtr(**v) for v in d["out"]],
        )


# -------------------------  CLI COMMANDS --------------------------------------
@dataclass
class ShellOutput:
    """Holds the result of running a command, with its stdout and err cached.."""

    returncode: int
    command: str
    stdout: FilePtr
    stderr: FilePtr

    # Maybe we just want to get rid of these classes altogether.
    @classmethod
    def from_dict(  # type:ignore
        cls: Type["ShellOutput"], c: Dict[str, Any]
    ) -> "ShellOutput":
        """Populate from a dictionary."""
        return ShellOutput(
            returncode=c["returncode"],
            command=c["command"],
            stdout=FilePtr(**c["stdout"]),
            stderr=FilePtr(**c["stderr"]),
        )


# --------------------------------- TASK -------------------------------------
@dataclass(frozen=True)
class RTask(Thing):
    """Holds a registered task."""

    # commands and outputs
    commands: List[ShellOutput]
    env: Optional[Dict[str, str]]

    # input & output files
    inp: Dict[str, FilePtr]
    out: Dict[str, FilePtr]

    @classmethod
    def unpack(cls: Type["RTask"], inp: bytes) -> "RTask":
        """Build a RTask from packed form."""
        d = unpackb(inp)

        return RTask(
            task_id=d["task_id"],
            commands=[ShellOutput.from_dict(c) for c in d["commands"]],
            env=d["env"],
            inp=dict((k, FilePtr(**v)) for k, v in d["inp"].items()),
            out=dict((k, FilePtr(**v)) for k, v in d["out"].items()),
        )


# ----------------------------------------------------------------------------
# Invariants
def get_hash_id(invariants: bytes) -> str:
    """Transform a bytestring to a SHA256 hash string."""
    m = hashlib.sha256()
    m.update(invariants)
    task_id = m.hexdigest()
    logging.debug(f"invariants: \n{str(invariants)}")
    logging.debug(f"key: {task_id}")
    return task_id


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
