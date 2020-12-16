"""Cached tasks in redis."""
# std
from dataclasses import asdict, dataclass, field
import json
from typing import Dict, List, Optional, Type, Union

# external
from redis import Redis

# module
from .cached import CachedFile, FileType
from .command import CachedCommandOutput, Command
from .constants import __DATA, __IDS, __TASK_ID


@dataclass
class UnregisteredTask:
    """Holds a task that is not yet registered with Redis."""

    commands: List[Command] = field(default_factory=list)
    inputs: Dict[str, CachedFile] = field(default_factory=dict)
    outputs: List[str] = field(default_factory=list)
    env: Optional[Dict[str, str]] = None

    def json(self: "UnregisteredTask") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["UnregisteredTask"], inp: str) -> "UnregisteredTask":
        """Make a Task from a json string."""
        d = json.loads(inp)
        return UnregisteredTask(
            commands=[Command(**c) for c in d["commands"]],
            inputs=dict((k, CachedFile(**v)) for k, v in d["inputs"].items()),
            outputs=d["outputs"],
            env=d["env"],
        )


@dataclass(frozen=True)
class RTask:
    """Holds a registered task."""

    # task info
    task_id: int

    # commands and outputs
    commands: List[CachedCommandOutput]
    env: Optional[Dict[str, str]]

    # input & output files
    inputs: Dict[str, CachedFile]
    outputs: Dict[str, CachedFile]

    def json(self: "RTask") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["RTask"], inp: Union[str, bytes]) -> "RTask":
        """Build a registered task from a json string."""
        d = json.loads(inp)

        return RTask(
            task_id=d["task_id"],
            commands=[
                # nasty I know...
                CachedCommandOutput.from_json(json.dumps(c))
                for c in d["commands"]
            ],
            env=d["env"],
            inputs=dict((k, CachedFile(**v)) for k, v in d["inputs"].items()),
            outputs=dict((k, CachedFile(**v)) for k, v in d["outputs"].items()),
        )


def pull_task(db: Redis, task_id: int) -> Optional[RTask]:
    """Pull a TaskOutput from redis using its task_id."""
    val = db.hget(__DATA, bytes(task_id))
    if val is None:
        return None
    else:
        return RTask.from_json(val)


# ------------------------------------------------------------------------------
# Register task on db
def register(cache: Redis, task: UnregisteredTask) -> RTask:
    """Register an UnregisteredTask into Redis to get an RTask."""
    # Check if the task output is already there
    task_key = task.json()

    if cache.hexists(__IDS, task_key):
        # if it does, get task id
        tmp = cache.hget(__IDS, task_key)
        # pull the id from the db
        assert tmp is not None
        out = pull_task(cache, int(tmp))
        assert out is not None
        return out

    # If it doesn't exist, we make the task
    # If not cached, get a new task_id and start running task.
    task_id = cache.incrby(__TASK_ID, 1)  # type:ignore
    task_id = int(task_id)

    # build cmd outputs
    couts = []
    for cmd_id, cmd in enumerate(task.commands):
        couts += [
            CachedCommandOutput(
                -1,
                cmd.executable,
                cmd.args,
                CachedFile(task_id=task_id, type=FileType.CMD, name=f"stdout{cmd_id}"),
                CachedFile(task_id=task_id, type=FileType.CMD, name=f"stderr{cmd_id}"),
            )
        ]

    # build output files
    outputs = {}
    for file in task.outputs:
        outputs[str(file)] = CachedFile(
            task_id=task_id, type=FileType.OUT, name=str(file)
        )

    # output object
    out = RTask(task_id, couts, task.env, task.inputs, outputs)

    # save task
    # TODO catch errors
    __cache_task(cache, out)

    return out


def __cache_task(cache: Redis, task: RTask) -> None:
    # save id
    # TODO catch errors
    cache.hset(__DATA, bytes(task.task_id), task.json())


# ------------------------------------------------------------------------------
# Logging
def __log_task(task: RTask) -> None:
    """Log a task."""
    info = "TASK\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} ${}\n".format(i, c)

    # # detailed trace of task
    # debug = f"environment variables: {task.env}\n"
    # for key, val in task.inputs.items():
    #     debug += "filename : {} contains\n{}\n EOF ----------\n".format(
    #         key, val.decode()
    #     )
    # debug += "expecting files: {}".format(task.outputs)
    # logging.info(info.rstrip())
    # logging.debug(debug)


def __log_output(task: RTask) -> None:
    """Log a completed task."""
    info = "TASK OUT\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} return code: {}\n".format(i, c.returncode)

    # # detailed trace of task
    # debug = "TASK OUT TRACE\n"
    # for key, val in task.outputs.items():
    #     debug += "filename : {} contains\n{}\n EOF ----------\n".format(
    #         key, val.decode()
    #     )
    # logging.info(info.rstrip())
    # logging.debug(debug.rstrip())
