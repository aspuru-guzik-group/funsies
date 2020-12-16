"""Cached tasks in redis."""
# std
from dataclasses import asdict, dataclass, field
import json
import logging
import os
import pickle
import tempfile
from typing import Dict, List, Optional, Type, Union

# external
from redis import Redis

# module
from .cached import CachedFile, FileType, pull_file, put_file
from .command import CachedCommandOutput, Command, run_command
from .constants import __IDS, __TASK_ID, __TASKS, _AnyPath

# ------------------------------------------------------------------------------
# Config
__TMPDIR: Optional[_AnyPath] = None


@dataclass
class UnregisteredTask:
    """Holds a task that is not yet registered with Redis."""

    commands: List[Command] = field(default_factory=list)
    inputs: Dict[str, Union[CachedFile, str]] = field(default_factory=dict)
    outputs: List[str] = field(default_factory=list)
    env: Optional[Dict[str, str]] = None

    def json(self: "UnregisteredTask") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["UnregisteredTask"], inp: str) -> "UnregisteredTask":
        """Make a Task from a json string."""
        d = json.loads(inp)

        inputs: Dict[str, Union[CachedFile, str]] = {}
        for k, v in d["inputs"].items():
            if isinstance(v, dict):
                inputs[k] = CachedFile(**v)
            elif isinstance(v, str):
                inputs[k] = v
            else:
                raise TypeError(f"file {k} = {v} not of type str or bytes")

        return UnregisteredTask(
            commands=[Command(**c) for c in d["commands"]],
            inputs=inputs,
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
    val = db.hget(__TASKS, bytes(task_id))
    if val is None:
        return None
    else:
        return RTask.from_json(val)


# ------------------------------------------------------------------------------
# Register task on db
def register_task(cache: Redis, task: UnregisteredTask) -> RTask:
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
        outputs[file] = CachedFile(task_id=task_id, type=FileType.OUT, name=file)

    inputs = {}
    # build input files
    for file, val in task.inputs.items():
        if isinstance(val, CachedFile):
            inputs[file] = val
        else:
            inputs[file] = put_file(
                cache,
                CachedFile(task_id=task_id, type=FileType.INP, name=file),
                val.encode(),
            )

    # output object
    out = RTask(task_id, couts, task.env, inputs, outputs)

    # save task
    # TODO catch errors
    __cache_task(cache, out)

    return out


def __cache_task(cache: Redis, task: RTask) -> None:
    # save id
    # TODO catch errors
    cache.hset(__TASKS, bytes(task.task_id), task.json())


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


# runner
def run_rtask(objcache: Redis, task: RTask) -> int:
    """Execute a registered task and return its task id."""
    # Check status
    task_id = task.task_id

    # todo:add caching here

    # TODO expandvar, expandusr for tempdir
    # TODO setable tempdir
    with tempfile.TemporaryDirectory(dir=__TMPDIR) as dir:
        # Put in dir the input files
        for fn, cachedfile in task.inputs.items():
            with open(os.path.join(dir, fn), "wb") as f:
                val = pull_file(objcache, cachedfile)
                if val is not None:
                    f.write(val)
                else:
                    logging.error(f"could not pull file from cache: {fn}")

        # List of inputs and outputs for possible transformers
        with open(os.path.join(dir, "__metadata__.pkl"), "wb") as fi:
            pickle.dump(
                {
                    "inputs": list(task.inputs.keys()),
                    "outputs": list(task.outputs.keys()),
                },
                fi,
            )

        couts = []
        for _, c in enumerate(task.commands):
            cout = run_command(dir, task.env, c)
            couts += [
                CachedCommandOutput(
                    cout.returncode,
                    c.executable,
                    c.args,
                    put_file(
                        objcache,
                        c.stdout,
                        cout.stdout,
                    ),
                    put_file(
                        objcache,
                        c.stderr,
                        cout.stderr,
                    ),
                )
            ]
            if cout.raises:
                # Stop, something went wrong
                logging.warning(f"command did not run: {c}")
                break

        # Output files
        outputs = {}
        for file, value in task.outputs.items():
            try:
                with open(os.path.join(dir, file), "rb") as f:
                    outputs[str(file)] = put_file(
                        objcache,
                        value,
                        f.read(),
                    )

            except FileNotFoundError:
                logging.warning(f"expected file {file}, but didn't find it")

    # Make output object and update db
    task = RTask(task_id, couts, task.env, task.inputs, outputs)

    # update cached copy
    __cache_task(objcache, task)

    return task_id
