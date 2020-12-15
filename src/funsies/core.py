"""Functional wrappers for commandline programs."""
# std
from dataclasses import asdict, dataclass, field
import json
import logging
import os
import subprocess
import tempfile
from typing import Dict, List, Optional, Type, Union

# external
from redis import Redis
import rq

# module
from .cached import CachedFile, FileType, pull_file, put_file
from .constants import __DATA, __IDS, _AnyPath


# ------------------------------------------------------------------------------
# Types for Tasks and Commands
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
class CachedCommandOutput:
    """Holds the result of running a command, with its stdout and err cached.."""

    returncode: int
    stdout: Optional[CachedFile]
    stderr: Optional[CachedFile]

    def json(self: "CachedCommandOutput") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["CachedCommandOutput"], inp: str) -> "CachedCommandOutput":
        """Make a CachedCommandOutput from a json string."""
        d = json.loads(inp)
        return CachedCommandOutput(
            returncode=d["returncode"],
            stdout=CachedFile(**d["stdout"]),
            stderr=CachedFile(**d["stderr"]),
        )


@dataclass
class Task:
    """A Task holds commands and input files."""

    commands: List[Command] = field(default_factory=list)
    inputs: Dict[str, CachedFile] = field(default_factory=dict)
    outputs: List[str] = field(default_factory=list)
    env: Optional[Dict[str, str]] = None

    def json(self: "Task") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["Task"], inp: str) -> "Task":
        """Make a Task from a json string."""
        d = json.loads(inp)
        return Task(
            commands=[Command(**c) for c in d["commands"]],
            inputs=dict((k, CachedFile(**v)) for k, v in d["inputs"].items()),
            outputs=d["outputs"],
            env=d["env"],
        )


@dataclass(frozen=True)
class TaskOutput:
    """Holds the (cached) result of running a Task."""

    # task info
    task_id: int

    # commands and outputs
    commands: List[CachedCommandOutput]

    # input & output files
    inputs: Dict[str, CachedFile]
    outputs: Dict[str, CachedFile]

    def json(self: "TaskOutput") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["TaskOutput"], inp: Union[str, bytes]) -> "TaskOutput":
        """Make a TaskOutput from a json string."""
        print(inp)
        d = json.loads(inp)

        return TaskOutput(
            task_id=d["task_id"],
            commands=[
                # nasty I know...
                CachedCommandOutput.from_json(json.dumps(c))
                for c in d["commands"]
            ],
            inputs=dict((k, CachedFile(**v)) for k, v in d["inputs"].items()),
            outputs=dict((k, CachedFile(**v)) for k, v in d["outputs"].items()),
        )


def pull_task(db: Redis, task_id: int) -> Optional[TaskOutput]:
    """Pull a TaskOutput from redis using its task_id."""
    val = db.hget(__DATA, bytes(task_id))
    if val is None:
        return None
    else:
        return TaskOutput.from_json(val)


def put_task(db: Redis, out: TaskOutput) -> None:
    """Put a TaskOutput in redis."""
    # add to cache, making sure that TaskOutput is there before its reference.
    db.hset(__DATA, bytes(out.task_id), out.json())


# ------------------------------------------------------------------------------
# Module global variables
__TMPDIR: Optional[_AnyPath] = None


def open_cache() -> Redis:
    """Get a connection to the current cache."""
    job = rq.get_current_job()
    connect: Redis = job.connection
    return connect


# ------------------------------------------------------------------------------
# Logging
def __log_task(task: Task) -> None:
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


def __log_output(task: TaskOutput) -> None:
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


# ------------------------------------------------------------------------------
# Task execution
def run(task: Task) -> int:
    """Execute a task and return all outputs."""
    # log task input
    __log_task(task)

    # Get cache
    objcache = open_cache()

    # Check if the task output is already there
    task_key = task.json()

    if objcache.hexists(__IDS, task_key):
        # if it does, get task id
        tmp = objcache.hget(__IDS, task_key)
        assert tmp is not None
        return int(tmp)

    # If not cached, get a new task_id and start running task.
    task_id = objcache.incrby("funsies.task_id", 1)  # type:ignore
    task_id = int(task_id)

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

        couts = []
        for cmd_id, c in enumerate(task.commands):
            cout = run_command(dir, task.env, c)
            couts += [
                CachedCommandOutput(
                    cout.returncode,
                    put_file(
                        objcache,
                        CachedFile(
                            task_id=task_id, type=FileType.CMD, name=f"stdout{cmd_id}"
                        ),
                        cout.stdout,
                    ),
                    put_file(
                        objcache,
                        CachedFile(
                            task_id=task_id, type=FileType.CMD, name=f"stderr{cmd_id}"
                        ),
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
        for file in task.outputs:
            try:
                with open(os.path.join(dir, file), "rb") as f:
                    outputs[str(file)] = put_file(
                        objcache,
                        CachedFile(task_id=task_id, type=FileType.OUT, name=str(file)),
                        f.read(),
                    )

            except FileNotFoundError:
                logging.warning(f"expected file {file}, but didn't find it")

    # Make output object
    out = TaskOutput(task_id, couts, task.inputs, outputs)

    # save id
    objcache.hset(__IDS, task_key, task_id)

    # save task
    put_task(objcache, out)

    return task_id


def run_command(
    dir: _AnyPath,
    environ: Optional[Dict[str, str]],
    command: Command,
) -> CommandOutput:
    """Run a Command."""
    args = [command.executable] + [a for a in command.args]
    error = None

    try:
        proc = subprocess.run(args, cwd=dir, capture_output=True, env=environ)
    except Exception as e:
        logging.exception("run_command failed with exception")
        return CommandOutput(-1, b"", b"", e)

    return CommandOutput(
        proc.returncode, stdout=proc.stdout, stderr=proc.stderr, raises=error
    )
