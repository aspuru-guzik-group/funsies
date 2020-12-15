"""Functional wrappers for commandline programs."""
# std
from dataclasses import dataclass, field
import logging
import os
import pickle
import subprocess
import tempfile
from typing import Dict, Optional, Sequence, Tuple

# external
from redis import Redis
import rq

# module
from .cached import put_file, CachedFile, FileType, pull_file
from .constants import _AnyPath, __IDS, __DATA


# ------------------------------------------------------------------------------
# Types for Tasks and Commands
@dataclass
class Command:
    """A shell command executed by a task."""

    executable: str
    args: Sequence[str] = field(default_factory=tuple)

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
    raises: Optional[Exception] = None


@dataclass
class Task:
    """A Task holds commands and input files."""

    commands: Sequence[Command] = field(default_factory=tuple)
    inputs: Dict[str, CachedFile] = field(default_factory=dict)
    outputs: Sequence[str] = field(default_factory=tuple)
    env: Optional[Dict[str, str]] = None

    def bytes(self: "Task") -> bytes:
        """Return a pickled version of myself."""
        return pickle.dumps(self)


@dataclass(frozen=True)
class TaskOutput:
    """Holds the (cached) result of running a Task."""

    # task info
    task_id: int

    # commands and outputs
    commands: Tuple[CachedCommandOutput, ...]

    # input & output files
    inputs: Dict[str, CachedFile]
    outputs: Dict[str, CachedFile]

    # problems
    raises: Optional[Exception] = None

    def bytes(self: "TaskOutput") -> bytes:
        """Return a pickled version of myself."""
        return pickle.dumps(self)


def pull_task(db: Redis, task_id: int) -> TaskOutput:
    return pickle.loads(db.hget(__DATA, task_id))


def put_task(db: Redis, out: TaskOutput):
    # add to cache, making sure that TaskOutput is there before its reference.
    db.hset(__DATA, out.task_id, out.bytes())


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
    task_key = task.bytes()

    if objcache.hexists(__IDS, task_key):
        # if it does, get task id
        tmp = objcache.hget(__IDS, task_key)
        assert tmp is not None
        return int(tmp)

    # If not cached, get a new task_id and start running task.
    tmp = objcache.incr("funsies.task_id", amount=1)
    assert tmp is not None
    task_id = int(tmp)

    # TODO expandvar, expandusr for tempdir
    # TODO setable tempdir
    with tempfile.TemporaryDirectory(dir=__TMPDIR) as dir:
        # Put in dir the input files
        for fn, cachedfile in task.inputs.items():
            with open(os.path.join(dir, fn), "wb") as f:
                val = pull_file(objcache, cachedfile)
                f.write(val)

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
                    cout.raises,
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
    out = TaskOutput(task_id, tuple(couts), task.inputs, outputs)

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
