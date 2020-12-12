"""Functional wrappers for commandline programs."""
# std
from dataclasses import astuple, dataclass, field
import os
import subprocess
import tempfile
from typing import Dict, Optional, Sequence, Union

# external
from dask.distributed import get_worker
from diskcache import FanoutCache

# module
# from .logger import logger

# Type for paths
_AnyPath = Union[str, os.PathLike]


# Variable holding cache
__CACHE: Optional[FanoutCache] = None


# ------------------------------------------------------------------------------
# Types
@dataclass(frozen=True)
class Command:
    """A shell command executed by a task."""

    executable: _AnyPath
    args: Sequence[_AnyPath] = field(default_factory=tuple)

    def __str__(self: "Command") -> str:
        """Return command as a string."""
        return str(self.executable) + " " + " ".join([str(a) for a in self.args])


@dataclass
class CommandOutput:
    """Holds the result of running a command."""

    returncode: int
    stdout: bytes
    stderr: bytes
    exception: Optional[Exception] = None


@dataclass
class Task:
    """A task holds commands to be executed on specific files."""

    commands: Sequence[Command]
    inputs: Dict[_AnyPath, bytes] = field(default_factory=dict)
    outputs: Sequence[_AnyPath] = field(default_factory=tuple)
    environ: Optional[Dict[str, str]] = None


@dataclass
class TaskOutput:
    """Holds the result of running a command."""

    commands: Sequence[CommandOutput]
    outputs: Dict[_AnyPath, bytes]
    cached: bool = False


# ------------------------------------------------------------------------------
# Dask utils
def __worker_id() -> int:
    """Return worker id or 0 if not run with dask."""
    try:
        out: int = get_worker().id
    except ValueError:
        out = 0
    return out


def setup_cache(dir: _AnyPath, shards: int, timeout: float) -> None:
    """Setup the on-disk cache for Tasks."""
    global __CACHE
    __CACHE = FanoutCache(dir, shards=shards, timeout=timeout)
    logger.trace(f"id: {__worker_id()} has cache {__CACHE}")


# ------------------------------------------------------------------------------
# Logging
def log_task(task: Task) -> None:
    """Log a task."""
    debug = "TASK\n"
    debug += f"id: {__worker_id()}\n"
    for i, c in enumerate(task.commands):
        debug += "cmd {} exec: {}\n".format(i, str(c))

    # detailed trace of task
    trace = f"id: {__worker_id()}\n"
    trace += f"environment variables: {task.environ}\n"
    for key, val in task.inputs.items():
        trace += "filename : {} contains\n{}\n EOF ----------\n".format(
            key, val.decode()
        )
    trace += "expecting files: {}".format(task.outputs)
    logger.debug(debug.rstrip())
    logger.trace(trace)


def log_output(task: TaskOutput) -> None:
    """Log a completed task."""
    debug = "TASK OUT\n"
    debug += f"id: {__worker_id()}\n"
    for i, c in enumerate(task.commands):
        debug += "cmd {} return code: {}\n stderr: {}\n".format(
            i, c.returncode, c.stderr.decode()
        )

    # detailed trace of task
    trace = f"id: {__worker_id()}\n"
    for key, val in task.outputs.items():
        trace += "filename : {} contains\n{}\n EOF ----------\n".format(
            key, val.decode()
        )
    logger.debug(debug.rstrip())
    logger.trace(trace.rstrip())


# ------------------------------------------------------------------------------
# Task execution
def run(task: Task) -> TaskOutput:
    """Execute a task and return all outputs."""
    # log task input
    log_task(task)

    # Check if it is cached
    if __CACHE is not None:
        key = astuple(task)  # convert to a tuple for storing in db
        if key in __CACHE:
            logger.debug("task was cached.")
            result = __CACHE[key]
            if isinstance(result, TaskOutput):
                return result
            else:
                logger.error(
                    "Cache entry for task is not of type TaskOutput, recomputing."
                )

    # TODO make directory location setable
    with tempfile.TemporaryDirectory() as dir:
        # Put in dir the input files
        for filename, contents in task.inputs.items():
            with open(os.path.join(dir, filename), "wb") as f:
                f.write(contents)

        couts = []
        for c in task.commands:
            couts += [run_command(dir, task.environ, c)]
            if couts[-1].exception:
                # Stop, something went wrong
                logger.warning("command did not run: {}", c)
                break

        # Read files
        outputs = {}
        for name in task.outputs:
            try:
                with open(os.path.join(dir, name), "rb") as f:
                    outputs[name] = f.read()
            except FileNotFoundError:
                logger.warning(f"expected file {name}, but didn't find it")
                pass

        out = TaskOutput(couts, outputs)
        log_output(out)

        if __CACHE is not None:
            # Cache the result if currently active
            __CACHE[key] = TaskOutput(couts, outputs, cached=True)

        return out


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
        logger.exception("failed with exception ", e)
        return CommandOutput(-1, b"", b"", e)

    return CommandOutput(
        proc.returncode, stdout=proc.stdout, stderr=proc.stderr, exception=error
    )
