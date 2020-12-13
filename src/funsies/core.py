"""Functional wrappers for commandline programs."""
# std
from dataclasses import dataclass, field
import logging
import os
import subprocess
import tempfile
from threading import Lock
from typing import Dict, Optional, Sequence, Union

# external
from diskcache import FanoutCache


# Type for paths
_AnyPath = Union[str, os.PathLike]

# ------------------------------------------------------------------------------
# Dask utils
try:
    from dask.distributed import get_worker as __get_worker

    __DASK_AVAIL = True
except ImportError:
    __DASK_AVAIL = False


def __worker_id() -> int:
    """Return worker id or 0 if not run with dask."""
    if __DASK_AVAIL:
        try:
            out: int = __get_worker().id
        except ValueError:
            out = 0
    else:
        out = 0

    return out


# ------------------------------------------------------------------------------
# Types for Tasks and Commands
@dataclass(frozen=True)
class Command:
    """A shell command executed by a task."""

    executable: _AnyPath
    args: Sequence[_AnyPath] = field(default_factory=tuple)

    def __str__(self: "Command") -> str:
        """Return command as a string."""
        return str(self.executable) + " " + " ".join([str(a) for a in self.args])


@dataclass(frozen=True)
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
    env: Optional[Dict[str, str]] = None


@dataclass
class TaskOutput:
    """Holds the result of running a command."""

    commands: Sequence[CommandOutput]
    outputs: Dict[_AnyPath, bytes]
    cached: bool = False


# ------------------------------------------------------------------------------
# Context related variables
@dataclass(frozen=True)
class CacheSettings:
    """Describes the cache used for memoization."""

    path: _AnyPath
    shards: int
    timeout: float


@dataclass
class Context:
    """Execution context for tasks."""

    tmpdir: Optional[_AnyPath] = None
    cache: Optional[CacheSettings] = None


# Module global variables

# These are used to cache the current... cache, because opening and closing it
# is expensive, at the cost of introducing mutable state. We avoid (user) mutable
# state by always requiring the proper Context.

# However, there is still some state involved: multi-threaded applications
# will all be using the same __CACHE. This is fine (good actually! it's
# faster)... but it means we need to synchronize __CACHE __set__(). This is
# why we have a lock here.
__CACHE: Optional[FanoutCache] = None
__CACHE_DESC: Optional[CacheSettings] = None
__CACHE_LOCK = Lock()


def __get_cache(
    context: Optional[Context],
) -> Optional[FanoutCache]:
    """Take an (optional) context object and get the Cache associated with it.

    This function takes a description of Context (which can be None) and
    returns the Cache object associated with it, which can also be None.

    Arguments:
        context: An optional Context object.

    Returns:
        An optional Cache.

    """
    global __CACHE, __CACHE_DESC
    # acquire lock so that other threads don't look at __CACHE / __CACHE_DESC
    # until I'm done with them.
    __CACHE_LOCK.acquire()

    if context is None:
        __CACHE = None
        __CACHE_DESC = None

    elif context.cache is None:
        __CACHE = None
        __CACHE_DESC = None

    else:
        if context.cache == __CACHE_DESC:
            pass
        else:
            __CACHE_DESC = context.cache
            try:
                __CACHE = FanoutCache(
                    str(context.cache.path),
                    shards=context.cache.shards,
                    timeout=context.cache.timeout,
                )
            except Exception:
                logging.exception(
                    f"id {__worker_id()} cache setup failed with exception"
                )
                __CACHE = None
                __CACHE_DESC = None
            else:
                logging.debug(f"id {__worker_id()} setting up {context.cache}")

    __CACHE_LOCK.release()
    return __CACHE


# ------------------------------------------------------------------------------
# Logging
def __log_task(task: Task) -> None:
    """Log a task."""
    info = "TASK\n"
    info += f"id: {__worker_id()}\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} ${}\n".format(i, str(c))

    # detailed trace of task
    debug = f"id: {__worker_id()}\n"
    debug += f"environment variables: {task.env}\n"
    for key, val in task.inputs.items():
        debug += "filename : {} contains\n{}\n EOF ----------\n".format(
            key, val.decode()
        )
    debug += "expecting files: {}".format(task.outputs)
    logging.info(info.rstrip())
    logging.debug(debug)


def __log_output(task: TaskOutput) -> None:
    """Log a completed task."""
    info = "TASK OUT\n"
    info += f"id: {__worker_id()}\n"
    info += f"cached? {task.cached}\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} return code: {}\n stderr: {}\n".format(
            i, c.returncode, c.stderr.decode()
        )

    # detailed trace of task
    debug = f"id: {__worker_id()}\n"
    for key, val in task.outputs.items():
        debug += "filename : {} contains\n{}\n EOF ----------\n".format(
            key, val.decode()
        )
    logging.info(info.rstrip())
    logging.debug(debug.rstrip())


# ------------------------------------------------------------------------------
# Task execution
def __cached(cache: Optional[FanoutCache], task: Task) -> Optional[TaskOutput]:
    # Check if it is cached
    if cache is not None:
        if task in cache:
            result = cache[task]
            if isinstance(result, TaskOutput):
                return result
            else:
                logging.warning(
                    "Cache entry for task is not of type TaskOutput, recomputing."
                )

    return None


def run(task: Task, context: Optional[Context] = None) -> TaskOutput:
    """Execute a task and return all outputs."""
    # get cache
    cache = __get_cache(context)

    # log task input
    __log_task(task)

    # Load from cache
    cached = __cached(cache, task)

    if cached is not None:
        __log_output(cached)
        return cached

    if context is None:
        tmpdir = None
    else:
        tmpdir = context.tmpdir

    with tempfile.TemporaryDirectory(dir=tmpdir) as dir:
        # Put in dir the input files
        for filename, contents in task.inputs.items():
            with open(os.path.join(dir, filename), "wb") as f:
                f.write(contents)

        couts = []
        for c in task.commands:
            couts += [run_command(dir, task.env, c)]
            if couts[-1].exception:
                # Stop, something went wrong
                logging.warning(f"command did not run: {c}")
                break

        # Read files
        outputs = {}
        for name in task.outputs:
            try:
                with open(os.path.join(dir, name), "rb") as f:
                    outputs[name] = f.read()
            except FileNotFoundError:
                logging.warning(f"expected file {name}, but didn't find it")
                pass

        out = TaskOutput(couts, outputs)
        __log_output(out)

        if cache is not None:
            # Cache the result if currently active
            cache.add(task, TaskOutput(couts, outputs, cached=True))

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
        logging.exception("run_command failed with exception")
        return CommandOutput(-1, b"", b"", e)

    return CommandOutput(
        proc.returncode, stdout=proc.stdout, stderr=proc.stderr, exception=error
    )
