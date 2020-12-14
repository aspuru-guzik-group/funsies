"""Functional wrappers for commandline programs."""
# std
from dataclasses import dataclass, field
from enum import Enum, unique
import logging
import os
import subprocess
import tempfile
import shutil
from threading import Lock
from typing import Dict, Optional, Sequence, Tuple, Union, IO, Literal, overload

# external
from diskcache import FanoutCache

# module
from .cached import CachedFile, get_file, set_file, FileType
from .constants import _AnyPath


# ------------------------------------------------------------------------------
# Cache settings
@dataclass(frozen=True)
class Cache:
    """Specification for Cache access."""

    # Required to open the cache
    path: _AnyPath
    shards: int = 1
    timeout: float = 1.0


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
    raises: Optional[Exception] = None


@dataclass(frozen=True)
class CachedCommandOutput:
    """Holds the result of running a command, with its stdout and err cached.."""

    returncode: int
    stdout: CachedFile
    stderr: CachedFile
    raises: Optional[Exception] = None


@dataclass(frozen=True)
class Task:
    """A Task holds commands and input files."""

    cache: Cache
    commands: Sequence[Command] = field(default_factory=tuple)
    inputs: Dict[_AnyPath, bytes] = field(default_factory=dict)
    outputs: Sequence[_AnyPath] = field(default_factory=tuple)
    env: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class TaskOutput:
    """Holds the (cached) result of running a Task."""

    # task info
    cache: Cache
    task_id: int

    # commands
    commands: Tuple[CachedCommandOutput, ...]

    # output files
    outputs: Dict[_AnyPath, CachedFile]

    # problems
    raises: Optional[Exception] = None


# ------------------------------------------------------------------------------
# Module global variables

# These are used to cache the current... cache, because opening and closing it
# is expensive, at the cost of introducing mutable state. We avoid (user) mutable
# state by always requiring a proper Cache object.

# However, there is still some state involved: multi-threaded applications
# will all be using the same __CACHE. This is fine (good actually! it's
# faster)... but it means we need to synchronize __CACHE __set__(). This is
# why we have a lock here.
__CACHE: Optional[FanoutCache] = None
__CACHE_DESC: Optional[Cache] = None
__CACHE_LOCK = Lock()


def open_cache(cache: Cache) -> Union[FanoutCache, Exception]:
    """Take a CacheSettings and get the Cache associated with it.

    This function takes a description of cache and returns the Cache object
    associated with it.

    Arguments:
        cache: A CacheSettings object.

    Returns:
        A corresponding FanoutCache.
    """
    global __CACHE, __CACHE_DESC
    # acquire lock so that other threads don't look at __CACHE / __CACHE_DESC
    # until I'm done with them.
    __CACHE_LOCK.acquire()

    if cache == __CACHE_DESC:
        pass
    else:
        __CACHE_DESC = cache
        try:
            __CACHE = FanoutCache(
                str(cache.path),
                shards=cache.shards,
                timeout=cache.timeout,
            )

            # Add the task id for the cache
            if "id" not in __CACHE:
                __CACHE.add("id", 0)

        except Exception as e:
            logging.exception("cache setup failed with exception")
            __CACHE = None
            __CACHE_DESC = None
            return e
        else:
            logging.debug(f"worker accessing {cache}")

    __CACHE_LOCK.release()
    return __CACHE


# ------------------------------------------------------------------------------
# Logging
def __log_task(task: Task) -> None:
    """Log a task."""
    info = "TASK\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} ${}\n".format(i, str(c))

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
    info += f"was cached? {task.cached}\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} return code: {}\n stderr: {}\n".format(
            i, c.returncode, c.stderr.decode()
        )

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
def __cached(cache: FanoutCache, task: Task) -> Optional[TaskOutput]:
    # Check if it is cached
    if task in cache:
        result = cache[task]
        if isinstance(result, TaskOutput):
            return result
        else:
            logging.warning(
                "Cache entry for task is not of type TaskOutput, recomputing."
            )
    return None


def run(task: Task, tmpdir: Optional[_AnyPath] = None) -> TaskOutput:
    """Execute a task and return all outputs."""
    # log task input
    __log_task(task)

    # Instantiate / get current FanoutCache
    cache = open_cache(task.cache)
    if isinstance(cache, Exception):
        return TaskOutput(task.cache, -1, task.commands, tuple(), cache)

    # Check if task was previously cached, and return the cached value if it
    # was.
    cached = __cached(cache, task)
    if cached is not None:
        __log_output(cached)
        return cached

    # If not cached, get a new task_id and start running task.
    task_id = cache.incr("id")

    # TODO expandvar, expandusr for tmpdir
    with tempfile.TemporaryDirectory(dir=tmpdir) as dir:
        # Put in dir the input files
        for fn, val in task.inputs.items():
            with open(os.path.join(dir, fn), "wb") as f:
                f.write(val)

        couts = []
        for cmd_id, c in enumerate(task.commands):
            couts += [
                __cache_command(cache, task_id, cmd_id, run_command(dir, task.env, c))
            ]
            if couts[-1].raises:
                # Stop, something went wrong
                logging.warning(f"command did not run: {c}")
                break

        # Output files
        outputs = {}
        for file in task.outputs:
            try:
                with open(os.path.join(dir, file), "rb") as f:
                    outputs[file] = set_file(
                        cache,
                        CachedFile(task_id, FileType.FILE_OUTPUT, str(file)),
                        f.read(),
                    )
            except FileNotFoundError:
                logging.warning(f"expected file {file}, but didn't find it")

    out = TaskOutput(task.cache, task_id, tuple(couts), outputs)

    # add to cache
    # cache[task.as_key()] = task_output
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
        proc.returncode, stdout=proc.stdout, stderr=proc.stderr, raises=error
    )


def __cache_command(cache: FanoutCache, task_id: int, cmd_id: int, c: CommandOutput):
    return CachedCommandOutput(
        c.returncode,
        set_file(
            cache, CachedFile(task_id, FileType.CMD, "stdout", cmd_id=cmd_id), c.stdout
        ),
        set_file(
            cache, CachedFile(task_id, FileType.CMD, "stderr", cmd_id=cmd_id), c.stderr
        ),
        c.raises,
    )
