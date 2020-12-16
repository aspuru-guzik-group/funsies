"""Cached tasks in redis."""
# std
from dataclasses import asdict, dataclass, field
import logging
import os
import tempfile
from typing import Dict, List, Optional, Sequence, Type

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from .cached import FilePtr, pull_file, put_file, register_file
from .command import CachedCommandOutput, Command, run_command
from .constants import __IDS, __OBJECTS, __SDONE, __STATUS, __TASK_ID, _AnyPath

# ------------------------------------------------------------------------------
# Config
__TMPDIR: Optional[_AnyPath] = None


@dataclass
class RTask:
    """Holds a registered task."""

    # task info
    task_id: int

    # commands and outputs
    commands: List[CachedCommandOutput]
    env: Optional[Dict[str, str]]

    # input & output files
    inputs: Dict[str, FilePtr]
    outputs: Dict[str, FilePtr]

    def pack(self: "RTask") -> str:
        """Return a packed version of myself."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["RTask"], inp: bytes) -> "RTask":
        """Build a registered task from packed representation."""
        d = unpackb(inp)

        return RTask(
            task_id=d["task_id"],
            commands=[CachedCommandOutput.from_dict(c) for c in d["commands"]],
            env=d["env"],
            inputs=dict((k, FilePtr(**v)) for k, v in d["inputs"].items()),
            outputs=dict((k, FilePtr(**v)) for k, v in d["outputs"].items()),
        )


def pull_task(db: Redis, task_id: int) -> Optional[RTask]:
    """Pull a TaskOutput from redis using its task_id."""
    val = db.hget(__OBJECTS, task_id)
    if val is None:
        return None
    else:
        return RTask.unpack(val)


# ------------------------------------------------------------------------------
# Register task on db
@dataclass
class UnregisteredTask:
    """Holds a task that is not yet registered with Redis."""

    commands: List[Command] = field(default_factory=list)
    inputs: Dict[str, FilePtr] = field(default_factory=dict)
    outputs: Dict[str, FilePtr] = field(default_factory=dict)
    env: Optional[Dict[str, str]] = None

    def pack(self: "UnregisteredTask") -> bytes:
        """Return unpacked version of myself."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["UnregisteredTask"], inp: bytes) -> "UnregisteredTask":
        """Make an UnregisteredTask from packed representation."""
        d = unpackb(inp)

        return UnregisteredTask(
            commands=[Command(**c) for c in d["commands"]],
            inputs=dict((k, FilePtr(**v)) for k, v in d["inputs"].items()),
            outputs=dict((k, FilePtr(**v)) for k, v in d["outputs"].items()),
            env=d["env"],
        )


def register_task(
    cache: Redis,
    commands: List[Command],
    inputs: Dict[str, FilePtr],
    outputs: Sequence[str],
    env: Optional[Dict[str, str]] = None,
) -> RTask:
    """Register a Task into Redis and get an RTask."""
    # Check if the task already exists. The key that we build the task on is
    # effectively the same as the final RTask BUT:
    # - invariant to order of outputs.
    # - invariant to order of inputs
    # - invariant to order of env

    # Note that although in principle we could return a cache copy when
    # outputs are a strictly smaller set, we don't do that yet for ease of
    # implementation.
    invariants = {
        "commands": [asdict(c) for c in commands],
        "inputs": sorted([(k, asdict(v)) for k, v in inputs.items()]),
        "outputs": sorted(list(outputs)),
    }
    if env is not None:
        invariants["env"] = sorted([(k, v) for k, v in env.items()])

    task_key = packb(invariants)

    if cache.hexists(__IDS, task_key):
        logging.debug("task key already exists.")
        # if it does, get task id
        tmp = cache.hget(__IDS, task_key)
        # TODO better error catching
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
    for cmd_id, cmd in enumerate(commands):
        couts += [
            CachedCommandOutput(
                -1,
                cmd.executable,
                cmd.args,
                register_file(cache, f"task{task_id}.cmd{cmd_id}.stdout"),
                register_file(cache, f"task{task_id}.cmd{cmd_id}.stderr"),
            )
        ]

    # build file outputs
    myoutputs = {}
    for f in outputs:
        myoutputs[f] = register_file(cache, f)

    # output object
    out = RTask(task_id, couts, env, inputs, myoutputs)

    # save task
    __cache_task(cache, out)
    cache.hset(__IDS, task_key, task_id)  # save task

    return out


def __cache_task(cache: Redis, task: RTask) -> None:
    # save id
    # TODO catch errors
    cache.hset(__OBJECTS, task.task_id, task.pack())


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

    if objcache.hget(__STATUS, task_id) == __SDONE:
        logging.debug("task is cached.")
        out = pull_task(objcache, task_id)
        if out is not None:
            return task_id
        else:
            logging.warning("Pulling task out of cache failed, re-executing.")

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
    # TODO: possible race condition?
    objcache.hset(__STATUS, task_id, __SDONE)  # set done

    return task_id
