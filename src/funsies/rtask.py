"""Cached tasks in redis."""
# std
from dataclasses import asdict
import logging
import os
import tempfile
from typing import cast, Dict, List, Optional, Sequence

# external
from msgpack import packb
from redis import Redis

# module
from .cached import pull_file, put_file, register_file
from .command import run_command
from .constants import __DONE, __IDS, __OBJECTS, __TASK_ID, _AnyPath
from .types import Command, FilePtr, pull, RTask, SavedCommand

# ------------------------------------------------------------------------------
# Config
__TMPDIR: Optional[_AnyPath] = None


# ------------------------------------------------------------------------------
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
        # TODO: switch to using packb for invariants as opposed to asdict
        "commands": [asdict(c) for c in commands],
        "inputs": sorted([(k, asdict(v)) for k, v in inputs.items()]),
        "outputs": sorted(list(outputs)),
    }
    if env is not None:
        invariants["env"] = sorted([(k, v) for k, v in env.items()])

    task_key = packb(invariants)
    logging.debug(f"task invariants: \n{str(invariants)}")
    logging.debug(f"task key: {str(task_key)}")

    if cache.hexists(__IDS, task_key):
        logging.info("task key already exists.")
        # if it does, get task id
        tmp = cache.hget(__IDS, task_key)
        # TODO better error catching
        # pull the id from the db
        assert tmp is not None
        out = pull(cache, tmp.decode(), which="RTask")
        if out is None:
            logging.error("Tried to extract RTask but failed! recomputing...")
        else:
            return out

    # If it doesn't exist, we make the task with a new id
    task_id = cast(str, str(cache.incrby(__TASK_ID, 1)))  # type:ignore

    # build cmd outputs
    couts = []
    for cmd_id, cmd in enumerate(commands):
        couts += [
            SavedCommand(
                -1,
                cmd.executable,
                cmd.args,
                register_file(
                    cache, f"task{str(task_id)}.cmd{cmd_id}.stdout", comefrom=task_id
                ),
                register_file(
                    cache, f"task{str(task_id)}.cmd{cmd_id}.stderr", comefrom=task_id
                ),
            )
        ]

    # build file outputs
    myoutputs = {}
    for f in outputs:
        myoutputs[f] = register_file(cache, f, comefrom=task_id)

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
def run_rtask(objcache: Redis, task: RTask) -> str:
    """Execute a registered task and return its task id."""
    # Check status
    task_id = task.task_id

    if objcache.sismember(__DONE, task_id) == 1:  # type:ignore
        logging.info("task is cached.")
        return task_id
    else:
        logging.info(f"evaluating task {task_id}.")

    # TODO expandvar, expandusr for tempdir
    # TODO setable tempdir
    with tempfile.TemporaryDirectory(dir=__TMPDIR) as dir:
        # Put in dir the input files
        for fn, cachedfile in task.inp.items():
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
                SavedCommand(
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
        for file, value in task.out.items():
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
    task = RTask(task_id, couts, task.env, task.inp, outputs)

    # update cached copy
    __cache_task(objcache, task)
    objcache.sadd(__DONE, task_id)  # type:ignore

    return task_id
