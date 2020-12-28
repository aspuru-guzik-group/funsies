"""Cached tasks in redis."""
# std
from dataclasses import asdict
import logging
import os
import tempfile
from typing import Dict, List, Optional, Sequence

# external
from msgpack import packb
from redis import Redis

# module
from .cached import pull_file, put_file, register_file
from .command import run_command
from .constants import __DONE, __OBJECTS, _AnyPath
from .types import Command, FilePtr, get_hash_id, pull, RTask, SavedCommand

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
    invariants = b""
    for c in commands:
        invariants += packb(asdict(c))
    for k, v in sorted(inputs.items()):
        invariants += k.encode() + v.pack()
    for o in outputs:
        invariants += o.encode()
    if env is not None:
        for k2, v2 in sorted(env.items()):
            invariants += k2.encode() + v2.encode()

    task_id = get_hash_id(invariants)

    if cache.hexists(__OBJECTS, task_id):
        logging.info("task key already exists.")
        out = pull(cache, task_id, which="RTask")
        if out is None:
            logging.error("Tried to extract RTask but failed! recomputing...")
        else:
            return out

    # build cmd outputs
    couts = []
    for cmd_id, cmd in enumerate(commands):
        couts += [
            SavedCommand(
                -1,
                cmd.executable,
                cmd.args,
                register_file(
                    cache, f"task{task_id}.cmd{cmd_id}.stdout", comefrom=task_id
                ),
                register_file(
                    cache, f"task{task_id}.cmd{cmd_id}.stderr", comefrom=task_id
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
    cache.hset(__OBJECTS, out.task_id, out.pack())

    return out


# ------------------------------------------------------------------------------
# Logging
def __log_task(task: RTask) -> None:
    """Log a task."""
    info = "TASK\n"
    for i, c in enumerate(task.commands):
        info += "cmd {} ${}\n".format(i, c)

    # detailed trace of task
    debug = f"\nenvironment variables: {task.env}\ninput files\n"
    for key, val in task.inp.items():
        debug += "  {} -> {}\n".format(key, val)
    debug += "output files\n"
    for key, val in task.out.items():
        debug += "  {} -> {}\n".format(key, val)
    logging.info(info.rstrip())
    logging.debug(debug)


# runner
def run_rtask(objcache: Redis, task: RTask, no_exec: bool = False) -> str:
    """Execute a registered task and return its task id."""
    # logging
    __log_task(task)
    # Check status
    task_id = task.task_id

    if objcache.sismember(__DONE, task_id) == 1:  # type:ignore
        logging.info("task is cached.")
        return task_id
    else:
        logging.info(f"evaluating task {task_id}.")

    if no_exec:
        logging.critical("no_exec flag is specifically set but task needs evaluation!")
        raise RuntimeError("execution denied.")

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
    objcache.hset(__OBJECTS, task.task_id, task.pack())
    objcache.sadd(__DONE, task_id)  # type:ignore

    return task_id
