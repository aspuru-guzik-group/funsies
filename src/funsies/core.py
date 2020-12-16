"""Functional wrappers for commandline programs."""
# std
import logging
import os
import pickle
import tempfile
from typing import Optional

# external
from redis import Redis
import rq

# module
from .cached import pull_file, put_file
from .command import CachedCommandOutput, run_command
from .constants import _AnyPath
from .rtask import __cache_task, RTask


# ------------------------------------------------------------------------------
# Module global variables
__TMPDIR: Optional[_AnyPath] = None


def __job_connection() -> Redis:
    """Get a connection to the current cache."""
    job = rq.get_current_job()
    connect: Redis = job.connection
    return connect


def run(task: RTask) -> int:
    """Execute a registered task and return its task id."""
    # Get cache
    objcache = __job_connection()

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
