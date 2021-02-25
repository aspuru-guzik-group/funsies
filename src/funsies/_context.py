"""Contextual DB usage."""
from __future__ import annotations

# std
from contextlib import contextmanager
from dataclasses import replace
import shutil
import subprocess
import tempfile
import time
from typing import Any, Iterator, Optional, Sequence

# external
from redis import Redis
import rq
from rq.local import LocalStack

# module
from ._constants import _AnyPath
from ._logging import logger
from .config import Options

# A thread local stack of connections (adapted from RQ)
_connect_stack = LocalStack()
_options_stack = LocalStack()


def cleanup_funsies(connection: Redis[bytes]) -> None:
    """Clean up Redis instance of DAGs and Queues."""
    # Clean up the Redis instance of old jobs (not of job data though.)
    queues = rq.Queue.all(connection=connection)
    for queue in queues:
        queue.delete(delete_jobs=True)


# --------------------------------------------------------------------------------
# Main DB context manager
@contextmanager
def Fun(
    connection: Optional[Redis[bytes]] = None,
    defaults: Optional[Options] = None,
    cleanup: bool = False,
) -> Iterator[Redis[bytes]]:
    """Context manager for redis connections."""
    if connection is None:
        logger.warning("Opening new redis connection with default settings...")
        connection = Redis(decode_responses=False)

    if defaults is None:
        defaults = Options()

    if cleanup:
        cleanup_funsies(connection)

    _connect_stack.push(connection)
    _options_stack.push(defaults)

    # also push on rq
    # TODO maybe just use the RQ version of this?
    rq.connections.push_connection(connection)
    try:
        yield _connect_stack.top
    finally:
        popped = _connect_stack.pop()
        assert popped == connection, (
            "Unexpected Redis connection was popped off the stack. "
            "Check your Redis connection setup."
        )
        rq.connections.pop_connection()
        _ = _options_stack.pop()


def get_db(db: Optional[Redis[bytes]] = None) -> Redis[bytes]:
    """Get Redis instance."""
    if db is None:
        myjob = rq.get_current_job()
        if _connect_stack.top is not None:
            # try context instance
            out: Redis[bytes] = _connect_stack.top
            return out
        elif myjob is not None:
            out2: Redis[bytes] = myjob.connection
            return out2
        else:
            raise RuntimeError("No redis instance available.")
    else:
        return db


def get_options(opt: Optional[Options] = None) -> Options:
    """Get Options instance."""
    if opt is not None:
        return opt
    else:
        if _options_stack.top is not None:
            out: Options = _options_stack.top
            return out
        else:
            raise RuntimeError("No Options instance available.")


# TODO: Document better
def options(**kwargs: Any) -> Options:
    """Set runtime options."""
    if _options_stack.top is None:
        return Options(**kwargs)
    else:
        defaults: Options = _options_stack.top
        return replace(defaults, **kwargs)


# ---------------------------------------------------------------------------------
# Utility contexts
@contextmanager
def ManagedFun(
    nworkers: int = 1,
    worker_args: Optional[Sequence[str]] = None,
    redis_args: Optional[Sequence[str]] = None,
    defaults: Optional[Options] = None,
    directory: Optional[_AnyPath] = None,
) -> Iterator[Redis[bytes]]:
    """Make a fully managed funsies db."""
    if directory is None:
        dir = tempfile.mkdtemp()
    else:
        dir = str(directory)

    logger.debug(f"running redis-server in {dir}")

    if worker_args is not None:
        wargs = [el for el in worker_args]
    else:
        wargs = []

    if redis_args is not None:
        rargs = [el for el in redis_args]
    else:
        rargs = []

    # Start redis
    port = 16379
    url = f"redis://localhost:{port}"
    cmdline = ["redis-server"] + rargs + ["--port", f"{port}"]

    redis_server = subprocess.Popen(
        cmdline,
        cwd=dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # TODO:CHECK that server started successfully
    time.sleep(0.1)
    logger.debug(f"redis running at {url}")

    # spawn workers
    logger.debug(f"spawning {nworkers} funsies workers")
    worker_pool = [
        subprocess.Popen(["funsies", "--url", url, "worker"] + wargs, cwd=dir)
        for i in range(nworkers)
    ]

    try:
        logger.success(f"{nworkers} workers connected to {url}")
        with Fun(Redis.from_url(url), defaults=defaults) as db:
            yield db
    finally:
        logger.debug("terminating worker pool and server")
        for w in worker_pool:
            w.kill()
            w.wait()
        # stop db
        db.shutdown()  # type:ignore
        db.connection_pool.disconnect()
        redis_server.wait()
        if directory is None:
            shutil.rmtree(dir)
        logger.success("stopping managed fun")
