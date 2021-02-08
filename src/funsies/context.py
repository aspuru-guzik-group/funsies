"""Contextual DB usage."""
# std
from contextlib import contextmanager
from dataclasses import replace
import shutil
import subprocess
import tempfile
import time
from typing import Any, Iterator, Optional

# external
from redis import Redis
import rq
from rq.local import LocalStack

# module
from .config import Options
from .dag import delete_all_dags
from .logging import logger

# A thread local stack of connections (adapted from RQ)
_connect_stack = LocalStack()
_options_stack = LocalStack()


def cleanup_funsies(connection: Redis) -> None:
    """Clean up Redis instance of DAGs and Queues."""
    # Clean up the Redis instance of old jobs (not of job data though.)
    queues = rq.Queue.all(connection=connection)
    for queue in queues:
        queue.delete(delete_jobs=True)

    # Now we cleanup all the old dags that are lying around
    delete_all_dags(connection)


# --------------------------------------------------------------------------------
# Main DB context manager
@contextmanager
def Fun(
    connection: Optional[Redis] = None,
    defaults: Optional[Options] = None,
    cleanup: bool = False,
) -> Iterator[Redis]:
    """Context manager for redis connections."""
    if connection is None:
        connection = Redis()
        logger.warning("Opening new redis connection with default settings...")

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


def get_db(db: Optional[Redis] = None) -> Redis:
    """Get Redis instance."""
    if isinstance(db, Redis):
        # explicit redis instance
        return db
    elif db is None:
        if _connect_stack.top is not None:
            # try context instance
            out: Redis = _connect_stack.top
            return out
        elif (job := rq.get_current_job()) is not None:
            out2: Redis = job.connection
            return out2
        else:
            raise RuntimeError("No redis instance available.")
    else:
        raise TypeError(f"object {db} not of type Optional[Redis]")


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
    nworkers: int = 1, defaults: Optional[Options] = None
) -> Iterator[Redis]:
    """Make a fully managed funsies db."""
    dir = tempfile.mkdtemp()
    logger.debug(f"running redis-server in {dir}")

    # Start redis
    port = 7777  # TODO make adjustable
    url = f"redis://localhost:{port}"
    redis_server = subprocess.Popen(
        ["redis-server", "--port", f"{port}"],
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
        subprocess.Popen(["funsies", "worker", "--url", url], cwd=dir)
        for i in range(nworkers)
    ]

    try:
        logger.success(f"{nworkers} workers connected to {url}")
        with Fun(Redis.from_url(url), defaults=defaults) as db:
            yield db
    finally:
        logger.debug("terminating worker pool and server")
        time.sleep(0.1)
        # stop db
        for w in worker_pool:
            w.kill()
        redis_server.kill()
        shutil.rmtree(dir)
        logger.success("stopping managed fun")
