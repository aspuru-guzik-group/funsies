"""Contextual DB usage."""
from __future__ import annotations

# std
from contextlib import contextmanager
from dataclasses import replace
import shutil
import subprocess
import tempfile
import time
from typing import Any, Iterator, Optional, Sequence, Tuple, Union

# external
from redis import Redis
import rq
from rq import command, Worker
from rq.local import LocalStack

# module
from ._constants import _AnyPath, hash_t, join, OPERATIONS
from ._logging import logger
from ._storage import StorageEngine
from .config import Options, Server

# A thread local stack of connections (adapted from RQ)
_options_stack = LocalStack()
_storage_stack = LocalStack()


def cleanup_funsies(db: Redis[bytes]) -> None:
    """Clean up Redis instance of DAGs, Queues and clear workers."""
    queues = rq.Queue.all(connection=db)
    for queue in queues:
        queue.delete(delete_jobs=True)

    # Reset operation status
    ops = join(OPERATIONS, hash_t("*"), "owner")
    keys = db.keys(ops)
    if len(keys):
        logger.info(f"clearing {len(keys)} unfinished ops")
        for k in keys:
            db.delete(k)


def shutdown_workers(db: Redis[bytes], force: bool) -> None:
    """Shutdown funsies workers."""
    workers = Worker.all(db)
    logger.info(f"shutting down {len(workers)} workers")
    for worker in workers:
        command.send_shutdown_command(db, worker.name)  # Tells worker to shutdown
        if force:
            command.send_kill_horse_command(db, worker.name)


# --------------------------------------------------------------------------------


def get_redis(db: Optional[Redis[bytes]] = None) -> Redis[bytes]:
    """Get Redis instance."""
    if db is None:
        myjob = rq.get_current_job()
        connect: Optional[Redis[bytes]] = rq.connections.get_current_connection()
        if connect is not None:
            return connect
        elif myjob is not None:
            out2: Redis[bytes] = myjob.connection
            return out2
        else:
            raise RuntimeError("No redis instance available.")
    else:
        return db


def get_storage(store: Optional[StorageEngine] = None) -> StorageEngine:
    """Get current storage method."""
    if store is not None:
        return store
    else:
        if _storage_stack.top is not None:
            out: StorageEngine = _storage_stack.top
            return out
        else:
            raise RuntimeError("No Storage instance available.")


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


def options(**kwargs: Any) -> Options:
    """Set operation and workflow options.

    This function sets specific configuration options for an operation or a
    workflow that do not change hash values or cause re-execution, but do
    change runtime behaviour, such as job timeouts, queue selection, etc.
    Available options and their names are described in the entry for
    `config.Options`.

    This function wraps the `config.Options` with layering of default values.
    The value of each attribute of `config.Options` is set based on:

    1. The values set by the `**kwargs` dictionary.

    2. The values set in the `**kwargs` dictionary of the enclosing
    `funsies.Fun()` context, if `default=options(**kwargs)` is passed to
    `funsies.Fun()`.

    3. The default values in `config.Options`.

    This allows layering of fairly complex runtime behaviour.

    """
    if _options_stack.top is None:
        return Options(**kwargs)
    else:
        defaults: Options = _options_stack.top
        return replace(defaults, **kwargs)


Connection = Union[
    Redis,
    StorageEngine,
    Tuple[Redis, StorageEngine],
    Tuple[StorageEngine, Redis],
    None,
]
"""A convenience type for information about a funsies server setup."""


def get_connection(inp: Connection = None) -> tuple[Redis[bytes], StorageEngine]:
    """Destructure a Connection."""
    db: Optional[Redis[bytes]] = None
    st = None
    if isinstance(inp, tuple):
        if isinstance(inp[0], Redis):
            db = inp[0]  # type:ignore
            assert isinstance(inp[1], StorageEngine)
            st = inp[1]
        elif isinstance(inp[0], StorageEngine):
            assert isinstance(inp[1], Redis)
            db = inp[1]  # type:ignore
            st = inp[0]
        else:
            raise RuntimeError(f"Wrong input for get_connection, {inp}")
    else:
        if isinstance(inp, Redis):
            db = inp  # type:ignore
        elif isinstance(inp, StorageEngine):
            st = inp
        elif inp is None:
            pass
        else:
            raise RuntimeError(f"Wrong input for get_connection, {inp}")

    return get_redis(db), get_storage(st)


# ---------------------------------------------------------------------------------
# Contexts
@contextmanager
def Fun(
    server: Optional[Server] = None,
    defaults: Optional[Options] = None,
    cleanup: bool = False,
) -> Iterator[Redis[bytes]]:
    """Context manager for redis connections."""
    if server is None:
        logger.warning("Opening new redis connection with default settings...")
        server = Server()

    if defaults is None:
        defaults = Options()
    _options_stack.push(defaults)

    db, store = server.new_connection()
    _storage_stack.push(store)

    if cleanup:
        cleanup_funsies(db)

    # also push on rq
    rq.connections.push_connection(db)

    try:
        yield rq.connections.get_current_connection()
    finally:
        popped = rq.connections.pop_connection()
        assert popped == db, (
            "Unexpected Redis connection was popped off the stack. "
            "Check your Redis connection setup."
        )
        _ = _options_stack.pop()


@contextmanager
def ManagedFun(
    nworkers: int = 1,
    worker_args: Optional[Sequence[str]] = None,
    redis_args: Optional[Sequence[str]] = None,
    defaults: Optional[Options] = None,
    directory: Optional[_AnyPath] = None,
    data_url: Optional[str] = None,
    pw: Optional[str] = None,
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

    if pw is None:
        # std
        import secrets

        pw = secrets.token_hex(12)

    # Start redis
    port = 16379
    url = f"redis://:{pw}@localhost:{port}"
    cmdline = ["redis-server"] + rargs + ["--port", f"{port}", "--requirepass" f" {pw}"]
    if data_url is None:
        data_url = url
    server = Server(jobs_url=url, data_url=data_url)

    redis_server = subprocess.Popen(
        cmdline,
        cwd=dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(0.1)
    stat = redis_server.poll()
    if stat is not None:
        assert redis_server.stderr is not None
        stdout = redis_server.stderr.read().decode()
        raise RuntimeError(f"Redis server failed to start, errcode={stat}\n{stdout}")

    logger.debug(f"redis running at {url}")

    # spawn workers
    logger.debug(f"spawning {nworkers} funsies workers")
    worker_pool = [
        subprocess.Popen(
            ["funsies", "--jobs", url, "--data", data_url, "worker"] + wargs, cwd=dir
        )
        for i in range(nworkers)
    ]

    try:
        logger.success(f"{nworkers} workers connected to {url.split('@')[-1]}")
        with Fun(server=server, defaults=defaults) as db:
            db.set("bla", 3)
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
