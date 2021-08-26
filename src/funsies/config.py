"""Configuration dictionaries for jobs."""
from __future__ import annotations

# std
from dataclasses import asdict, dataclass
import json
import os
import sys
from typing import Any, Mapping, Optional, Type

# external
# redis
from redis import Redis

# module
from ._logging import logger
from ._storage import DiskStorage, RedisStorage, StorageEngine

# Constants
INFINITE = -1
ONE_DAY = 86400
ONE_MINUTE = 60


def _redis_connection(url: str, try_fail: bool = True) -> Redis[bytes]:
    """Open a new redis connection."""
    hn = _extract_hostname(url)
    logger.info(f"connecting to {hn}")
    db = Redis.from_url(url, decode_responses=False)
    try:
        db.ping()
    except Exception as e:
        if try_fail:
            raise e
        else:
            logger.error("could not connect to server! exiting")
            logger.error(str(e))
            sys.exit(-1)
    logger.debug("connection sucessful")
    logger.success(f"connected to {hn}")
    return db


# --------------------------------------------------------------------------
# Redis configuration
# --------------------------------------------------------------------------
def _get_redis_url(url: Optional[str] = None) -> str:
    """Get the default funsies redis URL."""
    if url is not None:
        return url
    else:
        try:
            default = os.environ["FUNSIES_JOBS"]
        except KeyError:
            try:
                default = os.environ["FUNSIES_URL"]
            except KeyError:
                default = "redis://localhost:6379"

    return default


def _get_storage_url(url: Optional[str] = None) -> Optional[str]:
    """Get the default funsies storage URL."""
    if url is not None:
        return url
    else:
        try:
            default: Optional[str] = os.environ["FUNSIES_DATA"]
        except KeyError:
            default = None
        return default


def _extract_hostname(url: str) -> str:
    """Get the hostname part of the url."""
    if "@" in url:
        hn = url.split("@")[-1]
    else:
        hn = url.split("//")[-1]
    return hn


class Server:
    """Runtime options for the funsies redis server."""

    # Connection settings
    jobs_url: str
    data_url: str

    def __init__(
        self: Server,
        jobs_url: Optional[str] = None,
        data_url: Optional[str] = None,
    ) -> None:
        """Create a new funsies server configuration."""
        # defaults to the env variable FUNSIES_URL
        self.jobs_url = _get_redis_url(jobs_url)
        data_url = _get_storage_url(data_url)
        if data_url is None:
            self.data_url = self.jobs_url
        else:
            self.data_url = data_url

    def new_connection(
        self: Server, try_fail: bool = True
    ) -> tuple[Redis[bytes], StorageEngine]:
        """Setup job server and data connections."""
        rdb = _redis_connection(self.jobs_url, try_fail)
        if self.data_url == self.jobs_url:
            store: StorageEngine = RedisStorage(rdb)

        elif self.data_url[:5] == "redis":
            logger.info(f"saving artefacts to {self.data_url}")
            rstore = _redis_connection(self.data_url, try_fail)
            store = RedisStorage(rstore)

        elif self.data_url[:7] == "file://":
            store = DiskStorage(self.data_url[7:])

        else:
            raise NotImplementedError(
                f"No storage protocol corresponding to {self.data_url}\n"
                + "did you forget to start the url with redis:// or file://?"
            )

        return rdb, store


class MockServer(Server):
    """Mock redis server using FakeRedis for testing."""

    # Connection settings
    redis_url: str
    _instance: Redis[bytes]

    def __init__(self: MockServer, redis_url: Optional[str] = None) -> None:
        """Create a new funsies server configuration."""
        if redis_url is None:
            self.redis_url = "fakeredis://mock_server"
        else:
            self.redis_url = redis_url

        # make the connection
        # external
        from fakeredis import FakeStrictRedis as Redis

        self._instance = Redis()

    def new_connection(
        self: MockServer, try_fail: bool = True
    ) -> tuple[Redis[bytes], StorageEngine]:
        """Create a new redis connection."""
        return self._instance, RedisStorage(self._instance)


# --------------------------------------------------------------------------
# Job configuration
# --------------------------------------------------------------------------
@dataclass
class Options:
    """Runtime options for an Operation.

    This is a class that basically contains all the random options that may
    need to be set when building a workflow, such as timeouts, heterogeneous
    compute etc. It should generally be instantiated using the
    `funsies.options()` function.

    """

    timeout: int = INFINITE
    """Max execution time for this operation, in seconds or -1 for an operation
    that never timeouts. Defaults to -1."""

    queue: str = "default"
    """Defines which queue this operation should be executed by. For example,
    if a complex workflow requires GPUs for certain jobs, those jobs would be
    setup with option `queue="gpu"` and workers on the nodes with available
    GPUs would be instantiated with `funsies worker gpu`. Then, only worker
    processes in the GPU queue would execute the GPU jobs."""

    distributed: bool = True
    """If False, jobs are executed by the local enqueuing process. Used to
    test workflows without having to start workers."""

    reset: bool = False
    """If `True`, this operation is `funsies.reset()` when generated."""

    evaluate: bool = True
    """If False, calling `funsies.execute()` on this job or its dependencies will fail.
    Can be used to ensure a specific branch is never executed."""

    ttl: int = ONE_DAY
    """Time to live (ttl) in queue for the operation. Defaults to 24h. Equivalent
    to the [rq keyword with the same name](https://python-rq.org/docs/). """

    result_ttl: int = ONE_MINUTE
    """Time to live (ttl) in queue for the rq result objects. Defaults to one
    minute. Equivalent to the [rq keyword with the same
    name](https://python-rq.org/docs/). (Note that this has nothing to do with
    the actual data results.) """

    failure_ttl: int = ONE_DAY
    """Time to live (ttl) in queue for the rq result objects of failing jobs.
    Defaults to one day. Equivalent to the [rq keyword with the same
    name](https://python-rq.org/docs/). (Note that this has nothing to do with
    the actual data results.) """

    # TODO: make meaningfully adjustable
    serializer: str = "rq.serializers.JSONSerializer"

    @property
    def job_args(self: "Options") -> Mapping[str, Any]:
        """Return a dictionary of arguments for rq.enqueue's job_args."""
        return dict(
            timeout=self.timeout,
            ttl=self.ttl,
            result_ttl=self.result_ttl,
            failure_ttl=self.failure_ttl,
        )

    @property
    def task_args(self: "Options") -> Mapping[str, Any]:
        """Return a dictionary of arguments for dag.task()."""
        return dict(
            evaluate=self.evaluate,
        )

    @property
    def queue_args(self: "Options") -> Mapping[str, Any]:
        """Return a dictionary of arguments for rq.Queue."""
        return dict(is_async=self.distributed, serializer=self.serializer)

    def pack(self: "Options") -> str:
        """Pack an Options instance to a bytestring."""
        return json.dumps(asdict(self))

    @classmethod
    def unpack(cls: Type["Options"], data: str) -> "Options":
        """Unpack an Options instance from a byte string."""
        return Options(**json.loads(data))
