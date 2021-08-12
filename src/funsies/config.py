"""Configuration dictionaries for jobs."""
from __future__ import annotations

# std
from dataclasses import asdict, dataclass
from io import BytesIO
import json
import os
import sys
import traceback
from typing import Any, Mapping, Optional, Type, Union

# external
# redis
from redis import Redis

# module
from ._constants import _AnyPath, ARTEFACTS, hash_t, join
from ._logging import logger
from ._storage import StorageEngine
from .errors import Error, ErrorKind, Result

# Constants
INFINITE = -1
ONE_DAY = 86400
ONE_MINUTE = 60


# --------------------------------------------------------------------------
# Redis configuration
# --------------------------------------------------------------------------
def _get_redis_url(url: Optional[str] = None) -> str:
    """Get the default funsies redis URL."""
    if url is not None:
        return url
    else:
        try:
            default = os.environ["FUNSIES_URL"]
        except KeyError:
            default = "redis://localhost:6379"
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
    redis_url: str

    def __init__(
        self: Server,
        redis_url: Optional[str] = None,
        mock: bool = False,
    ) -> None:
        """Create a new funsies server configuration."""
        # defaults to the env variable FUNSIES_URL
        self.redis_url = _get_redis_url(redis_url)

    def new_connection(self: Server, try_fail: bool = True) -> Redis[bytes]:
        """Open a new redis connection."""
        hn = _extract_hostname(self.redis_url)
        logger.info(f"connecting to {hn}")
        db = Redis.from_url(self.redis_url, decode_responses=False)
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
        logger.info(f"connected to {hn}")
        return db


class MockServer(Server):
    """Mock redis server using FakeRedis for testing."""

    # Connection settings
    redis_url: str
    _instance: Redis[bytes]

    def __init__(self: MockServer, redis_url: Optional[str] = None):
        """Create a new funsies server configuration."""
        if redis_url is None:
            self.redis_url = "fakeredis://mock_server"
        else:
            self.redis_url = redis_url

        # make the connection
        # external
        from fakeredis import FakeStrictRedis as Redis

        self._instance = Redis()  # type:ignore

    def new_connection(self: MockServer, try_fail: bool = True) -> Redis[bytes]:
        """Create a new redis connection."""
        return self._instance  # type:ignore


# --------------------------------------------------------------------------
# Storage engine configuration
# --------------------------------------------------------------------------

# Data storage on disk
class DiskStorage(StorageEngine):
    """Storage engine that puts artefact data on disk."""

    def __init__(self, path: _AnyPath):
        self.path = path


_DEFAULT_BLOCK_SIZE = 30 * 1024 * 1024  # 30 MB


class RedisStorage(StorageEngine):
    """Storage engine that puts artefact data in Redis."""

    def __init__(
        self, instance_or_url: Union[Redis[bytes], str], block_size=_DEFAULT_BLOCK_SIZE
    ):
        if isinstance(instance_or_url, str):
            self.instance = Server(instance_or_url).new_connection()
        else:
            self.instance = instance_or_url
        self.block_size = block_size

    def get_key(self, hash: hash_t) -> str:
        key = join(ARTEFACTS, hash, "data")
        return key

    def take(self, key: str) -> Result[BytesIO]:
        # TODO: stream it?
        # TODO: check for truncation
        out = self.instance.lrange(key, 0, -1)
        if len(out) == 0:
            return Error(
                ErrorKind.DataNotFound,
                details=f"No data at address {key} in redis instance.",
            )
        else:
            return BytesIO(b"".join(out))

    def put(self, key: str, data: BytesIO) -> Optional[Error]:
        first = True  # this workaround is to make sure that writing no data is ok.
        pipe = self.instance.pipeline(transaction=True)
        pipe.delete(key)
        while True:
            try:
                dat = data.read(self.block_size)
            except Exception:
                tb_exc = traceback.format_exc()
                return Error(
                    kind=ErrorKind.ExceptionRaised,
                    details=tb_exc,
                )

            if len(dat) == 0 and not first:
                break
            else:
                pipe.rpush(key, dat)

            first = False
        pipe.execute()
        return None


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
