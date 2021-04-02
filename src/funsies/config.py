"""Configuration dictionaries for jobs."""
# std
from dataclasses import asdict, dataclass
import json
import os
from typing import Any, Mapping, Optional, Type

# Constants
INFINITE = -1
ONE_DAY = 86400
ONE_MINUTE = 60


def get_funsies_url(url: Optional[str] = None) -> str:
    """Get the default funsies URL."""
    if url is not None:
        return url
    else:
        try:
            default = os.environ["FUNSIES_URL"]
        except KeyError:
            default = "redis://localhost:6379"
        return default


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
