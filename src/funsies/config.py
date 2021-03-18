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
    """Runtime options for a funsie.

    Options for the rq Job instance:
        - timeout: Max exec time for the job in seconds. Defaults to 24h.
        - ttl: Time that a job can be on queue before it's executed. Default to 24h.
        - result_ttl: Expiration of a rq job result. Defaults to one minute,
            as we are not currently using those values anyway.
        - failure_ttl: Expiration of rq job failure. Same as above.

    Options for the rq Queue:
        - distributed: Whether jobs should run async on workers. Defaults to True.
        - queue: Name of the queue this should be executed on.

    Options for funsies logic:

    """

    # Instantiation arguments
    reset: bool = False

    # Job options
    timeout: int = INFINITE
    ttl: int = ONE_DAY
    result_ttl: int = ONE_MINUTE
    failure_ttl: int = ONE_MINUTE
    evaluate: bool = True

    # Queue options
    distributed: bool = True
    queue: str = "default"

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
