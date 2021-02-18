"""Configuration dictionaries for jobs."""
# std
from dataclasses import asdict, dataclass
import json
from typing import Any, Mapping, Type


# Constants
INFINITE = -1
ONE_DAY = 86400
ONE_MINUTE = 60


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
        return dict(is_async=self.distributed)

    def pack(self: "Options") -> str:
        """Pack an Options instance to a bytestring."""
        return json.dumps(asdict(self))

    @classmethod
    def unpack(cls: Type["Options"], data: str) -> "Options":
        """Unpack an Options instance from a byte string."""
        return Options(**json.loads(data))
