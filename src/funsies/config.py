"""Configuration dictionaries for jobs."""
# std
from dataclasses import dataclass
from typing import Any, List, Mapping


# Constants
ONE_DAY = 86400.0
ONE_MINUTE = 60.0


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

    # Job options
    timeout: float = ONE_DAY
    ttl: float = ONE_DAY
    result_ttl: float = ONE_MINUTE
    failure_ttl: float = ONE_MINUTE

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
    def queue_args(self: "Options") -> Mapping[str, Any]:
        """Return a dictionary of arguments for rq.Queue."""
        return dict(is_async=self.distributed)
