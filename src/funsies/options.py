"""Configuration dictionaries for jobs."""
# std
from dataclasses import dataclass
from typing import Optional

# external
from redis import Redis

# module

ONE_DAY = 86400.0
ONE_MINUTE = 60.0


@dataclass(frozen=True)
class Options:
    """Runtime options for a funsie.


    Connection options:
        - connection: Redis instance to use. Defaults to None, which takes the instance from a Fun() context.

    Options to be pass to RQ enqueue:
        - timeout: Max exec time for the job in seconds. Defaults to 24h.
        - ttl: Time that a job can be on queue before it's executed. Default to 24h.
        - result_ttl: Expiration of a rq job result. Defaults to one minute,
            as we are not currently using those values anyway.
        - failure_ttl: Expiration of rq job failure. Same as above.


    """

    # Passed to RQ directly
    timeout: float = ONE_DAY
    ttl: float = ONE_DAY
    result_ttl: float = ONE_MINUTE
    failure_ttl: float = ONE_MINUTE
