"""Functional wrappers for commandline programs."""
# std
from typing import Union

# external
from redis import Redis
import rq

# module
from .rtask import RTask, run_rtask
from .rtransformer import RTransformer, run_rtransformer


def __job_connection() -> Redis:
    """Get a connection to the current cache."""
    job = rq.get_current_job()
    connect: Redis = job.connection
    return connect


def run(thing: Union[RTask, RTransformer]) -> str:
    """Run a task or transformer."""
    # Get cache
    objcache = __job_connection()

    if isinstance(thing, RTask):
        return run_rtask(objcache, thing)
    elif isinstance(thing, RTransformer):
        return run_rtransformer(objcache, thing)
    else:
        raise TypeError(f"{thing} not task or transformer.")
