"""Functional wrappers for commandline programs."""
# std
import logging

# external
from redis import Redis
import rq

# module
from .rtask import run_rtask
from .rtransformer import run_rtransformer
from .types import pull, RTask, RTransformer


def __job_connection() -> Redis:
    """Get a connection to the current cache."""
    job = rq.get_current_job()
    connect: Redis = job.connection
    return connect


def run(task_id: str) -> str:
    """Run a task or transformer."""
    # Get cache
    objcache = __job_connection()
    logging.debug(f"pulling {task_id} from redis.")
    thing = pull(objcache, task_id)

    # dispatch based on task id
    if isinstance(thing, RTask):
        return run_rtask(objcache, thing)
    elif isinstance(thing, RTransformer):
        return run_rtransformer(objcache, thing)
    else:
        logging.critical(
            "Thing to run is {type(thing)} not a Task or Transformer, aborting."
        )
        raise RuntimeError("Run failed.")


# def get_dependents(thing: Union[RTask, RTransformer]) -> str:
#     """Run a task or transformer."""
#     # Get cache
#     objcache = __job_connection()

#     if isinstance(thing, RTask):
#         return run_rtask(objcache, thing)
#     elif isinstance(thing, RTransformer):
#         return run_rtransformer(objcache, thing)
#     else:
#         raise TypeError(f"{thing} not task or transformer.")
