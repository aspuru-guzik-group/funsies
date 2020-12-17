"""Functional wrappers for commandline programs."""
# std
import logging
from typing import List

# external
from redis import Redis
import rq
from rq.job import Job

# module
from .rtask import run_rtask
from .rtransformer import run_rtransformer
from .types import FilePtr, pull, RTask, RTransformer


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
    try:
        if int(task_id) < 1:
            logging.warning(f"task_id={task_id} is pre-initialization, nothing to run.")
            return "0"
    except ValueError as e:
        logging.critical(
            f"task_id={task_id} not a valid string representation for an int."
        )
        raise e

    thing = pull(objcache, task_id)

    # dispatch based on task id
    if isinstance(thing, RTask):
        return run_rtask(objcache, thing)
    elif isinstance(thing, RTransformer):
        return run_rtransformer(objcache, thing)
    elif isinstance(thing, FilePtr):
        # TODO check that the file is the same here.
        return task_id
    else:
        logging.critical(
            f"Thing to run is {type(thing)} not a Task or Transformer, aborting."
        )
        raise RuntimeError("Run failed.")


def get_dependencies(cache: Redis, task_id: str) -> List[str]:
    """Return the dependencies of a given object as a list."""
    thing = pull(cache, task_id)
    out = []
    if thing is None:
        raise RuntimeError("{task_id} is an invalid key.")

    if isinstance(thing, FilePtr):
        if int(thing.comefrom) > 0:
            out = [thing.comefrom]
        else:
            return []

    elif isinstance(thing, RTask):
        out = [i.task_id for i in thing.inp.values()]

    elif isinstance(thing, RTransformer):
        out = [i.task_id for i in thing.inp]

    return out


def runall(queue: rq.Queue, task_id: str) -> Job:
    """Execute a job and any / all of its dependencies."""
    # TODO add task metadata.
    logging.info(f"runall : {task_id}")
    dependencies = get_dependencies(queue.connection, task_id)
    logging.info(f"n of dependencies : {len(dependencies)}")
    logging.debug(f"dependencies: {dependencies}")

    # Fetch the dependencies
    djobs = []
    for tid in dependencies:
        # why not fetch_many you ask? because while creating one job we may
        # end up creating dependencies for a different job. This is a mess in
        # general, so here we just fetch one at a time.
        try:
            djob = Job.fetch(tid, connection=queue.connection)
        except rq.exceptions.NoSuchJobError:
            # generate the dependency
            djob = runall(queue, tid)
        djobs += [djob.id]

    job = Job.create(
        run,
        args=(task_id,),
        connection=queue.connection,
        timeout="10d",
        result_ttl="10d",
        ttl="10d",
        failure_ttl="10d",
        id=task_id,
    )

    # hack around multi dependencies not yet supported,
    for i in djobs:
        queue.connection.sadd(job.dependencies_key, i)

    queue.enqueue_job(job)

    return job
