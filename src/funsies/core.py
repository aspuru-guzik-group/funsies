"""Functional wrappers for commandline programs."""
# std
import logging
from typing import Any, List, Mapping, Optional

# external
from redis import Redis
import rq
from rq.job import Job

# module
from .rtask import run_rtask
from .rtransformer import run_rtransformer
from .types import FilePtr, pull, RTask, RTransformer


def run(objcache: Redis, task_id: str, no_exec: bool = False) -> bool:
    """Run a task or transformer."""
    logging.debug(f"pulling {task_id} from redis.")
    thing = pull(objcache, task_id)

    # dispatch based on task id
    if isinstance(thing, RTask):
        return run_rtask(objcache, thing, no_exec)
    elif isinstance(thing, RTransformer):
        return run_rtransformer(objcache, thing, no_exec)
    elif isinstance(thing, FilePtr):
        # TODO check that the file is the same here.
        return True
    else:
        logging.critical(
            f"Thing to run is {type(thing)} not a Task, Transformer or FilePtr, aborting."
        )
        return False


def run_rq(task_id: str, no_exec: bool = False) -> bool:
    """Run a task on the Redis Queue."""
    job = rq.get_current_job()
    cache: Redis = job.connection
    return run(cache, task_id, no_exec)


def get_dependencies(cache: Redis, task_id: str) -> List[str]:
    """Return the dependencies of a given object as a list."""
    thing = pull(cache, task_id)
    out = []
    if thing is None:
        raise RuntimeError("{task_id} is an invalid key.")

    if isinstance(thing, FilePtr):
        if len(thing.comefrom) > 0:
            out = [thing.comefrom]
        else:
            return []

    elif isinstance(thing, RTask):
        out = [i.task_id for i in thing.inp.values()]

    elif isinstance(thing, RTransformer):
        out = [i.task_id for i in thing.inp]

    return out


def runall(
    queue: rq.Queue,
    task_id: str,
    no_exec: bool = False,
    job_params: Optional[Mapping[str, Any]] = None,
) -> Job:
    """Execute a job and any / all of its dependencies."""
    # TODO add task metadata.
    # TODO readonly flag.
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
            djob = runall(queue, tid, no_exec=no_exec, job_params=job_params)
        djobs += [djob.id]

    p = dict(
        args=(task_id,),
        kwargs=dict(no_exec=no_exec),
        connection=queue.connection,
        id=task_id,
    )
    if job_params is not None:
        p.update(job_params)

    job = Job.create(run_rq, **p)

    # hack around multi dependencies not yet supported,
    for i in djobs:
        queue.connection.sadd(job.dependencies_key, i)

    queue.enqueue_job(job)

    return job
