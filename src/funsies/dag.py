"""DAG related utilities."""
from __future__ import annotations

# external
from redis import Redis
from redis.client import Pipeline
import rq
from rq.queue import Queue

# module
from ._graph import get_artefact, get_op, get_op_options
from ._short_hash import shorten_hash
from .constants import DAG_INDEX, DAG_STORE, hash_t
from .logging import logger
from .run import run_op, RunStatus


def __set_as_hashes(db: Redis[bytes], key: str) -> set[hash_t]:
    mem = db.smembers(key)
    out = set()
    for k in mem:
        if isinstance(k, bytes):
            out.add(hash_t(k.decode()))
        elif isinstance(k, str):
            out.add(hash_t(k))
        else:
            out.add(hash_t(str(k)))
    return out


def __dag_append(
    db: Redis[bytes], dag_of: hash_t, op_from: hash_t, op_to: hash_t
) -> None:
    """Append to a DAG."""
    key = DAG_STORE + dag_of + "." + op_from
    db.sadd(key, op_to)
    db.sadd(DAG_STORE + dag_of + ".keys", key)


def _dag_dependents(db: Redis[bytes], dag_of: hash_t, op_from: hash_t) -> set[hash_t]:
    """Get dependents of an op."""
    key = DAG_STORE + dag_of + "." + op_from
    return __set_as_hashes(db, key)


def delete_dag(db: Redis[bytes], dag_of: hash_t) -> None:
    """Delete DAG corresponding to a given output hash."""
    which = __set_as_hashes(db, DAG_STORE + dag_of + ".keys")
    for key in which:
        _ = db.delete(key)
    # Remove from index
    db.srem(DAG_INDEX, dag_of)


def delete_all_dags(db: Redis[bytes]) -> None:
    """Delete all currently stored DAGs."""
    for dag in __set_as_hashes(db, DAG_INDEX):
        delete_dag(db, dag)


def register_dag(db: Redis[bytes], dag_of: hash_t) -> int:
    """Register a new DAG."""
    there: int = db.sadd(DAG_INDEX, dag_of)
    return there


def build_dag(db: Redis[bytes], address: hash_t) -> None:  # noqa:C901
    """Setup DAG required to compute the result at a specific address."""
    # first delete any previous dag at this address
    delete_dag(db, address)

    # register dag
    register_dag(db, address)

    root = "root"
    art = None
    try:
        node = get_op(db, address)
    except RuntimeError:
        # one possibility is that address is an artefact...
        try:
            art = get_artefact(db, address)
        except RuntimeError:
            raise RuntimeError(
                f"address {address} neither a valid operation nor a valid artefact."
            )

    if art is not None:
        if art.parent == root:
            # We have basically just a single artefact as the network...
            return
        else:
            node = get_op(db, art.parent)

    # Ok, so now we finally know we have a node, and we want to extract the whole DAG
    # from it.
    queue = [node]
    pipe: Pipeline = db.pipeline(transaction=False)
    while len(queue) != 0:
        curr = queue.pop()
        if len(curr.inp) == 0:
            # DAG has no inputs or is cached.
            __dag_append(pipe, address, hash_t("root"), curr.hash)
        else:
            only_root = True
            for el in curr.inp.values():
                art = get_artefact(db, el)
                if art.parent != root:
                    queue.append(get_op(db, art.parent))
                    __dag_append(pipe, address, art.parent, curr.hash)
                    only_root = False

            if only_root:
                # ONLY IF ALL THE PARENTS ARE ROOT DO WE ADD THIS DAG TO
                # ROOT!! This is to avoid having root-dependent steps that get
                # re-run over and over again.
                __dag_append(pipe, address, hash_t("root"), curr.hash)
    pipe.execute()


def task(
    dag_of: hash_t,
    current: hash_t,
) -> RunStatus:
    """Worker evaluation of a given step in a DAG."""
    # load database
    logger.debug(f"executing {current} on worker.")
    job = rq.get_current_job()
    db: Redis[bytes] = job.connection

    # Load operation
    op = get_op(db, current)

    with logger.contextualize(op=shorten_hash(op.hash)):
        # Now we run the job
        stat = run_op(db, op)

        if stat > 0:
            # Success! Let's enqueue dependents.
            depen = _dag_dependents(db, dag_of, current)
            logger.info(f"enqueuing {len(depen)} dependents")

            for dependent in depen:
                # Run the dependent task
                options = get_op_options(db, dependent)
                queue = Queue(connection=db, **options.queue_args)

                logger.info(f"-> {shorten_hash(dependent)}")
                queue.enqueue_call(task, args=(dag_of, dependent), **options.job_args)

    return stat


def start_dag_execution(db: Redis[bytes], data_output: hash_t) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    # make dag
    build_dag(db, data_output)

    # enqueue everything starting from root
    for element in _dag_dependents(db, data_output, hash_t("root")):
        options = get_op_options(db, element)
        queue = Queue(connection=db, **options.queue_args)
        queue.enqueue_call(task, args=(data_output, element), **options.job_args)
