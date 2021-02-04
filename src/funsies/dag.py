"""DAG related utilities."""
# std
from typing import Optional, Set

# external
from redis import Redis
import rq
from rq.queue import Queue

# module
from ._graph import get_artefact, get_op, get_op_options
from .constants import DAG_INDEX, DAG_STORE, hash_t
from .logging import logger
from .run import is_it_cached, run_op, RunStatus


def __set_as_hashes(db: Redis, key: str) -> Set[hash_t]:
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


def __dag_append(db: Redis, dag_of: hash_t, op_from: hash_t, op_to: hash_t) -> None:
    """Append to a DAG."""
    key = DAG_STORE + dag_of + "." + op_from
    db.sadd(key, op_to)  # type:ignore
    db.sadd(DAG_STORE + dag_of + ".keys", key)  # type:ignore


def __dag_dependents(db: Redis, dag_of: hash_t, op_from: hash_t) -> Set[hash_t]:
    """Get dependents of an op."""
    key = DAG_STORE + dag_of + "." + op_from
    return __set_as_hashes(db, key)


def delete_dag(db: Redis, dag_of: hash_t) -> None:
    """Delete DAG corresponding to a given output hash."""
    which = __set_as_hashes(db, DAG_STORE + dag_of + ".keys")
    for key in which:
        _ = db.delete(key)
    # Remove from index
    db.srem(DAG_INDEX, dag_of)


def delete_all_dags(db: Redis) -> None:
    """Delete all currently stored DAGs."""
    for dag in __set_as_hashes(db, DAG_INDEX):
        delete_dag(db, dag)


def register_dag(db: Redis, dag_of: hash_t) -> int:
    """Register a new DAG."""
    there: int = db.sadd(DAG_INDEX, dag_of)  # type:ignore
    return there


def build_dag(db: Redis, address: hash_t) -> Optional[str]:  # noqa:C901
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
            return DAG_STORE + address
        else:
            node = get_op(db, art.parent)

    # Ok, so now we finally know we have a node, and we want to extract the whole DAG
    # from it.
    queue = [node]
    while len(queue) != 0:
        curr = queue.pop()

        # This is a bad idea because two dags could end in different places
        # but start from the same initial point. if that's the case and we
        # preemptively remove that initial point, the other dag wont get run!

        # if is_it_cached(db, curr):
        #     # We don't need to run this because all of its outputs are cached
        #     # anyway.
        #     logger.debug(f"operation {curr.hash} is cached, keeping off dag.")
        #     continue

        # Instead we add the cached operation as descending from root, and do
        # "run" it, to ensure that it's dependents are also run, but we don't
        # include it's inputs.
        if len(curr.inp) == 0 or is_it_cached(db, curr):
            __dag_append(db, address, hash_t("root"), curr.hash)
        else:
            for el in curr.inp.values():
                art = get_artefact(db, el)

                if art.parent != root:
                    queue.append(get_op(db, art.parent))
                    __dag_append(db, address, art.parent, curr.hash)
                else:
                    __dag_append(db, address, hash_t("root"), curr.hash)

    return DAG_STORE + address


def rq_eval(
    dag_of: hash_t,
    current: hash_t,
) -> RunStatus:
    """Worker evaluation of a given step in a DAG."""
    # load database
    logger.debug(f"executing {current} on worker.")
    job = rq.get_current_job()
    db: Redis = job.connection

    # Load operation
    op = get_op(db, current)

    # Now we run the job
    stat = run_op(db, op)

    if stat > 0:
        # Success! Let's enqueue dependents.
        for element in __dag_dependents(db, dag_of, current):
            options = get_op_options(db, element)
            queue = Queue(connection=db, **options.queue_args)
            queue.enqueue_call(rq_eval, args=(dag_of, element), **options.job_args)

    return stat


def start_dag_execution(db: Redis, data_output: hash_t) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    # make dag
    build_dag(db, data_output)

    # enqueue everything starting from root
    for element in __dag_dependents(db, data_output, hash_t("root")):
        options = get_op_options(db, element)
        queue = Queue(connection=db, **options.queue_args)
        queue.enqueue_call(rq_eval, args=(data_output, element), **options.job_args)
