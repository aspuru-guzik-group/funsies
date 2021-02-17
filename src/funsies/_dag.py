"""DAG related utilities."""
from __future__ import annotations

# external
from redis import Redis
import rq
from rq.queue import Queue

# module
from ._constants import DAG_INDEX, DAG_STORE, hash_t, join, OPERATIONS
from ._graph import Artefact, get_op_options, Operation
from ._logging import logger
from ._run import run_op, RunStatus
from ._short_hash import shorten_hash


def __set_as_hashes(db: Redis[bytes], key1: str, key2: str) -> set[hash_t]:
    mem = db.sinter(key1, key2)
    out = set()
    for k in mem:
        if isinstance(k, bytes):
            out.add(hash_t(k.decode()))
        elif isinstance(k, str):
            out.add(hash_t(k))
        else:
            out.add(hash_t(str(k)))
    return out


def _dag_dependents(db: Redis[bytes], dag_of: hash_t, op_from: hash_t) -> set[hash_t]:
    """Get dependents of an op within a given DAG."""
    return __set_as_hashes(
        db, DAG_STORE + dag_of, join(OPERATIONS, op_from, "children")
    )


def delete_all_dags(db: Redis[bytes]) -> None:
    """Delete all currently stored DAGs."""
    for dag in db.smembers(DAG_INDEX):
        db.delete(DAG_STORE + dag.decode())  # type:ignore

    # Remove old index
    db.delete(DAG_INDEX)


def ancestors(db: Redis[bytes], address: hash_t) -> set[hash_t]:
    """Get all ancestors of a given hash."""
    queue = [address]
    out = set()

    while len(queue) > 0:
        curr = queue.pop()

        for el in db.smembers(join(OPERATIONS, curr, "parents")):
            if el == b"root":
                continue

            h = hash_t(el.decode())  # type:ignore
            out.add(h)
            if h not in queue:
                queue.append(h)
    return out


def descendants(db: Redis[bytes], address: hash_t) -> set[hash_t]:
    """Get all descendants of a given hash."""
    queue = [address]
    out = set()

    while len(queue) > 0:
        curr = queue.pop()
        for el in db.smembers(join(OPERATIONS, curr, "children")):
            h = hash_t(el.decode())  # type:ignore
            out.add(h)
            if h not in queue:
                queue.append(h)
    return out


def build_dag(db: Redis[bytes], address: hash_t) -> None:  # noqa:C901
    """Setup DAG required to compute the result at a specific address."""
    root = "root"
    art = None
    try:
        node = Operation.grab(db, address)
        logger.debug(f"building dag for op at {address[:6]}")
    except RuntimeError:
        # one possibility is that address is an artefact...
        try:
            art = Artefact.grab(db, address)
            logger.debug(f"artefact at {address[:6]}")
        except RuntimeError:
            raise RuntimeError(
                f"address {address} neither a valid operation nor a valid artefact."
            )

    if art is not None:
        if art.parent == root:
            # We have basically just a single artefact as the network...
            logger.debug("no dependencies to execute")
            return
        else:
            node = Operation.grab(db, art.parent)
            logger.debug(f"building dag for op at {node.hash[:6]}")

    # Ok, so now we finally know we have a node, and we want to extract the whole DAG
    # from it.
    ancs = ancestors(db, node.hash)
    logger.debug(f"{node.hash[:6]} has {len(ancs)} ancestors")

    pipe = db.pipeline()
    key = DAG_STORE + address
    pipe.sadd(key, node.hash)  # need to run this at least
    for k in ancs:
        pipe.sadd(key, k)
    pipe.sadd(DAG_INDEX, address)
    pipe.execute()


def task(
    dag_of: hash_t,
    current: hash_t,
    *,
    evaluate: bool = True,
) -> RunStatus:
    """Worker evaluation of a given step in a DAG."""
    # load database
    logger.debug(f"executing {current} on worker.")
    job = rq.get_current_job()
    db: Redis[bytes] = job.connection

    # Load operation
    op = Operation.grab(db, current)

    with logger.contextualize(op=shorten_hash(op.hash)):
        # Now we run the job
        stat = run_op(db, op, evaluate=evaluate)

        if stat > 0:
            # Success! Let's enqueue dependents.
            depen = _dag_dependents(db, dag_of, current)
            logger.info(f"enqueuing {len(depen)} dependents")

            for dependent in depen:
                # Run the dependent task
                options = get_op_options(db, dependent)
                queue = Queue(connection=db, **options.queue_args)

                logger.info(f"-> {shorten_hash(dependent)}")
                queue.enqueue_call(
                    task,
                    args=(dag_of, dependent),
                    kwargs=options.task_args,
                    **options.job_args,
                )

    return stat


def start_dag_execution(db: Redis[bytes], data_output: hash_t) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    # make dag
    build_dag(db, data_output)

    # enqueue everything starting from root
    for element in _dag_dependents(db, data_output, hash_t("root")):
        options = get_op_options(db, element)
        queue = Queue(connection=db, **options.queue_args)
        queue.enqueue_call(
            task,
            args=(data_output, element),
            kwargs=options.task_args,
            **options.job_args,
        )
