"""DAG related utilities."""
from __future__ import annotations

# std
import time
from typing import Any, Optional

# external
from redis import Redis
import rq
from rq.queue import Queue
from rq.worker import Worker

# module
from ._constants import DAG_INDEX, DAG_OPERATIONS, DAG_STATUS, hash_t, join, OPERATIONS
from ._context import get_storage
from ._graph import Artefact, get_op_options, Operation, resolve_link
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
        db, join(DAG_OPERATIONS, dag_of), join(OPERATIONS, op_from, "children")
    )


def delete_all_dags(db: Redis[bytes]) -> None:
    """Delete all currently stored DAGs."""
    for dag in db.smembers(DAG_INDEX):
        db.delete(join(DAG_OPERATIONS, dag.decode()))  # type:ignore
        db.delete(join(DAG_STATUS, dag.decode()))  # type:ignore
    # Remove old index
    db.delete(DAG_INDEX)


def ancestors(
    db: Redis[bytes], *addresses: hash_t, include_subdags: bool = False
) -> set[hash_t]:
    """Get all ancestors of a given hash."""
    queue = list(addresses)
    out = set()

    while len(queue) > 0:
        curr = queue.pop()
        parents = db.smembers(join(OPERATIONS, curr, "parents"))

        if include_subdags:
            # special case for subdag operations
            parents = parents.union(
                db.smembers(join(OPERATIONS, curr, "parents.subdag"))
            )

        for el in parents:
            if el == b"root":
                continue

            h = hash_t(el.decode())
            out.add(h)
            if h not in queue:
                queue.append(h)
    return out


def descendants(db: Redis[bytes], *addresses: hash_t) -> set[hash_t]:
    """Get all descendants of a given hash."""
    queue = list(addresses)
    out = set()

    while len(queue) > 0:
        curr = queue.pop()
        for el in db.smembers(join(OPERATIONS, curr, "children")):
            h = hash_t(el.decode())
            out.add(h)
            if h not in queue:
                queue.append(h)
    return out


def get_nearest_operation(
    db: Redis[bytes], address: hash_t, subdag: Optional[str] = None
) -> Optional[Operation]:
    """Return the operation at address or the operation generating address."""
    root = "root"
    art = None
    try:
        node = Operation.grab(db, address)
        return node
    except RuntimeError:
        # one possibility is that address is an artefact...
        try:
            art = Artefact[Any].grab(db, address)
        except RuntimeError:
            raise RuntimeError(
                f"address {address} neither a valid operation nor a valid artefact."
            )

    if art.parent == root:
        # We have basically just a single artefact as the network...
        return None
    else:
        node = Operation.grab(db, art.parent)
        return node


def build_dag(
    db: Redis[bytes], address: hash_t, subdag: Optional[str] = None
) -> None:  # noqa:C901
    """Setup DAG required to compute the result at a specific address."""
    node = get_nearest_operation(db, address)
    if node is None:
        return

    # Ok, so now we finally know we have a node, and we want to extract the whole DAG
    # from it.
    ancs = ancestors(db, node.hash)
    logger.debug(f"{node.hash[:6]} has {len(ancs)} ancestors")

    if subdag is None:
        dag_of = address
    else:
        dag_of = hash_t(f"{subdag}/{address}")

    table = join(DAG_STATUS, dag_of)
    ops = join(DAG_OPERATIONS, dag_of)

    # Initialize the dependencies count for each DAG operation.
    pipe = db.pipeline(transaction=True)
    pipe.delete(table)  # get rid of previous status data
    for address in ancs.union([node.hash]):
        ndepen = db.scard(join(OPERATIONS, address, "parents"))
        pipe.hset(table, address, ndepen)
        pipe.sadd(ops, address)

    pipe.sadd(DAG_INDEX, dag_of)
    pipe.execute()


def enqueue_dependents(
    dag_of: hash_t,
    current: hash_t,
) -> None:
    """Enqueue dependents."""
    job = rq.get_current_job()
    db: Redis[bytes] = job.connection
    depen = _dag_dependents(db, dag_of, current)
    logger.info(f"has {len(depen)} dependents")

    dagtable = join(DAG_STATUS, dag_of)
    for dependent in depen:
        # First, atomically update dependencies status
        ndepen = db.hincrby(dagtable, dependent, -1)

        if ndepen == 0:
            # Operation is ready to be executed.
            options = get_op_options(db, dependent)
            queue = Queue(options.queue, connection=db, **options.queue_args)

            logger.info(f"-> {shorten_hash(dependent)}")
            queue.enqueue_call(
                "funsies._dag.task",
                args=(dag_of, dependent),
                kwargs=options.task_args,
                **options.job_args,
            )

    # We may want to execute a subdag dependent
    components = dag_of.split("/")
    if len(components) > 1:
        evaluating = components[-1]
        from_op = components[-2]
        in_parent_dag = components[:-2]
        if current == evaluating:
            logger.info("done evaluating subdag")
            logger.info(f"enqueuing dependents of {shorten_hash(hash_t(from_op))}")
            logger.info(
                "within dag of"
                + f' {"/".join([shorten_hash(hash_t(el)) for el in in_parent_dag])}'
            )
            enqueue_dependents(hash_t("/".join(in_parent_dag)), hash_t(from_op))


def acquire_task(db: Redis[bytes], op_hash: hash_t, worker_name: Optional[str]) -> bool:
    """Check if someone else is currently executing this job."""
    if worker_name is None:
        # running in non-distributed mode
        return True

    owner_key = join(OPERATIONS, op_hash, "owner")
    response = db.setnx(owner_key, worker_name)
    if response:
        return True
    else:
        key = db.get(owner_key)
        if key is None:
            # the other worker just just drop off like right now
            # to avoid a race condition, will wait till later.
            logger.info("issue acquiring lock, will try again later")
            return False

        holder = key.decode()
        logger.info(f"job currently held by {holder}")
        if holder == worker_name:
            logger.error("other worker is myself! HOW!?")
            return True

        # grab other holder
        worker = Worker.find_by_key(
            Worker.redis_worker_namespace_prefix + holder, connection=db
        )
        if worker is None:
            # holder is gooooonnnneee. let's take over.
            db.set(owner_key, worker_name)
            logger.warning("other worker is gone, I'm taking over")
            return True
        else:
            if worker.state == "busy":
                # get other worker's job
                ojob = worker.get_current_job()
                if op_hash in ojob.description:
                    logger.info("will try again later")
                    return False
                else:
                    db.set(owner_key, worker_name)
                    logger.error("other worker has moved on, I'm taking over")
                    return True
            else:
                db.set(owner_key, worker_name)
                logger.error("other worker is not working, I'm taking over")
                return True


def task(
    dag_of: hash_t,
    current: hash_t,
    *,
    evaluate: bool = True,
) -> RunStatus:
    """Worker evaluation of a given step in a DAG."""
    # load database
    job = rq.get_current_job()
    db: Redis[bytes] = job.connection
    worker_name: Optional[str] = job.worker_name
    logger.debug(f"attempting {current} on {worker_name}.")

    # TODO: Fix
    store = get_storage(None)

    # Run job
    with logger.contextualize(op=shorten_hash(current)):
        # Start by checking if another worker is currently executing this operation
        acquired = acquire_task(db, current, worker_name)
        if not acquired:
            # Do job later
            time.sleep(0.5)  # delay so as to not hit the db too often
            options = get_op_options(db, current)
            queue = Queue(name=options.queue, connection=db, **options.queue_args)
            queue.enqueue_call(
                "funsies._dag.task",
                args=(dag_of, current),
                kwargs=options.task_args,
                at_front=False,
                **options.job_args,
            )
            stat = RunStatus.delayed
        else:
            # Run operation
            op = Operation.grab(db, current)
            stat = run_op(db, store, op, evaluate=evaluate)

            if stat == RunStatus.subdag_ready:
                # We have created a subdag
                for value in op.out.values():
                    ln = resolve_link(db, value)
                    art = Artefact[Any].grab(db, ln)
                    logger.info(f"starting subdag -> {shorten_hash(art.parent)}")
                    start_dag_execution(db, art.parent, subdag=f"{dag_of}/{current}")

            if stat > 0:
                # Success! Let's (possibly) enqueue dependents.
                enqueue_dependents(dag_of, current)

    # reset lock
    db.delete(join(OPERATIONS, current, "owner"))
    return stat


def start_dag_execution(
    db: Redis[bytes], data_output: hash_t, subdag: Optional[str] = None
) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    # make dag
    build_dag(db, data_output, subdag)

    if subdag is not None:
        dag_of = hash_t(f"{subdag}/{data_output}")
    else:
        dag_of = data_output

    # enqueue everything starting from root
    for element in _dag_dependents(db, dag_of, hash_t("root")):
        options = get_op_options(db, element)
        queue = Queue(name=options.queue, connection=db, **options.queue_args)
        queue.enqueue_call(
            "funsies._dag.task",
            args=(dag_of, element),
            kwargs=options.task_args,
            **options.job_args,
        )
