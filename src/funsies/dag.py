"""DAG related utilities."""
# std
import logging
from typing import Optional, Set, Union

# external
from redis import Redis
import rq
from rq.queue import Queue

# module
from ._graph import Artefact, get_artefact, get_op, Operation
from .constants import DAG_STORE, hash_t
from .run import run_op, RunStatus


def __set_as_str(db: Redis, key: str) -> Set[str]:
    mem = db.smembers(key)
    out = set()
    for k in mem:
        if isinstance(k, bytes):
            out.add(k.decode())
        elif isinstance(k, str):
            out.add(k)
        else:
            out.add(str(k))
    return out


def __dag_append(db: Redis, dag_of: hash_t, op_from: hash_t, op_to: hash_t) -> None:
    """Append to a DAG."""
    key = DAG_STORE + dag_of + "." + op_from
    db.sadd(key, op_to)  # type:ignore
    db.sadd(DAG_STORE + dag_of + ".keys", key)  # type:ignore


def __dag_dependents(db: Redis, dag_of: hash_t, op_from: hash_t) -> Set[str]:
    """Get dependents of an op."""
    key = DAG_STORE + dag_of + "." + op_from
    return __set_as_str(db, key)


def delete_dag(db: Redis, dag_of: hash_t) -> None:
    """Delete DAG corresponding to a given output hash."""
    which = __set_as_str(db, DAG_STORE + dag_of + ".keys")
    for key in which:
        _ = db.delete(key)


def build_dag(db: Redis, address: hash_t) -> Optional[str]:  # noqa:C901
    """Setup DAG required to compute the result at a specific address."""
    # first delete any previous dag at this address
    delete_dag(db, address)

    # TODO add custom root for sub-DAGs
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
    while True:
        curr = queue.pop()
        for el in curr.inp.values():
            art = get_artefact(db, el)

            if art.parent != root:
                queue.append(get_op(db, art.parent))
                __dag_append(db, address, art.parent, curr.hash)
            else:
                __dag_append(db, address, "root", curr.hash)

        if len(queue) == 0:
            break
    return DAG_STORE + address


def rq_eval(dag_of: hash_t, current: hash_t) -> RunStatus:
    """Worker evaluation of a given step in a DAG."""
    # load database
    logging.debug(f"executing {current} on worker.")
    job = rq.get_current_job()
    db: Redis = job.connection

    # load current queue
    queue = Queue(name=job.origin, connection=job.connection)

    # Now we run the job
    stat = run_op(db, current)

    if stat > 0:
        # Success! Let's enqueue dependents.
        for element in __dag_dependents(db, dag_of, current):
            queue.enqueue_call(rq_eval, args=(dag_of, element))

    return stat


def execute(
    db: Redis, queue: Queue, output: Union[hash_t, Operation, Artefact]
) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    if isinstance(output, Operation) or isinstance(output, Artefact):
        dag_of = output.hash
    else:
        dag_of = output

    # make dag
    build_dag(db, dag_of)

    # enqueue everything starting from root
    for element in __dag_dependents(db, dag_of, "root"):
        queue.enqueue_call(rq_eval, args=(dag_of, element))
