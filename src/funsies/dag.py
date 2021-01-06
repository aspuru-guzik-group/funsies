"""DAG related utilities."""
# std
import logging
from typing import Optional

# external
from redis import Redis
import rq
from rq.queue import Queue

# module
from ._graph import get_artefact, get_op, get_status, ArtefactStatus
from .constants import DAG_STORE, hash_t
from .run import run_op


def __dag_append(db: Redis, dag_of: hash_t, op_from: hash_t, op_to: hash_t) -> None:
    """Append to a DAG."""
    key = DAG_STORE + dag_of + "." + op_from
    db.sadd(key, op_to)  # type:ignore
    db.sadd(DAG_STORE + dag_of + ".keys", key)  # type:ignore


def delete_dag(db: Redis, dag_of: hash_t) -> None:
    """Delete DAG corresponding to a given output hash."""
    which = db.smembers(DAG_STORE + dag_of + ".keys")  # type:ignore
    for key in which:
        val = db.delete(key.decode())


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


# def rq_sub(current, network: nx.DiGraph):
#     job = rq.get_current_job()
#     q = Queue(name=job.origin, connection=job.connection)
#     for element in network.successors(current):
#         q.enqueue_call(rq_eval, args=(element, network))


# def rq_eval(current, network):
#     # load database
#     print(current)
#     job = rq.get_current_job()
#     db: Redis = job.connection

#     # pull op
#     node = get_op(db, current)

#     # Check if the current job is already done

#     # We do this by checking whether all of it's outputs are already saved.
#     # This ensures that there is no mismatch between artefact statuses and the
#     # status of generating functions.
#     for val in node.out.values():
#         stat = get_status(db, val)
#         if stat is not ArtefactStatus.done:
#             break
#     else:
#         # All outputs are ok. We exit this run.
#         logging.info(f"{node.hash} all outputs are already processed, skipping ")
#         rq_sub(current, network)
#         return True

#     # Then we check if all the inputs are ready to be processed.
#     for val in node.inp.values():
#         stat = get_status(db, val)
#         if stat is not ArtefactStatus.done:
#             # One of the inputs is not processed yet, we return.
#             logging.info(f"{node.hash} not all inputs are ready, skipping")
#             return True

#     # Now we run the job
#     logging.info(f"{node.hash} -> running")
#     stat = run_op(db, current)
#     # We push dependents on the queue
#     rq_sub(current, network)
#     return stat
