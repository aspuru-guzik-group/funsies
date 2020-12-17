"""Data transformations."""
# std
from inspect import getsource
import logging
from typing import cast, List, Sequence

# external
import cloudpickle
from msgpack import packb
from redis import Redis

# module
from .cached import pull_file, put_file, register_file
from .constants import __DONE, __IDS, __OBJECTS, __TASK_ID, _TransformerFun
from .types import FilePtr, pull, RTransformer


def register_transformer(
    cache: Redis,
    fun: _TransformerFun,
    inputs: Sequence[FilePtr],
    nout: int,
) -> RTransformer:
    """Register a transformation function into Redis to get an RTransformer."""
    # TODO: Document this
    # Make a key for the transformer. Note that the key is defined as:
    # - the source of fun().
    # - the (NOT order invariant) inputs to fun.
    # - the number of outputs to fun.

    # Using the source _won't work_ for lambdas or with different comments
    # etc. Not sure if it's a good idea... The alternative, using cloudpickle,
    # will give different result with different python and python library versions.
    invariants = {
        "fun": getsource(fun).strip(),
        "inputs": [inp.pack() for inp in inputs],
        "nout": nout,
    }
    key = packb(invariants)
    logging.debug(f"transformer invariants: \n{str(invariants)}")
    logging.debug(f"transformer key: {str(key)}")

    if cache.hexists(__IDS, key):
        logging.debug("transformer key already exists.")
        # if it does, get task id
        tmp = cache.hget(__IDS, key)
        # pull the id from the db
        assert tmp is not None
        out = pull(cache, tmp.decode(), which="RTransformer")
        if out is None:
            logging.error("Tried to extract RTask but failed! recomputing...")
        else:
            return out

    # If it doesn't exist, we make the RTransformer (this is the same code
    # basically as rtask).
    task_id = cast(str, str(cache.incrby(__TASK_ID, 1)))  # type:ignore

    # register outputs
    outputs = [register_file(cache, f"out{i+1}", comefrom=task_id) for i in range(nout)]

    # output object
    out = RTransformer(task_id, cloudpickle.dumps(fun), list(inputs), list(outputs))

    # save transformer
    # TODO catch errors
    cache.hset(__OBJECTS, task_id, out.pack())
    cache.hset(__IDS, key, task_id)

    return out


def run_rtransformer(objcache: Redis, task: RTransformer) -> str:
    """Execute a registered transformer and return its task id."""
    # Check status
    task_id = task.task_id

    if objcache.sismember(__DONE, task_id) == 1:  # type:ignore
        logging.debug("transformer is cached.")
        return task_id
    else:
        logging.debug(f"evaluating transformer {task_id}.")

    # build function
    fun = cloudpickle.loads(task.fun)

    # load inputs and outputs
    inputs = []
    for el in task.inp:
        source = pull_file(objcache, el)
        assert source is not None  # TODO:fix
        inputs.append(source)

    # Call the function
    outputs_ = fun(*inputs)
    if isinstance(outputs_, bytes):
        outputs: List[bytes] = [outputs_]
    else:
        outputs = list(outputs_)

    # race conditions?
    # Update output files
    for i, el in enumerate(task.out):
        put_file(objcache, el, outputs[i])

    # Mark as evaluated
    objcache.sadd(__DONE, task_id)  # type:ignore

    return task_id
