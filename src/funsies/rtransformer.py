"""Data transformations."""
# std
from dataclasses import asdict, dataclass
import logging
from typing import cast, List, Optional, Sequence, Type

# external
import cloudpickle
from msgpack import packb, unpackb
from redis import Redis

# module
from .cached import FilePtr, pull_file, put_file, register_file
from .constants import __IDS, __OBJECTS, __DONE, __TASK_ID, _TransformerFun


@dataclass(frozen=True)
class RTransformer:
    """Holds a registered transformer."""

    # task info
    task_id: str

    # The transformation function
    fun: bytes

    # input & output files
    inputs: List[FilePtr]
    outputs: List[FilePtr]

    def pack(self: "RTransformer") -> bytes:
        """Pack a transformer."""
        d = asdict(self)
        return packb(d)

    @classmethod
    def unpack(cls: Type["RTransformer"], inp: bytes) -> "RTransformer":
        """Build a transformer from packed form."""
        d = unpackb(inp)

        return RTransformer(
            task_id=d["task_id"],
            fun=d["fun"],
            inputs=[FilePtr(**v) for v in d["inputs"]],
            outputs=[FilePtr(**v) for v in d["outputs"]],
        )


def pull_transformer(db: Redis, task_id: str) -> Optional[RTransformer]:
    """Pull a TaskOutput from redis using its task_id."""
    val = db.hget(__OBJECTS, task_id)
    if val is None:
        return None
    else:
        return RTransformer.unpack(val)


# ------------------------------------------------------------------------------
# Register transformer on db
def register_transformer(
    cache: Redis,
    fun: _TransformerFun,
    inputs: Sequence[FilePtr],
    nout: int,
) -> RTransformer:
    """Register a transformation function into Redis to get an RTransformer."""
    # load function
    fun_bytes = cloudpickle.dumps(fun)

    # make a key for the transformer
    key = packb(
        {
            "fun": fun_bytes,
            "inputs": [asdict(inp) for inp in inputs],
            "nout": nout,
        }
    )

    if cache.hexists(__IDS, key):
        logging.debug("transformer key already exists.")
        # if it does, get task id
        tmp = cache.hget(__IDS, key)
        # pull the id from the db
        assert tmp is not None
        out = pull_transformer(cache, tmp.decode())
        assert out is not None
        # done
        return out

    # If it doesn't exist, we make the RTransformer (this is the same code
    # basically as rtask).
    task_id = cast(Optional[str], cache.incrby(__TASK_ID, 1))  # type:ignore
    assert task_id is not None  # TODO fix

    # register outputs
    outputs = [register_file(cache, f"out{i+1}") for i in range(nout)]

    # output object
    out = RTransformer(task_id, fun_bytes, list(inputs), list(outputs))

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
    for el in task.inputs:
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
    for i, el in enumerate(task.outputs):
        put_file(objcache, el, outputs[i])

    # Mark as evaluated
    objcache.sadd(__DONE, task_id)  # type:ignore

    return task_id
