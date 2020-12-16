"""Data transformations."""
# std
import base64
from dataclasses import asdict, dataclass
from io import BytesIO, StringIO
import json
import logging
from typing import Callable, List, Optional, Sequence, Type, Union

# external
import cloudpickle
from redis import Redis

# module
from .cached import FilePtr, FileType, pull_file, put_file
from .constants import __IDS, __SDONE, __STATUS, __TASK_ID, __TRANSFORMERS


@dataclass(frozen=True)
class RTransformer:
    """Holds a registered transformer."""

    # task info
    task_id: int

    # The transformation function
    fun: bytes

    # input & output files
    inputs: List[FilePtr]
    outputs: List[FilePtr]
    bytesio: bool = False

    def json(self: "RTransformer") -> str:
        """Return a json version of myself."""
        d = asdict(self)
        d["fun"] = base64.b64encode(d["fun"]).decode()
        return json.dumps(d)

    @classmethod
    def from_json(cls: Type["RTransformer"], inp: Union[str, bytes]) -> "RTransformer":
        """Build a registered task from a json string."""
        d = json.loads(inp)

        return RTransformer(
            task_id=d["task_id"],
            fun=base64.b64decode(d["fun"].encode()),
            inputs=[FilePtr(**v) for v in d["inputs"]],
            outputs=[FilePtr(**v) for v in d["outputs"]],
            bytesio=d["bytesio"],
        )


def pull_transformer(db: Redis, task_id: int) -> Optional[RTransformer]:
    """Pull a TaskOutput from redis using its task_id."""
    val = db.hget(__TRANSFORMERS, bytes(task_id))
    if val is None:
        return None
    else:
        return RTransformer.from_json(val)


# ------------------------------------------------------------------------------
# Register transformer on db
def transformer(
    cache: Redis,
    fun: Callable,
    inputs: Sequence[FilePtr],
    outputs: Sequence[str],
) -> RTransformer:
    """Register a transformation function into Redis to get an RTransformer."""
    # load function
    fun_bytes = cloudpickle.dumps(fun)
    # make a key for the transformer
    key = json.dumps(
        {
            "fun": base64.b64encode(fun_bytes).decode(),
            "inputs": [asdict(inp) for inp in inputs],
            "outputs": [out for out in outputs],
        }
    )

    if cache.hexists(__IDS, key):
        logging.debug("transformer key already exists.")
        # if it does, get task id
        tmp = cache.hget(__IDS, key)
        # pull the id from the db
        assert tmp is not None
        out = pull_transformer(cache, int(tmp))
        assert out is not None
        return out

    # If it doesn't exist, we make the RTransformer (this is the same code
    # basically as rtask).
    task_id = cache.incrby(__TASK_ID, 1)  # type:ignore
    task_id = int(task_id)

    # make output files
    trans_outputs = []
    for file in outputs:
        trans_outputs += [FilePtr(task_id=task_id, type=FileType.OUT, name=file)]

    # output object
    out = RTransformer(task_id, fun_bytes, list(inputs), trans_outputs)

    # save transformer
    # TODO catch errors
    cache.hset(__TRANSFORMERS, bytes(out.task_id), out.json())
    cache.hset(__IDS, key, task_id)

    return out


def run_rtransformer(objcache: Redis, task: RTransformer) -> int:
    """Execute a registered transformer and return its task id."""
    # Check status
    task_id = task.task_id

    if objcache.hget(__STATUS, bytes(task_id)) == __SDONE:
        logging.debug("transformer is cached.")
        out = pull_transformer(objcache, task_id)
        if out is not None:
            return task_id
        else:
            logging.warning("Pulling transformer out of cache failed, re-executing.")

    # build function
    fun = cloudpickle.loads(task.fun)

    # load inputs and outputs
    inputs = []
    outputs = []
    for el in task.inputs:
        source = pull_file(objcache, el)
        assert source is not None  # TODO:fix
        if task.bytesio:
            inputs.append(BytesIO(source))
        else:
            inputs.append(StringIO(source.decode()))

    for _ in task.outputs:
        if task.bytesio:
            outputs.append(BytesIO(b""))
        else:
            outputs.append(StringIO(""))

    # Call the function
    fun(*inputs, *outputs)

    # Change output files
    for i, el in enumerate(task.outputs):
        if task.bytesio:
            put_file(objcache, el, outputs[i].get_value())
        else:
            put_file(objcache, el, outputs[i].getvalue().encode())

    objcache.hset(__STATUS, bytes(task_id), __SDONE)

    return task_id
