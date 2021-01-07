"""User-friendly interfaces to funsies functionality."""
# std
import logging
import os
import time
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Union,
)

# external
from redis import Redis

# module
from ._graph import (
    Artefact,
    ArtefactStatus,
    get_artefact,
    get_data,
    get_status,
    make_op,
    Operation,
    store_explicit_artefact,
)
from ._pyfunc import python_funsie
from ._shell import shell_funsie
from .constants import hash_t

# Types
_AnyPath = Union[str, os.PathLike]
_INP_FILES = Optional[Mapping[_AnyPath, Union[Artefact, str, bytes]]]
_OUT_FILES = Optional[Iterable[_AnyPath]]


# --------------------------------------------------------------------------------
# Shell and Shell outputs
class ShellOutput:
    """A convenience wrapper for a shell operation."""

    op: Operation
    hash: hash_t
    out: Dict[str, Artefact]
    inp: Dict[str, Artefact]

    def __init__(self: "ShellOutput", store: Redis, op: Operation) -> None:
        """Generate a ShellOutput wrapper around a shell operation."""
        # import the constants
        from ._shell import SPECIAL, RETURNCODE, STDOUT, STDERR

        # stuff that is the same
        self.op = op
        self.hash = op.hash

        self.out = {}
        self.n = 0
        for key, val in op.out.items():
            if SPECIAL in key:
                if RETURNCODE in key:
                    self.n += 1  # count the number of commands
            else:
                self.out[key] = get_artefact(store, val)

        self.inp = {}
        for key, val in op.inp.items():
            self.inp[key] = get_artefact(store, val)

        self.stdouts = []
        self.stderrs = []
        self.returncodes = []
        for i in range(self.n):
            self.stdouts += [get_artefact(store, op.out[f"{STDOUT}{i}"])]
            self.stderrs += [get_artefact(store, op.out[f"{STDERR}{i}"])]
            self.returncodes += [get_artefact(store, op.out[f"{RETURNCODE}{i}"])]

    def __check_len(self: "ShellOutput") -> None:
        if self.n > 1:
            raise AttributeError(
                "More than one shell command are included in this run."
            )

    @property
    def returncode(self: "ShellOutput") -> Artefact:
        """Return code of a shell command."""
        self.__check_len()
        return self.returncodes[0]

    @property
    def stdout(self: "ShellOutput") -> Artefact:
        """Stdout of a shell command."""
        self.__check_len()
        return self.stdouts[0]

    @property
    def stderr(self: "ShellOutput") -> Artefact:
        """Stderr of a shell command."""
        self.__check_len()
        return self.stderrs[0]


def shell(  # noqa:C901
    db: Redis,
    *args: str,
    inp: _INP_FILES = None,
    out: _OUT_FILES = None,
    env: Optional[Dict[str, str]] = None,
) -> ShellOutput:
    """Add one or multiple shell commands to the call graph.

    Make a shell operationfor running with run(). This is a more user-friendly
    interface than the direct constructor in ._shell, and it is much more
    lenient on types.

    Arguments:
        db: Redis instance.
        *args: Shell commands.
        inp: Input files for task.
        out: Output files for task.
        env: Environment variables to be set.

    Returns:
        A Task object.

    Raises:
        TypeError: when types of arguments are wrong.

    """
    if not isinstance(db, Redis):
        raise TypeError("First argument is not a Redis connection.")

    # Parse args --------------------------------------------
    cmds: List[str] = []
    inputs: Dict[str, Artefact] = {}

    for arg in args:
        if isinstance(arg, str):
            cmds += [arg]
        else:
            raise TypeError(f"argument {arg} not str.")

    # Parse input files -------------------------------------
    if inp is None:
        pass
    # multiple input files as a mapping
    elif isinstance(inp, Mapping):
        for key, val in inp.items():
            skey = str(key)
            if isinstance(val, Artefact):
                inputs[skey] = val
            else:
                inputs[skey] = put(db, val)
    else:
        raise TypeError(f"{inp} not a valid file input")

    if out is None:
        outputs = []
    else:
        outputs = [str(o) for o in out]

    funsie = shell_funsie(cmds, list(inputs.keys()), outputs, env)
    operation = make_op(db, funsie, inputs)
    return ShellOutput(db, operation)


# --------------------------------------------------------------------------------
# Data transformers
# TODO:add overload for strict / better Result()
def reduce(  # noqa:C901
    db: Redis,
    fun: Callable[..., bytes],
    *inp: Union[Artefact, str, bytes],
    name: Optional[str] = None,
) -> Artefact:
    """Add to call graph a function that reduce multiple artefacts."""
    arg_names = []
    inputs = {}
    for k, arg in enumerate(inp):
        arg_names += [f"in{k}"]
        if isinstance(arg, Artefact):
            inputs[arg_names[-1]] = arg
        else:
            inputs[arg_names[-1]] = put(db, arg)

    if name is not None:
        red_name = name
    else:
        red_name = f"reduce_{len(inp)}:{fun.__qualname__}"

    def reducer(inpd: Dict[str, bytes]) -> Dict[str, bytes]:
        """Perform a reduction."""
        args = [inpd[key] for key in arg_names]
        return dict(out=fun(*args))

    funsie = python_funsie(reducer, arg_names, ["out"], name=red_name)
    operation = make_op(db, funsie, inputs)
    return get_artefact(db, operation.out["out"])


def morph(
    db: Redis,
    fun: Callable[[bytes], bytes],
    inp: Union[Artefact, str, bytes],
    *,
    name: Optional[str] = None,
) -> Artefact:
    """Add to call graph a function that transforms a single artefact."""
    if name is not None:
        morpher_name = name
    else:
        morpher_name = f"morph:{fun.__qualname__}"

    # It's really just another name for a 1-input reduction
    return reduce(db, fun, inp, name=morpher_name)


# --------------------------------------------------------------------------------
# Data loading and saving
def put(
    db: Redis,
    value: Union[bytes, str],
) -> Artefact:
    """Put an artefact in the database."""
    if isinstance(value, str):
        return store_explicit_artefact(db, value.encode())
    elif isinstance(value, bytes):
        return store_explicit_artefact(db, value)
    else:
        raise TypeError("value of {name_or_path} not bytes or string")


def take(
    db: Redis,
    where: Union[Artefact, hash_t],
) -> Optional[bytes]:
    """Take an artefact from the database."""
    if isinstance(where, Artefact):
        obj = where
    else:
        obj = get_artefact(db, where)
        if obj is None:
            raise RuntimeError(f"Address {where} does not point to a valid artefact.")

    dat = get_data(db, obj)
    return dat


def takeout(
    db: Redis,
    where: Union[Artefact, hash_t],
    filename: _AnyPath,
) -> None:
    """Take an artefact and save it to a file."""
    if isinstance(where, Artefact):
        obj = where
    else:
        obj = get_artefact(db, where)
        if obj is None:
            raise RuntimeError(f"Address {where} does not point to a valid artefact.")

    dat = get_data(db, obj)

    with open(filename, "wb") as f:
        if dat is None:
            logging.warning(f"No data available for artefact {where}")
        else:
            f.write(dat)


def wait_for(
    db: Redis, artefact: Union[Artefact, hash_t], timeout: float = 120.0
) -> None:
    """Block until an artefact is computed."""
    if isinstance(artefact, Artefact):
        h = artefact.hash
    else:
        h = artefact

    t0 = time.time()
    while True:
        t1 = time.time()
        if t1 - t0 > timeout:
            raise RuntimeError("timed out.")

        stat = get_status(db, h)
        if stat == ArtefactStatus.done:
            return

        time.sleep(0.3)
