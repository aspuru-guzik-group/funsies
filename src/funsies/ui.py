"""User-friendly interfaces to funsies functionality."""
from __future__ import annotations

# std
import time
from typing import (
    Callable,
    Iterable,
    Literal,
    Mapping,
    Optional,
    overload,
    Union,
)

# external
from redis import Redis

# module
from ._constants import _AnyPath, hash_t
from ._context import get_db, get_options
from ._dag import descendants, start_dag_execution
from ._graph import (
    Artefact,
    constant_artefact,
    delete_artefact,
    get_data,
    get_status,
    make_op,
    Operation,
)
from ._logging import logger
from ._pyfunc import python_funsie
from ._shell import shell_funsie, ShellOutput
from ._short_hash import shorten_hash
from .config import Options
from .errors import Error, Result, unwrap

# Types
_INP_FILES = Optional[Mapping[_AnyPath, Union[Artefact, str, bytes]]]
_OUT_FILES = Optional[Iterable[_AnyPath]]


# --------------------------------------------------------------------------------
# Dag execution
def execute(
    output: Union[Operation, Artefact, ShellOutput],
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    # get redis
    db = get_db(connection)

    # run dag
    start_dag_execution(db, output.hash)


# --------------------------------------------------------------------------------
# Shell command
def shell(  # noqa:C901
    *args: str,
    inp: _INP_FILES = None,
    out: _OUT_FILES = None,
    env: Optional[dict[str, str]] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> ShellOutput:
    """Add a shell command to the workflow.

    `shell()` puts a shell command in the workflow and returns a `types.ShellOutput`
    instance that provides a convenient wrapper to stdout, stderr and output
    files.

    The `strict` flag determines how to interpret errors in input files. When
    set to False, input files with errors will simply (and silently) be
    excluded from the shell script.

    Shell commands are run in a temporary directory which conveys some measure
    of encapsulation, but it is quite weak, so the callee should make sure
    that commands only use relative paths etc. to ensure proper cleanup and
    function purity. This is done using python's tempfile; the temporary
    directory can be set using the TMPDIR environment variable.

    Args:
        *args: Lines of shell script to be evaluated.
        inp: Input files to pass to the shell comand.
        out: Output files to keep from the shell command.
        env: Environment variables to be set.
        strict: Strictness of operation.
        connection: A Redis connection.
        opt: An `types.Options` instance.

    Returns:
        A `types.ShellOutput` object, populated with the generated
        `types.Artefact` instances.

    Raises:
        TypeError: when types of arguments are wrong.

    """
    opt = get_options(opt)
    db = get_db(connection)

    # Parse args --------------------------------------------
    cmds: list[str] = []
    inputs: dict[str, Artefact] = {}

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
                inputs[skey] = put(val, connection=db)
    else:
        raise TypeError(f"{inp} not a valid file input")

    if out is None:
        outputs = []
    else:
        outputs = [str(o) for o in out]

    funsie = shell_funsie(cmds, list(inputs.keys()), outputs, env, strict=strict)
    operation = make_op(db, funsie, inputs, opt)
    return ShellOutput(db, operation)


# --------------------------------------------------------------------------------
# Data transformers
# class __LaxMapping(Protocol):
#     def __call__(


def mapping(  # noqa:C901
    fun: Callable,  # type:ignore
    *inp: Union[Artefact, str, bytes],
    noutputs: int,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> tuple[Artefact, ...]:
    """Add to the execution graph a general n->m function."""
    opt = get_options(opt)
    db = get_db(connection)
    arg_names = []
    inputs = {}
    for k, arg in enumerate(inp):
        arg_names += [f"in{k}"]
        if isinstance(arg, Artefact):
            inputs[arg_names[-1]] = arg
        else:
            inputs[arg_names[-1]] = put(arg, connection=db)

    # output slots
    if noutputs == 1:
        outputs = ["out"]  # for legacy reasons
    else:
        outputs = [f"out{k}" for k in range(noutputs)]

    if name is not None:
        fun_name = name
    else:
        fun_name = f"mapping_{len(inp)}:{fun.__qualname__}"

    # This copy paste is a MyPy exclusive! :S
    if strict:

        def strict_map(inpd: dict[str, bytes]) -> dict[str, bytes]:
            """Perform a reduction."""
            args = [inpd[key] for key in arg_names]
            out = fun(*args)
            if noutputs == 1:
                out = (out,)
            return dict(zip(outputs, out))

        funsie = python_funsie(
            strict_map, arg_names, outputs, name=fun_name, strict=True
        )
    else:

        def lax_map(inpd: dict[str, Result[bytes]]) -> dict[str, bytes]:
            """Perform a reduction."""
            args = [inpd[key] for key in arg_names]
            out = fun(*args)
            if noutputs == 1:
                out = (out,)
            return dict(zip(outputs, out))

        funsie = python_funsie(lax_map, arg_names, outputs, name=fun_name, strict=False)

    operation = make_op(db, funsie, inputs, opt)
    return tuple([Artefact.grab(db, operation.out[o]) for o in outputs])


def morph(
    fun: Callable[[bytes], bytes],
    inp: Union[Artefact, str, bytes],
    *,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Add to call graph a one-to-one python function."""
    if name is not None:
        morpher_name = name
    else:
        morpher_name = f"morph:{fun.__qualname__}"

    # It's really just another name for a 1-input mapping
    return mapping(fun, inp, noutputs=1, name=morpher_name, strict=strict, opt=opt)[0]


def reduce(
    fun: Callable[..., bytes],
    *inp: Union[Artefact, str, bytes],
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Add a many-to-one python function to the workflow.

    `reduce()` puts a python function `fun` on the workflow and returns its
    output artefact. `fun` should have the following signature,

    `fun(arg1, arg2, arg3, ...) -> bytes`

    As many arguments will be passed to `fun()` as there are input
    `types.Artefact` instances in `*inp`. Arguments are bytestring of object
    of type `types.Result`, depending on the strictness of the operation.

    This function is a convenience wrapper around the more general `mapping()`
    function. Conversely, the `morph()` function is a special case of this one
    with only one input.

    Args:
        fun:
            Python function that operates on input artefacts and produces a
            single output artefact.
        *inp: Input artefacts.
        name:
            Override the name of the python function used in hash generation.
        strict: Strictness of operation.
        opt: An `types.Options` instance.
        connection: A Redis connection.

    Returns:
        A `types.Artefact` instance that corresponds to the reduction output.

    """
    if name is not None:
        red_name = name
    else:
        red_name = f"reduce_{len(inp)}:{fun.__qualname__}"
    return mapping(fun, *inp, noutputs=1, name=red_name, strict=strict, opt=opt)[0]


# --------------------------------------------------------------------------------
# Data loading and saving
def put(
    value: Union[bytes, str],
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Put an artefact in the database."""
    db = get_db(connection)
    if isinstance(value, str):
        return constant_artefact(db, value.encode())
    elif isinstance(value, bytes):
        return constant_artefact(db, value)
    else:
        raise TypeError(f"value of type {type(value)} not bytes or string")


def __log_error(where: hash_t, dat: Result[bytes]) -> None:
    if isinstance(dat, Error):
        logger.warning(f"data error at hash {shorten_hash(where)}")


# fmt:off
@overload
def take(where: Artefact, strict: Literal[True] = True, connection: Optional[Redis[bytes]]=None) -> bytes:  # noqa
    ...


@overload
def take(where: Artefact, strict: Literal[False] = False, connection: Optional[Redis[bytes]]=None) -> Result[bytes]:  # noqa
    ...
# fmt:on


def take(
    where: Artefact,
    strict: bool = True,
    connection: Optional[Redis[bytes]] = None,
) -> Result[bytes]:
    """Take an artefact from the database."""
    db = get_db(connection)
    dat = get_data(db, where)
    __log_error(where.hash, dat)
    if strict:
        return unwrap(dat)
    else:
        return dat


def takeout(
    where: Artefact,
    filename: _AnyPath,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Take an artefact and save it to a file."""
    db = get_db(connection)
    dat = get_data(db, where)
    __log_error(where.hash, dat)
    dat = unwrap(dat)
    with open(filename, "wb") as f:
        f.write(dat)


def wait_for(
    thing: Union[ShellOutput, Artefact],
    timeout: Optional[float] = None,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Block until a thing is computed."""
    db = get_db(connection)
    if isinstance(thing, Artefact):
        h = thing.hash
    elif isinstance(thing, ShellOutput):
        h = thing.stdouts[0].hash
    else:
        raise TypeError("can only wait for artefacts or shell outputs.")

    t0 = time.time()
    while True:
        t1 = time.time()

        stat = get_status(db, h)
        if stat > 0:
            return

        if timeout is not None:
            if t1 - t0 > timeout:
                raise RuntimeError("timed out.")

        # avoids hitting the DB way too often
        time.sleep(0.3)


def reset(
    what: Union[ShellOutput, Operation, Artefact],
    *,
    recursive: bool = True,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Delete an artefact and its dependents."""
    db = get_db(connection)
    if isinstance(what, Artefact):
        h = what.parent
        if h == "root":
            logger.error("attempted to delete const artefact.")
            return
    else:
        h = what.hash

    # Delete everything from the operation
    op = Operation.grab(db, h)
    for art in op.out.values():
        delete_artefact(db, art)

    if recursive:
        # and its dependencies
        for el in descendants(db, h):
            op = Operation.grab(db, el)
            for art in op.out.values():
                delete_artefact(db, art)
