"""User-friendly interfaces to funsies functionality."""
# std
import os
import time
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    overload,
    Tuple,
    Union,
)

# external
from redis import Redis

# module
from ._graph import (
    Artefact,
    constant_artefact,
    get_artefact,
    get_data,
    get_status,
    is_artefact,
    make_op,
    Operation,
    tag_artefact,
)
from ._pyfunc import python_funsie
from ._shell import shell_funsie, ShellOutput
from .config import Options
from .constants import hash_t
from .context import get_db, get_options
from .dag import start_dag_execution
from .errors import Result, unwrap

# Types
_AnyPath = Union[str, os.PathLike]
_INP_FILES = Optional[Mapping[_AnyPath, Union[Artefact, str, bytes]]]
_OUT_FILES = Optional[Iterable[_AnyPath]]


# --------------------------------------------------------------------------------
# Dag execution
def execute(
    output: Union[hash_t, Operation, Artefact, ShellOutput],
    connection: Optional[Redis] = None,
) -> None:
    """Execute a DAG to obtain a given output using an RQ queue."""
    if (
        isinstance(output, Operation)
        or isinstance(output, Artefact)
        or isinstance(output, ShellOutput)
    ):
        dag_of = output.hash
    else:
        dag_of = output

    # get redis
    db = get_db(connection)

    # run dag
    start_dag_execution(db, dag_of)


# --------------------------------------------------------------------------------
# Shell command
def shell(  # noqa:C901
    *args: str,
    inp: _INP_FILES = None,
    out: _OUT_FILES = None,
    env: Optional[Dict[str, str]] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis] = None,
) -> ShellOutput:
    """Add one or multiple shell commands to the call graph.

    Make a shell operation. This is a more user-friendly interface than the
    direct constructor, and it is much more lenient on types.

    The strict= flag determines how to interpret errors in input files. When
    set to False, input files with errors will simply (and silently) not be
    passed to the shell script.

    Arguments:
        *args: Shell commands.
        inp: Input files for task.
        out: Output files for task.
        env: Environment variables to be set.
        strict: Whether this command should still run even if inputs are errored.
        connection: A Redis connection.
        opt: An Options instance.

    Returns:
        A Task object.

    Raises:
        TypeError: when types of arguments are wrong.

    """
    opt = get_options(opt)
    db = get_db(connection)

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
    connection: Optional[Redis] = None,
) -> Tuple[Artefact, ...]:
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

        def strict_map(inpd: Dict[str, bytes]) -> Dict[str, bytes]:
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

        def lax_map(inpd: Dict[str, Result[bytes]]) -> Dict[str, bytes]:
            """Perform a reduction."""
            args = [inpd[key] for key in arg_names]
            out = fun(*args)
            if noutputs == 1:
                out = (out,)
            return dict(zip(outputs, out))

        funsie = python_funsie(lax_map, arg_names, outputs, name=fun_name, strict=False)

    operation = make_op(db, funsie, inputs, opt)
    return tuple([get_artefact(db, operation.out[o]) for o in outputs])


def morph(
    fun: Callable[[bytes], bytes],
    inp: Union[Artefact, str, bytes],
    *,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis] = None,
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
    connection: Optional[Redis] = None,
) -> Artefact:
    """Add to call graph a many-to-one python function."""
    if name is not None:
        red_name = name
    else:
        red_name = f"reduce_{len(inp)}:{fun.__qualname__}"
    return mapping(fun, *inp, noutputs=1, name=red_name, strict=strict, opt=opt)[0]


# --------------------------------------------------------------------------------
# Data loading and saving
def put(
    value: Union[bytes, str],
    connection: Optional[Redis] = None,
) -> Artefact:
    """Put an artefact in the database."""
    db = get_db(connection)
    if isinstance(value, str):
        return constant_artefact(db, value.encode())
    elif isinstance(value, bytes):
        return constant_artefact(db, value)
    else:
        raise TypeError(f"value of type {type(value)} not bytes or string")


# fmt:off
@overload
def take(where: Union[Artefact, hash_t], strict: Literal[True] = True, connection: Optional[Redis]=None) -> bytes:  # noqa
    ...


@overload
def take(where: Union[Artefact, hash_t], strict: Literal[False] = False, connection: Optional[Redis]=None) -> Result[bytes]:  # noqa
    ...
# fmt:on


def take(
    where: Union[Artefact, hash_t],
    strict: bool = True,
    connection: Optional[Redis] = None,
) -> Result[bytes]:
    """Take an artefact from the database."""
    db = get_db(connection)
    if isinstance(where, Artefact):
        obj = where
    else:
        obj = get_artefact(db, where)
        if obj is None:
            raise RuntimeError(f"Address {where} does not point to a valid artefact.")

    dat = get_data(db, obj)
    if strict:
        return unwrap(dat)
    else:
        return dat


def takeout(
    where: Union[Artefact, hash_t],
    filename: _AnyPath,
    connection: Optional[Redis] = None,
) -> None:
    """Take an artefact and save it to a file."""
    db = get_db(connection)
    if isinstance(where, Artefact):
        obj = where
    else:
        obj = get_artefact(db, where)
        if obj is None:
            raise RuntimeError(f"Address {where} does not point to a valid artefact.")

    dat = unwrap(get_data(db, obj))

    with open(filename, "wb") as f:
        f.write(dat)


# This introduces all kind of side-effects.
# def rm(
#     where: Union[Artefact, hash_t],
#     connection: Optional[Redis] = None,
# ) -> None:
#     """Delete data associated with an artefact from the DB."""
#     db = get_db(connection)
#     if isinstance(where, Artefact):
#         obj = where
#     else:
#         obj = get_artefact(db, where)
#         if obj is None:
#             raise RuntimeError(f"Address {where} does not point to a valid artefact.")
#     rm_data(db, obj)


def wait_for(
    thing: Union[ShellOutput, Artefact, hash_t],
    timeout: Optional[float] = None,
    connection: Optional[Redis] = None,
) -> None:
    """Block until a thing is computed."""
    db = get_db(connection)
    if isinstance(thing, Artefact):
        h = thing.hash
    elif isinstance(thing, ShellOutput):
        h = thing.stdouts[0].hash
    else:
        h = thing
        assert is_artefact(db, h)

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


# object tags
def tag(
    tag: str,
    *artefacts: Union[Artefact, hash_t],
    connection: Optional[Redis] = None,
) -> None:
    """Tag artefacts in the database."""
    db = get_db(connection)
    for where in artefacts:
        if isinstance(where, Artefact):
            h = where.hash
        else:
            h = where
            assert is_artefact(db, h)

        tag_artefact(db, h, tag)
