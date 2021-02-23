"""User-friendly interfaces to funsies functionality."""
from __future__ import annotations

# std
import time
from typing import (
    Callable,
    Iterable,
    Mapping,
    Optional,
    overload,
    Union,
)

# external
from redis import Redis

# python 3.7 imports Literal from typing_extensions
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type:ignore

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
    resolve_link,
)
from ._logging import logger
from ._pyfunc import python_funsie
from ._run import is_it_cached
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
    *outputs: Union[Operation, Artefact, ShellOutput],
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Trigger execution of a workflow to obtain a given output.

    Args:
        *outputs: Final artefacts or operations to be evaluated in the
            workflow. These objects and all of their dependencies will be
            executed by workers.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.
    """
    # get redis
    db = get_db(connection)

    # run dag
    for el in outputs:
        start_dag_execution(db, el.hash)


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

    Input and output files need to be explicitly given as arguments `inp` and
    `out`. Input and output files containing path separators (`/`) are assumed
    to belong to the corresponding directory tree structures, which will be
    automatically generated for input files.

    The `strict` flag determines how to interpret errors in input files. If
    `True` (the default), errors are propagated down: shell commands will not
    be executed if any input values currently hold `Error`. Instead, all
    output values will also be replaced by `Error`.

    When `strict=False`, input files with errors will simply (and silently) be
    excluded from the shell script.

    Shell commands are run in a temporary directory which conveys some measure
    of encapsulation, but it is quite weak, so the callee should make sure
    that commands only use relative paths etc. to ensure proper cleanup and
    function purity. This is done using python's `tempfile` module: the temporary
    directory can be set using the $TMPDIR environment variable.

    Environment variables can be passed to the executed command with the
    `env=` keyword. In contrast with `subprocess.Popen()`, the environment of
    worker processes will be updated with those values, *not* replaced by
    them. Environment variables are not hashed as part of the operation's id
    and thus changing them will not result in workflow re-execution.

    Args:
        *args: Lines of shell script to be evaluated.
        inp (optional): Input files to pass to the shell comand. This should
            be a Mapping from filenames (str, path etc.) to values. Values can
            either be `types.Artefact` instances or of type `str` or `bytes`,
            in which case they will be automatically converted using `put()`.
        out (optional): Filenames of output files that will be used to
            populate the return `types.ShellOutput` object. Note that any file not
            included in this list will be deleted when the shell command
            terminates.
        env (optional): Environment variables to be set before calling the
            shell command.
        strict (optional): If `False`, error handling will be deferred to the
            shell command by not populating input files of type `Error`.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.
        opt (optional): An `types.Options` instance as returned by
            `options()`. Not required if called within a `Fun()` context.

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
def mapping(  # noqa:C901
    fun: Callable,  # type:ignore
    *inp: Union[Artefact, str, bytes],
    noutputs: int,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> tuple[Artefact, ...]:
    """Add a many-to-many python function to the workflow.

    `mapping()` puts a python function `fun` on the workflow and returns its
    output artefact. `fun` should have the following signature,

        fun(bytes, bytes, ...) -> bytes, bytes, ...

    As many arguments will be passed to `fun()` as there are input
    `types.Artefact` instances in `*inp` and `fun()` should return as many
    outputs as the value of `noutputs`.

    If `strict=False`, the function is taken to do it's own error handling and
    arguments will be of type `errors.Result[bytes]` instead of `bytes`. See
    `utils.match_results()` for a convenient way to process these values.

    Python function hashes are generated based on their names (as given by
    `fun.__qualname__`) and functions are distributed to workers using
    `cloudpickle`. This is important because it means that:

    - Workers must have access to the function if it is imported, and must
        have access to any imported libraries.

    - Changing a function without modifiying its name (or modifying the
        `name=` argument) will not recompute the graph.

    It is the therefore the caller's responsibility to `reset()` one of the
    return value of `mapping()` or call it with `options(reset=True)` if
    the function is modified to ensure re-excution of its dependents.

    This function is a convenience wrapper around the more general `mapping()`
    function. Conversely, the `morph()` function is a special case of this one
    with only one input.

    Args:
        fun: Python function that operates on input artefacts and produces a
            single output artefact.
        *inp: Input artefacts.
        noutputs: Number of outputs of function `fun()`.
        name (optional): Override the name of `fun()` used in hash generation.
        strict (optional): If `False`, error handling will be deferred to
            `fun()` by passing it argument of type `errors.Result[bytes]` instead
            of `bytes`.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.
        opt (optional): An `types.Options` instance as returned by
            `options()`. Not required if called within a `Fun()` context.

    Returns:
        A tuple of `types.Artefact` instances that corresponds to the mapping
        `noutputs` return values.

    """
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
    *,  # noqa:DAR101,DAR201
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Add to workflow a one-to-one python function `y = f(x)`.

    This is syntactic sugar around `mapping()`.
    """
    if name is not None:
        morpher_name = name
    else:
        morpher_name = f"morph:{fun.__qualname__}"

    # It's really just another name for a 1-input mapping
    return mapping(fun, inp, noutputs=1, name=morpher_name, strict=strict, opt=opt)[0]


def reduce(
    fun: Callable[..., bytes],
    *inp: Union[Artefact, str, bytes],  # noqa:DAR101,DAR201
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact:
    """Add to workflow a many-to-one python function `y = f(*x)`.

    This is syntactic sugar around `mapping()`.
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
    """Save data to Redis and return an Artefact.

    `put()` explicitly saves `value`, a bytes or string value, to the database
    and return an `types.Artefact` pointing to this value.

    The returned artefact's status is `types.ArtefactStatus.const` and its
    parent hash is `root`. This means that:

    - The arterfact is populated before any workflow operation is executed.
    - It has no dependencies
    - It is hashed according to content, not history.

    Thus, `put()` is used to set input values to workflows.

    Args:
        value: Data to be held in database. `str` data is encoded to `bytes`.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.

    Returns:
        An `types.Artefact` instance with status `const`.

    Raises:
        TypeError: if `value` is not of type bytes or str.
    """
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
    """Take data corresponding to a given artefact from Redis.

    `take()` returns the currently held value of pointed to by the
    `types.Artefact` instance `where` as `bytes`.

    If `strict=True` (the default) and `where` points to an `types.Error`
    value, this function will raise `errors.UnwrapError`. This is equivalent
    to running `unwrap()` on the return value.

    However if `strict=False`, the return value of `take()` is a
    `errors.Result[bytes]` variable, that is, either an instance of `bytes` or
    whatever `types.Error` is currently held by `where`.

    Finally, if `where` does not point to a valid redis-backed
    `types.Artefact` an `errors.Error` is returned of kind
    `errors.ErrorKind.Mismatch`.

    Args:
        where: `types.Artefact` pointer to data taken from the database.
        strict (optional): If `False`, return a value of type
            `errors.Result[bytes]`.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.

    Returns:
        Either `bytes` or `errors.Result[bytes]` depending on strictness.

    Raises:
        errors.UnwrapError: if `where` contains an `errors.Error` and `strict=True`.

    """  # noqa:DAR402
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
) -> None:  # noqa:DAR101,DAR201
    """`take()` an artefact and save it to `filename`.

    This is syntactic sugar around `take()`. This function is always strict.
    """
    db = get_db(connection)
    dat = get_data(db, where)
    __log_error(where.hash, dat)
    dat = unwrap(dat)
    with open(filename, "wb") as f:
        f.write(dat)


def wait_for(
    thing: Union[ShellOutput, Artefact, Operation],
    timeout: Optional[float] = None,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Block execution until an artefact is generated or an operation is executed.

    Args:
        thing: `types.Artefact` or operation to wait on.
        timeout (optional): Number of seconds to wait for before raising an
            exception. If unspecified, timeout is taken to be infinite.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.

    Raises:
        TimeoutError: if timeout is exceeded.
    """
    db = get_db(connection)
    if isinstance(thing, Artefact):

        def __stat() -> bool:
            return get_status(db, resolve_link(db, thing.hash)) > 0

    else:
        if isinstance(thing, Operation):
            op = thing
        else:
            op = thing.op

        def __stat() -> bool:
            return is_it_cached(db, op)

    t0 = time.time()
    while True:
        t1 = time.time()

        if __stat():
            return

        if timeout is not None:
            if t1 - t0 > timeout:
                raise TimeoutError(
                    f"waited on {shorten_hash(thing.hash)} " + f"for {t1-t0} seconds."
                )

        # avoids hitting the DB way too often
        time.sleep(0.3)


def reset(
    thing: Union[ShellOutput, Operation, Artefact],
    *,
    recursive: bool = True,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Reset data associated with an operation and its dependents.

    This function deletes data associated with an operation or the operation
    generating a given artefact without actually removing it from the
    workflow. This is useful if an operation failed due to circumstances
    outside of the control of `funsies`, such as a non-reproducible step or
    worker setup error. When the workflow is executed again, all the `reset()`
    steps will be re-computed.

    By default, `reset()` is applied recursively to all dependents of an
    operation.

    Args:
        thing: Operation to reset. If an `types.Artefact` is given, its parent
            operation is `reset()`.
        recursive (optional): If False, only this operation is reset; its
            dependents are untouched. Note that this is dangerous, as it can yield
            to non-reproducible workflows.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.

    Raises:
        AttributeError: if an `types.Artefact` is reset that has status
        `types.ArtefactStatus.const`.
    """
    db = get_db(connection)
    if isinstance(thing, Artefact):
        h = thing.parent
        if h == "root":
            raise AttributeError("attempted to delete a const artefact.")
    else:
        h = thing.hash

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
