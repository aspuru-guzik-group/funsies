"""User-friendly interfaces to funsies functionality."""
from __future__ import annotations

# std
import shutil
import time
from typing import Iterable, Mapping, Optional, overload, TypeVar, Union

# external
from redis import Redis

# python 3.7 imports Literal from typing_extensions
try:
    # std
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type:ignore

# module
from ._constants import _AnyPath, _Data, hash_t
from ._context import Connection, get_connection, get_options
from ._dag import descendants, start_dag_execution
from ._graph import (
    Artefact,
    constant_artefact,
    delete_artefact,
    get_data,
    get_status,
    get_stream,
    make_op,
    Operation,
)
from ._logging import logger
from ._run import is_it_cached
from ._shell import shell_funsie, ShellOutput
from ._short_hash import shorten_hash
from ._storage import StorageEngine
from .config import Options
from .errors import Error, Result, unwrap

# Types
_Target = Union[Artefact, _Data]
_INP_FILES = Optional[Mapping[_AnyPath, _Target]]
_OUT_FILES = Optional[Iterable[_AnyPath]]
T = TypeVar("T", bound=_Data)


def _artefact(
    db: Redis[bytes], store: StorageEngine, data: Union[T, Artefact[T]]
) -> Artefact[T]:
    if isinstance(data, Artefact):
        return data
    else:
        return constant_artefact(db, store, data)


# --------------------------------------------------------------------------------
# Dag execution
def execute(
    *outputs: Union[Operation, Artefact, ShellOutput],
    connection: Connection = None,
) -> None:
    """Trigger execution of a workflow to obtain a given output.

    Args:
        *outputs: Final artefacts or operations to be evaluated in the
            workflow. These objects and all of their dependencies will be
            executed by workers.
        connection: An explicit Redis connection. Not required if called
            within a `Fun()` context.
    """
    # get redis
    db, _ = get_connection(connection)

    # run dag
    for el in outputs:
        start_dag_execution(db, el.hash)


# --------------------------------------------------------------------------------
# Shell command
def shell(
    *args: str,
    inp: _INP_FILES = None,
    out: _OUT_FILES = None,
    env: Optional[dict[str, str]] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Connection = None,
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
    be executed if any input values currently hold `errors.Error`. Instead, all
    output values will also be replaced by `errors.Error`.

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
        inp: Input files to pass to the shell comand. This should be a Mapping
            from filenames (str, path etc.) to values. Values can either be
            `types.Artefact` instances or of type `bytes`, in which case they
            will be automatically converted using `put()`.
        out: Filenames of output files that will be used to populate the return
            `types.ShellOutput` object. Note that any file not included in
            this list will be deleted when the shell command terminates.
        env: Environment variables to be set before calling the shell command.
        strict: If `False`, error handling will be deferred to the shell command
            by not populating input files of type `Error`.
        connection: An explicit Redis connection. Not required if called within a
            `Fun()` context.
        opt: An `types.Options` instance as returned by `options()`. Not
            required if called within a `Fun()` context.

    Returns:
        A `types.ShellOutput` object, populated with the generated
        `types.Artefact` instances.

    Raises:
        TypeError: when types of arguments are wrong.

    """
    opt = get_options(opt)
    db, store = get_connection(connection)

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
            if isinstance(val, str):
                logger.warning(
                    f"{key} passed to shell as a string.\nif you don't want it to be"
                    + ' converted to json (and wrapped with "), \nyou NEED to pass it'
                    + " as bytes (by .encode()-ing it first)"
                )
            inputs[str(key)] = _artefact(db, store, val)
    else:
        raise TypeError(f"{inp} not a valid file input")

    if out is None:
        outputs = []
    else:
        outputs = [str(o) for o in out]

    inputs_types = dict([(k, v.kind) for k, v in inputs.items()])
    funsie = shell_funsie(cmds, inputs_types, outputs, env, strict=strict)
    operation = make_op(db, funsie, inputs, opt)
    return ShellOutput(db, operation)


# --------------------------------------------------------------------------------
# Data loading and saving
def put(
    value: T,
    *,
    connection: Connection = None,
) -> Artefact[T]:
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
    """
    db, store = get_connection(connection)
    return _artefact(db, store, value)


def __log_error(where: hash_t, dat: Result[object]) -> None:
    if isinstance(dat, Error):
        logger.warning(f"data error at hash {shorten_hash(where)}")


# fmt:off
@overload
def take(where: Artefact[T], *, strict: Literal[True] = True, connection: Connection=None) -> T:  # noqa
    ...


@overload
def take(where: Artefact[T], *, strict: Literal[False] = False, connection: Connection=None) -> Result[T]:  # noqa
    ...
# fmt:on


def take(
    where: Artefact[T],
    *,
    strict: bool = True,
    connection: Connection = None,
) -> Union[T, Result[T]]:
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
        strict: If `False`, return a value of type `errors.Result[bytes]`.
        connection: An explicit Redis connection. Not required if called
            within a `Fun()` context.

    Returns:
        Either `bytes` or `errors.Result[bytes]` depending on strictness.

    Raises:
        errors.UnwrapError:
            if `where` contains an `errors.Error` and `strict=True`.

    """
    db, store = get_connection(connection)
    dat = get_data(db, store, where)
    __log_error(where.hash, dat)
    if strict:
        return unwrap(dat)
    else:
        return dat


def takeout(
    where: Artefact, filename: _AnyPath, *, connection: Connection = None
) -> None:  # noqa:DAR101,DAR201
    """`take()` an artefact and save it to `filename`.

    This is syntactic sugar around `take()`. This function is always strict.
    """
    db, store = get_connection(connection)
    dat = get_stream(db, store, where.hash)
    __log_error(where.hash, dat)
    dat = unwrap(dat)
    with open(filename, "wb") as f:
        shutil.copyfileobj(dat, f)


def wait_for(
    thing: Union[ShellOutput, Artefact, Operation],
    timeout: Optional[float] = None,
    *,
    connection: Connection = None,
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
    db, _ = get_connection(connection)
    if isinstance(thing, Artefact):

        def __stat() -> bool:
            return get_status(db, thing.hash, resolve_links=True) > 0

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
    connection: Connection = None,
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
        recursive: If False, only this operation is reset; its dependents are
            untouched. Note that this is dangerous, as it can make
            non-reproducible workflows.
        connection: An explicit Redis connection. Not required if called
            within a `Fun()` context.

    Raises:
        AttributeError:
            when an `types.Artefact` is reset that has status
            `types.ArtefactStatus.const`.
    """
    db, _ = get_connection(connection)
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
