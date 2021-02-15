"""Functions for describing redis-backed DAGs."""
from __future__ import annotations

# std
from enum import IntEnum
import traceback
from typing import Union

# external
from redis import Redis, WatchError

# module
from ._funsies import FunsieHow, Funsie
from ._graph import (
    Artefact,
    ArtefactStatus,
    get_data,
    get_status,
    mark_error,
    Operation,
    set_data,
)
from ._pyfunc import run_python_funsie  # runner for python functions
from ._shell import run_shell_funsie  # runner for shell
from .constants import hash_t
from .errors import Error, ErrorKind, Result
from .logging import logger

# Dictionary of runners
RUNNERS = {FunsieHow.shell: run_shell_funsie, FunsieHow.python: run_python_funsie}


class RunStatus(IntEnum):
    """Possible status of running an operation."""

    # <= 0 -> issue prevented running job.
    unmet_dependencies = -2
    not_ready = -1
    # > 0 -> executed, can run dependents.
    executed = 1
    using_cached = 2
    input_error = 4


def is_it_cached(db: Redis[bytes], op: Operation) -> bool:
    """Check if an operation is fully cached and doesn't need to be recomputed."""
    # We do this by checking whether all of it's outputs are already saved.
    # This ensures that there is no mismatch between artefact statuses and the
    # status of generating operations.
    pipe = db.pipeline(transaction=True)
    cached = True
    while True:
        try:
            # TODO
            # pipe.watch(DATA_STATUS)
            for val in op.out.values():
                stat = get_status(db, val)
                if stat <= ArtefactStatus.no_data:
                    cached = False
                    break
            break
        except WatchError:
            # Someone changed artefact status while we were watching.
            continue
    return cached


def dependencies_are_met(db: Redis[bytes], op: Operation) -> bool:
    """Check if all the dependencies of an operation are met."""
    for val in op.inp.values():
        stat = get_status(db, val)
        if stat <= ArtefactStatus.no_data:
            return False

    return True


def run_op(  # noqa:C901
    db: Redis[bytes], op: Union[Operation, hash_t], evaluate: bool = True
) -> RunStatus:
    """Run an Operation from its hash address."""
    # Compatibility feature
    if not isinstance(op, Operation):
        op = Operation.grab(db, op)

    logger.info(f"=== {op.hash} ===")
    logger.info("evaluating...")

    # Check if the current job needs to be done at all
    # ------------------------------------------------
    if is_it_cached(db, op):
        # All outputs are ok. We exit this run.
        logger.success("DONE: using cached data.")
        return RunStatus.using_cached

    if not evaluate:
        raise RuntimeError("Attempting to run an operation, but evaluate = False.")

    # # Then we check if all the inputs are ready to be processed.
    if not dependencies_are_met(db, op):
        logger.success("DONE: waiting on dependencies.")
        return RunStatus.unmet_dependencies

    # load the funsie
    funsie = Funsie.grab(db, op.funsie)
    runner = RUNNERS[funsie.how]

    # load input files
    input_data: dict[str, Result[bytes]] = {}
    for key, val in op.inp.items():
        artefact = Artefact.grab(db, val)
        dat = get_data(db, artefact, source=op.hash)
        if isinstance(dat, Error):
            if funsie.error_tolerant:
                logger.warning(f"error on input {key} (tolerated).")
                input_data[key] = dat
            else:
                # forward errors and stop
                for val in op.out.values():
                    mark_error(db, val, dat)
                logger.error(f"DONE: error on input {key} (fragile).")
                return RunStatus.input_error
        else:
            input_data[key] = dat

    logger.info("running...")
    try:
        out_data = runner(funsie, input_data)
    except Exception:
        logger.exception("runner raised!")
        tb_exc = traceback.format_exc()
        # much trouble
        for val in op.out.values():
            mark_error(
                db,
                val,
                error=Error(
                    kind=ErrorKind.ExceptionRaised,
                    source=op.hash,
                    details=tb_exc,
                ),
            )
        logger.error("DONE: runner raised exception.")
        return RunStatus.executed

    for key, val in out_data.items():
        artefact = Artefact.grab(db, op.out[key])
        if val is None:
            logger.warning(f"no output data for {key}")
            mark_error(
                db,
                artefact.hash,
                error=Error(
                    kind=ErrorKind.MissingOutput,
                    source=op.hash,
                    details="output not returned by runner",
                ),
            )
            # not that in this case, the other outputs are not necesserarily
            # invalidated, only this one.
        else:
            set_data(db, artefact, val)

    logger.success("DONE: successful eval.")
    return RunStatus.executed
