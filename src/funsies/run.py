"""Functions for describing redis-backed DAGs."""
# std
from enum import IntEnum
import traceback
from typing import Dict, Union

# external
from redis import Redis

# module
from ._funsies import FunsieHow, get_funsie
from ._graph import (
    ArtefactStatus,
    get_artefact,
    get_data,
    get_op,
    get_status,
    mark_error,
    Operation,
    set_data,
)
from ._pyfunc import run_python_funsie  # runner for python functions
from ._shell import run_shell_funsie  # runner for shell
from .constants import hash_t, SREADY, SRUNNING
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


def __make_ready(db: Redis, address: hash_t) -> None:
    # Move back to the ready list
    val: int = db.smove(SRUNNING, SREADY, address)  # type:ignore
    if val != 1:
        raise RuntimeError(
            f"Critical error in running {address}!"
            + " Someone moved it out of SRUNNING while it was running!"  # noqa
        )


def run_op(  # noqa:C901
    db: Redis, op: Union[Operation, hash_t], check_only: bool = False
) -> RunStatus:
    """Run an Operation from its hash address."""
    # Compatibility feature
    if not isinstance(op, Operation):
        op = get_op(db, op)

    # Check if job is ready for execution
    # -----------------------------------
    # First, we check if this address is even ready to go. We do this by
    # moving the job from READY->DONE. We do it this way because this
    # operation is atomic. Thus, if any other worker is also attempting to
    # start this job, they'll know we currently are processing it.
    val: int = db.smove(SREADY, SRUNNING, op.hash)  # type:ignore
    if val == 0:
        # job is NOT ready. return.
        logger.info(f"op skipped: {op.hash} is being processed elsewhere")
        return RunStatus.not_ready

    # Check if the current job needs to be done at all
    # ------------------------------------------------
    # We do this by checking whether all of it's outputs are already saved.
    # This ensures that there is no mismatch between artefact statuses and the
    # status of generating functions.
    for val in op.out.values():
        stat = get_status(db, val)
        if stat <= ArtefactStatus.no_data:
            break
    else:
        # All outputs are ok. We exit this run.
        logger.info(f"op skipped: {op.hash} has cached results")
        __make_ready(db, op.hash)
        return RunStatus.using_cached

    if check_only:
        raise RuntimeError("Attempting to run an operation, but check_only is set.")

    # # Then we check if all the inputs are ready to be processed.
    for val in op.inp.values():
        stat = get_status(db, val)
        if stat <= ArtefactStatus.no_data:
            # One of the inputs is not processed yet, we return.
            logger.info(f"op skipped: {op.hash} has unmet dependencies.")
            __make_ready(db, op.hash)
            return RunStatus.unmet_dependencies

    # load the funsie
    funsie = get_funsie(db, op.funsie)
    runner = RUNNERS[funsie.how]

    # load input files
    input_data: Dict[str, Result[bytes]] = {}
    for key, val in op.inp.items():
        artefact = get_artefact(db, val)
        dat = get_data(db, artefact, source=op.hash)
        if isinstance(dat, Error):
            if funsie.error_tolerant:
                logger.warning(f"input {key} to error-tolerant funsie has errors.")
                input_data[key] = dat
            else:
                logger.error(f"input {key} to error-fragile funsie has errors.")
                # forward errors and stop
                for val in op.out.values():
                    mark_error(db, val, dat)
                return RunStatus.input_error
        else:
            input_data[key] = dat

    logger.info(f"op running: {op.hash}")
    try:
        out_data = runner(funsie, input_data)
    except Exception:
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
        return RunStatus.executed

    for key, val in out_data.items():
        artefact = get_artefact(db, op.out[key])
        if val is None:
            logger.warning(f"{op.hash} -> missing output data for {key}")
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

    __make_ready(db, op.hash)
    return RunStatus.executed
